"""Apply Hub scaling requests on the compute host, after draining active jobs."""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select

from api.config import get_settings
from api.db import SessionLocal
from api.models import ExecutionTarget, Job

LOGGER = logging.getLogger("detecdiv-worker-manager")


def reconcile_automatic(session, target, metadata, args, settings) -> None:
    from worker.autoscaling import plan_workers

    now = datetime.now(timezone.utc)
    applied = int(metadata.get("worker_scale_applied_instances") or args.initial_instances)
    baseline = max(1, int(metadata.get("worker_instances_baseline") or 6))
    plan = plan_workers(session, target=target, settings=settings, baseline=baseline)
    metadata["worker_autoscale_plan"] = {**plan, "planned_at": now.isoformat()}
    desired = plan["desired"]
    if metadata.get("drain_new_jobs"):
        metadata["worker_scale_state"] = "drained"
        target.metadata_json = metadata
        return
    # Growth is immediate. Shrink only after one minute of stable lower demand.
    if desired < applied:
        if metadata.get("worker_autoscale_shrink_target") != desired:
            metadata["worker_autoscale_shrink_target"] = desired
            metadata["worker_autoscale_shrink_since"] = now.isoformat()
        since = datetime.fromisoformat(metadata["worker_autoscale_shrink_since"])
        if (now - since).total_seconds() < 60:
            target.metadata_json = metadata
            return
    else:
        metadata.pop("worker_autoscale_shrink_target", None)
        metadata.pop("worker_autoscale_shrink_since", None)
    if desired != applied:
        command = ["sudo", "-n", "/bin/bash", args.configure_script,
                   "--repo-root", args.repo_root, "--service-user", args.service_user,
                   "--unit-dir", args.unit_dir, "--pool-instances-live", str(desired)]
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=45)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            metadata["worker_scale_state"] = "failed"
            metadata["worker_scale_last_message"] = str(getattr(exc, "stderr", "") or exc)[-2000:]
            LOGGER.error("Automatic worker scaling failed: %s", metadata["worker_scale_last_message"])
            target.metadata_json = metadata
            return
        metadata["worker_scale_applied_instances"] = desired
        metadata["worker_scale_last_applied_at"] = now.isoformat()
        metadata["worker_scale_last_message"] = result.stdout.strip()[-2000:]
        LOGGER.info("Automatic worker pool: %s -> %s; %s", applied, desired, plan)
    metadata["worker_instances_desired"] = desired
    metadata["worker_scale_state"] = "automatic"
    target.metadata_json = metadata


def reconcile(args) -> None:
    settings = get_settings()
    with SessionLocal.begin() as session:
        target = session.scalars(select(ExecutionTarget).where(
            ExecutionTarget.target_key == settings.worker_target_key,
        ).with_for_update()).one()
        metadata = dict(target.metadata_json or {})
        metadata["worker_manager_seen_at"] = datetime.now(timezone.utc).isoformat()
        metadata["worker_manager_enabled"] = True
        if metadata.get("worker_autoscale_enabled", True):
            reconcile_automatic(session, target, metadata, args, settings)
            return
        applied = int(metadata.get("worker_scale_applied_instances") or args.initial_instances)
        metadata.setdefault("worker_scale_applied_instances", applied)
        desired = int(metadata.get("worker_instances_desired") or applied)
        requested_at = metadata.get("worker_scale_requested_at") or f"initial-{desired}"
        if desired == applied:
            if metadata.get("worker_scale_state") == "requested":
                metadata["drain_new_jobs"] = metadata.pop("worker_scale_previous_drain", False)
                metadata["worker_scale_state"] = "complete"
                metadata["worker_scale_last_applied_at"] = datetime.now(timezone.utc).isoformat()
            target.metadata_json = metadata
            return
        if metadata.get("worker_scale_failed_request") == requested_at:
            target.metadata_json = metadata
            return
        metadata.setdefault("worker_scale_previous_drain", bool(metadata.get("drain_new_jobs")))
        metadata["drain_new_jobs"] = True
        active = session.scalar(select(func.count(Job.id)).where(
            Job.execution_target_id == target.id, Job.status.in_(("running", "cancelling")),
        ))
        if active:
            metadata["worker_scale_state"] = "waiting_for_jobs"
            target.metadata_json = metadata
            return
        # Keep the same target lock used by job admission until systemd has
        # installed all new limits. No worker can claim during this operation.
        command = ["sudo", "-n", "/bin/bash", args.configure_script,
                   "--repo-root", args.repo_root, "--service-user", args.service_user,
                   "--env-file", args.env_file, "--unit-dir", args.unit_dir,
                   "--worker-instances", str(desired), "--skip-manager-restart"]
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=120)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            metadata["worker_scale_state"] = "failed"
            metadata["worker_scale_failed_request"] = requested_at
            metadata["worker_scale_last_message"] = str(getattr(exc, "stderr", "") or exc)[-2000:]
            LOGGER.error("Worker scaling failed: %s", metadata["worker_scale_last_message"])
        else:
            metadata["worker_scale_applied_instances"] = desired
            metadata["worker_scale_state"] = "complete"
            metadata["worker_scale_last_applied_at"] = datetime.now(timezone.utc).isoformat()
            metadata["worker_scale_last_message"] = result.stdout.strip()[-2000:]
            metadata["drain_new_jobs"] = metadata.pop("worker_scale_previous_drain", False)
            metadata.pop("worker_scale_failed_request", None)
        target.metadata_json = metadata


def main() -> None:
    parser = argparse.ArgumentParser()
    for key in ("configure-script", "repo-root", "service-user", "env-file", "unit-dir"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--initial-instances", type=int, required=True)
    args = parser.parse_args()
    # Manager is outside the compute slice. Read its persisted pool budgets,
    # rather than inheriting a particular worker's mutable job quota.
    resource_file = Path(args.unit_dir) / "detecdiv-worker-resources.conf"
    names = {"SAVED_MEMORY_BUDGET_MB": "DETECDIV_HUB_WORKER_MEMORY_BUDGET_MB",
             "SAVED_CPU_BUDGET": "DETECDIV_HUB_WORKER_CPU_BUDGET",
             "SAVED_SWAP_BUDGET_MB": "DETECDIV_HUB_WORKER_SWAP_BUDGET_MB",
             "SAVED_HOST_MEMORY_RESERVE_MB": "DETECDIV_HUB_HOST_MEMORY_RESERVE_MB"}
    for line in resource_file.read_text().splitlines():
        key, _, value = line.partition("=")
        if key in names and value.isdigit():
            os.environ[names[key]] = value
    os.environ["DETECDIV_HUB_WORKER_DYNAMIC_RESOURCES"] = "1"
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            reconcile(args)
        except Exception:
            LOGGER.exception("Worker manager reconciliation failed")
        time.sleep(5)


if __name__ == "__main__":
    main()
