from __future__ import annotations

import logging
import subprocess
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import Settings
from api.models import ExecutionTarget, Job
from api.services.assistant_control import (
    ASSISTANT_CONTROL_JOB_KIND,
    get_assistant_desired_state,
)

LOGGER = logging.getLogger("detecdiv-hub-worker")


class GpuArbitrationError(RuntimeError):
    pass


def job_requires_gpu(session: Session, *, job: Job) -> bool:
    """Return whether the worker scheduler reserved a GPU for this pipeline."""
    if (job.params_json or {}).get("job_kind") != "pipeline_run":
        return False
    params = dict(job.params_json or {})
    allocation = params.get("_hub_resource_allocation")
    if isinstance(allocation, dict) and "gpu_required" in allocation:
        return bool(allocation.get("gpu_required"))
    from worker.job_resources import resolve_job_resource_allocation

    target = session.get(ExecutionTarget, job.execution_target_id) if job.execution_target_id else None
    return bool(resolve_job_resource_allocation(session, job=job, target=target).get("gpu_required"))


def pause_qwen_for_gpu_job(*, settings: Settings) -> None:
    if not settings.assistant_gpu_arbitration_enabled:
        return
    _run_systemctl(settings=settings, action="stop")


def _target_has_active_gpu_job(session: Session, *, target_id: UUID) -> bool:
    active_jobs = session.scalars(
        select(Job).where(
            Job.execution_target_id == target_id,
            Job.status.in_(("running", "cancelling")),
        )
    )
    return any(job_requires_gpu(session, job=job) for job in active_jobs)


def resume_qwen_if_gpu_is_idle(
    session: Session, *, settings: Settings, target: ExecutionTarget | None
) -> None:
    if not settings.assistant_gpu_arbitration_enabled or target is None:
        return
    if get_assistant_desired_state(session) != "running":
        return
    # Worker sessions disable autoflush. Publish the just-completed job's state
    # before querying the remaining reservations inside this transaction.
    session.flush()
    if _target_has_active_gpu_job(session, target_id=target.id):
        return
    try:
        _run_systemctl(settings=settings, action="start")
    except GpuArbitrationError:
        # A completed DetecDiv job must not be marked failed merely because the
        # optional assistant did not restart. Operators can restart it manually.
        LOGGER.exception("Could not resume Qwen after GPU jobs completed")


def execute_assistant_control_job(session: Session, *, job: Job, settings: Settings) -> dict:
    params = dict(job.params_json or {})
    if params.get("job_kind") != ASSISTANT_CONTROL_JOB_KIND:
        raise GpuArbitrationError("Invalid Qwen control job.")
    action = str(params.get("action") or "")
    if action not in {"start", "stop"}:
        raise GpuArbitrationError("Invalid Qwen control action.")
    if action == "start":
        # The scheduler locks the same target before claiming jobs for this GPU.
        target = None
        if job.execution_target_id is not None:
            target = session.get(ExecutionTarget, job.execution_target_id, with_for_update=True)
        if target is None:
            raise GpuArbitrationError("Qwen start requires an assigned execution target.")
        if _target_has_active_gpu_job(session, target_id=target.id):
            raise GpuArbitrationError(f"Qwen cannot start while a GPU job is active on {target.display_name}.")
    _run_systemctl(settings=settings, action=action)
    return {
        "job_kind": ASSISTANT_CONTROL_JOB_KIND,
        "action": action,
        "service": settings.assistant_qwen_service_name,
        "message": f"Qwen service {action} request applied.",
    }


def _run_systemctl(*, settings: Settings, action: str) -> None:
    if action not in {"start", "stop"}:
        raise ValueError(f"Unsupported Qwen service action: {action}")
    command = [settings.assistant_sudo_command, "-n", settings.assistant_systemctl_command, action, settings.assistant_qwen_service_name]
    try:
        completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=60)
    except OSError as exc:
        raise GpuArbitrationError("Could not invoke the Qwen service controller.") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "unknown error").strip()
        raise GpuArbitrationError(f"Qwen service {action} failed: {detail}")
