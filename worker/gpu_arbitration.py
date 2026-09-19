from __future__ import annotations

import logging
import subprocess

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import Settings
from api.models import ExecutionTarget, Job

LOGGER = logging.getLogger("detecdiv-hub-worker")


class GpuArbitrationError(RuntimeError):
    pass


def job_requires_gpu(session: Session, *, job: Job) -> bool:
    """Return whether this pipeline run will claim the configured GPU target."""
    if (job.params_json or {}).get("job_kind") != "pipeline_run":
        return False
    gpu = dict(((job.params_json or {}).get("run_request") or {}).get("gpu") or {})
    mode = str(gpu.get("mode") or "").strip().lower()
    if mode == "force_gpu":
        return True
    if mode in {"force_cpu", "disabled", "none"}:
        return False
    if job.execution_target_id is None:
        return False
    target = session.get(ExecutionTarget, job.execution_target_id)
    return bool(target is not None and target.supports_gpu)


def pause_qwen_for_gpu_job(*, settings: Settings) -> None:
    if not settings.assistant_gpu_arbitration_enabled:
        return
    _run_systemctl(settings=settings, action="stop")


def resume_qwen_if_gpu_is_idle(session: Session, *, settings: Settings) -> None:
    if not settings.assistant_gpu_arbitration_enabled:
        return
    active_jobs = session.scalars(
        select(Job).where(Job.status.in_(("running", "cancelling")))
    ).all()
    if any(job_requires_gpu(session, job=job) for job in active_jobs):
        return
    try:
        _run_systemctl(settings=settings, action="start")
    except GpuArbitrationError:
        # A completed DetecDiv job must not be marked failed merely because the
        # optional assistant did not restart. Operators can restart it manually.
        LOGGER.exception("Could not resume Qwen after GPU jobs completed")


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
