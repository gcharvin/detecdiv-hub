"""systemd ExecStopPost: attribute a cgroup OOM to its job immediately."""
from __future__ import annotations

import os

from sqlalchemy import select

from api.config import get_settings
from api.models import ExecutionTarget, Job, WorkerInstance
from worker.run_worker import get_worker_instance_id, mark_job_failed, session_scope


def record_exit() -> None:
    if os.getenv("SERVICE_RESULT") != "oom-kill":
        return
    settings = get_settings()
    with session_scope() as session:
        worker = session.scalars(
            select(WorkerInstance).join(ExecutionTarget).where(
                ExecutionTarget.target_key == settings.worker_target_key,
                WorkerInstance.worker_instance == get_worker_instance_id(),
            )
        ).first()
        if worker is None or worker.current_job_id is None:
            return
        job = session.get(Job, worker.current_job_id)
        if job is None or job.status not in {"running", "cancelling"}:
            return
        job_id = job.id
    limit_mb = os.getenv("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB", "unknown")
    swap_mb = os.getenv("DETECDIV_HUB_WORKER_SWAP_LIMIT_MB", "unknown")
    mark_job_failed(
        job_id,
        f"Worker process tree exhausted its RAM/swap budget ({limit_mb} MiB RAM, {swap_mb} MiB swap); "
        "systemd contained the OOM to this worker. Increase its memory budget "
        "or reduce MATLAB pool size / input batch size before retrying.",
    )


if __name__ == "__main__":
    record_exit()
