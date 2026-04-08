from __future__ import annotations

import logging
import socket
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import select

from api.config import get_settings
from api.db import SessionLocal
from api.models import ExecutionTarget, Job
from worker.archive_policy_scheduler import run_archive_policy_if_due
from worker.micromanager_ingest_scheduler import run_micromanager_ingest_if_due
from worker.pipeline_run_executor import execute_pipeline_run_job
from worker.storage_lifecycle import execute_storage_lifecycle_job, finalize_storage_lifecycle_failure


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("detecdiv-hub-worker")


@contextmanager
def session_scope():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def claim_next_job() -> Job | None:
    settings = get_settings()
    with session_scope() as session:
        target = resolve_worker_target(session, settings.worker_target_key)
        stmt = (
            select(Job)
            .where(Job.status == "queued")
            .order_by(Job.priority.asc(), Job.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if target is not None:
            stmt = stmt.where((Job.execution_target_id.is_(None)) | (Job.execution_target_id == target.id))
        job = session.scalars(stmt).first()
        if job is None:
            update_worker_target_state(
                session,
                target=target,
                health="idle",
                current_job=None,
                last_job_status=None,
            )
            return None

        job.status = "running"
        job.resolved_mode = job.requested_mode if job.requested_mode != "auto" else "server"
        job.started_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.started_at
        job.updated_at = datetime.now(timezone.utc)
        if target is not None and job.execution_target_id is None:
            job.execution_target_id = target.id
        update_worker_target_state(
            session,
            target=target,
            health="busy",
            current_job=job,
            last_job_status="running",
        )
        session.flush()
        session.expunge(job)
        return job


def mark_job_done(job_id, result_json: dict) -> None:
    settings = get_settings()
    with session_scope() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = "done"
        job.result_json = result_json
        job.finished_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.finished_at
        job.updated_at = datetime.now(timezone.utc)
        target = resolve_target_for_job(session, job=job, configured_target_key=settings.worker_target_key)
        update_worker_target_state(
            session,
            target=target,
            health="online",
            current_job=None,
            last_job_status="done",
            last_job=job,
        )


def mark_job_failed(job_id, error_text: str) -> None:
    settings = get_settings()
    with session_scope() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = "failed"
        job.error_text = error_text
        job.finished_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.finished_at
        job.updated_at = datetime.now(timezone.utc)
        target = resolve_target_for_job(session, job=job, configured_target_key=settings.worker_target_key)
        update_worker_target_state(
            session,
            target=target,
            health="error",
            current_job=None,
            last_job_status="failed",
            last_job=job,
            error_text=error_text,
        )


def execute_job(job: Job) -> dict:
    job_kind = (job.params_json or {}).get("job_kind")
    LOGGER.info("Executing job %s on host %s", job.id, socket.gethostname())
    if job_kind == "pipeline_run":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            return execute_pipeline_run_job(session, job=job_record)
    if job_kind in {"archive_raw_dataset", "restore_raw_dataset"}:
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            return execute_storage_lifecycle_job(session, job=job_record)

    return {
        "worker_host": socket.gethostname(),
        "message": "Placeholder worker execution completed.",
        "job_kind": job_kind or "generic",
        "requested_mode": job.requested_mode,
        "resolved_mode": job.resolved_mode,
    }


def run_forever() -> None:
    settings = get_settings()
    last_archive_policy_run_at: datetime | None = None
    last_micromanager_ingest_run_at: datetime | None = None
    LOGGER.info("Starting DetecDiv hub worker")
    while True:
        try:
            with session_scope() as session:
                target = resolve_worker_target(session, settings.worker_target_key)
                update_worker_target_state(session, target=target, health="online", current_job=None, last_job_status=None)
        except Exception:  # pragma: no cover - defensive around heartbeat
            LOGGER.exception("Execution target heartbeat update failed")

        try:
            with session_scope() as session:
                last_archive_policy_run_at = run_archive_policy_if_due(
                    session,
                    last_run_at=last_archive_policy_run_at,
                )
        except Exception:  # pragma: no cover - defensive around periodic maintenance
            LOGGER.exception("Automatic archive policy run failed")

        try:
            with session_scope() as session:
                last_micromanager_ingest_run_at = run_micromanager_ingest_if_due(
                    session,
                    last_run_at=last_micromanager_ingest_run_at,
                )
        except Exception:  # pragma: no cover - defensive around periodic maintenance
            LOGGER.exception("Micro-Manager ingest run failed")

        job = claim_next_job()
        if job is None:
            time.sleep(settings.worker_poll_interval_sec)
            continue

        try:
            result_json = execute_job(job)
            mark_job_done(job.id, result_json)
            LOGGER.info("Job %s completed", job.id)
        except Exception as exc:  # pragma: no cover - defensive for worker loop
            with session_scope() as session:
                job_record = session.get(Job, job.id)
                if job_record is not None:
                    finalize_storage_lifecycle_failure(session, job=job_record, error_text=str(exc))
            mark_job_failed(job.id, str(exc))
            LOGGER.exception("Job %s failed", job.id)


def resolve_worker_target(session, configured_target_key: str | None) -> ExecutionTarget | None:
    target_key = str(configured_target_key or "").strip()
    if not target_key:
        return None
    return session.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key == target_key)).first()


def resolve_target_for_job(session, *, job: Job, configured_target_key: str | None) -> ExecutionTarget | None:
    if job.execution_target_id:
        return session.get(ExecutionTarget, job.execution_target_id)
    return resolve_worker_target(session, configured_target_key)


def update_worker_target_state(
    session,
    *,
    target: ExecutionTarget | None,
    health: str,
    current_job: Job | None,
    last_job_status: str | None,
    last_job: Job | None = None,
    error_text: str | None = None,
) -> None:
    if target is None:
        return

    now = datetime.now(timezone.utc)
    metadata = dict(target.metadata_json or {})
    worker_health = dict(metadata.get("worker_health") or {})
    worker_health["worker_host"] = socket.gethostname()
    worker_health["last_seen_at"] = now.isoformat()
    worker_health["health"] = health
    worker_health["poll_interval_sec"] = get_settings().worker_poll_interval_sec
    if current_job is not None:
        worker_health["current_job_id"] = str(current_job.id)
        worker_health["claimed_at"] = now.isoformat()
    else:
        worker_health["current_job_id"] = None
    if last_job is not None:
        worker_health["last_job_id"] = str(last_job.id)
    if last_job_status is not None:
        worker_health["last_job_status"] = last_job_status
    if error_text:
        worker_health["last_error"] = error_text
    elif health != "error":
        worker_health["last_error"] = None
    metadata["worker_health"] = worker_health
    target.metadata_json = metadata
    if health == "error":
        target.status = "degraded"
    elif health == "busy":
        target.status = "online"
    else:
        target.status = "online"


if __name__ == "__main__":
    run_forever()
