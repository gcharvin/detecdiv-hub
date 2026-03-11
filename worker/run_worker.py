from __future__ import annotations

import logging
import socket
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import select

from api.config import get_settings
from api.db import SessionLocal
from api.models import Job
from worker.archive_policy_scheduler import run_archive_policy_if_due
from worker.micromanager_ingest_scheduler import run_micromanager_ingest_if_due
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
    with session_scope() as session:
        stmt = (
            select(Job)
            .where(Job.status == "queued")
            .order_by(Job.priority.asc(), Job.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        job = session.scalars(stmt).first()
        if job is None:
            return None

        job.status = "running"
        job.resolved_mode = job.requested_mode if job.requested_mode != "auto" else "server"
        job.started_at = datetime.now(timezone.utc)
        job.updated_at = datetime.now(timezone.utc)
        session.flush()
        session.expunge(job)
        return job


def mark_job_done(job_id, result_json: dict) -> None:
    with session_scope() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = "done"
        job.result_json = result_json
        job.finished_at = datetime.now(timezone.utc)
        job.updated_at = datetime.now(timezone.utc)


def mark_job_failed(job_id, error_text: str) -> None:
    with session_scope() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = "failed"
        job.error_text = error_text
        job.finished_at = datetime.now(timezone.utc)
        job.updated_at = datetime.now(timezone.utc)


def execute_job(job: Job) -> dict:
    job_kind = (job.params_json or {}).get("job_kind")
    LOGGER.info("Executing job %s on host %s", job.id, socket.gethostname())
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


if __name__ == "__main__":
    run_forever()
