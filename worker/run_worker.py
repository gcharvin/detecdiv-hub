from __future__ import annotations

import logging
import os
import socket
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import func, select

from api.config import get_settings
from api.db import SessionLocal
from api.models import ExecutionTarget, Job, RawDatasetPosition
from worker.archive_policy_scheduler import run_archive_policy_if_due
from worker.micromanager_ingest_scheduler import run_micromanager_ingest_if_due
from worker.pipeline_run_executor import PipelineRunCancelled, execute_pipeline_run_job
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


def get_worker_instance_id() -> str:
    configured = str(get_settings().worker_instance or "").strip()
    if configured:
        return configured
    return f"{socket.gethostname()}-pid{os.getpid()}"


def summarize_worker_health(worker_healths: dict[str, dict], *, max_concurrent_jobs: int | None) -> dict:
    if not worker_healths:
        return {}

    now = datetime.now(timezone.utc)
    active_entries = []
    stale_workers = 0
    for item in worker_healths.values():
        poll_interval_sec = read_positive_int(item.get("poll_interval_sec")) or 5
        stale_after_sec = max(poll_interval_sec * 3, 30)
        try:
            last_seen_at = datetime.fromisoformat(str(item.get("last_seen_at") or ""))
        except ValueError:
            last_seen_at = None
        if last_seen_at is None or (now - last_seen_at).total_seconds() > stale_after_sec:
            stale_workers += 1
            continue
        active_entries.append(item)

    entries = active_entries
    busy_workers = sum(1 for item in entries if item.get("health") == "busy" or item.get("current_job_id"))
    error_workers = sum(1 for item in entries if item.get("health") == "error")
    online_workers = sum(1 for item in entries if item.get("health") in {"online", "idle", "busy"})
    latest_seen_at = max((str(item.get("last_seen_at") or "") for item in entries), default="")
    latest_claimed_at = max((str(item.get("claimed_at") or "") for item in entries), default="")
    current_job_ids = [str(item.get("current_job_id")) for item in entries if item.get("current_job_id")]

    if busy_workers:
        summary_health = "busy"
    elif error_workers:
        summary_health = "error"
    elif online_workers:
        summary_health = "online"
    else:
        summary_health = "unknown"

    summary = {
        "worker_host": socket.gethostname(),
        "worker_count": len(entries),
        "registered_workers": len(worker_healths),
        "stale_workers": stale_workers,
        "busy_workers": busy_workers,
        "online_workers": online_workers,
        "error_workers": error_workers,
        "current_job_count": len(current_job_ids),
        "current_job_ids": current_job_ids,
        "health": summary_health,
        "last_seen_at": latest_seen_at or None,
        "claimed_at": latest_claimed_at or None,
        "worker_instances": sorted(worker_healths.keys()),
    }
    if max_concurrent_jobs is not None:
        summary["max_concurrent_jobs"] = max_concurrent_jobs
        summary["capacity_full"] = busy_workers >= max_concurrent_jobs
    return summary


def worker_health_is_active(worker_health: dict) -> bool:
    poll_interval_sec = read_positive_int(worker_health.get("poll_interval_sec")) or 5
    stale_after_sec = max(poll_interval_sec * 3, 30)
    try:
        last_seen_at = datetime.fromisoformat(str(worker_health.get("last_seen_at") or ""))
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - last_seen_at).total_seconds() <= stale_after_sec


def running_job_is_stale(job: Job) -> bool:
    now = datetime.now(timezone.utc)
    reference_time = job.heartbeat_at or job.updated_at or job.started_at or job.created_at
    if reference_time is None:
        return True
    return (now - reference_time).total_seconds() > 60


def recover_orphaned_jobs(session, *, target: ExecutionTarget | None) -> int:
    if target is None:
        return 0

    metadata = dict(target.metadata_json or {})
    worker_healths = dict(metadata.get("worker_healths") or {})
    active_job_ids = {
        str(item.get("current_job_id"))
        for item in worker_healths.values()
        if worker_health_is_active(item) and item.get("current_job_id")
    }
    running_jobs = list(
        session.scalars(
            select(Job).where(
                Job.execution_target_id == target.id,
                Job.status.in_(("running", "cancelling")),
            )
        )
    )
    recovered = 0
    for job in running_jobs:
        if str(job.id) in active_job_ids:
            continue
        if not running_job_is_stale(job):
            continue
        job.status = "failed"
        job.error_text = "Job was orphaned after worker restart or heartbeat loss."
        job.finished_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.finished_at
        job.updated_at = job.finished_at
        if (job.params_json or {}).get("job_kind") == "raw_preview_video":
            update_raw_preview_position_state(session, job=job, status="failed")
        recovered += 1
        LOGGER.warning("Recovered orphaned job %s on target %s", job.id, target.display_name)
    if recovered:
        update_worker_target_state(
            session,
            target=target,
            health="online",
            current_job=None,
            last_job_status="recovered_orphaned_jobs",
        )
    return recovered


def claim_next_job() -> Job | None:
    settings = get_settings()
    with session_scope() as session:
        target = resolve_worker_target(session, settings.worker_target_key)
        if target is not None:
            target = session.scalars(select(ExecutionTarget).where(ExecutionTarget.id == target.id).with_for_update()).one()
            if bool((target.metadata_json or {}).get("drain_new_jobs")):
                update_worker_target_state(
                    session,
                    target=target,
                    health="online",
                    current_job=None,
                    last_job_status="drain_active",
                )
                return None
            max_concurrent_jobs = read_positive_int((target.metadata_json or {}).get("max_concurrent_jobs"))
            if max_concurrent_jobs is not None:
                running_jobs = session.scalar(
                    select(func.count(Job.id)).where(
                        Job.execution_target_id == target.id,
                        Job.status.in_(("running", "cancelling")),
                    )
                )
                if int(running_jobs or 0) >= max_concurrent_jobs:
                    update_worker_target_state(
                        session,
                        target=target,
                        health="busy",
                        current_job=None,
                        last_job_status="capacity_full",
                    )
                    return None
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
        if (job.params_json or {}).get("job_kind") == "raw_preview_video":
            update_raw_preview_position_state(session, job=job, status=result_json.get("preview_status", "done"))
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
        if (job.params_json or {}).get("job_kind") == "raw_preview_video":
            update_raw_preview_position_state(session, job=job, status="failed")
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


def mark_job_cancelled(job_id, message: str) -> None:
    settings = get_settings()
    with session_scope() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = "cancelled"
        result_json = dict(job.result_json or {})
        result_json["status"] = "cancelled"
        result_json["message"] = message or "Pipeline run cancelled by user."
        job.result_json = result_json
        job.error_text = message or None
        job.finished_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.finished_at
        job.updated_at = datetime.now(timezone.utc)
        target = resolve_target_for_job(session, job=job, configured_target_key=settings.worker_target_key)
        update_worker_target_state(
            session,
            target=target,
            health="online",
            current_job=None,
            last_job_status="cancelled",
            last_job=job,
        )


def execute_job(job: Job) -> dict:
    job_kind = (job.params_json or {}).get("job_kind")
    LOGGER.info("Executing job %s on host %s instance %s", job.id, socket.gethostname(), get_worker_instance_id())
    if job_kind == "pipeline_run":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_pipeline_run_job(session, job=job_record)
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind in {"archive_raw_dataset", "restore_raw_dataset"}:
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_storage_lifecycle_job(session, job=job_record)
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind == "raw_preview_video":
        try:
            from worker.raw_preview_video import execute_raw_preview_video_job
        except ImportError as exc:
            raise RuntimeError(
                "Raw preview video dependencies are missing. Install numpy, tifffile, and zarr in the worker environment."
            ) from exc
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_raw_preview_video_job(session, job=job_record)
            result_json["worker_host"] = socket.gethostname()
            result_json["worker_instance"] = get_worker_instance_id()
            result_json["requested_mode"] = job.requested_mode
            result_json["resolved_mode"] = job.resolved_mode
            return result_json

    return {
        "worker_host": socket.gethostname(),
        "worker_instance": get_worker_instance_id(),
        "message": "Placeholder worker execution completed.",
        "job_kind": job_kind or "generic",
        "requested_mode": job.requested_mode,
        "resolved_mode": job.resolved_mode,
    }


def run_forever() -> None:
    settings = get_settings()
    last_archive_policy_run_at: datetime | None = None
    last_micromanager_ingest_run_at: datetime | None = None
    LOGGER.info("Starting DetecDiv hub worker instance %s", get_worker_instance_id())
    while True:
        try:
            with session_scope() as session:
                target = resolve_worker_target(session, settings.worker_target_key)
                update_worker_target_state(session, target=target, health="online", current_job=None, last_job_status=None)
                recover_orphaned_jobs(session, target=target)
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
        except PipelineRunCancelled as exc:
            mark_job_cancelled(job.id, str(exc))
            LOGGER.info("Job %s cancelled", job.id)
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


def read_positive_int(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 1:
        return None
    return parsed


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
    max_concurrent_jobs = read_positive_int(metadata.get("max_concurrent_jobs"))
    worker_healths = dict(metadata.get("worker_healths") or {})
    worker_instance_id = get_worker_instance_id()
    worker_health = dict(worker_healths.get(worker_instance_id) or {})
    worker_health["worker_instance"] = worker_instance_id
    worker_health["worker_host"] = socket.gethostname()
    worker_health["last_seen_at"] = now.isoformat()
    worker_health["health"] = health
    worker_health["poll_interval_sec"] = get_settings().worker_poll_interval_sec
    if current_job is not None:
        worker_health["current_job_id"] = str(current_job.id)
        worker_health["current_job_kind"] = str((current_job.params_json or {}).get("job_kind") or "generic")
        worker_health["current_job_status"] = str(current_job.status or "running")
        worker_health["current_job_started_at"] = (
            current_job.started_at.isoformat() if getattr(current_job, "started_at", None) is not None else None
        )
        worker_health["claimed_at"] = now.isoformat()
    else:
        worker_health["current_job_id"] = None
        worker_health["current_job_kind"] = None
        worker_health["current_job_status"] = None
        worker_health["current_job_started_at"] = None
    if last_job is not None:
        worker_health["last_job_id"] = str(last_job.id)
    if last_job_status is not None:
        worker_health["last_job_status"] = last_job_status
    if error_text:
        worker_health["last_error"] = error_text
    elif health != "error":
        worker_health["last_error"] = None
    worker_healths[worker_instance_id] = worker_health
    metadata["worker_healths"] = worker_healths
    metadata["worker_health_summary"] = summarize_worker_health(
        worker_healths,
        max_concurrent_jobs=max_concurrent_jobs,
    )
    metadata["worker_health"] = metadata["worker_health_summary"]
    target.metadata_json = metadata
    if health == "error":
        target.status = "degraded"
    elif health == "busy":
        target.status = "online"
    else:
        target.status = "online"


def update_raw_preview_position_state(session, *, job: Job, status: str) -> None:
    position_id = (job.params_json or {}).get("position_id")
    if not position_id:
        return
    position = session.get(RawDatasetPosition, position_id)
    if position is None:
        return
    position.preview_status = status
    position.updated_at = datetime.now(timezone.utc)


if __name__ == "__main__":
    run_forever()
