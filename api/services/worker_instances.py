from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from api.models import ExecutionTarget, Job, WorkerInstance


def normalize_worker_instance_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return text
    if text.lower() == "main":
        return "main"
    core = text[1:] if text.startswith("@") else text
    if core.isdigit():
        return f"@{core}"
    return text


def worker_instance_is_active(worker: WorkerInstance | dict, *, now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    if isinstance(worker, dict):
        poll_interval_sec = read_positive_float(worker.get("poll_interval_sec")) or 5.0
        last_seen_at = parse_datetime(worker.get("last_seen_at"))
    else:
        poll_interval_sec = worker.poll_interval_sec or 5.0
        last_seen_at = worker.last_seen_at
    if last_seen_at is None:
        return False
    stale_after_sec = max(poll_interval_sec * 3, 30.0)
    return (now - last_seen_at).total_seconds() <= stale_after_sec


def summarize_worker_instances(
    workers: list[WorkerInstance],
    *,
    max_concurrent_jobs: int | None,
    now: datetime | None = None,
) -> dict:
    now = now or datetime.now(timezone.utc)
    if not workers:
        return {}

    active_workers = [worker for worker in workers if worker_instance_is_active(worker, now=now)]
    busy_workers = sum(1 for worker in active_workers if worker.health == "busy" or worker.current_job_id)
    error_workers = sum(1 for worker in active_workers if worker.health == "error")
    online_workers = sum(1 for worker in active_workers if worker.health in {"online", "idle", "busy"})
    current_job_ids = [str(worker.current_job_id) for worker in active_workers if worker.current_job_id]
    latest_seen_at = max((worker.last_seen_at for worker in active_workers if worker.last_seen_at), default=None)
    latest_claimed_at = max((worker.claimed_at for worker in active_workers if worker.claimed_at), default=None)
    latest_worker = max(
        (worker for worker in active_workers if worker.last_seen_at),
        key=lambda worker: worker.last_seen_at,
        default=None,
    )

    if busy_workers:
        health = "busy"
    elif error_workers:
        health = "error"
    elif online_workers:
        health = "online"
    else:
        health = "unknown"

    summary = {
        "health": health,
        "worker_host": getattr(latest_worker, "worker_host", None) if latest_worker is not None else None,
        "worker_count": len(active_workers),
        "registered_workers": len(workers),
        "stale_workers": len(workers) - len(active_workers),
        "busy_workers": busy_workers,
        "online_workers": online_workers,
        "error_workers": error_workers,
        "current_job_count": len(current_job_ids),
        "current_job_ids": current_job_ids,
        "last_seen_at": latest_seen_at.isoformat() if latest_seen_at else None,
        "claimed_at": latest_claimed_at.isoformat() if latest_claimed_at else None,
        "worker_instances": sorted(worker.worker_instance for worker in workers),
    }
    if max_concurrent_jobs is not None:
        summary["max_concurrent_jobs"] = max_concurrent_jobs
        summary["capacity_full"] = busy_workers >= max_concurrent_jobs
    return summary


def worker_instance_to_health(worker: WorkerInstance) -> dict:
    return {
        "worker_instance": worker.worker_instance,
        "worker_host": worker.worker_host,
        "process_id": worker.process_id,
        "health": worker.health,
        "current_job_id": str(worker.current_job_id) if worker.current_job_id else None,
        "current_job_kind": worker.current_job_kind,
        "current_job_status": worker.current_job_status,
        "current_job_started_at": worker.current_job_started_at.isoformat() if worker.current_job_started_at else None,
        "last_job_id": str(worker.last_job_id) if worker.last_job_id else None,
        "last_job_status": worker.last_job_status,
        "last_error": worker.last_error,
        "poll_interval_sec": worker.poll_interval_sec,
        "claimed_at": worker.claimed_at.isoformat() if worker.claimed_at else None,
        "last_seen_at": worker.last_seen_at.isoformat() if worker.last_seen_at else None,
        "updated_at": worker.updated_at.isoformat() if worker.updated_at else None,
    }


def worker_healths_from_instances(workers: list[WorkerInstance]) -> dict[str, dict]:
    return {worker.worker_instance: worker_instance_to_health(worker) for worker in workers}


def execution_target_worker_metadata(session: Session, target: ExecutionTarget) -> dict:
    metadata = dict(target.metadata_json or {})
    max_concurrent_jobs = read_positive_int(metadata.get("max_concurrent_jobs"))
    workers = list(
        session.scalars(
            select(WorkerInstance)
            .where(WorkerInstance.execution_target_id == target.id)
            .order_by(WorkerInstance.worker_instance.asc())
        )
    )
    if not workers:
        return metadata
    worker_healths = worker_healths_from_instances(workers)
    worker_summary = summarize_worker_instances(workers, max_concurrent_jobs=max_concurrent_jobs)
    metadata["worker_healths"] = worker_healths
    metadata["worker_health_summary"] = worker_summary
    metadata["worker_health"] = worker_summary
    return metadata


def enrich_execution_target(session: Session, target: ExecutionTarget) -> ExecutionTarget:
    target.metadata_json = execution_target_worker_metadata(session, target)
    return target


def upsert_worker_instance(
    session: Session,
    *,
    target: ExecutionTarget,
    worker_instance: str,
    health: str,
    current_job: Job | None,
    poll_interval_sec: float,
    last_job: Job | None = None,
    last_job_status: str | None = None,
    error_text: str | None = None,
    now: datetime | None = None,
) -> WorkerInstance:
    now = now or datetime.now(timezone.utc)
    worker_instance = normalize_worker_instance_id(worker_instance)
    current_job_id = current_job.id if current_job is not None else None
    current_job_kind = str((current_job.params_json or {}).get("job_kind") or "generic") if current_job is not None else None
    current_job_status = str(current_job.status or "running") if current_job is not None else None
    current_job_started_at = getattr(current_job, "started_at", None) if current_job is not None else None
    claimed_at = now if current_job is not None else None

    insert_values = {
        "id": uuid.uuid4(),
        "execution_target_id": target.id,
        "worker_instance": worker_instance,
        "worker_host": socket.gethostname(),
        "process_id": os.getpid(),
        "health": health,
        "current_job_id": current_job_id,
        "current_job_kind": current_job_kind,
        "current_job_status": current_job_status,
        "current_job_started_at": current_job_started_at,
        "last_job_id": last_job.id if last_job is not None else None,
        "last_job_status": last_job_status,
        "last_error": error_text,
        "poll_interval_sec": poll_interval_sec,
        "claimed_at": claimed_at,
        "last_seen_at": now,
        "updated_at": now,
    }
    update_values = dict(insert_values)
    update_values.pop("id", None)
    update_values.pop("execution_target_id", None)
    update_values.pop("worker_instance", None)
    if last_job is None:
        update_values.pop("last_job_id", None)
    if last_job_status is None:
        update_values.pop("last_job_status", None)
    if error_text is None and health != "error":
        update_values["last_error"] = None
    elif error_text is None:
        update_values.pop("last_error", None)
    if current_job is None:
        update_values["current_job_id"] = None
        update_values["current_job_kind"] = None
        update_values["current_job_status"] = None
        update_values["current_job_started_at"] = None
    else:
        update_values["claimed_at"] = claimed_at

    statement = (
        insert(WorkerInstance)
        .values(**insert_values)
        .on_conflict_do_update(
            index_elements=[WorkerInstance.execution_target_id, WorkerInstance.worker_instance],
            set_=update_values,
        )
        .returning(WorkerInstance)
    )
    return session.scalars(statement).one()


def active_worker_current_job_ids(session: Session, *, target: ExecutionTarget) -> set[str]:
    workers = list(
        session.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id == target.id))
    )
    return {
        str(worker.current_job_id)
        for worker in workers
        if worker.current_job_id and worker_instance_is_active(worker)
    }


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


def read_positive_float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def parse_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None
