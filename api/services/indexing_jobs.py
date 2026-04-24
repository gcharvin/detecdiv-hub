from __future__ import annotations

import os
import socket
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, joinedload

from api.db import SessionLocal
from api.config import get_settings
from api.models import ExecutionTarget, IndexingJob, Job, User
from api.schemas import IndexRequest
from api.services.project_indexing import ProjectIndexResult, index_project_root
from api.services.users import get_or_create_user


def indexing_worker_instance_id() -> str:
    configured = str(get_settings().worker_instance or "").strip()
    if configured:
        return configured
    return f"{socket.gethostname()}-pid{os.getpid()}"


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

    busy_workers = sum(1 for item in active_entries if item.get("health") == "busy" or item.get("current_job_id"))
    error_workers = sum(1 for item in active_entries if item.get("health") == "error")
    online_workers = sum(1 for item in active_entries if item.get("health") in {"online", "idle", "busy"})
    latest_seen_at = max((str(item.get("last_seen_at") or "") for item in active_entries), default="")
    latest_claimed_at = max((str(item.get("claimed_at") or "") for item in active_entries), default="")
    current_job_ids = [str(item.get("current_job_id")) for item in active_entries if item.get("current_job_id")]

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
        "worker_count": len(active_entries),
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


def update_indexing_worker_telemetry(
    session,
    *,
    worker_job: Job | None,
    now: datetime,
) -> None:
    if worker_job is None:
        return
    worker_job.heartbeat_at = now
    worker_job.updated_at = now

    target = session.get(ExecutionTarget, worker_job.execution_target_id) if worker_job.execution_target_id else None
    if target is None:
        return

    metadata = dict(target.metadata_json or {})
    max_concurrent_jobs = read_positive_int(metadata.get("max_concurrent_jobs"))
    worker_healths = dict(metadata.get("worker_healths") or {})
    worker_instance = indexing_worker_instance_id()
    worker_health = dict(worker_healths.get(worker_instance) or {})
    worker_health["worker_instance"] = worker_instance
    worker_health["worker_host"] = socket.gethostname()
    worker_health["last_seen_at"] = now.isoformat()
    worker_health["health"] = "busy"
    worker_health["poll_interval_sec"] = get_settings().worker_poll_interval_sec
    worker_health["current_job_id"] = str(worker_job.id)
    worker_health["current_job_kind"] = str((worker_job.params_json or {}).get("job_kind") or "generic")
    worker_health["current_job_status"] = str(worker_job.status or "running")
    worker_health["current_job_started_at"] = (
        worker_job.started_at.isoformat() if getattr(worker_job, "started_at", None) is not None else None
    )
    worker_health["claimed_at"] = (
        worker_job.started_at.isoformat() if getattr(worker_job, "started_at", None) is not None else now.isoformat()
    )
    worker_health["last_error"] = None
    worker_healths[worker_instance] = worker_health
    metadata["worker_healths"] = worker_healths
    metadata["worker_health_summary"] = summarize_worker_health(worker_healths, max_concurrent_jobs=max_concurrent_jobs)
    metadata["worker_health"] = metadata["worker_health_summary"]
    target.metadata_json = metadata
    target.status = "online"


def run_indexing_job_keepalive(indexing_job_id: UUID, stop_event: threading.Event, *, interval_sec: float = 10.0) -> None:
    while not stop_event.wait(interval_sec):
        with SessionLocal() as session:
            job = session.get(IndexingJob, indexing_job_id)
            if job is None or job.status != "running":
                return
            now = datetime.now(timezone.utc)
            job.heartbeat_at = now
            job.updated_at = now
            worker_job_id = (job.metadata_json or {}).get("worker_job_id")
            worker_job = session.get(Job, worker_job_id) if worker_job_id else None
            update_indexing_worker_telemetry(session, worker_job=worker_job, now=now)
            session.commit()


def create_indexing_job(
    session: Session,
    *,
    payload: IndexRequest,
    current_user: User,
) -> IndexingJob:
    owner_user_key = payload.owner_user_key or current_user.user_key
    owner = get_or_create_user(session, user_key=owner_user_key, display_name=owner_user_key)
    job = IndexingJob(
        requested_by_user_id=current_user.id,
        owner_user_id=owner.id,
        source_kind=payload.source_kind,
        source_path=payload.source_path,
        storage_root_name=payload.storage_root_name,
        host_scope=payload.host_scope,
        root_type=payload.root_type,
        visibility=payload.visibility,
        clear_existing_for_root=payload.clear_existing_for_root,
        status="queued",
        phase="queued",
        message="Queued for indexing.",
        metadata_json=payload.metadata_json or {},
    )
    session.add(job)
    session.flush()
    session.refresh(job)
    return job


def enqueue_indexing_worker_job(
    session: Session,
    *,
    indexing_job: IndexingJob,
    current_user: User,
) -> Job:
    worker_job = Job(
        requested_mode="server",
        priority=100,
        requested_by=current_user.user_key,
        requested_from_host="api-indexing",
        params_json={
            "job_kind": "project_indexing",
            "indexing_job_id": str(indexing_job.id),
        },
        status="queued",
    )
    session.add(worker_job)
    session.flush()
    metadata = dict(indexing_job.metadata_json or {})
    metadata["worker_job_id"] = str(worker_job.id)
    indexing_job.metadata_json = metadata
    indexing_job.message = "Queued for worker execution."
    indexing_job.phase = "queued"
    indexing_job.status = "queued"
    session.flush()
    return worker_job


def execute_indexing_job(indexing_job_id: UUID) -> dict[str, Any]:
    with SessionLocal() as session:
        job = session.get(IndexingJob, indexing_job_id)
        if job is None:
            raise ValueError(f"Indexing job {indexing_job_id} not found")
        job.status = "running"
        job.phase = "discovering"
        job.started_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.started_at
        job.message = "Starting project scan."
        session.commit()

    try:
        with SessionLocal() as session:
            job = session.get(IndexingJob, indexing_job_id)
            if job is None:
                raise ValueError(f"Indexing job {indexing_job_id} disappeared before execution")
            owner = job.owner
            owner_user_key = owner.user_key if owner is not None else "localdev"
            worker_job_id = (job.metadata_json or {}).get("worker_job_id")
            worker_job = session.get(Job, worker_job_id) if worker_job_id else None
            update_indexing_worker_telemetry(session, worker_job=worker_job, now=datetime.now(timezone.utc))
            session.commit()
            keepalive_stop = threading.Event()
            keepalive_thread = threading.Thread(
                target=run_indexing_job_keepalive,
                args=(indexing_job_id, keepalive_stop),
                kwargs={"interval_sec": 10.0},
                daemon=True,
                name=f"indexing-keepalive-{indexing_job_id}",
            )
            keepalive_thread.start()

            def on_progress(**kwargs: Any) -> None:
                job_record = session.get(IndexingJob, indexing_job_id)
                if job_record is None:
                    return
                now = datetime.now(timezone.utc)
                job_record.total_projects = int(kwargs.get("total_projects", job_record.total_projects or 0))
                job_record.scanned_projects = int(kwargs.get("scanned_projects", job_record.scanned_projects or 0))
                job_record.indexed_projects = int(kwargs.get("indexed_projects", job_record.indexed_projects or 0))
                job_record.failed_projects = int(kwargs.get("failed_projects", job_record.failed_projects or 0))
                job_record.deleted_projects = int(kwargs.get("deleted_projects", job_record.deleted_projects or 0))
                job_record.mat_files_seen = int(kwargs.get("mat_files_seen", job_record.mat_files_seen or 0))
                if kwargs.get("phase"):
                    job_record.phase = str(kwargs["phase"])
                job_record.current_project_path = kwargs.get("current_project_path")
                if kwargs.get("message"):
                    job_record.message = str(kwargs["message"])
                if kwargs.get("error_text"):
                    job_record.error_text = str(kwargs["error_text"])
                job_record.heartbeat_at = now
                job_record.updated_at = now
                session.flush()
                session.commit()

            result = index_project_root(
                session,
                root_path=job.source_path,
                storage_root_name=job.storage_root_name,
                host_scope=job.host_scope,
                root_type=job.root_type,
                owner_user_key=owner_user_key,
                visibility=job.visibility,
                clear_existing_for_root=job.clear_existing_for_root,
                continue_on_error=True,
                commit_each=True,
                progress_callback=on_progress,
            )
            keepalive_stop.set()
            keepalive_thread.join(timeout=1.0)
            finalize_indexing_job_success(session, job, result)
            session.commit()
            return {
                "job_kind": "project_indexing",
                "indexing_job_id": str(job.id),
                "status": job.status,
                "phase": job.phase,
                "result": dict(job.result_json or {}),
            }
    except Exception as exc:  # pragma: no cover - defensive for background worker
        keepalive_stop = locals().get("keepalive_stop")
        keepalive_thread = locals().get("keepalive_thread")
        if isinstance(keepalive_stop, threading.Event):
            keepalive_stop.set()
        if isinstance(keepalive_thread, threading.Thread):
            keepalive_thread.join(timeout=1.0)
        with SessionLocal() as session:
            job = session.get(IndexingJob, indexing_job_id)
            if job is None:
                raise
            job.status = "failed"
            job.phase = "failed"
            job.finished_at = datetime.now(timezone.utc)
            job.heartbeat_at = job.finished_at
            job.updated_at = job.finished_at
            job.error_text = str(exc)
            job.message = "Indexing failed."
            session.commit()
        raise


def finalize_indexing_job_success(session: Session, job: IndexingJob, result: ProjectIndexResult) -> None:
    job.status = "completed" if result.failed_projects == 0 else "completed_with_errors"
    job.phase = "completed"
    job.total_projects = result.total_projects
    job.scanned_projects = result.scanned_projects
    job.indexed_projects = result.indexed_projects
    job.failed_projects = result.failed_projects
    job.deleted_projects = result.deleted_projects
    job.current_project_path = None
    job.finished_at = datetime.now(timezone.utc)
    job.heartbeat_at = job.finished_at
    job.message = (
        f"Indexed {result.indexed_projects}/{result.total_projects} projects"
        f" and {result.indexed_pipelines} independent pipelines from {result.root_path}."
    )
    job.result_json = {
        "root_path": result.root_path,
        "storage_root_name": result.storage_root_name,
        "owner_user_key": result.owner_user_key,
        "visibility": result.visibility,
        "total_projects": result.total_projects,
        "scanned_projects": result.scanned_projects,
        "indexed_projects": result.indexed_projects,
        "failed_projects": result.failed_projects,
        "deleted_projects": result.deleted_projects,
        "stale_cleanup_skipped": result.stale_cleanup_skipped,
        "indexed_pipelines": result.indexed_pipelines,
        "failed_pipelines": result.failed_pipelines,
    }
    session.flush()


def mark_stale_indexing_jobs(session: Session) -> None:
    settings = get_settings()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=int(settings.indexing_stale_after_minutes))
    stmt = select(IndexingJob).where(
        IndexingJob.status == "running",
        or_(IndexingJob.heartbeat_at == None, IndexingJob.heartbeat_at < cutoff),  # noqa: E711
    )
    stale_jobs = list(session.scalars(stmt))
    for job in stale_jobs:
        job.status = "stale"
        job.phase = "stale"
        job.finished_at = datetime.now(timezone.utc)
        job.message = "Marked stale after missing heartbeat."
    if stale_jobs:
        session.flush()


def list_indexing_jobs_for_user(
    session: Session,
    *,
    current_user: User,
    limit: int = 20,
) -> list[IndexingJob]:
    mark_stale_indexing_jobs(session)
    stmt = (
        select(IndexingJob)
        .options(
            joinedload(IndexingJob.owner),
            joinedload(IndexingJob.requested_by),
        )
        .where(
            or_(
                IndexingJob.requested_by_user_id == current_user.id,
                IndexingJob.owner_user_id == current_user.id,
            )
        )
        .order_by(IndexingJob.created_at.desc())
        .limit(limit)
    )
    return list(session.scalars(stmt).unique().all())


def get_indexing_job_for_user(
    session: Session,
    *,
    job_id: UUID,
    current_user: User,
) -> IndexingJob | None:
    mark_stale_indexing_jobs(session)
    stmt = (
        select(IndexingJob)
        .options(
            joinedload(IndexingJob.owner),
            joinedload(IndexingJob.requested_by),
        )
        .where(IndexingJob.id == job_id)
        .where(
            or_(
                IndexingJob.requested_by_user_id == current_user.id,
                IndexingJob.owner_user_id == current_user.id,
            )
        )
    )
    return session.scalars(stmt).unique().first()


def delete_stale_indexing_jobs_for_user(
    session: Session,
    *,
    current_user: User,
) -> int:
    mark_stale_indexing_jobs(session)
    stmt = (
        select(IndexingJob)
        .where(IndexingJob.status == "stale")
        .where(
            or_(
                IndexingJob.requested_by_user_id == current_user.id,
                IndexingJob.owner_user_id == current_user.id,
            )
        )
    )
    jobs = list(session.scalars(stmt).all())
    deleted = len(jobs)
    for job in jobs:
        session.delete(job)
    if deleted:
        session.flush()
    return deleted
