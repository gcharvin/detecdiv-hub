from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.models import Job, Project, ProjectLock, User


ACTIVE_JOB_STATUSES = {"queued", "running", "cancelling"}
TERMINAL_LOCK_STATUS = {"released", "expired"}


class ProjectLockConflict(RuntimeError):
    def __init__(self, locks: list[ProjectLock]):
        self.locks = locks
        super().__init__("Project has an active write lock")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def expire_stale_project_locks(session: Session, *, now: datetime | None = None) -> int:
    now = now or utcnow()
    locks = list(
        session.scalars(
            select(ProjectLock).where(
                ProjectLock.status == "active",
                ProjectLock.job_id.is_(None),
                ProjectLock.expires_at.is_not(None),
                ProjectLock.expires_at <= now,
            )
        )
    )
    for lock in locks:
        lock.status = "expired"
        lock.released_at = now
        lock.updated_at = now
    return len(locks)


def active_project_locks(session: Session, *, project_id: UUID) -> list[ProjectLock]:
    expire_stale_project_locks(session)
    return list(
        session.scalars(
            select(ProjectLock)
            .options(joinedload(ProjectLock.owner))
            .where(ProjectLock.project_id == project_id, ProjectLock.status == "active")
            .order_by(ProjectLock.created_at.asc())
        )
    )


def active_jobs_for_project(session: Session, *, project_id: UUID) -> list[Job]:
    return list(
        session.scalars(
            select(Job)
            .where(Job.project_id == project_id, Job.status.in_(ACTIVE_JOB_STATUSES))
            .order_by(Job.created_at.asc())
        )
    )


def ensure_project_is_unlocked(
    session: Session,
    *,
    project_id: UUID,
    ignore_lock_ids: set[UUID] | None = None,
) -> None:
    session.execute(select(Project.id).where(Project.id == project_id).with_for_update()).first()
    ignore_lock_ids = ignore_lock_ids or set()
    locks = [lock for lock in active_project_locks(session, project_id=project_id) if lock.id not in ignore_lock_ids]
    if locks:
        raise ProjectLockConflict(locks)


def acquire_client_edit_lease(
    session: Session,
    *,
    project_id: UUID,
    owner: User,
    holder_key: str | None,
    holder_host: str | None,
    ttl_seconds: int,
    write_scope: str,
    reason: str | None,
    metadata_json: dict,
) -> ProjectLock:
    ensure_project_is_unlocked(session, project_id=project_id)
    now = utcnow()
    lease = ProjectLock(
        project_id=project_id,
        owner_user_id=owner.id,
        lock_kind="client_edit_lease",
        lock_scope="project",
        write_scope=write_scope or "project_update",
        status="active",
        holder_key=holder_key or owner.user_key,
        holder_host=holder_host,
        reason=reason,
        metadata_json=metadata_json or {},
        expires_at=now + timedelta(seconds=ttl_seconds),
        heartbeat_at=now,
    )
    session.add(lease)
    session.flush()
    return lease


def create_server_job_lock(
    session: Session,
    *,
    project_id: UUID,
    job: Job,
    owner: User | None,
    holder_host: str | None = None,
    write_scope: str = "project_update",
    reason: str | None = None,
    metadata_json: dict | None = None,
) -> ProjectLock:
    ensure_project_is_unlocked(session, project_id=project_id)
    now = utcnow()
    lock = ProjectLock(
        project_id=project_id,
        job_id=job.id,
        owner_user_id=owner.id if owner is not None else None,
        lock_kind="server_job",
        lock_scope="project",
        write_scope=write_scope or "project_update",
        status="active",
        holder_key=str(job.id),
        holder_host=holder_host,
        reason=reason or "pipeline_run",
        metadata_json=metadata_json or {},
        heartbeat_at=now,
    )
    session.add(lock)
    session.flush()
    return lock


def heartbeat_project_locks_for_job(session: Session, *, job_id: UUID) -> int:
    now = utcnow()
    locks = list(
        session.scalars(
            select(ProjectLock).where(ProjectLock.job_id == job_id, ProjectLock.status == "active")
        )
    )
    for lock in locks:
        lock.heartbeat_at = now
        lock.updated_at = now
    return len(locks)


def release_project_lock(session: Session, *, lock: ProjectLock, status: str = "released") -> ProjectLock:
    now = utcnow()
    lock.status = status
    lock.released_at = now
    lock.updated_at = now
    return lock


def release_project_locks_for_job(session: Session, *, job_id: UUID, status: str = "released") -> int:
    locks = list(
        session.scalars(
            select(ProjectLock).where(ProjectLock.job_id == job_id, ProjectLock.status == "active")
        )
    )
    for lock in locks:
        release_project_lock(session, lock=lock, status=status)
    return len(locks)


def project_lock_status(session: Session, *, project_id: UUID) -> dict:
    locks = active_project_locks(session, project_id=project_id)
    jobs = active_jobs_for_project(session, project_id=project_id)
    editable = not locks
    if editable:
        mode = "write_granted"
        reason = "No active project lock."
    else:
        mode = "read_only"
        first = locks[0]
        reason = f"Project locked by {first.lock_kind}"
        if first.job_id:
            reason = f"{reason} job {first.job_id}"
    return {
        "project_id": project_id,
        "editable": editable,
        "mode": mode,
        "reason": reason,
        "active_locks": locks,
        "active_jobs": [job_summary_dict(job) for job in jobs],
    }


def job_summary_dict(job: Job) -> dict:
    return {
        "id": job.id,
        "project_id": job.project_id,
        "raw_dataset_id": job.raw_dataset_id,
        "pipeline_id": job.pipeline_id,
        "execution_target_id": job.execution_target_id,
        "requested_mode": job.requested_mode,
        "resolved_mode": job.resolved_mode,
        "status": job.status,
        "priority": job.priority,
        "requested_by": job.requested_by,
        "requested_from_host": job.requested_from_host,
        "params_json": job.params_json or {},
        "result_json": job.result_json or {},
        "error_text": job.error_text,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "heartbeat_at": job.heartbeat_at,
        "finished_at": job.finished_at,
        "updated_at": job.updated_at,
    }
