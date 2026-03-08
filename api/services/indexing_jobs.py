from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, joinedload

from api.db import SessionLocal
from api.models import IndexingJob, User
from api.schemas import IndexRequest
from api.services.project_indexing import ProjectIndexResult, index_project_root
from api.services.users import get_or_create_user


INDEXING_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="detecdiv-indexing")


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
        message="Queued for indexing.",
        metadata_json=payload.metadata_json or {},
    )
    session.add(job)
    session.flush()
    session.refresh(job)
    return job


def launch_indexing_job(job_id: UUID) -> None:
    INDEXING_EXECUTOR.submit(run_indexing_job, job_id)


def run_indexing_job(job_id: UUID) -> None:
    with SessionLocal() as session:
        job = session.get(IndexingJob, job_id)
        if job is None:
            return
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        job.message = "Starting project scan."
        session.commit()

    try:
        with SessionLocal() as session:
            job = session.get(IndexingJob, job_id)
            if job is None:
                return
            owner = job.owner
            owner_user_key = owner.user_key if owner is not None else "localdev"

            def on_progress(**kwargs: Any) -> None:
                job.total_projects = int(kwargs.get("total_projects", job.total_projects or 0))
                job.scanned_projects = int(kwargs.get("scanned_projects", job.scanned_projects or 0))
                job.indexed_projects = int(kwargs.get("indexed_projects", job.indexed_projects or 0))
                job.failed_projects = int(kwargs.get("failed_projects", job.failed_projects or 0))
                job.deleted_projects = int(kwargs.get("deleted_projects", job.deleted_projects or 0))
                job.current_project_path = kwargs.get("current_project_path")
                if kwargs.get("message"):
                    job.message = str(kwargs["message"])
                if kwargs.get("error_text"):
                    job.error_text = str(kwargs["error_text"])

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
            finalize_indexing_job_success(session, job, result)
            session.commit()
    except Exception as exc:  # pragma: no cover - defensive for background worker
        with SessionLocal() as session:
            job = session.get(IndexingJob, job_id)
            if job is None:
                return
            job.status = "failed"
            job.finished_at = datetime.now(timezone.utc)
            job.error_text = str(exc)
            job.message = "Indexing failed."
            session.commit()


def finalize_indexing_job_success(session: Session, job: IndexingJob, result: ProjectIndexResult) -> None:
    job.status = "completed" if result.failed_projects == 0 else "completed_with_errors"
    job.total_projects = result.total_projects
    job.scanned_projects = result.scanned_projects
    job.indexed_projects = result.indexed_projects
    job.failed_projects = result.failed_projects
    job.deleted_projects = result.deleted_projects
    job.current_project_path = None
    job.finished_at = datetime.now(timezone.utc)
    job.message = (
        f"Indexed {result.indexed_projects}/{result.total_projects} projects"
        f" from {result.root_path}."
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
    }
    session.flush()


def list_indexing_jobs_for_user(
    session: Session,
    *,
    current_user: User,
    limit: int = 20,
) -> list[IndexingJob]:
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
