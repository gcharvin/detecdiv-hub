from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text
from sqlalchemy.orm import Session, joinedload

from api.models import BackupRun, Job, Project, ProjectLocation, RawDataset, RawDatasetLocation, StorageRoot, User
from api.services.backup_settings import BackupRuntimeConfig, resolve_backup_runtime_config
from api.services.users import get_or_create_user

LOGGER = logging.getLogger("detecdiv-hub")

BACKUP_POLICY_LOCK_KEY = 2026050801


# ---------------------------------------------------------------------------
# Skip predicates
# ---------------------------------------------------------------------------

def _should_skip_raw_dataset(raw_dataset: RawDataset, config: BackupRuntimeConfig) -> str | None:
    """Return skip reason string, or None if the dataset should be backed up."""
    if raw_dataset.backup_excluded:
        return "excluded"
    if raw_dataset.lifecycle_tier == "cold" and raw_dataset.archive_status == "archived":
        return "cold_archived"
    if raw_dataset.last_backup_at is not None:
        interval = timedelta(minutes=max(1, config.backup_raw_interval_minutes))
        if datetime.now(timezone.utc) - raw_dataset.last_backup_at < interval:
            return "recent_backup"
    return None


def _should_skip_project(project: Project, config: BackupRuntimeConfig) -> str | None:
    if project.backup_excluded:
        return "excluded"
    if project.status == "deleted":
        return "deleted"
    if project.last_backup_at is not None:
        interval = timedelta(minutes=max(1, config.backup_project_interval_minutes))
        if datetime.now(timezone.utc) - project.last_backup_at < interval:
            return "recent_backup"
    return None


# ---------------------------------------------------------------------------
# Job creation helpers
# ---------------------------------------------------------------------------

def _queue_raw_dataset_backup_job(session: Session, *, raw_dataset: RawDataset, config: BackupRuntimeConfig) -> Job:
    job = Job(
        raw_dataset_id=raw_dataset.id,
        status="queued",
        priority=200,
        params_json={
            "job_kind": "backup_raw_dataset",
            "raw_dataset_id": str(raw_dataset.id),
            "backup_repo": config.backup_repo,
        },
    )
    session.add(job)
    raw_dataset.backup_status = "queued"
    session.flush()
    return job


def _queue_project_backup_job(session: Session, *, project: Project, config: BackupRuntimeConfig) -> Job:
    job = Job(
        project_id=project.id,
        status="queued",
        priority=200,
        params_json={
            "job_kind": "backup_project",
            "project_id": str(project.id),
            "backup_repo": config.backup_repo,
        },
    )
    session.add(job)
    project.backup_status = "queued"
    session.flush()
    return job


# ---------------------------------------------------------------------------
# Policy execution
# ---------------------------------------------------------------------------

@dataclass
class BackupPolicyResult:
    run: BackupRun
    raw_datasets_total: int
    raw_datasets_backed_up: int
    raw_datasets_skipped: int
    raw_datasets_failed: int
    projects_total: int
    projects_backed_up: int
    projects_skipped: int
    projects_failed: int
    queued_job_ids: list[str] = field(default_factory=list)


def execute_backup_policy_run(
    session: Session,
    *,
    config: BackupRuntimeConfig,
    triggered_by_user: User,
    trigger_mode: str,
) -> BackupPolicyResult:
    now = datetime.now(timezone.utc)
    run = BackupRun(
        triggered_by_user_id=triggered_by_user.id,
        trigger_mode=trigger_mode,
        status="running",
        config_json=config.to_json_safe(),
        started_at=now,
    )
    session.add(run)
    session.flush()

    queued_job_ids: list[str] = []
    raw_backed_up = raw_skipped = raw_failed = 0
    proj_backed_up = proj_skipped = proj_failed = 0

    try:
        # --- Raw datasets ---
        if config.backup_include_raw_datasets:
            stmt = (
                select(RawDataset)
                .options(joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root))
                .where(RawDataset.backup_status.not_in(["queued"]))
                .order_by(RawDataset.created_at.asc())
            )
            raw_datasets = list(session.scalars(stmt).unique())
            for rd in raw_datasets:
                skip_reason = _should_skip_raw_dataset(rd, config)
                if skip_reason:
                    raw_skipped += 1
                    continue
                try:
                    job = _queue_raw_dataset_backup_job(session, raw_dataset=rd, config=config)
                    queued_job_ids.append(str(job.id))
                    raw_backed_up += 1
                except Exception:
                    LOGGER.exception("Failed to queue backup job for raw dataset %s", rd.id)
                    raw_failed += 1
        else:
            raw_datasets = []

        # --- Projects ---
        if config.backup_include_projects:
            stmt = (
                select(Project)
                .options(joinedload(Project.locations).joinedload(ProjectLocation.storage_root))
                .where(Project.backup_status.not_in(["queued"]))
                .order_by(Project.created_at.asc())
            )
            projects = list(session.scalars(stmt).unique())
            for proj in projects:
                skip_reason = _should_skip_project(proj, config)
                if skip_reason:
                    proj_skipped += 1
                    continue
                try:
                    job = _queue_project_backup_job(session, project=proj, config=config)
                    queued_job_ids.append(str(job.id))
                    proj_backed_up += 1
                except Exception:
                    LOGGER.exception("Failed to queue backup job for project %s", proj.id)
                    proj_failed += 1
        else:
            projects = []

        run.status = "done"
        run.raw_datasets_total = len(raw_datasets) if config.backup_include_raw_datasets else 0
        run.raw_datasets_backed_up = raw_backed_up
        run.raw_datasets_skipped = raw_skipped
        run.raw_datasets_failed = raw_failed
        run.projects_total = len(projects) if config.backup_include_projects else 0
        run.projects_backed_up = proj_backed_up
        run.projects_skipped = proj_skipped
        run.projects_failed = proj_failed
        run.result_json = {"queued_job_ids": queued_job_ids}
        run.finished_at = datetime.now(timezone.utc)
        session.flush()

    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = datetime.now(timezone.utc)
        session.flush()
        raise

    return BackupPolicyResult(
        run=run,
        raw_datasets_total=run.raw_datasets_total,
        raw_datasets_backed_up=raw_backed_up,
        raw_datasets_skipped=raw_skipped,
        raw_datasets_failed=raw_failed,
        projects_total=run.projects_total,
        projects_backed_up=proj_backed_up,
        projects_skipped=proj_skipped,
        projects_failed=proj_failed,
        queued_job_ids=queued_job_ids,
    )


def list_backup_runs(session: Session, *, limit: int = 20) -> list[BackupRun]:
    stmt = (
        select(BackupRun)
        .options(joinedload(BackupRun.triggered_by))
        .order_by(BackupRun.created_at.desc())
        .limit(max(1, min(limit, 100)))
    )
    return list(session.scalars(stmt).unique())


def latest_backup_run_timestamp(session: Session) -> datetime | None:
    stmt = (
        select(BackupRun)
        .order_by(BackupRun.finished_at.desc().nullslast(), BackupRun.created_at.desc())
        .limit(1)
    )
    run = session.scalars(stmt).first()
    if run is None:
        return None
    return run.finished_at or run.started_at or run.created_at


def try_acquire_backup_policy_lock(session: Session) -> bool:
    return bool(
        session.execute(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": BACKUP_POLICY_LOCK_KEY}).scalar()
    )


def resolve_backup_policy_user(session: Session, *, user_key: str) -> User:
    return get_or_create_user(session, user_key=user_key, display_name=user_key, role="service")
