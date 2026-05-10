from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import BackupRun, BackupSnapshot, Job, Project, ProjectLocation, RawDataset, RawDatasetLocation, User
from api.services.backup import (
    ResticError,
    list_files,
    project_tags,
    raw_dataset_tags,
    snapshots,
)
from api.services.backup_policy import (
    execute_backup_policy_run,
    list_backup_runs,
    resolve_backup_policy_user,
)
from api.services.backup_settings import resolve_backup_runtime_config, update_backup_runtime_config
from api.services.users import can_access_admin_portal, get_current_user


def _require_admin(current_user: User) -> None:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required.")


def _config_to_response(config) -> BackupSettingsResponse:
    return BackupSettingsResponse(
        backup_repo=config.backup_repo,
        backup_passphrase_set=bool(config.backup_passphrase),
        backup_enabled=config.backup_enabled,
        backup_scheduler_interval_minutes=config.backup_scheduler_interval_minutes,
        backup_raw_interval_minutes=config.backup_raw_interval_minutes,
        backup_project_interval_minutes=config.backup_project_interval_minutes,
        backup_run_as_user_key=config.backup_run_as_user_key,
        backup_include_raw_datasets=config.backup_include_raw_datasets,
        backup_include_projects=config.backup_include_projects,
    )

router = APIRouter(prefix="/backup", tags=["backup"])


# ---------------------------------------------------------------------------
# Pydantic schemas (local to this module)
# ---------------------------------------------------------------------------

class BackupSettingsResponse(BaseModel):
    backup_repo: str
    backup_passphrase_set: bool
    backup_enabled: bool
    backup_scheduler_interval_minutes: int
    backup_raw_interval_minutes: int
    backup_project_interval_minutes: int
    backup_run_as_user_key: str
    backup_include_raw_datasets: bool
    backup_include_projects: bool


class BackupSettingsUpdate(BaseModel):
    backup_repo: str | None = None
    backup_passphrase: str | None = None
    backup_enabled: bool | None = None
    backup_scheduler_interval_minutes: int | None = None
    backup_raw_interval_minutes: int | None = None
    backup_project_interval_minutes: int | None = None
    backup_run_as_user_key: str | None = None
    backup_include_raw_datasets: bool | None = None
    backup_include_projects: bool | None = None


class BackupRunResponse(BaseModel):
    id: str
    trigger_mode: str
    status: str
    triggered_by: str | None
    raw_datasets_total: int
    raw_datasets_backed_up: int
    raw_datasets_skipped: int
    raw_datasets_failed: int
    projects_total: int
    projects_backed_up: int
    projects_skipped: int
    projects_failed: int
    total_bytes_backed_up: int
    error_text: str | None
    started_at: str | None
    finished_at: str | None
    created_at: str


class BackupRunTriggerResponse(BaseModel):
    raw_datasets_queued: int
    projects_queued: int
    run_id: str


class SnapshotResponse(BaseModel):
    snapshot_id: str
    time: str
    hostname: str
    tags: list[str]
    paths: list[str]


class FileEntryResponse(BaseModel):
    path: str
    kind: str
    size: int
    mtime: str


class RestoreRequest(BaseModel):
    target_dir: str
    include_paths: list[str] | None = None


class BackupExcludeUpdate(BaseModel):
    backup_excluded: bool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_to_response(run: BackupRun) -> BackupRunResponse:
    return BackupRunResponse(
        id=str(run.id),
        trigger_mode=run.trigger_mode,
        status=run.status,
        triggered_by=(run.triggered_by.display_name if run.triggered_by else None),
        raw_datasets_total=run.raw_datasets_total,
        raw_datasets_backed_up=run.raw_datasets_backed_up,
        raw_datasets_skipped=run.raw_datasets_skipped,
        raw_datasets_failed=run.raw_datasets_failed,
        projects_total=run.projects_total,
        projects_backed_up=run.projects_backed_up,
        projects_skipped=run.projects_skipped,
        projects_failed=run.projects_failed,
        total_bytes_backed_up=run.total_bytes_backed_up,
        error_text=run.error_text,
        started_at=run.started_at.isoformat() if run.started_at else None,
        finished_at=run.finished_at.isoformat() if run.finished_at else None,
        created_at=run.created_at.isoformat(),
    )


def _load_config_and_repo(session: Session):
    config = resolve_backup_runtime_config(session)
    if not config.backup_repo:
        raise HTTPException(status_code=400, detail="Backup repo path is not configured.")
    if not config.backup_passphrase:
        raise HTTPException(status_code=400, detail="Backup passphrase is not configured.")
    return config


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@router.get("/settings", response_model=BackupSettingsResponse)
def get_backup_settings(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    config = resolve_backup_runtime_config(db)
    return _config_to_response(config)


@router.patch("/settings", response_model=BackupSettingsResponse)
def update_backup_settings(
    body: BackupSettingsUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    config = update_backup_runtime_config(db, updates=updates)
    db.commit()
    return _config_to_response(config)


@router.post("/init-repo", status_code=202)
def init_backup_repo(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    _load_config_and_repo(db)
    job = Job(
        status="queued",
        priority=200,
        requested_by=current_user.user_key,
        params_json={"job_kind": "init_backup_repo"},
    )
    db.add(job)
    db.commit()
    return {"job_id": str(job.id), "detail": "Repo init job queued."}


# ---------------------------------------------------------------------------
# Manual backup run
# ---------------------------------------------------------------------------

@router.post("/run", response_model=BackupRunTriggerResponse)
def trigger_backup_run(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    config = _load_config_and_repo(db)
    result = execute_backup_policy_run(
        db,
        config=config,
        triggered_by_user=current_user,
        trigger_mode="manual",
    )
    return BackupRunTriggerResponse(
        raw_datasets_queued=result.raw_datasets_backed_up,
        projects_queued=result.projects_backed_up,
        run_id=str(result.run.id),
    )


# ---------------------------------------------------------------------------
# Backup run log
# ---------------------------------------------------------------------------

@router.get("/runs", response_model=list[BackupRunResponse])
def get_backup_runs(
    limit: int = 20,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    runs = list_backup_runs(db, limit=limit)
    return [_run_to_response(r) for r in runs]


# ---------------------------------------------------------------------------
# Global snapshots list
# ---------------------------------------------------------------------------

@router.get("/snapshots", response_model=list[SnapshotResponse])
def get_all_snapshots(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    rows = db.execute(
        select(BackupSnapshot).order_by(BackupSnapshot.time.desc()).limit(200)
    ).scalars().all()
    return [
        SnapshotResponse(
            snapshot_id=r.snapshot_id,
            time=r.time.isoformat(),
            hostname=r.hostname,
            tags=r.tags,
            paths=r.paths,
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Global file browser (any snapshot by ID)
# ---------------------------------------------------------------------------

@router.get("/snapshots/{snapshot_id}/files", response_model=list[FileEntryResponse])
def browse_any_snapshot(
    snapshot_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    config = _load_config_and_repo(db)
    try:
        entries = list_files(repo=config.backup_repo, passphrase=config.backup_passphrase, snapshot_id=snapshot_id)
    except ResticError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return [FileEntryResponse(path=e.path, kind=e.kind, size=e.size, mtime=e.mtime) for e in entries]


# ---------------------------------------------------------------------------
# Raw dataset backup endpoints
# ---------------------------------------------------------------------------

@router.get("/raw-datasets/{raw_dataset_id}/snapshots", response_model=list[SnapshotResponse])
def get_raw_dataset_snapshots(
    raw_dataset_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    rows = db.execute(
        select(BackupSnapshot)
        .where(BackupSnapshot.raw_dataset_id == raw_dataset_id)
        .order_by(BackupSnapshot.time.desc())
    ).scalars().all()
    return [
        SnapshotResponse(snapshot_id=r.snapshot_id, time=r.time.isoformat(), hostname=r.hostname, tags=r.tags, paths=r.paths)
        for r in rows
    ]


@router.get("/raw-datasets/{raw_dataset_id}/snapshots/{snapshot_id}/files", response_model=list[FileEntryResponse])
def browse_raw_dataset_snapshot(
    raw_dataset_id: UUID,
    snapshot_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    config = _load_config_and_repo(db)
    try:
        entries = list_files(repo=config.backup_repo, passphrase=config.backup_passphrase, snapshot_id=snapshot_id)
    except ResticError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return [FileEntryResponse(path=e.path, kind=e.kind, size=e.size, mtime=e.mtime) for e in entries]


@router.post("/raw-datasets/{raw_dataset_id}/snapshots/{snapshot_id}/restore", status_code=202)
def restore_raw_dataset_from_snapshot(
    raw_dataset_id: UUID,
    snapshot_id: str,
    body: RestoreRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    job = Job(
        raw_dataset_id=raw_dataset_id,
        status="queued",
        priority=50,
        requested_by=current_user.user_key,
        params_json={
            "job_kind": "restore_raw_dataset_from_backup",
            "raw_dataset_id": str(raw_dataset_id),
            "snapshot_id": snapshot_id,
            "target_dir": body.target_dir,
            "include_paths": body.include_paths or [],
        },
    )
    db.add(job)
    db.commit()
    return {"job_id": str(job.id), "detail": "Restore job queued."}


@router.patch("/raw-datasets/{raw_dataset_id}/backup-settings")
def update_raw_dataset_backup_settings(
    raw_dataset_id: UUID,
    body: BackupExcludeUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    rd = db.get(RawDataset, raw_dataset_id)
    if rd is None:
        raise HTTPException(status_code=404, detail="Raw dataset not found")
    rd.backup_excluded = body.backup_excluded
    db.commit()
    return {"backup_excluded": rd.backup_excluded}


@router.post("/raw-datasets/{raw_dataset_id}/backup-now", status_code=202)
def backup_raw_dataset_now(
    raw_dataset_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    rd = db.get(RawDataset, raw_dataset_id)
    if rd is None:
        raise HTTPException(status_code=404, detail="Raw dataset not found")
    config = _load_config_and_repo(db)
    job = Job(
        raw_dataset_id=raw_dataset_id,
        status="queued",
        priority=200,
        requested_by=current_user.user_key,
        params_json={
            "job_kind": "backup_raw_dataset",
            "raw_dataset_id": str(raw_dataset_id),
            "backup_repo": config.backup_repo,
            "force": True,
        },
    )
    db.add(job)
    rd.backup_status = "queued"
    db.commit()
    return {"job_id": str(job.id), "detail": "Backup job queued."}


# ---------------------------------------------------------------------------
# Project backup endpoints
# ---------------------------------------------------------------------------

@router.get("/projects/{project_id}/snapshots", response_model=list[SnapshotResponse])
def get_project_snapshots(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    rows = db.execute(
        select(BackupSnapshot)
        .where(BackupSnapshot.project_id == project_id)
        .order_by(BackupSnapshot.time.desc())
    ).scalars().all()
    return [
        SnapshotResponse(snapshot_id=r.snapshot_id, time=r.time.isoformat(), hostname=r.hostname, tags=r.tags, paths=r.paths)
        for r in rows
    ]


@router.get("/projects/{project_id}/snapshots/{snapshot_id}/files", response_model=list[FileEntryResponse])
def browse_project_snapshot(
    project_id: UUID,
    snapshot_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    config = _load_config_and_repo(db)
    try:
        entries = list_files(repo=config.backup_repo, passphrase=config.backup_passphrase, snapshot_id=snapshot_id)
    except ResticError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return [FileEntryResponse(path=e.path, kind=e.kind, size=e.size, mtime=e.mtime) for e in entries]


@router.post("/projects/{project_id}/snapshots/{snapshot_id}/restore", status_code=202)
def restore_project_from_snapshot(
    project_id: UUID,
    snapshot_id: str,
    body: RestoreRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    job = Job(
        project_id=project_id,
        status="queued",
        priority=50,
        requested_by=current_user.user_key,
        params_json={
            "job_kind": "restore_project_from_backup",
            "project_id": str(project_id),
            "snapshot_id": snapshot_id,
            "target_dir": body.target_dir,
            "include_paths": body.include_paths or [],
        },
    )
    db.add(job)
    db.commit()
    return {"job_id": str(job.id), "detail": "Restore job queued."}


@router.post("/projects/{project_id}/backup-now", status_code=202)
def backup_project_now(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    project = db.get(Project, project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    config = _load_config_and_repo(db)
    job = Job(
        project_id=project_id,
        status="queued",
        priority=200,
        requested_by=current_user.user_key,
        params_json={
            "job_kind": "backup_project",
            "project_id": str(project_id),
            "backup_repo": config.backup_repo,
            "force": True,
        },
    )
    db.add(job)
    project.backup_status = "queued"
    db.commit()
    return {"job_id": str(job.id), "detail": "Backup job queued."}


@router.patch("/projects/{project_id}/backup-settings")
def update_project_backup_settings(
    project_id: UUID,
    body: BackupExcludeUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(current_user)
    project = db.get(Project, project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    project.backup_excluded = body.backup_excluded
    db.commit()
    return {"backup_excluded": project.backup_excluded}
