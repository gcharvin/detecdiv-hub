from __future__ import annotations

import logging
import os
import socket
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.models import BackupSnapshot, Job, Project, ProjectLocation, RawDataset, RawDatasetLocation
from api.services.backup import (
    ResticError,
    backup,
    ensure_repo_initialized,
    project_tags,
    raw_dataset_tags,
    restore,
)
from api.services.backup_settings import resolve_backup_runtime_config
from api.services.raw_dataset_lifecycle import pick_preferred_raw_location, resolve_raw_location_path

LOGGER = logging.getLogger("detecdiv-hub-worker")

BACKUP_JOB_KINDS = {
    "init_backup_repo",
    "backup_raw_dataset",
    "backup_project",
    "restore_raw_dataset_from_backup",
    "restore_project_from_backup",
    "list_snapshot_dir",
}


def execute_backup_job(session: Session, *, job: Job) -> dict:
    job_kind = (job.params_json or {}).get("job_kind")
    if job_kind == "init_backup_repo":
        return _execute_init_backup_repo(session, job=job)
    if job_kind == "backup_raw_dataset":
        return _execute_backup_raw_dataset(session, job=job)
    if job_kind == "backup_project":
        return _execute_backup_project(session, job=job)
    if job_kind == "restore_raw_dataset_from_backup":
        return _execute_restore_raw_dataset(session, job=job)
    if job_kind == "restore_project_from_backup":
        return _execute_restore_project(session, job=job)
    if job_kind == "list_snapshot_dir":
        return _execute_list_snapshot_dir(session, job=job)
    raise ValueError(f"Unknown backup job kind: {job_kind}")


def finalize_backup_failure(session: Session, *, job: Job, error_text: str) -> None:
    job_kind = (job.params_json or {}).get("job_kind")
    if job_kind == "backup_raw_dataset":
        raw_dataset = _load_raw_dataset(session, job=job)
        if raw_dataset and raw_dataset.backup_status == "queued":
            raw_dataset.backup_status = "failed"
    elif job_kind == "backup_project":
        project = _load_project(session, job=job)
        if project and project.backup_status == "queued":
            project.backup_status = "failed"


# ---------------------------------------------------------------------------
# Repo init
# ---------------------------------------------------------------------------

def _execute_init_backup_repo(session: Session, *, job: Job) -> dict:
    config = resolve_backup_runtime_config(session)
    LOGGER.info("Initializing backup repo at %s", config.backup_repo)
    ensure_repo_initialized(config.backup_repo, config.backup_passphrase)
    return {
        "job_kind": "init_backup_repo",
        "backup_repo": config.backup_repo,
        "worker_host": socket.gethostname(),
    }


# ---------------------------------------------------------------------------
# Raw dataset backup
# ---------------------------------------------------------------------------

def _execute_backup_raw_dataset(session: Session, *, job: Job) -> dict:
    config = resolve_backup_runtime_config(session)
    raw_dataset = _load_raw_dataset(session, job=job)
    if raw_dataset is None:
        raise ValueError(f"Raw dataset {job.raw_dataset_id} not found")

    ensure_repo_initialized(config.backup_repo, config.backup_passphrase)

    location = pick_preferred_raw_location(raw_dataset)
    source_path = resolve_raw_location_path(location)
    if not source_path.exists():
        raise FileNotFoundError(f"Raw dataset path does not exist: {source_path}")

    tags = raw_dataset_tags(str(raw_dataset.id))
    LOGGER.info("Backing up raw dataset %s from %s", raw_dataset.id, source_path)
    summary = backup(
        repo=config.backup_repo,
        passphrase=config.backup_passphrase,
        source_paths=[str(source_path)],
        tags=tags,
    )

    snapshot_id = summary.get("snapshot_id", "")
    now = datetime.now(timezone.utc)
    raw_dataset.backup_status = "backed_up"
    raw_dataset.last_backup_at = now
    raw_dataset.backup_snapshot_id = snapshot_id
    session.add(BackupSnapshot(
        snapshot_id=snapshot_id,
        time=now,
        hostname=socket.gethostname(),
        tags=tags,
        paths=[str(source_path)],
        raw_dataset_id=raw_dataset.id,
    ))
    session.flush()

    return {
        "job_kind": "backup_raw_dataset",
        "raw_dataset_id": str(raw_dataset.id),
        "source_path": str(source_path),
        "snapshot_id": snapshot_id,
        "backup_repo": config.backup_repo,
        "worker_host": socket.gethostname(),
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# Project backup
# ---------------------------------------------------------------------------

def _execute_backup_project(session: Session, *, job: Job) -> dict:
    config = resolve_backup_runtime_config(session)
    project = _load_project(session, job=job)
    if project is None:
        raise ValueError(f"Project {job.project_id} not found")

    ensure_repo_initialized(config.backup_repo, config.backup_passphrase)

    location = _pick_preferred_project_location(project)
    if location is None:
        raise ValueError(f"Project {project.id} has no storage location")
    source_path = Path(location.storage_root.path_prefix) / (location.relative_path or "")
    if not source_path.exists():
        raise FileNotFoundError(f"Project path does not exist: {source_path}")

    # Scope the backup to only project-specific files.
    # source_path is stored in the snapshot for file-browser browsing.
    # backup_paths are the actual paths passed to restic.
    exclude_patterns: list[str] | None = None
    if location.project_file_name:
        stem = Path(location.project_file_name).stem
        if (source_path / stem).is_dir():
            # Modern project: source_path is the projects container.
            # Back up only the .mat file and its adjacent directory.
            backup_paths = [
                str(source_path / location.project_file_name),
                str(source_path / stem),
            ]
        else:
            # Legacy project: source_path IS the project directory.
            # Back up the whole dir but exclude posN subdirs (raw datasets).
            backup_paths = [str(source_path)]
            project_name = source_path.name
            exclude_patterns = [f"{project_name}-pos*"]
    else:
        backup_paths = [str(source_path)]

    tags = project_tags(str(project.id))
    LOGGER.info(
        "Backing up project %s: paths=%s exclude=%s",
        project.id, backup_paths, exclude_patterns or "none",
    )
    summary = backup(
        repo=config.backup_repo,
        passphrase=config.backup_passphrase,
        source_paths=backup_paths,
        tags=tags,
        exclude_patterns=exclude_patterns,
    )

    snapshot_id = summary.get("snapshot_id", "")
    now = datetime.now(timezone.utc)
    project.backup_status = "backed_up"
    project.last_backup_at = now
    project.backup_snapshot_id = snapshot_id
    session.add(BackupSnapshot(
        snapshot_id=snapshot_id,
        time=now,
        hostname=socket.gethostname(),
        tags=tags,
        paths=[str(source_path)],
        project_id=project.id,
    ))
    session.flush()

    return {
        "job_kind": "backup_project",
        "project_id": str(project.id),
        "source_path": str(source_path),
        "snapshot_id": snapshot_id,
        "backup_repo": config.backup_repo,
        "worker_host": socket.gethostname(),
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# Restore — raw dataset
# ---------------------------------------------------------------------------

def _execute_restore_raw_dataset(session: Session, *, job: Job) -> dict:
    config = resolve_backup_runtime_config(session)
    params = job.params_json or {}
    snapshot_id = params.get("snapshot_id", "")
    target_dir = params.get("target_dir", "")
    include_paths: list[str] = params.get("include_paths") or []

    if not snapshot_id:
        raise ValueError("restore_raw_dataset_from_backup: missing snapshot_id")
    if not target_dir:
        raise ValueError("restore_raw_dataset_from_backup: missing target_dir")

    LOGGER.info("Restoring raw dataset snapshot %s to %s", snapshot_id, target_dir)
    output = restore(
        repo=config.backup_repo,
        passphrase=config.backup_passphrase,
        snapshot_id=snapshot_id,
        target_dir=target_dir,
        include_paths=include_paths or None,
    )
    return {
        "job_kind": "restore_raw_dataset_from_backup",
        "raw_dataset_id": str(job.raw_dataset_id),
        "snapshot_id": snapshot_id,
        "target_dir": target_dir,
        "include_paths": include_paths,
        "worker_host": socket.gethostname(),
        "restic_output": output[:2000] if output else "",
    }


# ---------------------------------------------------------------------------
# Restore — project files
# ---------------------------------------------------------------------------

def _execute_restore_project(session: Session, *, job: Job) -> dict:
    config = resolve_backup_runtime_config(session)
    params = job.params_json or {}
    snapshot_id = params.get("snapshot_id", "")
    target_dir = params.get("target_dir", "")
    include_paths: list[str] = params.get("include_paths") or []

    if not snapshot_id:
        raise ValueError("restore_project_from_backup: missing snapshot_id")
    if not target_dir:
        raise ValueError("restore_project_from_backup: missing target_dir")

    LOGGER.info("Restoring project snapshot %s to %s (files: %s)", snapshot_id, target_dir, include_paths or "all")
    output = restore(
        repo=config.backup_repo,
        passphrase=config.backup_passphrase,
        snapshot_id=snapshot_id,
        target_dir=target_dir,
        include_paths=include_paths or None,
    )
    return {
        "job_kind": "restore_project_from_backup",
        "project_id": str(job.project_id),
        "snapshot_id": snapshot_id,
        "target_dir": target_dir,
        "include_paths": include_paths,
        "worker_host": socket.gethostname(),
        "restic_output": output[:2000] if output else "",
    }


# ---------------------------------------------------------------------------
# Snapshot directory listing via FUSE mount
# ---------------------------------------------------------------------------

def _execute_list_snapshot_dir(session: Session, *, job: Job) -> dict:
    params = job.params_json or {}
    mount_path = params.get("mount_path", "").rstrip("/")
    snapshot_id = params.get("snapshot_id", "")
    source_path = params.get("source_path", "").lstrip("/")   # e.g. "data/projects/Foo"
    browse_path = params.get("browse_path", "").strip("/")     # relative, e.g. "Results"

    if not mount_path:
        raise ValueError("list_snapshot_dir: mount_path not configured")
    if not snapshot_id:
        raise ValueError("list_snapshot_dir: snapshot_id missing")

    # restic FUSE mount exposes short IDs (8 chars) under ids/
    short_id = snapshot_id[:8]
    full_path = os.path.join(mount_path, "ids", short_id, source_path, browse_path)
    LOGGER.info("Listing snapshot dir: %s", full_path)

    if not os.path.isdir(full_path):
        raise FileNotFoundError(f"Directory not found in mount: {full_path}")

    entries = []
    for name in sorted(os.listdir(full_path), key=lambda n: (not os.path.isdir(os.path.join(full_path, n)), n.lower())):
        entry_path = os.path.join(full_path, name)
        try:
            st = os.stat(entry_path)
            is_dir = os.path.isdir(entry_path)
            entries.append({
                "name": name,
                "type": "dir" if is_dir else "file",
                "size": 0 if is_dir else st.st_size,
                "mtime": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
            })
        except OSError:
            continue

    return {
        "job_kind": "list_snapshot_dir",
        "browse_path": browse_path,
        "source_path": "/" + source_path,
        "entries": entries,
        "worker_host": socket.gethostname(),
    }


# ---------------------------------------------------------------------------
# DB loaders
# ---------------------------------------------------------------------------

def _load_raw_dataset(session: Session, *, job: Job) -> RawDataset | None:
    if job.raw_dataset_id is None:
        return None
    stmt = (
        select(RawDataset)
        .options(joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root))
        .where(RawDataset.id == job.raw_dataset_id)
    )
    return session.scalars(stmt).unique().first()


def _load_project(session: Session, *, job: Job) -> Project | None:
    if job.project_id is None:
        return None
    stmt = (
        select(Project)
        .options(joinedload(Project.locations).joinedload(ProjectLocation.storage_root))
        .where(Project.id == job.project_id)
    )
    return session.scalars(stmt).unique().first()


def _pick_preferred_project_location(project: Project) -> ProjectLocation | None:
    if not project.locations:
        return None
    preferred = next((loc for loc in project.locations if loc.is_preferred), None)
    return preferred or project.locations[0]
