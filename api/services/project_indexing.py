from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
import time

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from api.models import Project, ProjectLocation, StorageRoot, User
from api.services.project_inventory import inspect_project_directory
from api.services.storage_metrics import safe_dir_size, safe_file_size
from api.services.users import get_or_create_user


@dataclass
class ProjectIndexResult:
    root_path: str
    storage_root_name: str
    owner_user_key: str
    visibility: str
    total_projects: int
    scanned_projects: int
    indexed_projects: int
    failed_projects: int
    deleted_projects: int
    stale_cleanup_skipped: bool


def index_project_root(
    session: Session,
    *,
    root_path: str,
    storage_root_name: str | None = None,
    host_scope: str = "server",
    root_type: str = "project_root",
    owner_user_key: str,
    visibility: str = "private",
    clear_existing_for_root: bool = False,
    continue_on_error: bool = True,
    commit_each: bool = False,
    progress_callback: Callable[..., None] | None = None,
) -> ProjectIndexResult:
    root = Path(root_path).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"Project root does not exist: {root}")

    owner = get_or_create_user(session, user_key=owner_user_key, display_name=owner_user_key)
    storage_root = get_or_create_storage_root(
        session,
        root_path=str(root),
        storage_root_name=storage_root_name,
        host_scope=host_scope,
        root_type=root_type,
    )

    existing_project_ids: set = set()
    if clear_existing_for_root:
        existing_project_ids = set(
            session.scalars(
                select(ProjectLocation.project_id).where(ProjectLocation.storage_root_id == storage_root.id)
            )
        )

    seen_project_ids: set = set()
    indexed_projects = 0
    scanned_projects = 0
    failed_projects = 0
    total_projects = 0
    if progress_callback is not None:
        progress_callback(
            status="running",
            total_projects=total_projects,
            scanned_projects=0,
            indexed_projects=0,
            failed_projects=0,
            deleted_projects=0,
            current_project_path=None,
            message=f"Scanning {root} for DetecDiv projects.",
        )

    for mat_path, project_dir in iter_project_candidates(root, progress_callback=progress_callback):
        total_projects += 1
        scanned_projects += 1
        try:
            project = upsert_project_from_paths(
                session,
                owner=owner,
                visibility=visibility,
                storage_root=storage_root,
                root_path=root,
                mat_path=mat_path,
                project_dir=project_dir,
            )
            seen_project_ids.add(project.id)
            indexed_projects += 1
            if progress_callback is not None:
                progress_callback(
                    status="running",
                    phase="indexing",
                    total_projects=total_projects,
                    scanned_projects=scanned_projects,
                    indexed_projects=indexed_projects,
                    failed_projects=failed_projects,
                    deleted_projects=0,
                    mat_files_seen=total_projects,
                    current_project_path=str(mat_path),
                    message=f"Indexed {mat_path.name} ({indexed_projects} indexed so far).",
                )
            if commit_each:
                session.commit()
        except Exception as exc:
            session.rollback()
            failed_projects += 1
            if progress_callback is not None:
                progress_callback(
                    status="running",
                    phase="indexing",
                    total_projects=total_projects,
                    scanned_projects=scanned_projects,
                    indexed_projects=indexed_projects,
                    failed_projects=failed_projects,
                    deleted_projects=0,
                    mat_files_seen=total_projects,
                    current_project_path=str(mat_path),
                    message=f"Failed to index {mat_path.name}: {exc}",
                    error_text=str(exc),
                )
            if not continue_on_error:
                raise

    deleted_projects = 0
    stale_cleanup_skipped = False
    if clear_existing_for_root and existing_project_ids and failed_projects == 0:
        stale_ids = existing_project_ids - seen_project_ids
        for project_id in stale_ids:
            session.execute(
                delete(ProjectLocation).where(
                    ProjectLocation.project_id == project_id,
                    ProjectLocation.storage_root_id == storage_root.id,
                )
            )
            remaining = session.scalars(
                select(ProjectLocation.id).where(ProjectLocation.project_id == project_id)
            ).first()
            if remaining is None:
                session.execute(delete(Project).where(Project.id == project_id))
            deleted_projects += 1
        if commit_each:
            if progress_callback is not None:
                progress_callback(
                    status="running",
                    phase="cleanup",
                    total_projects=total_projects,
                    scanned_projects=scanned_projects,
                    indexed_projects=indexed_projects,
                    failed_projects=failed_projects,
                    deleted_projects=deleted_projects,
                    mat_files_seen=total_projects,
                    current_project_path=None,
                    message=f"Removed {deleted_projects} stale project rows.",
                )
            session.commit()
    elif clear_existing_for_root and failed_projects > 0:
        stale_cleanup_skipped = True
        if progress_callback is not None:
            progress_callback(
                status="running",
                phase="cleanup",
                total_projects=total_projects,
                scanned_projects=scanned_projects,
                indexed_projects=indexed_projects,
                failed_projects=failed_projects,
                deleted_projects=0,
                mat_files_seen=total_projects,
                current_project_path=None,
                message="Skipped stale-row cleanup because some projects failed to index.",
            )

    if progress_callback is not None and total_projects == 0:
        progress_callback(
            status="running",
            phase="completed",
            total_projects=0,
            scanned_projects=0,
            indexed_projects=0,
            failed_projects=0,
            deleted_projects=0,
            mat_files_seen=0,
            current_project_path=None,
            message=f"No DetecDiv project candidates found under {root}.",
        )

    return ProjectIndexResult(
        root_path=str(root),
        storage_root_name=storage_root.name,
        owner_user_key=owner.user_key,
        visibility=visibility,
        total_projects=total_projects,
        scanned_projects=scanned_projects,
        indexed_projects=indexed_projects,
        failed_projects=failed_projects,
        deleted_projects=deleted_projects,
        stale_cleanup_skipped=stale_cleanup_skipped,
    )


def iter_project_candidates(root_path: Path, progress_callback: Callable[..., None] | None = None):
    mat_files_seen = 0
    candidate_projects = 0
    last_report_at = time.monotonic()
    for mat_path in root_path.rglob("*.mat"):
        mat_files_seen += 1
        now = time.monotonic()
        if progress_callback is not None and (mat_files_seen == 1 or mat_files_seen % 200 == 0 or now - last_report_at >= 3.0):
            progress_callback(
                status="running",
                phase="discovering",
                total_projects=candidate_projects,
                scanned_projects=0,
                indexed_projects=0,
                failed_projects=0,
                deleted_projects=0,
                mat_files_seen=mat_files_seen,
                current_project_path=str(mat_path),
                message=f"Discovering DetecDiv projects under {root_path} ({mat_files_seen} .mat files inspected).",
            )
            last_report_at = now
        project_dir = mat_path.with_suffix("")
        if not project_dir.is_dir():
            continue
        if not is_detecdiv_project_dir(project_dir):
            continue
        candidate_projects += 1
        if progress_callback is not None:
            progress_callback(
                status="running",
                phase="discovering",
                total_projects=candidate_projects,
                scanned_projects=0,
                indexed_projects=0,
                failed_projects=0,
                deleted_projects=0,
                mat_files_seen=mat_files_seen,
                current_project_path=str(mat_path),
                message=f"Found {candidate_projects} DetecDiv project candidate(s) after inspecting {mat_files_seen} .mat files.",
            )
        yield mat_path.resolve(), project_dir.resolve()


def is_detecdiv_project_dir(project_dir: Path) -> bool:
    """Heuristic filter to reject image/frame folders that happen to have sibling MAT files."""
    if not project_dir.is_dir():
        return False

    top_level_dir_markers = {"pipeline", "processor", "classification", "classifier", "fov"}
    try:
        entries = list(project_dir.iterdir())
    except OSError:
        return False

    child_dir_names = {entry.name.lower() for entry in entries if entry.is_dir()}
    if child_dir_names.intersection(top_level_dir_markers):
        return True

    # Many valid projects already contain derived H5 outputs somewhere in the tree.
    try:
        if next(project_dir.rglob("*.h5"), None) is not None:
            return True
    except OSError:
        return False

    # Some projects may have run metadata without the standard top-level folders.
    try:
        if next(project_dir.rglob("run.json"), None) is not None:
            return True
    except OSError:
        return False

    # Reject typical per-image folders such as Pos*/im_* trees when they have no project markers.
    return False


def get_or_create_storage_root(
    session: Session,
    *,
    root_path: str,
    storage_root_name: str | None,
    host_scope: str,
    root_type: str,
) -> StorageRoot:
    root_name = storage_root_name or build_storage_root_name(root_path)
    root = session.scalars(select(StorageRoot).where(StorageRoot.name == root_name)).first()
    if root is None:
        root = StorageRoot(
            name=root_name,
            root_type=root_type,
            host_scope=host_scope,
            path_prefix=root_path,
        )
        session.add(root)
        session.flush()
    else:
        root.root_type = root_type
        root.host_scope = host_scope
        root.path_prefix = root_path
        session.flush()
    return root


def upsert_project_from_paths(
    session: Session,
    *,
    owner: User,
    visibility: str,
    storage_root: StorageRoot,
    root_path: Path,
    mat_path: Path,
    project_dir: Path,
) -> Project:
    relative_parent = project_relative_parent(root_path, mat_path)
    project_key = build_project_key(str(mat_path), relative_parent, mat_path.stem)
    project = session.scalars(select(Project).where(Project.project_key == project_key)).first()

    project_mat_bytes = safe_file_size(mat_path)
    project_dir_bytes = safe_dir_size(project_dir)
    total_bytes = project_mat_bytes + project_dir_bytes
    size_timestamp = datetime.now(timezone.utc)
    inventory = inspect_project_directory(project_dir)
    metadata = {
        "source": "hub_indexer",
        "project_mat_abs": str(mat_path),
        "project_dir_abs": str(project_dir),
        "project_rel_from_root": str(project_dir.relative_to(root_path)),
        "inventory": inventory.metadata_json(),
    }

    if project is None:
        project = Project(
            owner_user_id=owner.id,
            project_key=project_key,
            project_name=mat_path.stem,
            visibility=visibility,
            status="indexed",
            health_status="ok",
            fov_count=0,
            roi_count=0,
            classifier_count=inventory.classifier_count,
            processor_count=inventory.processor_count,
            pipeline_run_count=inventory.pipeline_run_count,
            available_raw_count=0,
            missing_raw_count=0,
            run_json_count=inventory.run_json_count,
            h5_count=inventory.h5_count,
            h5_bytes=inventory.h5_bytes,
            latest_run_status=inventory.latest_run_status,
            latest_run_at=inventory.latest_run_at,
            project_mat_bytes=project_mat_bytes,
            project_dir_bytes=project_dir_bytes,
            estimated_raw_bytes=0,
            total_bytes=total_bytes,
            last_size_scan_at=size_timestamp,
            metadata_json=metadata,
        )
        session.add(project)
        session.flush()
    else:
        existing_fov_count = int(project.fov_count or 0)
        existing_roi_count = int(project.roi_count or 0)
        existing_available_raw_count = int(project.available_raw_count or 0)
        existing_missing_raw_count = int(project.missing_raw_count or 0)
        merged_metadata = dict(project.metadata_json or {})
        merged_metadata.update(metadata)
        project.owner_user_id = owner.id
        project.project_name = mat_path.stem
        project.visibility = visibility
        project.status = "indexed"
        project.health_status = "raw_missing" if existing_missing_raw_count > 0 else "ok"
        project.fov_count = existing_fov_count
        project.roi_count = existing_roi_count
        project.classifier_count = inventory.classifier_count
        project.processor_count = inventory.processor_count
        project.pipeline_run_count = inventory.pipeline_run_count
        project.available_raw_count = existing_available_raw_count
        project.missing_raw_count = existing_missing_raw_count
        project.run_json_count = inventory.run_json_count
        project.h5_count = inventory.h5_count
        project.h5_bytes = inventory.h5_bytes
        project.latest_run_status = inventory.latest_run_status
        project.latest_run_at = inventory.latest_run_at
        project.project_mat_bytes = project_mat_bytes
        project.project_dir_bytes = project_dir_bytes
        project.total_bytes = total_bytes
        project.last_size_scan_at = size_timestamp
        project.metadata_json = merged_metadata
        session.flush()

    location = session.scalars(
        select(ProjectLocation).where(
            ProjectLocation.project_id == project.id,
            ProjectLocation.storage_root_id == storage_root.id,
        )
    ).first()
    if location is None:
        location = ProjectLocation(
            project_id=project.id,
            storage_root_id=storage_root.id,
            relative_path=relative_parent,
            project_file_name=mat_path.name,
            access_mode="readwrite",
            is_preferred=True,
        )
        session.add(location)
    else:
        location.relative_path = relative_parent
        location.project_file_name = mat_path.name
        location.access_mode = "readwrite"
        location.is_preferred = True

    session.flush()
    return project


def project_relative_parent(root_path: Path, mat_path: Path) -> str:
    rel_parent = mat_path.parent.relative_to(root_path)
    rel_text = str(rel_parent)
    return "" if rel_text == "." else rel_text


def build_storage_root_name(root_path: str) -> str:
    stem = Path(root_path).name or "root"
    slug = slugify(stem)
    suffix = hashlib.sha1(root_path.encode("utf-8")).hexdigest()[:8]
    return f"hub_{slug}_{suffix}"


def build_project_key(project_mat_abs: str, relative_parent: str, project_name: str) -> str:
    label = str(Path(relative_parent) / project_name) if relative_parent else project_name
    slug = slugify(label)
    suffix = hashlib.sha1(project_mat_abs.encode("utf-8")).hexdigest()[:10]
    return f"{slug}_{suffix}"


def slugify(value: str) -> str:
    out: list[str] = []
    last_was_sep = False
    for ch in value.lower():
        if ch.isalnum():
            out.append(ch)
            last_was_sep = False
        elif not last_was_sep:
            out.append("_")
            last_was_sep = True
    slug = "".join(out).strip("_")
    return slug or "item"
