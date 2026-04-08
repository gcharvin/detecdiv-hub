from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session, joinedload

from api.models import (
    Project,
    ProjectDeletionEvent,
    ProjectLocation,
    ProjectRawLink,
    RawDataset,
    RawDatasetLocation,
    User,
)
from api.services.path_resolution import compose_storage_path


@dataclass
class DeletionPreviewData:
    project: Project
    delete_project_files: bool
    delete_linked_raw_data: bool
    reclaimable_bytes: int
    preview_json: dict


def build_deletion_preview(
    session: Session,
    *,
    project: Project,
    delete_project_files: bool,
    delete_linked_raw_data: bool,
) -> DeletionPreviewData:
    project_locations = list(project.locations)
    project_file_paths = []
    project_dir_paths = []
    project_bytes = 0

    if delete_project_files:
        for location in project_locations:
            file_path, dir_path = resolve_project_location_paths(location)
            if file_path:
                project_file_paths.append(file_path)
            if dir_path:
                project_dir_paths.append(dir_path)
        project_bytes = int(project.total_bytes or 0)

    linked_raw = []
    raw_bytes = 0
    skipped_raw = []
    if delete_linked_raw_data:
        linked_raw, raw_bytes, skipped_raw = preview_linked_raw_datasets(session, project)

    reclaimable_bytes = project_bytes + raw_bytes
    preview_json = {
        "project": {
            "project_name": project.project_name,
            "project_mat_bytes": int(project.project_mat_bytes or 0),
            "project_dir_bytes": int(project.project_dir_bytes or 0),
            "total_bytes": int(project.total_bytes or 0),
            "paths": {
                "project_files": project_file_paths,
                "project_dirs": project_dir_paths,
            },
        },
        "linked_raw_datasets": linked_raw,
        "skipped_linked_raw_datasets": skipped_raw,
    }
    return DeletionPreviewData(
        project=project,
        delete_project_files=delete_project_files,
        delete_linked_raw_data=delete_linked_raw_data,
        reclaimable_bytes=reclaimable_bytes,
        preview_json=preview_json,
    )


def record_deletion_preview(
    session: Session,
    *,
    preview: DeletionPreviewData,
    requested_by_user: User,
) -> ProjectDeletionEvent:
    event = ProjectDeletionEvent(
        project_id=preview.project.id,
        requested_by_user_id=requested_by_user.id,
        status="previewed",
        delete_project_files=preview.delete_project_files,
        delete_linked_raw_data=preview.delete_linked_raw_data,
        reclaimable_bytes=preview.reclaimable_bytes,
        preview_json=preview.preview_json,
        result_json={},
    )
    session.add(event)
    session.flush()
    return event


def execute_project_deletion(
    session: Session,
    *,
    preview: DeletionPreviewData,
    requested_by_user: User,
) -> ProjectDeletionEvent:
    event = record_deletion_preview(session, preview=preview, requested_by_user=requested_by_user)
    deleted_project_files = []
    deleted_project_dirs = []
    deleted_raw_locations = []
    deleted_raw_datasets = []
    errors = []

    if preview.delete_project_files:
        for file_path in preview.preview_json["project"]["paths"]["project_files"]:
            try:
                path = Path(file_path)
                if path.is_file():
                    path.unlink()
                    deleted_project_files.append(file_path)
            except OSError as exc:
                errors.append(f"Failed to delete file {file_path}: {exc}")

        for dir_path in preview.preview_json["project"]["paths"]["project_dirs"]:
            try:
                path = Path(dir_path)
                if path.is_dir():
                    shutil.rmtree(path)
                    deleted_project_dirs.append(dir_path)
            except OSError as exc:
                errors.append(f"Failed to delete folder {dir_path}: {exc}")

    if preview.delete_linked_raw_data:
        raw_section = preview.preview_json.get("linked_raw_datasets", [])
        for raw_item in raw_section:
            raw_dataset = session.scalars(
                select(RawDataset)
                .options(joinedload(RawDataset.locations))
                .where(RawDataset.id == raw_item["raw_dataset_id"])
            ).first()
            if raw_dataset is None:
                continue
            for location in list(raw_dataset.locations):
                raw_path = resolve_raw_location_path(location)
                try:
                    if raw_path.is_file():
                        raw_path.unlink()
                    elif raw_path.is_dir():
                        shutil.rmtree(raw_path)
                    deleted_raw_locations.append(str(raw_path))
                except OSError as exc:
                    errors.append(f"Failed to delete raw location {raw_path}: {exc}")
            deleted_raw_datasets.append(str(raw_dataset.id))
            session.execute(delete(RawDataset).where(RawDataset.id == raw_dataset.id))

    preview.project.status = "deleted"
    preview.project.health_status = "deleted"
    preview.project.visibility = "private"
    preview.project.metadata_json = {
        **(preview.project.metadata_json or {}),
        "deleted_at": datetime.now(timezone.utc).isoformat(),
    }
    event.status = "deleted" if not errors else "partial_failed"
    event.result_json = {
        "deleted_project_files": deleted_project_files,
        "deleted_project_dirs": deleted_project_dirs,
        "deleted_raw_locations": deleted_raw_locations,
        "deleted_raw_datasets": deleted_raw_datasets,
        "errors": errors,
    }
    event.executed_at = datetime.now(timezone.utc)
    session.flush()
    return event


def resolve_project_location_paths(location: ProjectLocation) -> tuple[str, str]:
    root = location.storage_root.path_prefix
    rel = location.relative_path or ""
    file_name = location.project_file_name or ""
    file_path = compose_storage_path(root, rel, file_name) if file_name else ""
    project_dir = ""
    if file_name:
        stem = Path(file_name).stem
        project_dir = compose_storage_path(root, rel, stem)
    return file_path, project_dir


def resolve_raw_location_path(location: RawDatasetLocation) -> Path:
    return Path(location.storage_root.path_prefix) / (location.relative_path or "")


def preview_linked_raw_datasets(session: Session, project: Project) -> tuple[list[dict], int, list[dict]]:
    links = list(
        session.scalars(
            select(ProjectRawLink).where(ProjectRawLink.project_id == project.id).order_by(ProjectRawLink.id.asc())
        )
    )
    linked_raw = []
    skipped = []
    total_bytes = 0
    for link in links:
        raw_dataset = session.scalars(
            select(RawDataset)
            .options(joinedload(RawDataset.locations))
            .where(RawDataset.id == link.raw_dataset_id)
        ).first()
        if raw_dataset is None:
            continue

        other_links = session.scalar(
            select(func.count(ProjectRawLink.id)).where(
                ProjectRawLink.raw_dataset_id == raw_dataset.id,
                ProjectRawLink.project_id != project.id,
            )
        )
        locations = [str(resolve_raw_location_path(loc)) for loc in raw_dataset.locations]
        raw_info = {
            "raw_dataset_id": str(raw_dataset.id),
            "acquisition_label": raw_dataset.acquisition_label,
            "total_bytes": int(raw_dataset.total_bytes or 0),
            "locations": locations,
        }
        if other_links and other_links > 0:
            raw_info["reason"] = "linked_to_other_projects"
            skipped.append(raw_info)
            continue

        linked_raw.append(raw_info)
        total_bytes += int(raw_dataset.total_bytes or 0)
    return linked_raw, total_bytes, skipped
