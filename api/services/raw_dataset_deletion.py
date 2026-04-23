from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload

from api.models import Project, ProjectLocation, ProjectRawLink, RawDataset
from api.services.project_deletion import build_deletion_preview, execute_project_deletion


@dataclass
class RawDatasetDeletionPreviewData:
    raw_dataset: RawDataset
    delete_source_files: bool
    delete_linked_projects: bool
    delete_linked_project_files: bool
    reclaimable_bytes: int
    preview_json: dict


def build_raw_dataset_deletion_preview(
    session: Session,
    *,
    raw_dataset: RawDataset,
    delete_source_files: bool,
    delete_linked_projects: bool,
    delete_linked_project_files: bool,
) -> RawDatasetDeletionPreviewData:
    source_locations = []
    source_bytes = 0
    if delete_source_files:
        source_locations = list_raw_source_locations(raw_dataset)
        source_bytes = int(raw_dataset.total_bytes or 0)

    linked_projects = []
    skipped_projects = []
    project_bytes = 0
    if delete_linked_projects:
        linked_projects, skipped_projects, project_bytes = preview_linked_projects_for_raw_dataset(
            session,
            raw_dataset=raw_dataset,
            delete_linked_project_files=delete_linked_project_files,
        )

    reclaimable_bytes = source_bytes + project_bytes
    preview_json = {
        "raw_dataset": {
            "raw_dataset_id": str(raw_dataset.id),
            "acquisition_label": raw_dataset.acquisition_label,
            "total_bytes": int(raw_dataset.total_bytes or 0),
            "locations": source_locations,
        },
        "linked_projects": linked_projects,
        "skipped_linked_projects": skipped_projects,
    }
    return RawDatasetDeletionPreviewData(
        raw_dataset=raw_dataset,
        delete_source_files=delete_source_files,
        delete_linked_projects=delete_linked_projects,
        delete_linked_project_files=delete_linked_project_files,
        reclaimable_bytes=reclaimable_bytes,
        preview_json=preview_json,
    )


def execute_raw_dataset_deletion(
    session: Session,
    *,
    preview: RawDatasetDeletionPreviewData,
    requested_by_user,
) -> dict:
    deleted_source_locations: list[str] = []
    deleted_projects: list[str] = []
    deleted_project_events: list[str] = []
    errors: list[str] = []

    if preview.delete_source_files:
        for location in preview.preview_json.get("raw_dataset", {}).get("locations", []):
            try:
                path = Path(location)
                if path.is_file():
                    path.unlink()
                    deleted_source_locations.append(str(path))
                elif path.is_dir():
                    shutil.rmtree(path)
                    deleted_source_locations.append(str(path))
            except OSError as exc:
                errors.append(f"Failed to delete raw source location {location}: {exc}")

    if preview.delete_linked_projects:
        for entry in preview.preview_json.get("linked_projects", []):
            project_id = entry.get("project_id")
            if not project_id:
                continue
            project = session.scalars(
                select(Project)
                .options(
                    joinedload(Project.locations).joinedload(ProjectLocation.storage_root),
                    joinedload(Project.raw_links),
                )
                .where(Project.id == UUID(str(project_id)), Project.status != "deleted")
            ).first()
            if project is None:
                continue
            try:
                project_preview = build_deletion_preview(
                    session,
                    project=project,
                    delete_project_files=preview.delete_linked_project_files,
                    delete_linked_raw_data=False,
                )
                event = execute_project_deletion(session, preview=project_preview, requested_by_user=requested_by_user)
                deleted_projects.append(str(project.id))
                deleted_project_events.append(str(event.id))
            except Exception as exc:  # pragma: no cover - defensive around mixed deletes
                errors.append(f"Failed to delete linked project {project.project_name} ({project.id}): {exc}")

    raw_dataset_id = str(preview.raw_dataset.id)
    try:
        raw_obj = session.get(RawDataset, preview.raw_dataset.id)
        if raw_obj is not None:
            session.delete(raw_obj)
    except Exception as exc:  # pragma: no cover - defensive
        errors.append(f"Failed to delete raw dataset row {raw_dataset_id}: {exc}")

    status = "deleted" if not errors else "partial_failed"
    result_json = {
        "deleted_source_locations": deleted_source_locations,
        "deleted_projects": deleted_projects,
        "deleted_project_events": deleted_project_events,
        "errors": errors,
        "deleted_at": datetime.now(timezone.utc).isoformat(),
    }
    return {
        "raw_dataset_id": raw_dataset_id,
        "status": status,
        "reclaimable_bytes": int(preview.reclaimable_bytes or 0),
        "result_json": result_json,
    }


def list_raw_source_locations(raw_dataset: RawDataset) -> list[str]:
    paths: list[str] = []
    for location in raw_dataset.locations or []:
        if location.storage_root is None:
            continue
        root = str(location.storage_root.path_prefix or "").rstrip("\\/")
        rel = str(location.relative_path or "").lstrip("\\/")
        if not root:
            continue
        joined = str(Path(root) / rel) if rel else root
        paths.append(joined)
    return paths


def preview_linked_projects_for_raw_dataset(
    session: Session,
    *,
    raw_dataset: RawDataset,
    delete_linked_project_files: bool,
) -> tuple[list[dict], list[dict], int]:
    links = list(
        session.scalars(
            select(ProjectRawLink).where(ProjectRawLink.raw_dataset_id == raw_dataset.id).order_by(ProjectRawLink.id.asc())
        )
    )
    linked: list[dict] = []
    skipped: list[dict] = []
    project_bytes = 0
    for link in links:
        project = session.scalars(
            select(Project)
            .options(joinedload(Project.locations).joinedload(ProjectLocation.storage_root))
            .where(Project.id == link.project_id, Project.status != "deleted")
        ).first()
        if project is None:
            continue

        other_raw_links = session.scalar(
            select(func.count(ProjectRawLink.id)).where(
                ProjectRawLink.project_id == project.id,
                ProjectRawLink.raw_dataset_id != raw_dataset.id,
            )
        )
        info = {
            "project_id": str(project.id),
            "project_name": project.project_name,
            "total_bytes": int(project.total_bytes or 0),
            "project_mat_bytes": int(project.project_mat_bytes or 0),
            "project_dir_bytes": int(project.project_dir_bytes or 0),
        }
        if other_raw_links and other_raw_links > 0:
            info["reason"] = "linked_to_other_raw_datasets"
            skipped.append(info)
            continue

        linked.append(info)
        if delete_linked_project_files:
            project_bytes += int(project.total_bytes or 0)
    return linked, skipped, project_bytes
