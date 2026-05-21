from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload

from api.models import Job, Project, ProjectRawLink, RawDataset, RawDatasetLocation, RawDatasetPosition
from api.services.path_resolution import compose_storage_path
from api.services.project_deletion import resolve_raw_location_path
from api.services.storage_metrics import safe_dir_size, safe_file_size


@dataclass
class RawDatasetPositionDeletionPreviewData:
    raw_dataset: RawDataset
    position_ids: list[UUID]
    reclaimable_bytes: int
    preview_json: dict


def build_raw_dataset_position_deletion_preview(
    session: Session,
    *,
    raw_dataset: RawDataset,
    position_ids: list[UUID],
) -> RawDatasetPositionDeletionPreviewData:
    requested_position_ids = {str(position_id) for position_id in position_ids}
    positions = list(
        session.scalars(
            select(RawDatasetPosition)
            .options(joinedload(RawDatasetPosition.preview_artifact))
            .where(
                RawDatasetPosition.raw_dataset_id == raw_dataset.id,
                RawDatasetPosition.id.in_(position_ids),
            )
            .order_by(RawDatasetPosition.position_index.asc().nullslast(), RawDatasetPosition.position_key.asc())
        )
    )
    if not positions:
        raise ValueError("No matching raw dataset positions were selected.")
    found_position_ids = {str(position.id) for position in positions}
    missing_position_ids = sorted(requested_position_ids - found_position_ids)
    if missing_position_ids:
        raise ValueError(f"Some selected positions were not found: {', '.join(missing_position_ids)}")

    linked_projects = preview_linked_projects_for_raw_dataset(session, raw_dataset=raw_dataset)
    preview_positions: list[dict] = []
    reclaimable_bytes = 0
    for position in positions:
        relative_path = resolve_position_relative_path(position)
        source_paths = resolve_position_source_paths(raw_dataset, relative_path)
        source_bytes = sum(count_position_source_bytes(path) for path in source_paths)
        preview_artifact_path = resolve_preview_artifact_path(position)
        preview_artifact_bytes = 0
        if preview_artifact_path and preview_artifact_path.exists():
            if not any(path_contains(source_path, preview_artifact_path) for source_path in source_paths):
                preview_artifact_bytes = safe_file_size(preview_artifact_path)
        reclaimable_bytes += source_bytes + preview_artifact_bytes
        preview_positions.append(
            {
                "position_id": str(position.id),
                "position_key": position.position_key,
                "display_name": position.display_name,
                "position_index": position.position_index,
                "relative_path": relative_path,
                "source_paths": [str(path) for path in source_paths],
                "source_bytes": source_bytes,
                "preview_artifact_id": str(position.preview_artifact_id) if position.preview_artifact_id else None,
                "preview_artifact_path": str(preview_artifact_path) if preview_artifact_path else None,
                "preview_artifact_bytes": preview_artifact_bytes,
                "reclaimable_bytes": source_bytes + preview_artifact_bytes,
            }
        )

    warning_messages = []
    if linked_projects:
        warning_messages.append(
            f"This raw dataset is used by {len(linked_projects)} project(s). Deleting positions may invalidate linked analysis."
        )

    preview_json = {
        "raw_dataset": {
            "raw_dataset_id": str(raw_dataset.id),
            "acquisition_label": raw_dataset.acquisition_label,
            "total_bytes": int(raw_dataset.total_bytes or 0),
        },
        "positions": preview_positions,
        "linked_projects": linked_projects,
        "warnings": warning_messages,
    }
    return RawDatasetPositionDeletionPreviewData(
        raw_dataset=raw_dataset,
        position_ids=[UUID(item["position_id"]) for item in preview_positions],
        reclaimable_bytes=reclaimable_bytes,
        preview_json=preview_json,
    )


def execute_raw_dataset_position_deletion(
    session: Session,
    *,
    preview: RawDatasetPositionDeletionPreviewData,
    requested_by_user,
) -> dict:
    deleted_positions: list[str] = []
    deleted_source_paths: list[str] = []
    deleted_preview_artifacts: list[str] = []
    missing_source_paths: list[str] = []
    errors: list[str] = []

    positions_by_id = {
        str(position.id): position
        for position in session.scalars(
            select(RawDatasetPosition)
            .options(joinedload(RawDatasetPosition.preview_artifact))
            .where(
                RawDatasetPosition.raw_dataset_id == preview.raw_dataset.id,
                RawDatasetPosition.id.in_(preview.position_ids),
            )
        )
    }

    for position_id in preview.position_ids:
        position = positions_by_id.get(str(position_id))
        if position is None:
            continue

        relative_path = resolve_position_relative_path(position)
        source_paths = resolve_position_source_paths(preview.raw_dataset, relative_path)
        for source_path in source_paths:
            try:
                if source_path.is_file():
                    source_path.unlink()
                    deleted_source_paths.append(str(source_path))
                elif source_path.is_dir():
                    shutil.rmtree(source_path)
                    deleted_source_paths.append(str(source_path))
                else:
                    missing_source_paths.append(str(source_path))
            except OSError as exc:
                errors.append(f"Failed to delete source path {source_path}: {exc}")

        preview_artifact = position.preview_artifact
        if preview_artifact is not None:
            artifact_path = Path(preview_artifact.uri)
            try:
                if artifact_path.exists() and not any(path_contains(source_path, artifact_path) for source_path in source_paths):
                    if artifact_path.is_file():
                        artifact_path.unlink()
                        deleted_preview_artifacts.append(str(artifact_path))
                    elif artifact_path.is_dir():
                        shutil.rmtree(artifact_path)
                        deleted_preview_artifacts.append(str(artifact_path))
                session.delete(preview_artifact)
            except OSError as exc:
                errors.append(f"Failed to delete preview artifact {artifact_path}: {exc}")
        session.delete(position)
        deleted_positions.append(str(position.id))

    session.flush()

    previous_total_bytes = int(preview.raw_dataset.total_bytes or 0)
    total_bytes = recalculate_raw_dataset_total_bytes(preview.raw_dataset)
    remaining_position_count = count_remaining_positions(session, raw_dataset=preview.raw_dataset)
    update_raw_dataset_after_position_deletion(
        preview.raw_dataset,
        total_bytes=total_bytes,
        remaining_position_count=remaining_position_count,
    )

    status = "deleted" if not errors else "partial_failed"
    result_json = {
        "deleted_positions": deleted_positions,
        "deleted_source_paths": deleted_source_paths,
        "deleted_preview_artifacts": deleted_preview_artifacts,
        "missing_source_paths": missing_source_paths,
        "previous_total_bytes": previous_total_bytes,
        "total_bytes": total_bytes,
        "remaining_position_count": remaining_position_count,
        "errors": errors,
        "deleted_at": datetime.now(timezone.utc).isoformat(),
    }
    return {
        "raw_dataset_id": str(preview.raw_dataset.id),
        "status": status,
        "position_count": len(deleted_positions),
        "reclaimable_bytes": int(preview.reclaimable_bytes or 0),
        "total_bytes": total_bytes,
        "remaining_position_count": remaining_position_count,
        "result_json": result_json,
    }


def queue_raw_dataset_position_deletion(
    session: Session,
    *,
    preview: RawDatasetPositionDeletionPreviewData,
    requested_by_user,
) -> Job:
    job = Job(
        raw_dataset_id=preview.raw_dataset.id,
        requested_mode="server",
        priority=20,
        requested_by=requested_by_user.user_key,
        requested_from_host="api",
        params_json={
            "job_kind": "raw_dataset_position_deletion",
            "position_ids": [str(position_id) for position_id in preview.position_ids],
            "position_count": len(preview.position_ids),
            "reclaimable_bytes": int(preview.reclaimable_bytes or 0),
        },
        status="queued",
    )
    session.add(job)
    session.flush()
    return job


def execute_raw_dataset_position_deletion_job(session: Session, *, job: Job) -> dict:
    if job.raw_dataset_id is None:
        raise ValueError(f"Raw dataset position deletion job {job.id} is missing raw_dataset_id")

    position_ids = parse_position_ids((job.params_json or {}).get("position_ids"))
    if not position_ids:
        raise ValueError(f"Raw dataset position deletion job {job.id} has no position_ids")

    raw_dataset = session.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.positions).joinedload(RawDatasetPosition.preview_artifact),
            joinedload(RawDataset.project_links).joinedload(ProjectRawLink.project).joinedload(Project.owner),
        )
        .where(RawDataset.id == job.raw_dataset_id)
    ).unique().first()
    if raw_dataset is None:
        return {
            "raw_dataset_id": str(job.raw_dataset_id),
            "status": "failed",
            "position_count": 0,
            "reclaimable_bytes": 0,
            "result_json": {"errors": [f"Raw dataset {job.raw_dataset_id} does not exist."]},
        }

    preview = build_raw_dataset_position_deletion_preview(
        session,
        raw_dataset=raw_dataset,
        position_ids=position_ids,
    )
    return execute_raw_dataset_position_deletion(session, preview=preview, requested_by_user=None)


def parse_position_ids(values: object) -> list[UUID]:
    if not isinstance(values, list):
        return []
    position_ids: list[UUID] = []
    for value in values:
        try:
            position_ids.append(UUID(str(value)))
        except (TypeError, ValueError):
            continue
    return position_ids


def recalculate_raw_dataset_total_bytes(raw_dataset: RawDataset) -> int:
    total_bytes = 0
    for location in raw_dataset.locations or []:
        if location.storage_root is None:
            continue
        dataset_root = resolve_raw_location_path(location)
        total_bytes += safe_dir_size(dataset_root)
    return total_bytes


def count_remaining_positions(session: Session, *, raw_dataset: RawDataset) -> int:
    return int(
        session.scalar(
            select(func.count(RawDatasetPosition.id)).where(
                RawDatasetPosition.raw_dataset_id == raw_dataset.id
            )
        )
        or 0
    )


def update_raw_dataset_after_position_deletion(
    raw_dataset: RawDataset,
    *,
    total_bytes: int,
    remaining_position_count: int,
) -> None:
    metadata = dict(raw_dataset.metadata_json or {})
    dimensions = metadata.get("dimensions")
    if not isinstance(dimensions, dict):
        dimensions = {}
    else:
        dimensions = dict(dimensions)
    dimensions["position_count"] = remaining_position_count
    metadata["dimensions"] = dimensions

    for summary_key in ("summary", "Summary"):
        summary = metadata.get(summary_key)
        if isinstance(summary, dict):
            summary = dict(summary)
            summary["Positions"] = remaining_position_count
            metadata[summary_key] = summary
    if "Positions" in metadata:
        metadata["Positions"] = remaining_position_count
    now = datetime.now(timezone.utc)
    raw_dataset.metadata_json = metadata
    raw_dataset.total_bytes = int(total_bytes or 0)
    raw_dataset.last_size_scan_at = now
    raw_dataset.updated_at = now


def resolve_position_relative_path(position: RawDatasetPosition) -> str:
    metadata = dict(position.metadata_json or {})
    relative_path = str(metadata.get("relative_path") or "").strip()
    if relative_path:
        return relative_path
    if position.display_name:
        return position.display_name
    return position.position_key


def resolve_position_source_paths(raw_dataset: RawDataset, relative_path: str) -> list[Path]:
    paths: list[Path] = []
    for location in raw_dataset.locations or []:
        if location.storage_root is None:
            continue
        dataset_root = resolve_raw_location_path(location)
        source_path = Path(compose_storage_path(str(dataset_root), relative_path))
        if str(source_path):
            paths.append(source_path)
    return paths


def resolve_preview_artifact_path(position: RawDatasetPosition) -> Path | None:
    artifact = position.preview_artifact
    if artifact is None or not artifact.uri:
        return None
    return Path(artifact.uri)


def count_position_source_bytes(path: Path) -> int:
    if path.is_dir():
        return safe_dir_size(path)
    if path.is_file():
        return safe_file_size(path)
    return 0


def path_contains(parent: Path, child: Path) -> bool:
    try:
        child.resolve(strict=False).relative_to(parent.resolve(strict=False))
        return True
    except ValueError:
        return False


def preview_linked_projects_for_raw_dataset(session: Session, *, raw_dataset: RawDataset) -> list[dict]:
    links = list(
        session.scalars(
            select(ProjectRawLink)
            .options(joinedload(ProjectRawLink.project).joinedload(Project.owner))
            .where(ProjectRawLink.raw_dataset_id == raw_dataset.id)
            .order_by(ProjectRawLink.id.asc())
        )
    )
    projects: list[dict] = []
    for link in links:
        project = link.project
        if project is None or project.status == "deleted":
            continue
        projects.append(
            {
                "project_id": str(project.id),
                "project_name": project.project_name,
                "status": project.status,
                "visibility": project.visibility,
                "total_bytes": int(project.total_bytes or 0),
                "owner_user_key": project.owner.user_key if project.owner else None,
                "owner_display_name": project.owner.display_name if project.owner else None,
            }
        )
    return projects
