from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.config import get_settings
from api.models import (
    Job,
    Project,
    ProjectLocation,
    RawDataset,
    RawDatasetLocation,
    StorageLifecycleEvent,
    User,
)
from api.services.archive_settings import resolve_raw_archive_runtime_config


ARCHIVE_JOB_PRIORITY = 150


@dataclass
class RawDatasetArchivePreviewData:
    raw_dataset: RawDataset
    target_tier: str
    reclaimable_bytes: int
    preview_json: dict


@dataclass
class LegacyArchiveBundleScope:
    root_path: Path
    projects: list[Project]
    blockers: list[str]


class RawDatasetLifecycleConflictError(RuntimeError):
    """Raised when a conflicting lifecycle job is already active."""


def resolve_raw_location_path(location: RawDatasetLocation) -> Path:
    return Path(location.storage_root.path_prefix) / (location.relative_path or "")


def pick_preferred_raw_location(raw_dataset: RawDataset) -> RawDatasetLocation:
    if not raw_dataset.locations:
        raise ValueError("Raw dataset has no storage locations")
    preferred = next((location for location in raw_dataset.locations if location.is_preferred), None)
    return preferred or raw_dataset.locations[0]


def normalized_storage_path(path: Path) -> Path:
    return Path(str(path).rstrip("/\\"))


def path_relation(*, root: Path, candidate: Path) -> str:
    root = normalized_storage_path(root)
    candidate = normalized_storage_path(candidate)
    if candidate == root:
        return "exact"
    if root in candidate.parents:
        return "descendant"
    if candidate in root.parents:
        return "ancestor"
    return "unrelated"


def is_legacy_shared_project(project: Project) -> bool:
    value = (project.metadata_json or {}).get("storage_is_shared_with_raw_dataset")
    return value is True or str(value or "").strip().lower() == "true"


def inspect_legacy_archive_bundle_scope(
    session: Session,
    *,
    raw_dataset: RawDataset,
) -> LegacyArchiveBundleScope:
    root_path = normalized_storage_path(resolve_raw_location_path(pick_preferred_raw_location(raw_dataset)))
    blockers: list[str] = []
    projects: list[Project] = []

    raw_locations = list(
        session.scalars(
            select(RawDatasetLocation)
            .options(
                joinedload(RawDatasetLocation.storage_root),
                joinedload(RawDatasetLocation.raw_dataset),
            )
            .where(RawDatasetLocation.is_preferred.is_(True))
        ).unique()
    )
    for location in raw_locations:
        if location.raw_dataset_id == raw_dataset.id:
            continue
        candidate_path = normalized_storage_path(resolve_raw_location_path(location))
        relation = path_relation(root=root_path, candidate=candidate_path)
        if relation == "ancestor":
            blockers.append(
                f"Selected raw dataset is nested under raw dataset {location.raw_dataset_id}: {candidate_path}"
            )
        elif relation == "descendant":
            blockers.append(
                f"Raw dataset {location.raw_dataset_id} is nested inside the archive root: {candidate_path}"
            )

    project_locations = list(
        session.scalars(
            select(ProjectLocation)
            .options(
                joinedload(ProjectLocation.storage_root),
                joinedload(ProjectLocation.project),
            )
            .where(ProjectLocation.is_preferred.is_(True))
        ).unique()
    )
    for location in project_locations:
        project = location.project
        if project is None or project.status == "deleted":
            continue
        candidate_path = normalized_storage_path(
            Path(location.storage_root.path_prefix) / (location.relative_path or "")
        )
        relation = path_relation(root=root_path, candidate=candidate_path)
        if relation == "unrelated":
            continue
        if relation == "exact":
            if is_legacy_shared_project(project):
                projects.append(project)
            else:
                blockers.append(
                    f"Non-legacy project {project.id} shares the raw dataset archive path: {candidate_path}"
                )
            continue
        blockers.append(
            f"Project {project.id} has a {relation} path relative to the archive root: {candidate_path}"
        )

    projects.sort(key=lambda project: (project.project_name.lower(), str(project.id)))
    return LegacyArchiveBundleScope(
        root_path=root_path,
        projects=projects,
        blockers=sorted(set(blockers)),
    )


def build_archive_preview(session: Session, *, raw_dataset: RawDataset, target_tier: str = "cold") -> RawDatasetArchivePreviewData:
    bundle_scope = inspect_legacy_archive_bundle_scope(session, raw_dataset=raw_dataset)
    paths = [str(resolve_raw_location_path(location)) for location in raw_dataset.locations]
    reclaimable_bytes = int(raw_dataset.total_bytes or 0)
    preview_json = {
        "raw_dataset": {
            "acquisition_label": raw_dataset.acquisition_label,
            "paths": paths,
            "current_tier": raw_dataset.lifecycle_tier,
            "archive_status": raw_dataset.archive_status,
            "total_bytes": reclaimable_bytes,
        },
        "legacy_bundle": {
            "root_path": str(bundle_scope.root_path),
            "project_count": len(bundle_scope.projects),
            "projects": [
                {"project_id": str(project.id), "project_name": project.project_name}
                for project in bundle_scope.projects
            ],
            "blockers": bundle_scope.blockers,
            "safe_for_archive": not bundle_scope.blockers,
        },
    }
    return RawDatasetArchivePreviewData(
        raw_dataset=raw_dataset,
        target_tier=target_tier,
        reclaimable_bytes=reclaimable_bytes,
        preview_json=preview_json,
    )


def transition_raw_dataset_to_archive(
    session: Session,
    *,
    raw_dataset: RawDataset,
    requested_by_user: User,
    archive_uri: str | None,
    archive_compression: str | None,
    mark_archived: bool | None,
) -> StorageLifecycleEvent:
    settings = get_settings()
    archive_config = resolve_raw_archive_runtime_config(session, settings=settings)
    existing_job = find_active_lifecycle_job(session, raw_dataset=raw_dataset)
    if existing_job is not None:
        raise RawDatasetLifecycleConflictError(
            f"Lifecycle job {existing_job.id} is already active for raw dataset {raw_dataset.id}"
        )

    from_tier = raw_dataset.lifecycle_tier
    effective_mark_archived = archive_config.delete_hot_source if mark_archived is None else bool(mark_archived)
    bundle_scope = inspect_legacy_archive_bundle_scope(session, raw_dataset=raw_dataset)
    if bundle_scope.blockers:
        raise RawDatasetLifecycleConflictError(
            "Archive bundle preflight failed: " + "; ".join(bundle_scope.blockers[:5])
        )
    bundle_project_ids = [str(project.id) for project in bundle_scope.projects]
    raw_dataset.archive_status = "archive_queued"
    raw_dataset.archive_uri = (
        archive_uri
        or raw_dataset.archive_uri
        or archive_config.archive_root
        or settings.default_archive_root
        or None
    )
    raw_dataset.archive_compression = (
        archive_compression
        or raw_dataset.archive_compression
        or archive_config.archive_compression
        or settings.default_archive_compression
    )
    raw_dataset.reclaimable_bytes = int(raw_dataset.total_bytes or 0)

    job = Job(
        raw_dataset_id=raw_dataset.id,
        requested_mode="server",
        priority=ARCHIVE_JOB_PRIORITY,
        requested_by=requested_by_user.user_key,
        requested_from_host="api",
        params_json={
            "job_kind": "archive_raw_dataset",
            "archive_uri": raw_dataset.archive_uri,
            "archive_compression": raw_dataset.archive_compression,
            "mark_archived": effective_mark_archived,
            "bundle_root_path": str(bundle_scope.root_path),
            "bundle_project_ids": bundle_project_ids,
        },
        status="queued",
    )
    session.add(job)
    session.flush()

    event = StorageLifecycleEvent(
        raw_dataset_id=raw_dataset.id,
        requested_by_user_id=requested_by_user.id,
        event_kind="archive_requested",
        from_tier=from_tier,
        to_tier=from_tier,
        archive_status=raw_dataset.archive_status,
        reclaimable_bytes=raw_dataset.reclaimable_bytes,
        metadata_json={
            "job_id": str(job.id),
            "archive_uri": raw_dataset.archive_uri,
            "archive_compression": raw_dataset.archive_compression,
            "mark_archived": effective_mark_archived,
            "bundle_root_path": str(bundle_scope.root_path),
            "bundle_project_ids": bundle_project_ids,
        },
    )
    session.add(event)
    for project in bundle_scope.projects:
        project.lifecycle_tier = from_tier
        project.archive_status = "archive_queued"
        project.archive_uri = raw_dataset.archive_uri
        project.archive_compression = raw_dataset.archive_compression
        project.metadata_json = {
            **(project.metadata_json or {}),
            "archive_bundle_raw_dataset_id": str(raw_dataset.id),
            "archive_bundle_root_path": str(bundle_scope.root_path),
        }
    session.flush()
    return event


def transition_raw_dataset_to_restore(
    session: Session,
    *,
    raw_dataset: RawDataset,
    requested_by_user: User,
) -> StorageLifecycleEvent:
    existing_job = find_active_lifecycle_job(session, raw_dataset=raw_dataset)
    if existing_job is not None:
        raise RawDatasetLifecycleConflictError(
            f"Lifecycle job {existing_job.id} is already active for raw dataset {raw_dataset.id}"
        )

    from_tier = raw_dataset.lifecycle_tier
    bundle_scope = inspect_legacy_archive_bundle_scope(session, raw_dataset=raw_dataset)
    if bundle_scope.blockers:
        raise RawDatasetLifecycleConflictError(
            "Restore bundle preflight failed: " + "; ".join(bundle_scope.blockers[:5])
        )
    bundle_project_ids = [str(project.id) for project in bundle_scope.projects]
    raw_dataset.archive_status = "restore_queued"
    raw_dataset.reclaimable_bytes = 0

    job = Job(
        raw_dataset_id=raw_dataset.id,
        requested_mode="server",
        priority=30,
        requested_by=requested_by_user.user_key,
        requested_from_host="api",
        params_json={
            "job_kind": "restore_raw_dataset",
            "archive_uri": raw_dataset.archive_uri,
            "archive_compression": raw_dataset.archive_compression,
            "bundle_root_path": str(bundle_scope.root_path),
            "bundle_project_ids": bundle_project_ids,
        },
        status="queued",
    )
    session.add(job)
    session.flush()

    event = StorageLifecycleEvent(
        raw_dataset_id=raw_dataset.id,
        requested_by_user_id=requested_by_user.id,
        event_kind="restore_requested",
        from_tier=from_tier,
        to_tier=from_tier,
        archive_status=raw_dataset.archive_status,
        reclaimable_bytes=0,
        metadata_json={
            "job_id": str(job.id),
            "archive_uri": raw_dataset.archive_uri,
            "bundle_root_path": str(bundle_scope.root_path),
            "bundle_project_ids": bundle_project_ids,
        },
    )
    session.add(event)
    for project in bundle_scope.projects:
        project.archive_status = "restore_queued"
    session.flush()
    return event


def complete_raw_dataset_archive(
    session: Session,
    *,
    raw_dataset: RawDataset,
    requested_by_user: User | None,
    archive_uri: str,
    archive_compression: str,
    source_deleted: bool,
    result_json: dict,
    bundle_project_ids: list[str] | None = None,
) -> StorageLifecycleEvent:
    from_tier = raw_dataset.lifecycle_tier
    raw_dataset.lifecycle_tier = "cold" if source_deleted else "warm"
    raw_dataset.archive_status = "archived"
    raw_dataset.archive_uri = archive_uri
    raw_dataset.archive_compression = archive_compression
    raw_dataset.reclaimable_bytes = 0 if source_deleted else int(raw_dataset.total_bytes or 0)
    update_bundle_projects_after_archive(
        session,
        project_ids=bundle_project_ids or [],
        archive_uri=archive_uri,
        archive_compression=archive_compression,
        source_deleted=source_deleted,
        bundle_raw_dataset_id=raw_dataset.id,
    )

    event = StorageLifecycleEvent(
        raw_dataset_id=raw_dataset.id,
        requested_by_user_id=requested_by_user.id if requested_by_user else None,
        event_kind="archived",
        from_tier=from_tier,
        to_tier=raw_dataset.lifecycle_tier,
        archive_status=raw_dataset.archive_status,
        reclaimable_bytes=raw_dataset.reclaimable_bytes,
        metadata_json=result_json,
    )
    session.add(event)
    session.flush()
    return event


def complete_raw_dataset_restore(
    session: Session,
    *,
    raw_dataset: RawDataset,
    requested_by_user: User | None,
    result_json: dict,
    bundle_project_ids: list[str] | None = None,
) -> StorageLifecycleEvent:
    from_tier = raw_dataset.lifecycle_tier
    raw_dataset.lifecycle_tier = "hot"
    raw_dataset.archive_status = "restored"
    raw_dataset.reclaimable_bytes = 0
    raw_dataset.last_accessed_at = datetime.now(timezone.utc)
    update_bundle_projects_after_restore(session, project_ids=bundle_project_ids or [])

    event = StorageLifecycleEvent(
        raw_dataset_id=raw_dataset.id,
        requested_by_user_id=requested_by_user.id if requested_by_user else None,
        event_kind="restored",
        from_tier=from_tier,
        to_tier=raw_dataset.lifecycle_tier,
        archive_status=raw_dataset.archive_status,
        reclaimable_bytes=0,
        metadata_json=result_json,
    )
    session.add(event)
    session.flush()
    return event


def fail_raw_dataset_lifecycle_job(
    session: Session,
    *,
    raw_dataset: RawDataset,
    requested_by_user: User | None,
    event_kind: str,
    archive_status: str,
    error_text: str,
    bundle_project_ids: list[str] | None = None,
) -> StorageLifecycleEvent:
    raw_dataset.archive_status = archive_status
    update_bundle_project_status(
        session,
        project_ids=bundle_project_ids or [],
        archive_status=archive_status,
    )

    event = StorageLifecycleEvent(
        raw_dataset_id=raw_dataset.id,
        requested_by_user_id=requested_by_user.id if requested_by_user else None,
        event_kind=event_kind,
        from_tier=raw_dataset.lifecycle_tier,
        to_tier=raw_dataset.lifecycle_tier,
        archive_status=raw_dataset.archive_status,
        reclaimable_bytes=raw_dataset.reclaimable_bytes,
        metadata_json={"error_text": error_text},
    )
    session.add(event)
    session.flush()
    return event


def load_bundle_projects(session: Session, *, project_ids: list[str]) -> list[Project]:
    parsed_ids: list[UUID] = []
    for project_id in project_ids:
        try:
            parsed_ids.append(UUID(str(project_id)))
        except (TypeError, ValueError):
            continue
    if not parsed_ids:
        return []
    return list(session.scalars(select(Project).where(Project.id.in_(parsed_ids))))


def update_bundle_project_status(
    session: Session,
    *,
    project_ids: list[str],
    archive_status: str,
) -> None:
    for project in load_bundle_projects(session, project_ids=project_ids):
        project.archive_status = archive_status


def update_bundle_projects_after_archive(
    session: Session,
    *,
    project_ids: list[str],
    archive_uri: str,
    archive_compression: str,
    source_deleted: bool,
    bundle_raw_dataset_id: UUID,
) -> None:
    for project in load_bundle_projects(session, project_ids=project_ids):
        project.lifecycle_tier = "cold" if source_deleted else "warm"
        project.archive_status = "archived"
        project.archive_uri = archive_uri
        project.archive_compression = archive_compression
        project.metadata_json = {
            **(project.metadata_json or {}),
            "archive_bundle_raw_dataset_id": str(bundle_raw_dataset_id),
        }


def update_bundle_projects_after_restore(session: Session, *, project_ids: list[str]) -> None:
    for project in load_bundle_projects(session, project_ids=project_ids):
        project.lifecycle_tier = "hot"
        project.archive_status = "restored"


def find_active_lifecycle_job(session: Session, *, raw_dataset: RawDataset) -> Job | None:
    stmt = (
        select(Job)
        .where(Job.raw_dataset_id == raw_dataset.id)
        .where(Job.status.in_(("queued", "running")))
        .where(Job.params_json.contains({"job_kind": "archive_raw_dataset"}) | Job.params_json.contains({"job_kind": "restore_raw_dataset"}))
        .order_by(Job.created_at.desc())
        .limit(1)
    )
    return session.scalars(stmt).first()
