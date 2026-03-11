from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.models import Job, RawDataset, RawDatasetLocation, StorageLifecycleEvent, User


@dataclass
class RawDatasetArchivePreviewData:
    raw_dataset: RawDataset
    target_tier: str
    reclaimable_bytes: int
    preview_json: dict


class RawDatasetLifecycleConflictError(RuntimeError):
    """Raised when a conflicting lifecycle job is already active."""


def resolve_raw_location_path(location: RawDatasetLocation) -> Path:
    return Path(location.storage_root.path_prefix) / (location.relative_path or "")


def pick_preferred_raw_location(raw_dataset: RawDataset) -> RawDatasetLocation:
    if not raw_dataset.locations:
        raise ValueError("Raw dataset has no storage locations")
    preferred = next((location for location in raw_dataset.locations if location.is_preferred), None)
    return preferred or raw_dataset.locations[0]


def build_archive_preview(session: Session, *, raw_dataset: RawDataset, target_tier: str = "cold") -> RawDatasetArchivePreviewData:
    _ = session
    paths = [str(resolve_raw_location_path(location)) for location in raw_dataset.locations]
    reclaimable_bytes = int(raw_dataset.total_bytes or 0)
    preview_json = {
        "raw_dataset": {
            "acquisition_label": raw_dataset.acquisition_label,
            "paths": paths,
            "current_tier": raw_dataset.lifecycle_tier,
            "archive_status": raw_dataset.archive_status,
            "total_bytes": reclaimable_bytes,
        }
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
    mark_archived: bool,
) -> StorageLifecycleEvent:
    settings = get_settings()
    existing_job = find_active_lifecycle_job(session, raw_dataset=raw_dataset)
    if existing_job is not None:
        raise RawDatasetLifecycleConflictError(
            f"Lifecycle job {existing_job.id} is already active for raw dataset {raw_dataset.id}"
        )

    from_tier = raw_dataset.lifecycle_tier
    raw_dataset.archive_status = "archive_queued"
    raw_dataset.archive_uri = archive_uri or raw_dataset.archive_uri or settings.default_archive_root or None
    raw_dataset.archive_compression = (
        archive_compression or raw_dataset.archive_compression or settings.default_archive_compression
    )
    raw_dataset.reclaimable_bytes = int(raw_dataset.total_bytes or 0)

    job = Job(
        raw_dataset_id=raw_dataset.id,
        requested_mode="server",
        priority=40,
        requested_by=requested_by_user.user_key,
        requested_from_host="api",
        params_json={
            "job_kind": "archive_raw_dataset",
            "archive_uri": raw_dataset.archive_uri,
            "archive_compression": raw_dataset.archive_compression,
            "mark_archived": mark_archived,
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
            "mark_archived": mark_archived,
        },
    )
    session.add(event)
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
        metadata_json={"job_id": str(job.id), "archive_uri": raw_dataset.archive_uri},
    )
    session.add(event)
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
) -> StorageLifecycleEvent:
    from_tier = raw_dataset.lifecycle_tier
    raw_dataset.lifecycle_tier = "cold" if source_deleted else "warm"
    raw_dataset.archive_status = "archived"
    raw_dataset.archive_uri = archive_uri
    raw_dataset.archive_compression = archive_compression
    raw_dataset.reclaimable_bytes = 0 if source_deleted else int(raw_dataset.total_bytes or 0)

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
) -> StorageLifecycleEvent:
    from_tier = raw_dataset.lifecycle_tier
    raw_dataset.lifecycle_tier = "hot"
    raw_dataset.archive_status = "restored"
    raw_dataset.reclaimable_bytes = 0
    raw_dataset.last_accessed_at = datetime.now(timezone.utc)

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
) -> StorageLifecycleEvent:
    raw_dataset.archive_status = archive_status

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
