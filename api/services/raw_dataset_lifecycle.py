from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from api.models import RawDataset, RawDatasetLocation, StorageLifecycleEvent, User


@dataclass
class RawDatasetArchivePreviewData:
    raw_dataset: RawDataset
    target_tier: str
    reclaimable_bytes: int
    preview_json: dict


def resolve_raw_location_path(location: RawDatasetLocation) -> Path:
    return Path(location.storage_root.path_prefix) / (location.relative_path or "")


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
    from_tier = raw_dataset.lifecycle_tier
    raw_dataset.lifecycle_tier = "cold" if mark_archived else "warm"
    raw_dataset.archive_status = "archived" if mark_archived else "pending"
    raw_dataset.archive_uri = archive_uri or raw_dataset.archive_uri
    raw_dataset.archive_compression = archive_compression or raw_dataset.archive_compression
    raw_dataset.reclaimable_bytes = int(raw_dataset.total_bytes or 0)

    event = StorageLifecycleEvent(
        raw_dataset_id=raw_dataset.id,
        requested_by_user_id=requested_by_user.id,
        event_kind="archive_requested" if not mark_archived else "archived",
        from_tier=from_tier,
        to_tier=raw_dataset.lifecycle_tier,
        archive_status=raw_dataset.archive_status,
        reclaimable_bytes=raw_dataset.reclaimable_bytes,
        metadata_json={
            "archive_uri": raw_dataset.archive_uri,
            "archive_compression": raw_dataset.archive_compression,
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
    from_tier = raw_dataset.lifecycle_tier
    raw_dataset.lifecycle_tier = "hot"
    raw_dataset.archive_status = "restoring"
    raw_dataset.reclaimable_bytes = 0
    raw_dataset.last_accessed_at = datetime.now(timezone.utc)

    event = StorageLifecycleEvent(
        raw_dataset_id=raw_dataset.id,
        requested_by_user_id=requested_by_user.id,
        event_kind="restore_requested",
        from_tier=from_tier,
        to_tier="hot",
        archive_status=raw_dataset.archive_status,
        reclaimable_bytes=0,
        metadata_json={},
    )
    session.add(event)
    session.flush()
    return event
