from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from api.models import RawDataset, StorageLifecycleEvent, User
from api.services.raw_dataset_lifecycle import pick_preferred_raw_location, resolve_raw_location_path


class RawArchiveDeleteConflictError(RuntimeError):
    pass


def resolve_archive_file_path(archive_uri: str | None) -> Path | None:
    text = str(archive_uri or "").strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path
    return Path.cwd() / path


def archive_file_size_bytes(archive_uri: str | None) -> int | None:
    path = resolve_archive_file_path(archive_uri)
    if path is None or not path.exists() or not path.is_file():
        return None
    return int(path.stat().st_size)


def delete_raw_dataset_archive_file(
    session: Session,
    *,
    raw_dataset: RawDataset,
    requested_by_user: User | None,
) -> dict:
    archive_path = resolve_archive_file_path(raw_dataset.archive_uri)
    if archive_path is None:
        raise FileNotFoundError(f"Raw dataset {raw_dataset.id} has no archive_uri")
    if not archive_path.exists() or not archive_path.is_file():
        raise FileNotFoundError(f"Archive file does not exist: {archive_path}")

    preferred_location = pick_preferred_raw_location(raw_dataset)
    source_path = resolve_raw_location_path(preferred_location)
    source_exists = source_path.exists()
    if not source_exists:
        raise RawArchiveDeleteConflictError(
            "Archive cannot be deleted while the hot source is missing. Restore the dataset first."
        )

    archive_uri = raw_dataset.archive_uri
    archive_path.unlink()

    from_tier = raw_dataset.lifecycle_tier
    raw_dataset.lifecycle_tier = "hot"
    raw_dataset.archive_status = "none"
    raw_dataset.archive_uri = None
    raw_dataset.archive_compression = None
    raw_dataset.reclaimable_bytes = 0
    raw_dataset.last_accessed_at = datetime.now(timezone.utc)

    event = StorageLifecycleEvent(
        raw_dataset_id=raw_dataset.id,
        requested_by_user_id=requested_by_user.id if requested_by_user else None,
        event_kind="archive_deleted",
        from_tier=from_tier,
        to_tier=raw_dataset.lifecycle_tier,
        archive_status=raw_dataset.archive_status,
        reclaimable_bytes=0,
        metadata_json={
            "deleted_archive_uri": archive_uri,
            "source_path": str(source_path),
        },
    )
    session.add(event)
    session.flush()
    return {
        "raw_dataset_id": str(raw_dataset.id),
        "deleted": True,
        "archive_uri": archive_uri,
        "message": "Archive file deleted.",
    }
