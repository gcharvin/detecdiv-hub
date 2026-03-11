from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, joinedload

from api.config import get_settings
from api.models import RawDataset, RawDatasetLocation, User
from api.services.raw_dataset_lifecycle import build_archive_preview, find_active_lifecycle_job


@dataclass
class ArchiveCandidateData:
    raw_dataset: RawDataset
    last_activity_at: datetime | None
    reclaimable_bytes: int
    suggested_archive_uri: str | None
    suggested_archive_compression: str


@dataclass
class ArchivePolicyPreviewData:
    generated_at: datetime
    candidate_count: int
    total_candidate_bytes: int
    total_reclaimable_bytes: int
    candidates: list[ArchiveCandidateData]
    skipped_conflicts: int


def build_archive_policy_preview(
    session: Session,
    *,
    current_user: User,
    older_than_days: int,
    min_total_bytes: int,
    limit: int,
    owner_key: str | None = None,
    search: str | None = None,
    lifecycle_tiers: list[str] | None = None,
    archive_statuses: list[str] | None = None,
    archive_uri: str | None = None,
    archive_compression: str | None = None,
) -> ArchivePolicyPreviewData:
    settings = get_settings()
    lifecycle_tiers = lifecycle_tiers or ["hot"]
    archive_statuses = archive_statuses or ["none", "restored", "archive_failed", "restore_failed"]
    generated_at = datetime.now(timezone.utc)
    threshold = generated_at - timedelta(days=max(0, older_than_days))

    stmt = (
        select(RawDataset)
        .options(joinedload(RawDataset.owner), joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root))
        .order_by(RawDataset.updated_at.asc(), RawDataset.acquisition_label.asc())
    )
    if current_user.role not in {"admin", "service"}:
        stmt = stmt.where(RawDataset.owner_user_id == current_user.id)
    if owner_key:
        stmt = stmt.join(User, RawDataset.owner_user_id == User.id).where(User.user_key == owner_key)
    if search:
        pattern = f"%{search.strip()}%"
        stmt = stmt.where(
            or_(
                RawDataset.acquisition_label.ilike(pattern),
                RawDataset.external_key.ilike(pattern),
                RawDataset.microscope_name.ilike(pattern),
            )
        )
    if lifecycle_tiers:
        stmt = stmt.where(RawDataset.lifecycle_tier.in_(tuple(lifecycle_tiers)))
    if archive_statuses:
        stmt = stmt.where(RawDataset.archive_status.in_(tuple(archive_statuses)))
    if min_total_bytes > 0:
        stmt = stmt.where(RawDataset.total_bytes >= int(min_total_bytes))

    candidates: list[ArchiveCandidateData] = []
    skipped_conflicts = 0
    for raw_dataset in session.scalars(stmt).unique():
        last_activity_at = (
            raw_dataset.last_accessed_at
            or raw_dataset.ended_at
            or raw_dataset.updated_at
            or raw_dataset.created_at
        )
        if last_activity_at and last_activity_at > threshold:
            continue

        if find_active_lifecycle_job(session, raw_dataset=raw_dataset) is not None:
            skipped_conflicts += 1
            continue

        preview = build_archive_preview(session, raw_dataset=raw_dataset)
        candidates.append(
            ArchiveCandidateData(
                raw_dataset=raw_dataset,
                last_activity_at=last_activity_at,
                reclaimable_bytes=preview.reclaimable_bytes,
                suggested_archive_uri=archive_uri or raw_dataset.archive_uri or settings.default_archive_root or None,
                suggested_archive_compression=archive_compression
                or raw_dataset.archive_compression
                or settings.default_archive_compression,
            )
        )
        if len(candidates) >= max(1, limit):
            break

    return ArchivePolicyPreviewData(
        generated_at=generated_at,
        candidate_count=len(candidates),
        total_candidate_bytes=sum(int(candidate.raw_dataset.total_bytes or 0) for candidate in candidates),
        total_reclaimable_bytes=sum(int(candidate.reclaimable_bytes or 0) for candidate in candidates),
        candidates=candidates,
        skipped_conflicts=skipped_conflicts,
    )
