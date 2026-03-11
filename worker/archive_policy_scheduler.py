from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from api.config import get_settings
from api.models import User
from api.services.archive_policy import ArchivePolicyPreviewData, build_archive_policy_preview
from api.services.raw_dataset_lifecycle import RawDatasetLifecycleConflictError, transition_raw_dataset_to_archive
from api.services.users import get_or_create_user


LOGGER = logging.getLogger("detecdiv-hub-worker")
ARCHIVE_POLICY_LOCK_KEY = 2026031101


@dataclass
class ArchivePolicyRunResult:
    generated_at: datetime
    queued_count: int
    skipped_count: int
    candidate_count: int
    total_reclaimable_bytes: int
    queued_job_ids: list[str]


def parse_csv_setting(value: str, *, default: list[str]) -> list[str]:
    parts = [part.strip() for part in (value or "").split(",")]
    filtered = [part for part in parts if part]
    return filtered or list(default)


def should_run_archive_policy(*, last_run_at: datetime | None, now: datetime | None = None) -> bool:
    settings = get_settings()
    if not settings.archive_policy_enabled:
        return False
    now = now or datetime.now(timezone.utc)
    if last_run_at is None:
        return True
    interval = timedelta(minutes=max(1, settings.archive_policy_interval_minutes))
    return now - last_run_at >= interval


def try_acquire_archive_policy_lock(session: Session) -> bool:
    return bool(session.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": ARCHIVE_POLICY_LOCK_KEY}).scalar())


def release_archive_policy_lock(session: Session) -> None:
    session.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": ARCHIVE_POLICY_LOCK_KEY})


def queue_archive_policy_run(session: Session) -> ArchivePolicyRunResult:
    settings = get_settings()
    current_user = resolve_archive_policy_user(session, user_key=settings.archive_policy_run_as_user_key)
    preview = build_archive_policy_preview(
        session,
        current_user=current_user,
        older_than_days=settings.archive_policy_older_than_days,
        min_total_bytes=settings.archive_policy_min_total_bytes,
        limit=settings.archive_policy_limit,
        owner_key=settings.archive_policy_owner_key or None,
        search=settings.archive_policy_search or None,
        lifecycle_tiers=parse_csv_setting(settings.archive_policy_lifecycle_tiers, default=["hot"]),
        archive_statuses=parse_csv_setting(
            settings.archive_policy_archive_statuses,
            default=["none", "restored", "archive_failed", "restore_failed"],
        ),
        archive_uri=settings.archive_policy_archive_uri or None,
        archive_compression=settings.archive_policy_archive_compression or None,
    )
    queued_job_ids: list[str] = []
    skipped_count = 0

    for candidate in preview.candidates:
        try:
            event = transition_raw_dataset_to_archive(
                session,
                raw_dataset=candidate.raw_dataset,
                requested_by_user=current_user,
                archive_uri=settings.archive_policy_archive_uri or None,
                archive_compression=settings.archive_policy_archive_compression or None,
                mark_archived=settings.archive_policy_delete_hot_source,
            )
            job_id = event.metadata_json.get("job_id")
            if job_id:
                queued_job_ids.append(job_id)
        except RawDatasetLifecycleConflictError:
            skipped_count += 1

    session.flush()
    return ArchivePolicyRunResult(
        generated_at=preview.generated_at,
        queued_count=len(queued_job_ids),
        skipped_count=skipped_count + preview.skipped_conflicts,
        candidate_count=preview.candidate_count,
        total_reclaimable_bytes=preview.total_reclaimable_bytes,
        queued_job_ids=queued_job_ids,
    )


def run_archive_policy_if_due(session: Session, *, last_run_at: datetime | None) -> datetime | None:
    if not should_run_archive_policy(last_run_at=last_run_at):
        return last_run_at

    if not try_acquire_archive_policy_lock(session):
        LOGGER.info("Archive policy run skipped because another worker holds the lock")
        return last_run_at

    try:
        result = queue_archive_policy_run(session)
        LOGGER.info(
            "Archive policy run completed: candidates=%s queued=%s skipped=%s reclaimable_bytes=%s",
            result.candidate_count,
            result.queued_count,
            result.skipped_count,
            result.total_reclaimable_bytes,
        )
        return datetime.now(timezone.utc)
    finally:
        release_archive_policy_lock(session)


def resolve_archive_policy_user(session: Session, *, user_key: str) -> User:
    return get_or_create_user(session, user_key=user_key, display_name=user_key, role="service")
