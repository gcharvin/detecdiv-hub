from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select, text
from sqlalchemy.orm import Session, joinedload

from api.config import Settings, get_settings
from api.models import ArchivePolicyRun, RawDataset, RawDatasetLocation, User
from api.services.archive_settings import resolve_raw_archive_runtime_config
from api.services.raw_dataset_lifecycle import (
    RawDatasetLifecycleConflictError,
    build_archive_preview,
    find_active_lifecycle_job,
    transition_raw_dataset_to_archive,
)
from api.services.users import get_or_create_user


ARCHIVE_POLICY_LOCK_KEY = 2026031101


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


@dataclass
class ArchivePolicyConfigData:
    enabled: bool
    interval_minutes: int
    run_as_user_key: str
    older_than_days: int
    min_total_bytes: int
    limit: int
    owner_key: str | None
    search: str | None
    lifecycle_tiers: list[str]
    archive_statuses: list[str]
    archive_uri: str | None
    archive_compression: str | None
    delete_hot_source: bool

    def to_json(self) -> dict:
        return {
            "enabled": self.enabled,
            "interval_minutes": self.interval_minutes,
            "run_as_user_key": self.run_as_user_key,
            "older_than_days": self.older_than_days,
            "min_total_bytes": self.min_total_bytes,
            "limit": self.limit,
            "owner_key": self.owner_key,
            "search": self.search,
            "lifecycle_tiers": list(self.lifecycle_tiers),
            "archive_statuses": list(self.archive_statuses),
            "archive_uri": self.archive_uri,
            "archive_compression": self.archive_compression,
            "delete_hot_source": self.delete_hot_source,
        }


@dataclass
class ArchivePolicyRunExecutionData:
    run: ArchivePolicyRun
    generated_at: datetime
    queued_job_ids: list[str]
    candidate_count: int
    queued_count: int
    skipped_count: int
    total_reclaimable_bytes: int
    report_only: bool


def parse_csv_setting(value: str, *, default: list[str]) -> list[str]:
    parts = [part.strip() for part in (value or "").split(",")]
    filtered = [part for part in parts if part]
    return filtered or list(default)


def automatic_archive_policy_config(settings: Settings | None = None) -> ArchivePolicyConfigData:
    settings = settings or get_settings()
    return ArchivePolicyConfigData(
        enabled=settings.archive_policy_enabled,
        interval_minutes=max(1, settings.archive_policy_interval_minutes),
        run_as_user_key=settings.archive_policy_run_as_user_key,
        older_than_days=max(0, settings.archive_policy_older_than_days),
        min_total_bytes=max(0, int(settings.archive_policy_min_total_bytes)),
        limit=max(1, settings.archive_policy_limit),
        owner_key=settings.archive_policy_owner_key or None,
        search=settings.archive_policy_search or None,
        lifecycle_tiers=parse_csv_setting(settings.archive_policy_lifecycle_tiers, default=["hot"]),
        archive_statuses=parse_csv_setting(
            settings.archive_policy_archive_statuses,
            default=["none", "restored", "archive_failed", "restore_failed"],
        ),
        archive_uri=settings.archive_policy_archive_uri or None,
        archive_compression=settings.archive_policy_archive_compression or None,
        delete_hot_source=settings.archive_policy_delete_hot_source,
    )


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
    archive_config = resolve_raw_archive_runtime_config(session, settings=settings)
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
                suggested_archive_uri=(
                    archive_uri
                    or raw_dataset.archive_uri
                    or archive_config.archive_root
                    or settings.default_archive_root
                    or None
                ),
                suggested_archive_compression=archive_compression
                or raw_dataset.archive_compression
                or archive_config.archive_compression
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


def execute_archive_policy_run(
    session: Session,
    *,
    config: ArchivePolicyConfigData,
    triggered_by_user: User,
    trigger_mode: str,
    report_only: bool,
) -> ArchivePolicyRunExecutionData:
    generated_at = datetime.now(timezone.utc)
    run = ArchivePolicyRun(
        triggered_by_user_id=triggered_by_user.id,
        trigger_mode=trigger_mode,
        status="running",
        report_only=report_only,
        config_json=config.to_json(),
        result_json={},
        started_at=generated_at,
    )
    session.add(run)
    session.flush()

    try:
        preview = build_archive_policy_preview(
            session,
            current_user=triggered_by_user,
            older_than_days=config.older_than_days,
            min_total_bytes=config.min_total_bytes,
            limit=config.limit,
            owner_key=config.owner_key,
            search=config.search,
            lifecycle_tiers=config.lifecycle_tiers,
            archive_statuses=config.archive_statuses,
            archive_uri=config.archive_uri,
            archive_compression=config.archive_compression,
        )
        queued_job_ids: list[str] = []
        skipped_count = preview.skipped_conflicts

        if not report_only:
            for candidate in preview.candidates:
                try:
                    event = transition_raw_dataset_to_archive(
                        session,
                        raw_dataset=candidate.raw_dataset,
                        requested_by_user=triggered_by_user,
                        archive_uri=config.archive_uri,
                        archive_compression=config.archive_compression,
                        mark_archived=config.delete_hot_source,
                    )
                    job_id = event.metadata_json.get("job_id")
                    if job_id:
                        queued_job_ids.append(job_id)
                except RawDatasetLifecycleConflictError:
                    skipped_count += 1

        run.status = "done"
        run.candidate_count = preview.candidate_count
        run.queued_count = 0 if report_only else len(queued_job_ids)
        run.skipped_count = skipped_count
        run.total_reclaimable_bytes = preview.total_reclaimable_bytes
        run.result_json = {
            "generated_at": preview.generated_at.isoformat(),
            "candidate_ids": [str(candidate.raw_dataset.id) for candidate in preview.candidates],
            "queued_job_ids": queued_job_ids,
            "candidate_count": preview.candidate_count,
            "queued_count": 0 if report_only else len(queued_job_ids),
            "skipped_count": skipped_count,
            "total_reclaimable_bytes": preview.total_reclaimable_bytes,
        }
        run.finished_at = datetime.now(timezone.utc)
        session.flush()
        return ArchivePolicyRunExecutionData(
            run=run,
            generated_at=preview.generated_at,
            queued_job_ids=queued_job_ids,
            candidate_count=preview.candidate_count,
            queued_count=0 if report_only else len(queued_job_ids),
            skipped_count=skipped_count,
            total_reclaimable_bytes=preview.total_reclaimable_bytes,
            report_only=report_only,
        )
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = datetime.now(timezone.utc)
        session.flush()
        raise


def latest_archive_policy_run_timestamp(session: Session) -> datetime | None:
    stmt = (
        select(ArchivePolicyRun)
        .order_by(
            ArchivePolicyRun.finished_at.desc().nullslast(),
            ArchivePolicyRun.started_at.desc().nullslast(),
            ArchivePolicyRun.created_at.desc(),
        )
        .limit(1)
    )
    run = session.scalars(stmt).first()
    if run is None:
        return None
    return run.finished_at or run.started_at or run.created_at


def list_archive_policy_runs(session: Session, *, limit: int = 10) -> list[ArchivePolicyRun]:
    stmt = (
        select(ArchivePolicyRun)
        .options(joinedload(ArchivePolicyRun.triggered_by))
        .order_by(
            ArchivePolicyRun.created_at.desc(),
        )
        .limit(max(1, min(limit, 50)))
    )
    return list(session.scalars(stmt).unique())


def latest_archive_policy_run(session: Session) -> ArchivePolicyRun | None:
    runs = list_archive_policy_runs(session, limit=1)
    return runs[0] if runs else None


def try_acquire_archive_policy_lock(session: Session) -> bool:
    return bool(
        session.execute(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": ARCHIVE_POLICY_LOCK_KEY}).scalar()
    )


def release_archive_policy_lock(session: Session) -> None:
    _ = session


def resolve_archive_policy_user(session: Session, *, user_key: str) -> User:
    return get_or_create_user(session, user_key=user_key, display_name=user_key, role="service")
