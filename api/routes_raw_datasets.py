from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import or_, select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import RawDataset, RawDatasetLocation, StorageLifecycleEvent, User
from api.schemas import (
    RawDatasetArchivePolicyPreview,
    RawDatasetArchivePolicyQueueResult,
    RawDatasetArchivePolicyRequest,
    RawDatasetArchivePreview,
    RawDatasetArchiveRequest,
    RawDatasetDetail,
    RawDatasetLocationSummary,
    RawDatasetSummary,
    RawDatasetUpdate,
    StorageLifecycleEventSummary,
)
from api.services.archive_policy import build_archive_policy_preview
from api.services.raw_dataset_lifecycle import (
    RawDatasetLifecycleConflictError,
    build_archive_preview,
    transition_raw_dataset_to_archive,
    transition_raw_dataset_to_restore,
)
from api.services.users import (
    ensure_raw_dataset_readable,
    get_current_user,
    get_or_create_user,
    raw_dataset_access_filter,
    user_can_edit_raw_dataset,
)


router = APIRouter(prefix="/raw-datasets", tags=["raw-datasets"])


@router.get("", response_model=list[RawDatasetSummary])
def list_raw_datasets(
    owned_only: bool = False,
    search: str | None = None,
    owner_key: str | None = None,
    lifecycle_tier: str | None = None,
    archive_status: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[RawDatasetSummary]:
    stmt = (
        select(RawDataset)
        .options(joinedload(RawDataset.owner))
        .where(raw_dataset_access_filter(current_user))
        .order_by(RawDataset.updated_at.desc(), RawDataset.acquisition_label.asc())
    )
    if owned_only:
        stmt = stmt.where(RawDataset.owner_user_id == current_user.id)
    if search:
        pattern = f"%{search.strip()}%"
        stmt = stmt.where(
            or_(
                RawDataset.acquisition_label.ilike(pattern),
                RawDataset.external_key.ilike(pattern),
                RawDataset.microscope_name.ilike(pattern),
            )
        )
    if owner_key:
        stmt = stmt.join(User, RawDataset.owner_user_id == User.id).where(User.user_key == owner_key)
    if lifecycle_tier:
        stmt = stmt.where(RawDataset.lifecycle_tier == lifecycle_tier)
    if archive_status:
        stmt = stmt.where(RawDataset.archive_status == archive_status)
    stmt = stmt.limit(min(max(limit, 1), 500))
    return [raw_dataset_summary_view(raw_dataset) for raw_dataset in db.scalars(stmt).unique()]


@router.get("/{raw_dataset_id}", response_model=RawDatasetDetail)
def get_raw_dataset(
    raw_dataset_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetDetail:
    raw_dataset = db.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.owner),
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.experiment_links),
            joinedload(RawDataset.project_links),
            joinedload(RawDataset.lifecycle_events).joinedload(StorageLifecycleEvent.requested_by),
        )
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    return raw_dataset_detail_view(raw_dataset)


@router.patch("/{raw_dataset_id}", response_model=RawDatasetDetail)
def update_raw_dataset(
    raw_dataset_id: UUID,
    payload: RawDatasetUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetDetail:
    raw_dataset = db.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.owner),
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.experiment_links),
            joinedload(RawDataset.project_links),
            joinedload(RawDataset.lifecycle_events).joinedload(StorageLifecycleEvent.requested_by),
        )
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    if not user_can_edit_raw_dataset(raw_dataset, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Raw dataset is not editable")

    if payload.owner_user_key:
        new_owner = get_or_create_user(db, user_key=payload.owner_user_key, display_name=payload.owner_user_key)
        raw_dataset.owner_user_id = new_owner.id
    if payload.visibility is not None:
        raw_dataset.visibility = payload.visibility
    if payload.lifecycle_tier is not None:
        raw_dataset.lifecycle_tier = payload.lifecycle_tier
    if payload.archive_status is not None:
        raw_dataset.archive_status = payload.archive_status
    if payload.archive_uri is not None:
        raw_dataset.archive_uri = payload.archive_uri
    if payload.archive_compression is not None:
        raw_dataset.archive_compression = payload.archive_compression
    if payload.metadata_json is not None:
        merged = dict(raw_dataset.metadata_json or {})
        merged.update(payload.metadata_json)
        raw_dataset.metadata_json = merged

    db.commit()
    return get_raw_dataset(raw_dataset_id=raw_dataset_id, db=db, current_user=current_user)


@router.post("/{raw_dataset_id}/archive-preview", response_model=RawDatasetArchivePreview)
def preview_raw_dataset_archive(
    raw_dataset_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetArchivePreview:
    raw_dataset = db.scalars(
        select(RawDataset)
        .options(joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root))
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    if not user_can_edit_raw_dataset(raw_dataset, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Raw dataset is not editable")

    preview = build_archive_preview(db, raw_dataset=raw_dataset)
    return RawDatasetArchivePreview(
        raw_dataset_id=raw_dataset.id,
        acquisition_label=raw_dataset.acquisition_label,
        current_tier=raw_dataset.lifecycle_tier,
        target_tier=preview.target_tier,
        reclaimable_bytes=preview.reclaimable_bytes,
        preview_json=preview.preview_json,
    )


@router.post("/archive-policy/preview", response_model=RawDatasetArchivePolicyPreview)
def preview_archive_policy(
    payload: RawDatasetArchivePolicyRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetArchivePolicyPreview:
    preview = build_archive_policy_preview(
        db,
        current_user=current_user,
        older_than_days=payload.older_than_days,
        min_total_bytes=payload.min_total_bytes,
        limit=payload.limit,
        owner_key=payload.owner_key,
        search=payload.search,
        lifecycle_tiers=payload.lifecycle_tiers,
        archive_statuses=payload.archive_statuses,
        archive_uri=payload.archive_uri,
        archive_compression=payload.archive_compression,
    )
    return RawDatasetArchivePolicyPreview(
        generated_at=preview.generated_at,
        older_than_days=payload.older_than_days,
        min_total_bytes=payload.min_total_bytes,
        candidate_count=preview.candidate_count,
        total_candidate_bytes=preview.total_candidate_bytes,
        total_reclaimable_bytes=preview.total_reclaimable_bytes,
        skipped_conflicts=preview.skipped_conflicts,
        candidates=[
            {
                "raw_dataset_id": candidate.raw_dataset.id,
                "acquisition_label": candidate.raw_dataset.acquisition_label,
                "owner": candidate.raw_dataset.owner,
                "lifecycle_tier": candidate.raw_dataset.lifecycle_tier,
                "archive_status": candidate.raw_dataset.archive_status,
                "total_bytes": candidate.raw_dataset.total_bytes,
                "reclaimable_bytes": candidate.reclaimable_bytes,
                "last_activity_at": candidate.last_activity_at,
                "suggested_archive_uri": candidate.suggested_archive_uri,
                "suggested_archive_compression": candidate.suggested_archive_compression,
            }
            for candidate in preview.candidates
        ],
    )


@router.post("/archive-policy/queue", response_model=RawDatasetArchivePolicyQueueResult)
def queue_archive_policy(
    payload: RawDatasetArchivePolicyRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetArchivePolicyQueueResult:
    preview = build_archive_policy_preview(
        db,
        current_user=current_user,
        older_than_days=payload.older_than_days,
        min_total_bytes=payload.min_total_bytes,
        limit=payload.limit,
        owner_key=payload.owner_key,
        search=payload.search,
        lifecycle_tiers=payload.lifecycle_tiers,
        archive_statuses=payload.archive_statuses,
        archive_uri=payload.archive_uri,
        archive_compression=payload.archive_compression,
    )

    queued_job_ids: list[UUID] = []
    raw_dataset_ids: list[UUID] = []
    skipped_count = 0
    for candidate in preview.candidates:
        try:
            event = transition_raw_dataset_to_archive(
                db,
                raw_dataset=candidate.raw_dataset,
                requested_by_user=current_user,
                archive_uri=payload.archive_uri,
                archive_compression=payload.archive_compression,
                mark_archived=payload.mark_archived,
            )
            job_id = event.metadata_json.get("job_id")
            if job_id:
                queued_job_ids.append(UUID(job_id))
            raw_dataset_ids.append(candidate.raw_dataset.id)
        except RawDatasetLifecycleConflictError:
            skipped_count += 1

    db.commit()
    queued_count = len(raw_dataset_ids)
    return RawDatasetArchivePolicyQueueResult(
        generated_at=preview.generated_at,
        queued_count=queued_count,
        skipped_count=skipped_count + preview.skipped_conflicts,
        queued_job_ids=queued_job_ids,
        raw_dataset_ids=raw_dataset_ids,
        message=f"Queued {queued_count} raw dataset archive job(s).",
    )


@router.post("/{raw_dataset_id}/archive", response_model=RawDatasetDetail)
def request_raw_dataset_archive(
    raw_dataset_id: UUID,
    payload: RawDatasetArchiveRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetDetail:
    raw_dataset = db.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.owner),
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.experiment_links),
            joinedload(RawDataset.project_links),
            joinedload(RawDataset.lifecycle_events).joinedload(StorageLifecycleEvent.requested_by),
        )
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    if not user_can_edit_raw_dataset(raw_dataset, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Raw dataset is not editable")

    try:
        transition_raw_dataset_to_archive(
            db,
            raw_dataset=raw_dataset,
            requested_by_user=current_user,
            archive_uri=payload.archive_uri,
            archive_compression=payload.archive_compression,
            mark_archived=payload.mark_archived,
        )
    except RawDatasetLifecycleConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    db.commit()
    return get_raw_dataset(raw_dataset_id=raw_dataset_id, db=db, current_user=current_user)


@router.post("/{raw_dataset_id}/restore", response_model=RawDatasetDetail)
def request_raw_dataset_restore(
    raw_dataset_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetDetail:
    raw_dataset = db.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.owner),
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.experiment_links),
            joinedload(RawDataset.project_links),
            joinedload(RawDataset.lifecycle_events).joinedload(StorageLifecycleEvent.requested_by),
        )
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    if not user_can_edit_raw_dataset(raw_dataset, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Raw dataset is not editable")

    try:
        transition_raw_dataset_to_restore(db, raw_dataset=raw_dataset, requested_by_user=current_user)
    except RawDatasetLifecycleConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    db.commit()
    return get_raw_dataset(raw_dataset_id=raw_dataset_id, db=db, current_user=current_user)


def raw_dataset_summary_view(raw_dataset: RawDataset) -> RawDatasetSummary:
    return RawDatasetSummary.model_validate(
        {
            "id": raw_dataset.id,
            "external_key": raw_dataset.external_key,
            "microscope_name": raw_dataset.microscope_name,
            "acquisition_label": raw_dataset.acquisition_label,
            "visibility": raw_dataset.visibility,
            "status": raw_dataset.status,
            "completeness_status": raw_dataset.completeness_status,
            "lifecycle_tier": raw_dataset.lifecycle_tier,
            "archive_status": raw_dataset.archive_status,
            "archive_uri": raw_dataset.archive_uri,
            "archive_compression": raw_dataset.archive_compression,
            "reclaimable_bytes": raw_dataset.reclaimable_bytes,
            "last_accessed_at": raw_dataset.last_accessed_at,
            "total_bytes": raw_dataset.total_bytes,
            "metadata_json": raw_dataset.metadata_json or {},
            "owner": raw_dataset.owner,
            "created_at": raw_dataset.created_at,
            "updated_at": raw_dataset.updated_at,
        }
    )


def raw_dataset_detail_view(raw_dataset: RawDataset) -> RawDatasetDetail:
    detail = RawDatasetDetail.model_validate(raw_dataset_summary_view(raw_dataset).model_dump())
    detail.locations = [
        RawDatasetLocationSummary.model_validate(
            {
                "id": location.id,
                "relative_path": location.relative_path,
                "access_mode": location.access_mode,
                "is_preferred": location.is_preferred,
                "storage_root": location.storage_root,
            }
        )
        for location in raw_dataset.locations or []
    ]
    detail.experiment_ids = [link.experiment_project_id for link in raw_dataset.experiment_links or []]
    detail.analysis_project_ids = [link.project_id for link in raw_dataset.project_links or []]
    detail.lifecycle_events = [
        StorageLifecycleEventSummary.model_validate(
            {
                "id": event.id,
                "event_kind": event.event_kind,
                "from_tier": event.from_tier,
                "to_tier": event.to_tier,
                "archive_status": event.archive_status,
                "reclaimable_bytes": event.reclaimable_bytes,
                "metadata_json": event.metadata_json,
                "requested_by": event.requested_by,
                "created_at": event.created_at,
            }
        )
        for event in sorted(
            raw_dataset.lifecycle_events or [],
            key=lambda value: value.created_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
    ]
    return detail
