from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy import or_, select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import Artifact, Job, Project, ProjectRawLink, RawDataset, RawDatasetLocation, RawDatasetPosition, StorageLifecycleEvent, User
from api.schemas import (
    ArchivePolicyAutomaticConfig,
    ArchivePolicyAutomaticRunRequest,
    ArchivePolicyAutomaticStatus,
    ArchivePolicyRunSummary,
    RawArchiveSettingsConfig,
    RawArchiveSettingsStatus,
    RawArchiveSettingsUpdate,
    RawDatasetArchiveBulkDeleteRequest,
    RawDatasetArchiveBulkDeleteResult,
    RawDatasetArchiveDeleteResult,
    RawDatasetArchivePolicyPreview,
    RawDatasetArchivePolicyQueueResult,
    RawDatasetArchivePolicyRequest,
    RawDatasetArchivePreview,
    RawDatasetArchiveRequest,
    RawDatasetDetail,
    RawDatasetLocationSummary,
    RawDatasetPositionSummary,
    RawDatasetDeletionPreview,
    RawDatasetDeletionRequest,
    RawDatasetDeletionResult,
    RawPreviewVideoQueueRequest,
    RawPreviewVideoQueueResult,
    RawPreviewVideoBulkQueueRequest,
    RawPreviewVideoBulkQueueResult,
    RawPreviewQualityConfig,
    RawPreviewQualitySample,
    RawPreviewQualityStatus,
    RawPreviewQualitySummary,
    RawPreviewQualityUpdate,
    RawDatasetSummary,
    RawDatasetUpdate,
    ProjectSummary,
    StorageLifecycleEventSummary,
)
from api.services.archive_settings import resolve_raw_archive_runtime_config, update_raw_archive_runtime_config
from api.services.raw_archive_delete import (
    RawArchiveDeleteConflictError,
    archive_file_size_bytes,
    delete_raw_dataset_archive_file,
)
from api.services.raw_dataset_deletion import build_raw_dataset_deletion_preview, execute_raw_dataset_deletion
from api.services.archive_policy import (
    automatic_archive_policy_config,
    build_archive_policy_preview,
    execute_archive_policy_run,
    latest_archive_policy_run,
    list_archive_policy_runs,
    release_archive_policy_lock,
    try_acquire_archive_policy_lock,
)
from api.services.raw_dataset_lifecycle import (
    RawDatasetLifecycleConflictError,
    build_archive_preview,
    transition_raw_dataset_to_archive,
    transition_raw_dataset_to_restore,
)
from api.services.path_resolution import compose_storage_path
from api.services.raw_preview_settings import resolve_raw_preview_runtime_config, update_raw_preview_runtime_config
from api.services.users import (
    ensure_raw_dataset_readable,
    get_current_user,
    get_or_create_user,
    raw_dataset_access_filter,
    user_can_edit_raw_dataset,
)


router = APIRouter(prefix="/raw-datasets", tags=["raw-datasets"])


@router.get("/artifacts/{artifact_id}/content")
def get_raw_preview_artifact_content(
    artifact_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    artifact = db.scalars(
        select(Artifact)
        .options(joinedload(Artifact.job))
        .where(Artifact.id == artifact_id)
    ).first()
    if artifact is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact not found")
    job = artifact.job
    if job is None or job.raw_dataset_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact is not linked to a raw dataset")
    raw_dataset = db.get(RawDataset, job.raw_dataset_id)
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    artifact_path = Path(str(artifact.uri or "").strip())
    if not artifact_path.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact content is missing on disk")
    media_type = "video/mp4" if artifact_path.suffix.lower() == ".mp4" else "application/octet-stream"
    return FileResponse(path=artifact_path, media_type=media_type, filename=artifact_path.name)


@router.get("/preview-quality", response_model=RawPreviewQualityStatus)
def get_raw_preview_quality_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawPreviewQualityStatus:
    ensure_archive_policy_admin(current_user)
    config = resolve_raw_preview_runtime_config(db)
    rows = db.execute(
        select(Artifact, Job, RawDataset, RawDatasetPosition)
        .join(Job, Artifact.job_id == Job.id)
        .outerjoin(RawDataset, Job.raw_dataset_id == RawDataset.id)
        .outerjoin(RawDatasetPosition, RawDatasetPosition.preview_artifact_id == Artifact.id)
        .where(Artifact.artifact_kind == "raw_position_preview_mp4")
        .order_by(Artifact.created_at.desc())
        .limit(10)
    ).all()

    samples: list[RawPreviewQualitySample] = []
    widths: list[int] = []
    heights: list[int] = []
    fps_values: list[int] = []
    bitrates: list[float] = []
    for artifact, _job, raw_dataset, position in rows:
        metadata = dict(artifact.metadata_json or {})
        width = _as_int(metadata.get("width"))
        height = _as_int(metadata.get("height"))
        fps = _as_int(metadata.get("fps"))
        frame_count = _as_int(metadata.get("frame_count"))
        duration = _as_float(metadata.get("duration_seconds"))
        file_size = _as_int(metadata.get("file_size_bytes"))
        bitrate = _as_float(metadata.get("bitrate_kbps"))
        if file_size is None:
            absolute_path = str(metadata.get("absolute_path") or "").strip()
            if absolute_path:
                path = Path(absolute_path)
                if path.exists():
                    file_size = int(path.stat().st_size)
        if duration is None and fps and frame_count:
            duration = float(frame_count) / float(fps) if fps > 0 else None
        if bitrate is None and duration and duration > 0 and file_size:
            bitrate = round((float(file_size) * 8.0) / duration / 1000.0, 2)
        if width is not None:
            widths.append(width)
        if height is not None:
            heights.append(height)
        if fps is not None:
            fps_values.append(fps)
        if bitrate is not None:
            bitrates.append(bitrate)
        samples.append(
            RawPreviewQualitySample(
                artifact_id=artifact.id,
                created_at=artifact.created_at,
                raw_dataset_id=raw_dataset.id if raw_dataset is not None else None,
                acquisition_label=raw_dataset.acquisition_label if raw_dataset is not None else None,
                position_key=position.position_key if position is not None else str(metadata.get("position_key") or ""),
                width=width,
                height=height,
                fps=fps,
                frame_count=frame_count,
                duration_seconds=duration,
                file_size_bytes=file_size,
                bitrate_kbps=bitrate,
            )
        )
    summary = RawPreviewQualitySummary(
        sample_count=len(samples),
        avg_width=round(sum(widths) / len(widths), 2) if widths else None,
        avg_height=round(sum(heights) / len(heights), 2) if heights else None,
        avg_fps=round(sum(fps_values) / len(fps_values), 2) if fps_values else None,
        avg_bitrate_kbps=round(sum(bitrates) / len(bitrates), 2) if bitrates else None,
    )
    return RawPreviewQualityStatus(
        config=RawPreviewQualityConfig(
            fps=config.fps,
            frame_mode=config.frame_mode,
            max_frames=config.max_frames,
            max_dimension=config.max_dimension,
            binning_factor=config.binning_factor,
            crf=config.crf,
            preset=config.preset,
            include_existing=config.include_existing,
            artifact_root=config.artifact_root,
            ffmpeg_command=config.ffmpeg_command,
        ),
        summary=summary,
        recent_samples=samples,
    )


@router.get("/settings/archive", response_model=RawArchiveSettingsStatus)
def get_raw_archive_settings_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawArchiveSettingsStatus:
    ensure_archive_policy_admin(current_user)
    config = resolve_raw_archive_runtime_config(db)
    return RawArchiveSettingsStatus(
        config=RawArchiveSettingsConfig(
            archive_root=config.archive_root,
            archive_compression=config.archive_compression,
            delete_hot_source=config.delete_hot_source,
        )
    )


@router.patch("/settings/archive", response_model=RawArchiveSettingsStatus)
def update_raw_archive_settings_status(
    payload: RawArchiveSettingsUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawArchiveSettingsStatus:
    ensure_archive_policy_admin(current_user)
    updates = payload.model_dump(exclude_unset=True)
    if updates:
        update_raw_archive_runtime_config(db, updates=updates)
        db.commit()
    return get_raw_archive_settings_status(db=db, current_user=current_user)


@router.patch("/preview-quality", response_model=RawPreviewQualityStatus)
def update_raw_preview_quality_status(
    payload: RawPreviewQualityUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawPreviewQualityStatus:
    ensure_archive_policy_admin(current_user)
    updates = payload.model_dump(exclude_none=True)
    if updates:
        update_raw_preview_runtime_config(db, updates=updates)
        db.commit()
    return get_raw_preview_quality_status(db=db, current_user=current_user)


def ensure_archive_policy_admin(current_user: User) -> None:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required for automatic archive policy controls",
        )


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
            joinedload(RawDataset.positions).joinedload(RawDatasetPosition.preview_artifact).joinedload(Artifact.job),
            joinedload(RawDataset.experiment_links),
            joinedload(RawDataset.project_links).joinedload(ProjectRawLink.project).joinedload(Project.owner),
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
            joinedload(RawDataset.positions).joinedload(RawDatasetPosition.preview_artifact).joinedload(Artifact.job),
            joinedload(RawDataset.experiment_links),
            joinedload(RawDataset.project_links).joinedload(ProjectRawLink.project).joinedload(Project.owner),
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
    if payload.data_format is not None:
        raw_dataset.data_format = payload.data_format
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


@router.post("/{raw_dataset_id}/deletion-preview", response_model=RawDatasetDeletionPreview)
def preview_raw_dataset_deletion(
    raw_dataset_id: UUID,
    payload: RawDatasetDeletionRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetDeletionPreview:
    raw_dataset = db.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.project_links).joinedload(ProjectRawLink.project),
        )
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    if not user_can_edit_raw_dataset(raw_dataset, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Raw dataset is not deletable")

    preview = build_raw_dataset_deletion_preview(
        db,
        raw_dataset=raw_dataset,
        delete_source_files=payload.delete_source_files,
        delete_linked_projects=payload.delete_linked_projects,
        delete_linked_project_files=payload.delete_linked_project_files,
    )
    return RawDatasetDeletionPreview(
        raw_dataset_id=raw_dataset.id,
        acquisition_label=raw_dataset.acquisition_label,
        delete_source_files=payload.delete_source_files,
        delete_linked_projects=payload.delete_linked_projects,
        delete_linked_project_files=payload.delete_linked_project_files,
        reclaimable_bytes=preview.reclaimable_bytes,
        preview_json=preview.preview_json,
    )


@router.delete("/{raw_dataset_id}", response_model=RawDatasetDeletionResult)
def delete_raw_dataset(
    raw_dataset_id: UUID,
    delete_source_files: bool = False,
    delete_linked_projects: bool = False,
    delete_linked_project_files: bool = False,
    confirm: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetDeletionResult:
    if not confirm:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Deletion requires confirm=true")

    raw_dataset = db.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.owner),
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.project_links).joinedload(ProjectRawLink.project),
        )
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    if not user_can_edit_raw_dataset(raw_dataset, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Raw dataset is not deletable")

    preview = build_raw_dataset_deletion_preview(
        db,
        raw_dataset=raw_dataset,
        delete_source_files=delete_source_files,
        delete_linked_projects=delete_linked_projects,
        delete_linked_project_files=delete_linked_project_files,
    )
    result = execute_raw_dataset_deletion(db, preview=preview, requested_by_user=current_user)
    db.commit()
    return RawDatasetDeletionResult(
        raw_dataset_id=raw_dataset_id,
        status=result["status"],
        reclaimable_bytes=result["reclaimable_bytes"],
        result_json=result["result_json"],
    )


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


@router.get("/archive-policy/automatic", response_model=ArchivePolicyAutomaticStatus)
def get_automatic_archive_policy_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ArchivePolicyAutomaticStatus:
    ensure_archive_policy_admin(current_user)
    config = automatic_archive_policy_config()
    last_run = latest_archive_policy_run(db)
    recent_runs = list_archive_policy_runs(db, limit=10)
    return ArchivePolicyAutomaticStatus(
        config=ArchivePolicyAutomaticConfig.model_validate(config.to_json()),
        last_run=archive_policy_run_summary_view(last_run) if last_run is not None else None,
        recent_runs=[archive_policy_run_summary_view(run) for run in recent_runs],
    )


@router.post("/archive-policy/automatic/run", response_model=ArchivePolicyRunSummary)
def run_automatic_archive_policy_now(
    payload: ArchivePolicyAutomaticRunRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ArchivePolicyRunSummary:
    ensure_archive_policy_admin(current_user)
    if not try_acquire_archive_policy_lock(db):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Automatic archive policy is already running on another worker or request",
        )

    try:
        config = automatic_archive_policy_config()
        result = execute_archive_policy_run(
            db,
            config=config,
            triggered_by_user=current_user,
            trigger_mode="manual",
            report_only=payload.report_only,
        )
        db.commit()
        db.refresh(result.run)
        return archive_policy_run_summary_view(result.run)
    except Exception:
        db.rollback()
        raise
    finally:
        release_archive_policy_lock(db)


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
            joinedload(RawDataset.positions).joinedload(RawDatasetPosition.preview_artifact).joinedload(Artifact.job),
            joinedload(RawDataset.experiment_links),
            joinedload(RawDataset.project_links).joinedload(ProjectRawLink.project).joinedload(Project.owner),
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


@router.delete("/{raw_dataset_id}/archive-file", response_model=RawDatasetArchiveDeleteResult)
def delete_raw_dataset_archive(
    raw_dataset_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetArchiveDeleteResult:
    raw_dataset = db.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.owner),
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.lifecycle_events).joinedload(StorageLifecycleEvent.requested_by),
        )
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    if not user_can_edit_raw_dataset(raw_dataset, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Raw dataset is not editable")
    try:
        result = delete_raw_dataset_archive_file(db, raw_dataset=raw_dataset, requested_by_user=current_user)
    except RawArchiveDeleteConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    db.commit()
    return RawDatasetArchiveDeleteResult.model_validate(result)


@router.post("/archive-files/delete-bulk", response_model=RawDatasetArchiveBulkDeleteResult)
def delete_bulk_raw_dataset_archives(
    payload: RawDatasetArchiveBulkDeleteRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawDatasetArchiveBulkDeleteResult:
    if not payload.raw_dataset_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="raw_dataset_ids is required")
    unique_raw_dataset_ids = list(dict.fromkeys(payload.raw_dataset_ids))
    deleted_raw_dataset_ids: list[UUID] = []
    skipped_raw_dataset_ids: list[UUID] = []
    for raw_dataset_id in unique_raw_dataset_ids:
        raw_dataset = db.scalars(
            select(RawDataset)
            .options(joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root))
            .where(RawDataset.id == raw_dataset_id)
        ).unique().first()
        if raw_dataset is None:
            skipped_raw_dataset_ids.append(raw_dataset_id)
            continue
        try:
            raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
        except HTTPException:
            skipped_raw_dataset_ids.append(raw_dataset_id)
            continue
        if not user_can_edit_raw_dataset(raw_dataset, current_user):
            skipped_raw_dataset_ids.append(raw_dataset_id)
            continue
        try:
            delete_raw_dataset_archive_file(db, raw_dataset=raw_dataset, requested_by_user=current_user)
            deleted_raw_dataset_ids.append(raw_dataset.id)
        except (RawArchiveDeleteConflictError, FileNotFoundError):
            skipped_raw_dataset_ids.append(raw_dataset_id)
    db.commit()
    return RawDatasetArchiveBulkDeleteResult(
        requested_count=len(unique_raw_dataset_ids),
        deleted_count=len(deleted_raw_dataset_ids),
        skipped_count=len(skipped_raw_dataset_ids),
        deleted_raw_dataset_ids=deleted_raw_dataset_ids,
        skipped_raw_dataset_ids=skipped_raw_dataset_ids,
        message=f"Deleted {len(deleted_raw_dataset_ids)} archive file(s).",
    )


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
            joinedload(RawDataset.positions).joinedload(RawDatasetPosition.preview_artifact).joinedload(Artifact.job),
            joinedload(RawDataset.experiment_links),
            joinedload(RawDataset.project_links).joinedload(ProjectRawLink.project).joinedload(Project.owner),
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


@router.post("/{raw_dataset_id}/preview-videos/queue", response_model=RawPreviewVideoQueueResult, status_code=status.HTTP_201_CREATED)
def queue_raw_preview_video(
    raw_dataset_id: UUID,
    payload: RawPreviewVideoQueueRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawPreviewVideoQueueResult:
    raw_dataset = db.scalars(
        select(RawDataset)
        .options(joinedload(RawDataset.positions))
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
    if not user_can_edit_raw_dataset(raw_dataset, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Raw dataset is not editable")

    selected_position: RawDatasetPosition | None = None
    if payload.position_id is not None:
        selected_position = db.scalars(
            select(RawDatasetPosition).where(
                RawDatasetPosition.id == payload.position_id,
                RawDatasetPosition.raw_dataset_id == raw_dataset.id,
            )
        ).first()
        if selected_position is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Raw dataset position not found")
    elif payload.position_key:
        selected_position = db.scalars(
            select(RawDatasetPosition).where(
                RawDatasetPosition.raw_dataset_id == raw_dataset.id,
                RawDatasetPosition.position_key == payload.position_key,
            )
        ).first()
        if selected_position is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Raw dataset position not found")

    params_json = dict(payload.params_json or {})
    params_json.update(
        {
            "job_kind": "raw_preview_video",
            "force": payload.force,
            "position_id": str(selected_position.id) if selected_position is not None else None,
            "position_key": selected_position.position_key if selected_position is not None else payload.position_key,
            "scope": "position" if selected_position is not None or payload.position_key else "dataset",
        }
    )
    job = Job(
        raw_dataset_id=raw_dataset.id,
        requested_mode=payload.requested_mode,
        priority=payload.priority,
        requested_by=current_user.user_key,
        params_json=params_json,
        status="queued",
    )
    db.add(job)
    if selected_position is not None:
        selected_position.preview_status = "queued"
    else:
        for position in raw_dataset.positions or []:
            position.preview_status = "queued"
    db.commit()
    db.refresh(job)
    return RawPreviewVideoQueueResult(
        raw_dataset_id=raw_dataset.id,
        position_id=selected_position.id if selected_position is not None else None,
        position_key=selected_position.position_key if selected_position is not None else payload.position_key,
        job=job,
        message="Raw preview video job queued.",
    )


@router.post("/preview-videos/queue-bulk", response_model=RawPreviewVideoBulkQueueResult, status_code=status.HTTP_201_CREATED)
def queue_bulk_raw_preview_videos(
    payload: RawPreviewVideoBulkQueueRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RawPreviewVideoBulkQueueResult:
    if not payload.raw_dataset_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="raw_dataset_ids is required")

    unique_raw_dataset_ids = list(dict.fromkeys(payload.raw_dataset_ids))
    queued_job_ids: list[UUID] = []
    queued_raw_dataset_ids: list[UUID] = []
    skipped_raw_dataset_ids: list[UUID] = []

    for raw_dataset_id in unique_raw_dataset_ids:
        raw_dataset = db.scalars(
            select(RawDataset)
            .options(joinedload(RawDataset.positions))
            .where(RawDataset.id == raw_dataset_id)
        ).unique().first()
        if raw_dataset is None:
            skipped_raw_dataset_ids.append(raw_dataset_id)
            continue
        try:
            raw_dataset = ensure_raw_dataset_readable(raw_dataset, current_user)
        except HTTPException:
            skipped_raw_dataset_ids.append(raw_dataset_id)
            continue
        if not user_can_edit_raw_dataset(raw_dataset, current_user):
            skipped_raw_dataset_ids.append(raw_dataset_id)
            continue

        params_json = dict(payload.params_json or {})
        params_json.update(
            {
                "job_kind": "raw_preview_video",
                "force": payload.force,
                "position_id": None,
                "position_key": None,
                "scope": "dataset",
            }
        )
        job = Job(
            raw_dataset_id=raw_dataset.id,
            requested_mode=payload.requested_mode,
            priority=payload.priority,
            requested_by=current_user.user_key,
            params_json=params_json,
            status="queued",
        )
        db.add(job)
        for position in raw_dataset.positions or []:
            position.preview_status = "queued"
        db.flush()
        queued_job_ids.append(job.id)
        queued_raw_dataset_ids.append(raw_dataset.id)

    db.commit()
    return RawPreviewVideoBulkQueueResult(
        raw_dataset_count=len(unique_raw_dataset_ids),
        queued_count=len(queued_job_ids),
        skipped_count=len(skipped_raw_dataset_ids),
        raw_dataset_ids=queued_raw_dataset_ids,
        queued_job_ids=queued_job_ids,
        skipped_raw_dataset_ids=skipped_raw_dataset_ids,
        message=f"Queued {len(queued_job_ids)} raw preview dataset job(s).",
    )


def _as_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def raw_dataset_summary_view(raw_dataset: RawDataset) -> RawDatasetSummary:
    return RawDatasetSummary.model_validate(
        {
            "id": raw_dataset.id,
            "external_key": raw_dataset.external_key,
            "microscope_name": raw_dataset.microscope_name,
            "acquisition_label": raw_dataset.acquisition_label,
            "data_format": raw_dataset.data_format,
            "visibility": raw_dataset.visibility,
            "status": raw_dataset.status,
            "completeness_status": raw_dataset.completeness_status,
            "lifecycle_tier": raw_dataset.lifecycle_tier,
            "archive_status": raw_dataset.archive_status,
            "archive_uri": raw_dataset.archive_uri,
            "archive_compression": raw_dataset.archive_compression,
            "archive_file_bytes": archive_file_size_bytes(raw_dataset.archive_uri),
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
                "absolute_path": compose_storage_path(location.storage_root.path_prefix, location.relative_path)
                if location.storage_root is not None
                else None,
                "access_mode": location.access_mode,
                "is_preferred": location.is_preferred,
                "storage_root": location.storage_root,
            }
        )
        for location in raw_dataset.locations or []
    ]
    detail.experiment_ids = [link.experiment_project_id for link in raw_dataset.experiment_links or []]
    detail.analysis_project_ids = [link.project_id for link in raw_dataset.project_links or []]
    detail.analysis_projects = [
        project_summary_view(link.project)
        for link in sorted(raw_dataset.project_links or [], key=lambda value: value.created_at or raw_dataset.created_at)
        if link.project is not None
    ]
    detail.positions = [
        raw_dataset_position_summary_view(position)
        for position in sorted(
            raw_dataset.positions or [],
            key=lambda value: (
                value.position_index if value.position_index is not None else 1_000_000_000,
                value.position_key,
            ),
        )
    ]
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


def project_summary_view(project: Project) -> ProjectSummary:
    return ProjectSummary.model_validate(
        {
            "id": project.id,
            "experiment_project_id": project.experiment_project_id,
            "project_key": project.project_key,
            "project_name": project.project_name,
            "status": project.status,
            "health_status": project.health_status,
            "visibility": project.visibility,
            "fov_count": project.fov_count,
            "roi_count": project.roi_count,
            "classifier_count": project.classifier_count,
            "processor_count": project.processor_count,
            "pipeline_run_count": project.pipeline_run_count,
            "available_raw_count": project.available_raw_count,
            "missing_raw_count": project.missing_raw_count,
            "run_json_count": project.run_json_count,
            "h5_count": project.h5_count,
            "h5_bytes": project.h5_bytes,
            "latest_run_status": project.latest_run_status,
            "latest_run_at": project.latest_run_at,
            "project_mat_bytes": project.project_mat_bytes,
            "project_dir_bytes": project.project_dir_bytes,
            "estimated_raw_bytes": project.estimated_raw_bytes,
            "total_bytes": project.total_bytes,
            "metadata_json": project.metadata_json or {},
            "owner": project.owner,
            "created_at": project.created_at,
            "updated_at": project.updated_at,
        }
    )


def raw_dataset_position_summary_view(position: RawDatasetPosition) -> RawDatasetPositionSummary:
    preview_artifact = None
    if position.preview_artifact is not None:
        preview_artifact = {
            "id": position.preview_artifact.id,
            "job_id": position.preview_artifact.job_id,
            "artifact_kind": position.preview_artifact.artifact_kind,
            "uri": f"/raw-datasets/artifacts/{position.preview_artifact.id}/content",
            "metadata_json": dict(position.preview_artifact.metadata_json or {}),
            "created_at": position.preview_artifact.created_at,
        }
    return RawDatasetPositionSummary.model_validate(
        {
            "id": position.id,
            "raw_dataset_id": position.raw_dataset_id,
            "position_key": position.position_key,
            "display_name": position.display_name,
            "position_index": position.position_index,
            "status": position.status,
            "preview_status": position.preview_status,
            "preview_artifact": preview_artifact,
            "metadata_json": position.metadata_json or {},
            "created_at": position.created_at,
            "updated_at": position.updated_at,
        }
    )


def archive_policy_run_summary_view(run) -> ArchivePolicyRunSummary:
    return ArchivePolicyRunSummary.model_validate(
        {
            "id": run.id,
            "trigger_mode": run.trigger_mode,
            "status": run.status,
            "report_only": run.report_only,
            "candidate_count": run.candidate_count,
            "queued_count": run.queued_count,
            "skipped_count": run.skipped_count,
            "total_reclaimable_bytes": run.total_reclaimable_bytes,
            "error_text": run.error_text,
            "config_json": run.config_json or {},
            "result_json": run.result_json or {},
            "triggered_by": run.triggered_by,
            "created_at": run.created_at,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
        }
    )
