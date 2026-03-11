from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import or_, select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import ExperimentProject, ExperimentRawLink, ExternalPublicationRecord, Project, RawDataset, User
from api.schemas import (
    ExperimentProjectCreate,
    ExperimentProjectDetail,
    ExperimentProjectSummary,
    ExperimentProjectUpdate,
    PublicationRecordSummary,
    ProjectSummary,
    RawDatasetSummary,
)
from api.services.external_publications import ensure_publication_records
from api.services.users import (
    ensure_experiment_readable,
    ensure_project_readable,
    experiment_access_filter,
    get_current_user,
    get_or_create_user,
    user_can_edit_experiment,
    user_can_edit_project,
)


router = APIRouter(prefix="/experiments", tags=["experiments"])


@router.get("", response_model=list[ExperimentProjectSummary])
def list_experiments(
    owned_only: bool = False,
    search: str | None = None,
    owner_key: str | None = None,
    visibility: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[ExperimentProjectSummary]:
    stmt = (
        select(ExperimentProject)
        .options(
            joinedload(ExperimentProject.owner),
            joinedload(ExperimentProject.raw_links).joinedload(ExperimentRawLink.raw_dataset).joinedload(RawDataset.owner),
            joinedload(ExperimentProject.analysis_projects).joinedload(Project.owner),
            joinedload(ExperimentProject.publication_records),
        )
        .where(experiment_access_filter(current_user))
        .order_by(ExperimentProject.title.asc())
    )
    if owned_only:
        stmt = stmt.where(ExperimentProject.owner_user_id == current_user.id)
    if search:
        pattern = f"%{search.strip()}%"
        stmt = stmt.where(
            or_(
                ExperimentProject.title.ilike(pattern),
                ExperimentProject.experiment_key.ilike(pattern),
                ExperimentProject.summary.ilike(pattern),
            )
        )
    if owner_key:
        stmt = stmt.join(User, ExperimentProject.owner_user_id == User.id).where(User.user_key == owner_key)
    if visibility:
        stmt = stmt.where(ExperimentProject.visibility == visibility)
    stmt = stmt.limit(min(max(limit, 1), 500))
    return [experiment_summary_view(experiment) for experiment in db.scalars(stmt).unique()]


@router.post("", response_model=ExperimentProjectSummary, status_code=status.HTTP_201_CREATED)
def create_experiment(
    payload: ExperimentProjectCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExperimentProjectSummary:
    experiment = ExperimentProject(
        owner_user_id=current_user.id,
        experiment_key=payload.experiment_key,
        title=payload.title,
        visibility=payload.visibility,
        status=payload.status,
        summary=payload.summary,
        metadata_json=payload.metadata_json,
        started_at=payload.started_at,
        ended_at=payload.ended_at,
        last_indexed_at=payload.last_indexed_at,
    )
    db.add(experiment)
    db.flush()
    ensure_publication_records(db, experiment=experiment)
    db.commit()
    db.refresh(experiment)
    return experiment_summary_view(experiment)


@router.get("/{experiment_id}", response_model=ExperimentProjectDetail)
def get_experiment(
    experiment_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExperimentProjectDetail:
    experiment = db.scalars(
        select(ExperimentProject)
        .options(
            joinedload(ExperimentProject.owner),
            joinedload(ExperimentProject.raw_links).joinedload(ExperimentRawLink.raw_dataset).joinedload(RawDataset.owner),
            joinedload(ExperimentProject.analysis_projects).joinedload(Project.owner),
            joinedload(ExperimentProject.publication_records),
        )
        .where(ExperimentProject.id == experiment_id)
    ).unique().first()
    experiment = ensure_experiment_readable(experiment, current_user)
    return experiment_detail_view(experiment)


@router.patch("/{experiment_id}", response_model=ExperimentProjectDetail)
def update_experiment(
    experiment_id: UUID,
    payload: ExperimentProjectUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExperimentProjectDetail:
    experiment = db.scalars(
        select(ExperimentProject)
        .options(
            joinedload(ExperimentProject.owner),
            joinedload(ExperimentProject.raw_links).joinedload(ExperimentRawLink.raw_dataset).joinedload(RawDataset.owner),
            joinedload(ExperimentProject.analysis_projects).joinedload(Project.owner),
            joinedload(ExperimentProject.publication_records),
        )
        .where(ExperimentProject.id == experiment_id)
    ).unique().first()
    experiment = ensure_experiment_readable(experiment, current_user)
    if not user_can_edit_experiment(experiment, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Experiment is not editable")

    if payload.owner_user_key:
        new_owner = get_or_create_user(db, user_key=payload.owner_user_key, display_name=payload.owner_user_key)
        experiment.owner_user_id = new_owner.id
    if payload.title is not None:
        experiment.title = payload.title
    if payload.visibility is not None:
        experiment.visibility = payload.visibility
    if payload.status is not None:
        experiment.status = payload.status
    if payload.summary is not None:
        experiment.summary = payload.summary
    if payload.metadata_json is not None:
        merged = dict(experiment.metadata_json or {})
        merged.update(payload.metadata_json)
        experiment.metadata_json = merged
    if payload.started_at is not None:
        experiment.started_at = payload.started_at
    if payload.ended_at is not None:
        experiment.ended_at = payload.ended_at
    if payload.last_indexed_at is not None:
        experiment.last_indexed_at = payload.last_indexed_at

    db.commit()
    return get_experiment(experiment_id=experiment_id, db=db, current_user=current_user)


@router.post("/{experiment_id}/raw-datasets/{raw_dataset_id}", response_model=ExperimentProjectDetail)
def link_raw_dataset_to_experiment(
    experiment_id: UUID,
    raw_dataset_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExperimentProjectDetail:
    experiment = db.scalars(
        select(ExperimentProject)
        .options(
            joinedload(ExperimentProject.owner),
            joinedload(ExperimentProject.raw_links).joinedload(ExperimentRawLink.raw_dataset).joinedload(RawDataset.owner),
            joinedload(ExperimentProject.analysis_projects).joinedload(Project.owner),
            joinedload(ExperimentProject.publication_records),
        )
        .where(ExperimentProject.id == experiment_id)
    ).unique().first()
    experiment = ensure_experiment_readable(experiment, current_user)
    if not user_can_edit_experiment(experiment, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Experiment is not editable")

    raw_dataset = db.get(RawDataset, raw_dataset_id)
    if raw_dataset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Raw dataset not found")

    existing = db.scalars(
        select(ExperimentRawLink).where(
            ExperimentRawLink.experiment_project_id == experiment_id,
            ExperimentRawLink.raw_dataset_id == raw_dataset_id,
        )
    ).first()
    if existing is None:
        db.add(ExperimentRawLink(experiment_project_id=experiment_id, raw_dataset_id=raw_dataset_id))
        db.commit()

    return get_experiment(experiment_id=experiment_id, db=db, current_user=current_user)


@router.post("/{experiment_id}/detecdiv-projects/{project_id}", response_model=ExperimentProjectDetail)
def link_analysis_project_to_experiment(
    experiment_id: UUID,
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExperimentProjectDetail:
    experiment = db.scalars(
        select(ExperimentProject)
        .options(
            joinedload(ExperimentProject.owner),
            joinedload(ExperimentProject.raw_links).joinedload(ExperimentRawLink.raw_dataset).joinedload(RawDataset.owner),
            joinedload(ExperimentProject.analysis_projects).joinedload(Project.owner),
            joinedload(ExperimentProject.publication_records),
        )
        .where(ExperimentProject.id == experiment_id)
    ).unique().first()
    experiment = ensure_experiment_readable(experiment, current_user)
    if not user_can_edit_experiment(experiment, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Experiment is not editable")

    project = db.scalars(
        select(Project).where(Project.id == project_id, Project.status != "deleted")
    ).first()
    project = ensure_project_readable(project, current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project is not editable")

    project.experiment_project_id = experiment_id
    db.commit()
    return get_experiment(experiment_id=experiment_id, db=db, current_user=current_user)


def experiment_summary_view(experiment: ExperimentProject) -> ExperimentProjectSummary:
    raw_links = list(experiment.raw_links or [])
    analysis_projects = [project for project in experiment.analysis_projects or [] if project.status != "deleted"]
    return ExperimentProjectSummary.model_validate(
        {
            "id": experiment.id,
            "experiment_key": experiment.experiment_key,
            "title": experiment.title,
            "visibility": experiment.visibility,
            "status": experiment.status,
            "summary": experiment.summary,
            "total_raw_bytes": experiment.total_raw_bytes,
            "total_derived_bytes": experiment.total_derived_bytes,
            "raw_dataset_count": len(raw_links),
            "analysis_project_count": len(analysis_projects),
            "metadata_json": summarize_metadata(experiment.metadata_json),
            "owner": experiment.owner,
            "started_at": experiment.started_at,
            "ended_at": experiment.ended_at,
            "last_indexed_at": experiment.last_indexed_at,
            "created_at": experiment.created_at,
            "updated_at": experiment.updated_at,
        }
    )


def experiment_detail_view(experiment: ExperimentProject) -> ExperimentProjectDetail:
    detail = ExperimentProjectDetail.model_validate(experiment_summary_view(experiment).model_dump())
    detail.raw_datasets = [
        raw_dataset_summary_view(link.raw_dataset)
        for link in sorted(
            experiment.raw_links or [],
            key=lambda value: value.created_at or datetime.min.replace(tzinfo=timezone.utc),
        )
        if link.raw_dataset is not None
    ]
    detail.analysis_projects = [
        project_summary_view(project)
        for project in sorted(
            (project for project in experiment.analysis_projects or [] if project.status != "deleted"),
            key=lambda value: value.project_name.lower(),
        )
    ]
    detail.publication_records = [
        publication_record_summary_view(record)
        for record in sorted(
            experiment.publication_records or [],
            key=lambda value: (value.system_key.lower(), value.created_at or datetime.min.replace(tzinfo=timezone.utc)),
        )
    ]
    return detail


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
            "metadata_json": summarize_metadata(raw_dataset.metadata_json),
            "owner": raw_dataset.owner,
            "created_at": raw_dataset.created_at,
            "updated_at": raw_dataset.updated_at,
        }
    )


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
            "metadata_json": summarize_metadata(project.metadata_json),
            "owner": project.owner,
            "created_at": project.created_at,
            "updated_at": project.updated_at,
        }
    )


def publication_record_summary_view(record: ExternalPublicationRecord) -> PublicationRecordSummary:
    return PublicationRecordSummary.model_validate(
        {
            "id": record.id,
            "system_key": record.system_key,
            "status": record.status,
            "external_id": record.external_id,
            "external_url": record.external_url,
            "payload_json": record.payload_json,
            "error_text": record.error_text,
            "last_attempt_at": record.last_attempt_at,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }
    )


def summarize_metadata(metadata_json: dict | None) -> dict:
    metadata = dict(metadata_json or {})
    inventory = metadata.get("inventory")
    if not isinstance(inventory, dict):
        return metadata

    trimmed_inventory = dict(inventory)
    for key in ("pipeline_runs", "classifier_runs", "processor_runs"):
        records = inventory.get(key)
        if isinstance(records, list):
            trimmed_inventory[f"{key}_count"] = len(records)
            trimmed_inventory.pop(key, None)
    metadata["inventory"] = trimmed_inventory
    return metadata
