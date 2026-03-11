from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import ExperimentProject, StorageMigrationBatch, StorageMigrationItem, User
from api.schemas import (
    ExperimentProjectSummary,
    StorageMigrationAttachExistingRequest,
    StorageMigrationItemSummary,
    StorageMigrationItemUpdate,
    StorageMigrationExecuteResponse,
    StorageMigrationPlanCreate,
    StorageMigrationPlanDetail,
    StorageMigrationPlanSummary,
)
from api.services.migration_planning import (
    attach_item_to_existing_experiment,
    create_migration_plan,
    execute_pilot_batch,
    materialize_migration_item,
)
from api.services.users import ensure_experiment_readable, get_current_user, user_can_edit_experiment


router = APIRouter(prefix="/migrations", tags=["migrations"])


@router.get("/plans", response_model=list[StorageMigrationPlanSummary])
def list_migration_plans(
    limit: int = 50,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[StorageMigrationPlanSummary]:
    stmt = (
        select(StorageMigrationBatch)
        .options(joinedload(StorageMigrationBatch.owner))
        .where(StorageMigrationBatch.owner_user_id == current_user.id)
        .order_by(StorageMigrationBatch.created_at.desc())
        .limit(min(max(limit, 1), 200))
    )
    return [StorageMigrationPlanSummary.model_validate(batch) for batch in db.scalars(stmt).unique()]


@router.post("/plans", response_model=StorageMigrationPlanDetail, status_code=status.HTTP_201_CREATED)
def create_plan(
    payload: StorageMigrationPlanCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StorageMigrationPlanDetail:
    try:
        batch = create_migration_plan(db, payload=payload, current_user=current_user)
        db.commit()
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return get_plan(plan_id=batch.id, db=db, current_user=current_user)


@router.get("/plans/{plan_id}", response_model=StorageMigrationPlanDetail)
def get_plan(
    plan_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StorageMigrationPlanDetail:
    batch = db.scalars(
        select(StorageMigrationBatch)
        .options(joinedload(StorageMigrationBatch.owner), joinedload(StorageMigrationBatch.items))
        .where(StorageMigrationBatch.id == plan_id, StorageMigrationBatch.owner_user_id == current_user.id)
    ).unique().first()
    if batch is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration plan not found")
    return StorageMigrationPlanDetail.model_validate(
        {
            **StorageMigrationPlanSummary.model_validate(batch).model_dump(),
            "items": [
                StorageMigrationItemSummary.model_validate(item)
                for item in sorted(batch.items or [], key=lambda value: (value.display_name.lower(), value.id))
            ],
        }
    )


@router.patch("/plans/{plan_id}/items/{item_id}", response_model=StorageMigrationItemSummary)
def update_plan_item(
    plan_id: UUID,
    item_id: int,
    payload: StorageMigrationItemUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StorageMigrationItemSummary:
    batch = db.scalars(
        select(StorageMigrationBatch).where(
            StorageMigrationBatch.id == plan_id,
            StorageMigrationBatch.owner_user_id == current_user.id,
        )
    ).first()
    if batch is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration plan not found")

    item = db.scalars(
        select(StorageMigrationItem).where(
            StorageMigrationItem.id == item_id,
            StorageMigrationItem.batch_id == plan_id,
        )
    ).first()
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration item not found")

    if payload.status is not None:
        item.status = payload.status
    if payload.action is not None:
        item.action = payload.action
    if payload.proposed_experiment_key is not None:
        item.proposed_experiment_key = payload.proposed_experiment_key
    if payload.proposed_project_key is not None:
        item.proposed_project_key = payload.proposed_project_key
    if payload.metadata_json is not None:
        merged = dict(item.metadata_json or {})
        merged.update(payload.metadata_json)
        item.metadata_json = merged

    db.commit()
    db.refresh(item)
    return StorageMigrationItemSummary.model_validate(item)


@router.post("/plans/{plan_id}/items/{item_id}/materialize", response_model=ExperimentProjectSummary)
def materialize_plan_item(
    plan_id: UUID,
    item_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExperimentProjectSummary:
    batch = db.scalars(
        select(StorageMigrationBatch).where(
            StorageMigrationBatch.id == plan_id,
            StorageMigrationBatch.owner_user_id == current_user.id,
        )
    ).first()
    if batch is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration plan not found")

    item = db.scalars(
        select(StorageMigrationItem).where(
            StorageMigrationItem.id == item_id,
            StorageMigrationItem.batch_id == plan_id,
        )
    ).first()
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration item not found")

    experiment = materialize_migration_item(db, batch=batch, item=item, current_user=current_user)
    db.commit()
    db.refresh(experiment)
    return experiment_summary_response(experiment)


@router.post("/plans/{plan_id}/items/{item_id}/attach-existing", response_model=ExperimentProjectSummary)
def attach_item_to_existing(
    plan_id: UUID,
    item_id: int,
    payload: StorageMigrationAttachExistingRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExperimentProjectSummary:
    batch = db.scalars(
        select(StorageMigrationBatch).where(
            StorageMigrationBatch.id == plan_id,
            StorageMigrationBatch.owner_user_id == current_user.id,
        )
    ).first()
    if batch is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration plan not found")

    item = db.scalars(
        select(StorageMigrationItem).where(
            StorageMigrationItem.id == item_id,
            StorageMigrationItem.batch_id == plan_id,
        )
    ).first()
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration item not found")

    experiment = db.scalars(
        select(ExperimentProject).where(ExperimentProject.experiment_key == payload.experiment_key)
    ).first()
    if experiment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment not found")
    experiment = ensure_experiment_readable(experiment, current_user)
    if not user_can_edit_experiment(experiment, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Experiment is not editable")

    experiment = attach_item_to_existing_experiment(
        db,
        batch=batch,
        item=item,
        experiment=experiment,
        fallback_user=current_user,
    )
    db.commit()
    db.refresh(experiment)
    return experiment_summary_response(experiment)


@router.post("/plans/{plan_id}/execute-pilot", response_model=StorageMigrationExecuteResponse)
def execute_plan_pilot(
    plan_id: UUID,
    max_items: int = 25,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StorageMigrationExecuteResponse:
    batch = db.scalars(
        select(StorageMigrationBatch).where(
            StorageMigrationBatch.id == plan_id,
            StorageMigrationBatch.owner_user_id == current_user.id,
        )
    ).first()
    if batch is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration plan not found")

    experiments = execute_pilot_batch(db, batch=batch, current_user=current_user, max_items=min(max(max_items, 1), 100))
    db.commit()
    return StorageMigrationExecuteResponse(
        plan_id=batch.id,
        processed_items=len(experiments),
        experiment_ids=[experiment.id for experiment in experiments],
        message=f"Materialized {len(experiments)} pilot item(s).",
    )


def experiment_summary_response(experiment: ExperimentProject) -> ExperimentProjectSummary:
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
            "raw_dataset_count": len(experiment.raw_links or []),
            "analysis_project_count": len(experiment.analysis_projects or []),
            "metadata_json": experiment.metadata_json,
            "owner": experiment.owner,
            "started_at": experiment.started_at,
            "ended_at": experiment.ended_at,
            "last_indexed_at": experiment.last_indexed_at,
            "created_at": experiment.created_at,
            "updated_at": experiment.updated_at,
        }
    )
