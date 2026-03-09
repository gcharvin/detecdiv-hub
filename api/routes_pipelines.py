from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import Pipeline, User
from api.schemas import PipelineCreate, PipelineSummary, PipelineUpdate
from api.services.users import get_current_user


router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.get("", response_model=list[PipelineSummary])
def list_pipelines(
    search: str | None = None,
    runtime_kind: str | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[Pipeline]:
    _ = current_user
    stmt = select(Pipeline).order_by(Pipeline.display_name.asc(), Pipeline.version.asc())
    if search:
        pattern = f"%{search.strip()}%"
        stmt = stmt.where(
            (Pipeline.display_name.ilike(pattern))
            | (Pipeline.pipeline_key.ilike(pattern))
            | (Pipeline.version.ilike(pattern))
        )
    if runtime_kind:
        stmt = stmt.where(Pipeline.runtime_kind == runtime_kind)
    return list(db.scalars(stmt))


@router.post("", response_model=PipelineSummary, status_code=status.HTTP_201_CREATED)
def create_pipeline(
    payload: PipelineCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Pipeline:
    if current_user.role not in {"admin", "service"}:
        # For now, allow normal users too. This guard documents the future tightening point.
        pass
    pipeline = Pipeline(
        pipeline_key=payload.pipeline_key,
        display_name=payload.display_name,
        version=payload.version,
        runtime_kind=payload.runtime_kind,
        metadata_json=payload.metadata_json,
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    return pipeline


@router.get("/{pipeline_id}", response_model=PipelineSummary)
def get_pipeline(
    pipeline_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Pipeline:
    _ = current_user
    pipeline = db.get(Pipeline, pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    return pipeline


@router.patch("/{pipeline_id}", response_model=PipelineSummary)
def update_pipeline(
    pipeline_id: UUID,
    payload: PipelineUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Pipeline:
    if current_user.role not in {"admin", "service"}:
        # Same future tightening point as create_pipeline.
        pass
    pipeline = db.get(Pipeline, pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    if payload.display_name is not None:
        pipeline.display_name = payload.display_name
    if payload.version is not None:
        pipeline.version = payload.version
    if payload.runtime_kind is not None:
        pipeline.runtime_kind = payload.runtime_kind
    if payload.metadata_json is not None:
        merged_metadata = dict(pipeline.metadata_json or {})
        merged_metadata.update(payload.metadata_json)
        pipeline.metadata_json = merged_metadata

    db.commit()
    db.refresh(pipeline)
    return pipeline
