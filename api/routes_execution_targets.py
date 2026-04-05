from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import ExecutionTarget
from api.schemas import ExecutionTargetCreate, ExecutionTargetSummary, ExecutionTargetUpdate


router = APIRouter(prefix="/execution-targets", tags=["execution-targets"])


@router.get("", response_model=list[ExecutionTargetSummary])
def list_execution_targets(
    status_filter: str | None = None,
    db: Session = Depends(get_db),
) -> list[ExecutionTarget]:
    stmt = select(ExecutionTarget).order_by(ExecutionTarget.display_name.asc())
    if status_filter:
        stmt = stmt.where(ExecutionTarget.status == status_filter)
    return list(db.scalars(stmt))


@router.post("", response_model=ExecutionTargetSummary, status_code=status.HTTP_201_CREATED)
def create_execution_target(
    payload: ExecutionTargetCreate,
    db: Session = Depends(get_db),
) -> ExecutionTarget:
    target = ExecutionTarget(
        target_key=payload.target_key,
        display_name=payload.display_name,
        target_kind=payload.target_kind,
        host_name=payload.host_name,
        supports_gpu=payload.supports_gpu,
        supports_matlab=payload.supports_matlab,
        supports_python=payload.supports_python,
        status=payload.status,
        metadata_json=payload.metadata_json,
    )
    db.add(target)
    db.commit()
    db.refresh(target)
    return target


@router.get("/{target_id}", response_model=ExecutionTargetSummary)
def get_execution_target(target_id: UUID, db: Session = Depends(get_db)) -> ExecutionTarget:
    target = db.get(ExecutionTarget, target_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Execution target not found")
    return target


@router.patch("/{target_id}", response_model=ExecutionTargetSummary)
def update_execution_target(
    target_id: UUID,
    payload: ExecutionTargetUpdate,
    db: Session = Depends(get_db),
) -> ExecutionTarget:
    target = db.get(ExecutionTarget, target_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Execution target not found")

    if payload.display_name is not None:
        target.display_name = payload.display_name
    if payload.target_kind is not None:
        target.target_kind = payload.target_kind
    if payload.host_name is not None:
        target.host_name = payload.host_name
    if payload.supports_gpu is not None:
        target.supports_gpu = payload.supports_gpu
    if payload.supports_matlab is not None:
        target.supports_matlab = payload.supports_matlab
    if payload.supports_python is not None:
        target.supports_python = payload.supports_python
    if payload.status is not None:
        target.status = payload.status
    if payload.metadata_json is not None:
        merged = dict(target.metadata_json or {})
        merged.update(payload.metadata_json)
        target.metadata_json = merged

    db.commit()
    db.refresh(target)
    return target
