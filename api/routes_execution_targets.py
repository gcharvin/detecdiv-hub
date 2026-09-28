from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import ExecutionTarget, User
from api.schemas import (
    ExecutionTargetCreate,
    ExecutionTargetSummary,
    ExecutionTargetUpdate,
    ExecutionTargetWorkerScaleRequest,
    ExecutionTargetWorkerScaleResponse,
)
from api.services.worker_instances import enrich_execution_target
from api.services.users import get_current_user


router = APIRouter(prefix="/execution-targets", tags=["execution-targets"])


@router.get("", response_model=list[ExecutionTargetSummary])
def list_execution_targets(
    status_filter: str | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[ExecutionTarget]:
    _ = current_user
    stmt = select(ExecutionTarget).order_by(ExecutionTarget.display_name.asc())
    if status_filter:
        stmt = stmt.where(ExecutionTarget.status == status_filter)
    return [enrich_execution_target(db, target) for target in db.scalars(stmt)]


@router.post("", response_model=ExecutionTargetSummary, status_code=status.HTTP_201_CREATED)
def create_execution_target(
    payload: ExecutionTargetCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExecutionTarget:
    require_admin(current_user)
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
def get_execution_target(
    target_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExecutionTarget:
    _ = current_user
    target = db.get(ExecutionTarget, target_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Execution target not found")
    return enrich_execution_target(db, target)


@router.patch("/{target_id}", response_model=ExecutionTargetSummary)
def update_execution_target(
    target_id: UUID,
    payload: ExecutionTargetUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExecutionTarget:
    require_admin(current_user)
    target = db.scalars(select(ExecutionTarget).where(ExecutionTarget.id == target_id).with_for_update()).first()
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
    return enrich_execution_target(db, target)


@router.post("/{target_id}/worker-scale", response_model=ExecutionTargetWorkerScaleResponse)
def scale_execution_target_workers(
    target_id: UUID,
    payload: ExecutionTargetWorkerScaleRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExecutionTargetWorkerScaleResponse:
    require_admin(current_user)
    target = db.scalars(select(ExecutionTarget).where(ExecutionTarget.id == target_id).with_for_update()).first()
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Execution target not found")

    metadata = dict(target.metadata_json or {})
    now = datetime.now(timezone.utc)
    try:
        seen = datetime.fromisoformat(str(metadata.get("worker_manager_seen_at") or ""))
        manager_online = metadata.get("worker_manager_enabled") and 0 <= (now - seen).total_seconds() < 30
    except (ValueError, TypeError):
        manager_online = False
    if not manager_online:
        raise HTTPException(status_code=503, detail="Compute-host worker manager is offline; configure it on the worker host before scaling.")
    cpu_budget = int(metadata.get("worker_cpu_capacity") or 36)
    if payload.worker_instances > cpu_budget:
        raise HTTPException(status_code=422, detail=f"At most {cpu_budget} workers fit the compute CPU budget.")
    if metadata.get("worker_autoscale_enabled", True):
        metadata["worker_autoscale_enabled"] = True
        metadata["worker_instances_baseline"] = payload.worker_instances
        metadata["worker_scale_state"] = "automatic"
        target.metadata_json = metadata
        db.commit()
        db.refresh(target)
        return ExecutionTargetWorkerScaleResponse(
            target_id=target.id, display_name=target.display_name,
            worker_instances_requested=payload.worker_instances,
            message=f"Automatic workers: baseline {payload.worker_instances}; pool grows with admissible queued jobs within CPU/RAM/VRAM/disk budgets.",
            metadata_json=target.metadata_json or {},
        )
    metadata.setdefault("worker_scale_previous_drain", bool(metadata.get("drain_new_jobs")))
    metadata["drain_new_jobs"] = True
    metadata["worker_instances_desired"] = payload.worker_instances
    metadata["worker_scale_requested_at"] = now.isoformat()
    metadata["worker_scale_state"] = "requested"
    metadata.pop("worker_scale_failed_request", None)
    target.metadata_json = metadata
    db.commit()
    db.refresh(target)
    return ExecutionTargetWorkerScaleResponse(
        target_id=target.id,
        display_name=target.display_name,
        worker_instances_requested=payload.worker_instances,
        message=f"Requested {payload.worker_instances} worker instance(s) for {target.display_name}; the compute host applies the change after active jobs finish.",
        metadata_json=target.metadata_json or {},
    )


def require_admin(user: User) -> None:
    if user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Execution target admin required")
