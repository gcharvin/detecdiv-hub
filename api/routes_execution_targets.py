import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.db import get_db
from api.models import ExecutionTarget, User
from api.schemas import (
    ExecutionTargetCreate,
    ExecutionTargetSummary,
    ExecutionTargetUpdate,
    ExecutionTargetWorkerScaleRequest,
    ExecutionTargetWorkerScaleResponse,
)
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
    return list(db.scalars(stmt))


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
    return target


@router.patch("/{target_id}", response_model=ExecutionTargetSummary)
def update_execution_target(
    target_id: UUID,
    payload: ExecutionTargetUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExecutionTarget:
    require_admin(current_user)
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


@router.post("/{target_id}/worker-scale", response_model=ExecutionTargetWorkerScaleResponse)
def scale_execution_target_workers(
    target_id: UUID,
    payload: ExecutionTargetWorkerScaleRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExecutionTargetWorkerScaleResponse:
    require_admin(current_user)
    target = db.get(ExecutionTarget, target_id)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Execution target not found")

    settings = get_settings()
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root.parent / "scripts" / "configure_worker_systemd.sh"
    if not script_path.exists():
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Worker systemd helper is missing")

    command = [
        "sudo",
        "-n",
        "bash",
        str(script_path),
        "--repo-root",
        str(repo_root.parent),
        "--service-user",
        settings.systemd_service_user,
        "--env-file",
        settings.systemd_env_file,
        "--unit-dir",
        settings.systemd_unit_dir,
        "--worker-instances",
        str(payload.worker_instances),
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            cwd=str(repo_root.parent),
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        detail = stderr or stdout or "Worker scaling command failed"
        if "sudo" in detail.lower() and ("password" in detail.lower() or "a password is required" in detail.lower()):
            detail = (
                "Worker scaling requires passwordless sudo for scripts/configure_worker_systemd.sh "
                "or equivalent systemctl commands."
            )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail) from exc

    metadata = dict(target.metadata_json or {})
    metadata["worker_instances_desired"] = payload.worker_instances
    metadata["worker_scale_last_applied_at"] = datetime.now(timezone.utc).isoformat()
    metadata["worker_scale_last_message"] = completed.stdout.strip() or None
    target.metadata_json = metadata
    db.commit()
    db.refresh(target)
    return ExecutionTargetWorkerScaleResponse(
        target_id=target.id,
        display_name=target.display_name,
        worker_instances_requested=payload.worker_instances,
        message=f"Configured {payload.worker_instances} worker instance(s) for {target.display_name}.",
        metadata_json=target.metadata_json or {},
    )


def require_admin(user: User) -> None:
    if user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Execution target admin required")
