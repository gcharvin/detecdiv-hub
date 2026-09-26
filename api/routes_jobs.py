from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import Job, RawDatasetPosition, User
from api.schemas import JobCreateRequest, JobSummary
from api.services.job_priority_settings import (
    job_priority_settings_items,
    resolve_job_resource_runtime_config,
    resolve_job_priority_runtime_config,
    update_job_resource_runtime_config,
    update_job_priority_runtime_config,
)
from api.services.users import get_current_user


router = APIRouter(prefix="/jobs", tags=["jobs"])


class JobPurgeQueuedRequest(BaseModel):
    job_kind: str | None = None


class JobPurgeQueuedResult(BaseModel):
    cancelled_count: int
    message: str


class JobPrioritySettingItem(BaseModel):
    job_kind: str
    label: str
    priority: int
    default_priority: int
    cpu_cores: int
    default_cpu_cores: int
    gpu_enabled: bool
    default_gpu_enabled: bool
    gpu_vram_mb: int
    default_gpu_vram_mb: int
    disk_io_units: int
    default_disk_io_units: int


class JobResourceCapacities(BaseModel):
    cpu_cores: int
    gpu_vram_mb: int
    disk_io_units: int


class JobResourceProfileUpdate(BaseModel):
    cpu_cores: int
    gpu_enabled: bool
    gpu_vram_mb: int
    disk_io_units: int


class JobPrioritySettingsStatus(BaseModel):
    items: list[JobPrioritySettingItem]
    capacities: JobResourceCapacities
    lower_values_run_first: bool = True


class JobPrioritySettingsUpdate(BaseModel):
    priorities: dict[str, int] = Field(default_factory=dict)
    resources: dict[str, JobResourceProfileUpdate] = Field(default_factory=dict)
    capacities: JobResourceCapacities | None = None


@router.get("/settings/priorities", response_model=JobPrioritySettingsStatus)
def get_job_priority_settings(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> JobPrioritySettingsStatus:
    del current_user
    priority_config = resolve_job_priority_runtime_config(db)
    resource_config = resolve_job_resource_runtime_config(db)
    return JobPrioritySettingsStatus(
        items=job_priority_settings_items(priority_config, resource_config),
        capacities=JobResourceCapacities(
            cpu_cores=resource_config.cpu_capacity_cores,
            gpu_vram_mb=resource_config.gpu_vram_capacity_mb,
            disk_io_units=resource_config.disk_io_capacity_units,
        ),
    )


@router.patch("/settings/priorities", response_model=JobPrioritySettingsStatus)
def patch_job_priority_settings(
    payload: JobPrioritySettingsUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> JobPrioritySettingsStatus:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required")
    try:
        if payload.priorities:
            update_job_priority_runtime_config(db, updates=payload.priorities)
        if payload.resources or payload.capacities is not None:
            resource_config = update_job_resource_runtime_config(
                db,
                profile_updates={key: value.model_dump() for key, value in payload.resources.items()},
                capacity_updates=payload.capacities.model_dump() if payload.capacities is not None else None,
            )
        else:
            resource_config = resolve_job_resource_runtime_config(db)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    db.commit()
    priority_config = resolve_job_priority_runtime_config(db)
    return JobPrioritySettingsStatus(
        items=job_priority_settings_items(priority_config, resource_config),
        capacities=JobResourceCapacities(
            cpu_cores=resource_config.cpu_capacity_cores,
            gpu_vram_mb=resource_config.gpu_vram_capacity_mb,
            disk_io_units=resource_config.disk_io_capacity_units,
        ),
    )


@router.get("", response_model=list[JobSummary])
def list_jobs(db: Session = Depends(get_db)) -> list[Job]:
    stmt = select(Job).order_by(Job.priority.asc(), Job.created_at.asc())
    return list(db.scalars(stmt))


@router.post("", response_model=JobSummary, status_code=status.HTTP_201_CREATED)
def create_job(payload: JobCreateRequest, db: Session = Depends(get_db)) -> Job:
    job = Job(
        project_id=payload.project_id,
        raw_dataset_id=payload.raw_dataset_id,
        pipeline_id=payload.pipeline_id,
        execution_target_id=payload.execution_target_id,
        requested_mode=payload.requested_mode,
        priority=payload.priority,
        requested_by=payload.requested_by,
        requested_from_host=payload.requested_from_host,
        params_json=payload.params_json,
        status="queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


@router.post("/purge-queued", response_model=JobPurgeQueuedResult)
def purge_queued_jobs(
    payload: JobPurgeQueuedRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> JobPurgeQueuedResult:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required")
    stmt = select(Job).where(Job.status == "queued")
    if payload.job_kind:
        stmt = stmt.where(Job.params_json["job_kind"].as_string() == payload.job_kind)
    jobs = list(db.scalars(stmt))
    now = datetime.now(timezone.utc)
    preview_ds_ids = set()
    for job in jobs:
        job.status = "cancelled"
        job.updated_at = now
        job.finished_at = now
        if (job.params_json or {}).get("job_kind") == "raw_preview_video" and job.raw_dataset_id:
            preview_ds_ids.add(job.raw_dataset_id)
    if preview_ds_ids:
        positions = list(db.scalars(
            select(RawDatasetPosition)
            .where(RawDatasetPosition.raw_dataset_id.in_(preview_ds_ids))
            .where(RawDatasetPosition.preview_status == "queued")
        ))
        for pos in positions:
            pos.preview_status = "missing"
    db.commit()
    return JobPurgeQueuedResult(
        cancelled_count=len(jobs),
        message=f"Cancelled {len(jobs)} queued job(s).",
    )


@router.get("/{job_id}", response_model=JobSummary)
def get_job(job_id: str, db: Session = Depends(get_db)) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job
