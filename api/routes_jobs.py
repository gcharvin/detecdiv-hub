from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import Job, RawDatasetPosition, User
from api.schemas import JobCreateRequest, JobSummary
from api.services.users import get_current_user


router = APIRouter(prefix="/jobs", tags=["jobs"])


class JobPurgeQueuedRequest(BaseModel):
    job_kind: str | None = None


class JobPurgeQueuedResult(BaseModel):
    cancelled_count: int
    message: str


@router.get("", response_model=list[JobSummary])
def list_jobs(db: Session = Depends(get_db)) -> list[Job]:
    stmt = select(Job).order_by(Job.created_at.desc())
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
