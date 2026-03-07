from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import Job
from api.schemas import JobCreateRequest, JobSummary


router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("", response_model=list[JobSummary])
def list_jobs(db: Session = Depends(get_db)) -> list[Job]:
    stmt = select(Job).order_by(Job.created_at.desc())
    return list(db.scalars(stmt))


@router.post("", response_model=JobSummary, status_code=status.HTTP_201_CREATED)
def create_job(payload: JobCreateRequest, db: Session = Depends(get_db)) -> Job:
    job = Job(
        project_id=payload.project_id,
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


@router.get("/{job_id}", response_model=JobSummary)
def get_job(job_id: str, db: Session = Depends(get_db)) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job

