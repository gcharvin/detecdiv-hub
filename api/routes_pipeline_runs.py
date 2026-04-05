from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import Job
from api.schemas import PipelineRunCreateRequest, PipelineRunSummary, PipelineRunUpdateRequest


router = APIRouter(prefix="/pipeline-runs", tags=["pipeline-runs"])


@router.get("", response_model=list[PipelineRunSummary])
def list_pipeline_runs(
    project_id: UUID | None = None,
    db: Session = Depends(get_db),
) -> list[Job]:
    stmt = (
        select(Job)
        .where(Job.params_json.contains({"job_kind": "pipeline_run"}))
        .order_by(Job.created_at.desc())
    )
    if project_id is not None:
        stmt = stmt.where(Job.project_id == project_id)
    return list(db.scalars(stmt))


@router.post("", response_model=PipelineRunSummary, status_code=status.HTTP_201_CREATED)
def create_pipeline_run(payload: PipelineRunCreateRequest, db: Session = Depends(get_db)) -> Job:
    params_json = {
        "job_kind": "pipeline_run",
        "project_ref": payload.project_ref,
        "pipeline_ref": payload.pipeline_ref,
        "run_request": payload.run_request,
        "execution": payload.execution,
    }
    job = Job(
        project_id=payload.project_id,
        pipeline_id=payload.pipeline_id,
        execution_target_id=payload.execution_target_id,
        requested_mode=payload.requested_mode,
        priority=payload.priority,
        requested_by=payload.requested_by,
        requested_from_host=payload.requested_from_host,
        params_json=params_json,
        status="queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


@router.get("/{job_id}", response_model=PipelineRunSummary)
def get_pipeline_run(job_id: UUID, db: Session = Depends(get_db)) -> Job:
    job = db.get(Job, job_id)
    if job is None or (job.params_json or {}).get("job_kind") != "pipeline_run":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found")
    return job


@router.patch("/{job_id}", response_model=PipelineRunSummary)
def update_pipeline_run(
    job_id: UUID,
    payload: PipelineRunUpdateRequest,
    db: Session = Depends(get_db),
) -> Job:
    job = db.get(Job, job_id)
    if job is None or (job.params_json or {}).get("job_kind") != "pipeline_run":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found")
    if job.status == "running":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Running pipeline runs cannot be edited")

    if payload.project_id is not None:
        job.project_id = payload.project_id
    if payload.pipeline_id is not None:
        job.pipeline_id = payload.pipeline_id
    if payload.execution_target_id is not None:
        job.execution_target_id = payload.execution_target_id
    if payload.requested_mode is not None:
        job.requested_mode = payload.requested_mode
    if payload.priority is not None:
        job.priority = payload.priority
    if payload.requested_by is not None:
        job.requested_by = payload.requested_by
    if payload.requested_from_host is not None:
        job.requested_from_host = payload.requested_from_host

    params_json = dict(job.params_json or {})
    params_json["job_kind"] = "pipeline_run"
    if payload.project_ref is not None:
        merged = dict(params_json.get("project_ref") or {})
        merged.update(payload.project_ref)
        params_json["project_ref"] = merged
    if payload.pipeline_ref is not None:
        merged = dict(params_json.get("pipeline_ref") or {})
        merged.update(payload.pipeline_ref)
        params_json["pipeline_ref"] = merged
    if payload.run_request is not None:
        merged = dict(params_json.get("run_request") or {})
        merged.update(payload.run_request)
        params_json["run_request"] = merged
    if payload.execution is not None:
        merged = dict(params_json.get("execution") or {})
        merged.update(payload.execution)
        params_json["execution"] = merged
    job.params_json = params_json

    db.commit()
    db.refresh(job)
    return job
