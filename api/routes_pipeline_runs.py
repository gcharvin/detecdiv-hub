from uuid import UUID
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import Job, Project, User
from api.schemas import PipelineRunCreateRequest, PipelineRunSummary, PipelineRunUpdateRequest
from api.services.users import ensure_project_readable, get_current_user, project_access_filter, user_can_edit_project
from worker.pipeline_run_executor import write_cancel_token


router = APIRouter(prefix="/pipeline-runs", tags=["pipeline-runs"])


@router.get("", response_model=list[PipelineRunSummary])
def list_pipeline_runs(
    project_id: UUID | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[Job]:
    stmt = (
        select(Job)
        .where(Job.params_json.contains({"job_kind": "pipeline_run"}))
        .order_by(Job.created_at.desc())
    )
    if project_id is not None:
        ensure_project_readable(db.get(Project, project_id), current_user)
        stmt = stmt.where(Job.project_id == project_id)
    elif current_user.role not in {"admin", "service"}:
        readable_project_ids = select(Project.id).where(Project.status != "deleted").where(project_access_filter(current_user))
        stmt = stmt.where(Job.project_id.in_(readable_project_ids))
    return list(db.scalars(stmt))


@router.post("", response_model=PipelineRunSummary, status_code=status.HTTP_201_CREATED)
def create_pipeline_run(
    payload: PipelineRunCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Job:
    project = ensure_project_readable(db.get(Project, payload.project_id), current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project edit access required")
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
def get_pipeline_run(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Job:
    job = db.get(Job, job_id)
    if job is None or (job.params_json or {}).get("job_kind") != "pipeline_run":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found")
    if job.project_id is not None:
        ensure_project_readable(db.get(Project, job.project_id), current_user)
    elif current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found")
    return job


@router.post("/{job_id}/cancel", response_model=PipelineRunSummary)
def cancel_pipeline_run(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Job:
    job = db.get(Job, job_id)
    if job is None or (job.params_json or {}).get("job_kind") != "pipeline_run":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found")
    if job.project_id is not None:
        project = ensure_project_readable(db.get(Project, job.project_id), current_user)
        if not user_can_edit_project(project, current_user):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project edit access required")
    elif current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found")

    if job.status in {"done", "failed"}:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Cannot cancel a {job.status} pipeline run")
    if job.status == "cancelled":
        return job

    now = datetime.now(timezone.utc)
    result_json = dict(job.result_json or {})
    result_json["status"] = "cancelling" if job.status == "running" else "cancelled"
    result_json["message"] = "Cancellation requested by user."
    result_json["cancel_requested_at"] = now.isoformat()
    job.result_json = result_json

    if job.status == "queued":
        job.status = "cancelled"
        job.finished_at = now
        job.heartbeat_at = now
    else:
        job.status = "cancelling"
        job.heartbeat_at = now
        write_cancel_token(job)

    job.updated_at = now
    db.commit()
    db.refresh(job)
    return job


@router.patch("/{job_id}", response_model=PipelineRunSummary)
def update_pipeline_run(
    job_id: UUID,
    payload: PipelineRunUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Job:
    job = db.get(Job, job_id)
    if job is None or (job.params_json or {}).get("job_kind") != "pipeline_run":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found")
    if job.status == "running":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Running pipeline runs cannot be edited")
    current_project_id = payload.project_id or job.project_id
    if current_project_id is not None:
        project = ensure_project_readable(db.get(Project, current_project_id), current_user)
        if not user_can_edit_project(project, current_user):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project edit access required")
    elif current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found")

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
