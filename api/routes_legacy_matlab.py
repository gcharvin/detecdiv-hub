"""Authenticated submission of published legacy MATLAB routines."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import Job, Project, User
from api.schemas import JobSummary
from api.services.project_locks import ProjectLockConflict, create_server_job_lock
from api.services.users import ensure_project_readable, get_current_user, user_can_edit_project

router = APIRouter(prefix="/legacy-matlab-runs", tags=["legacy-matlab"])


class LegacyMatlabRunRequest(BaseModel):
    project_id: UUID
    routine_path: str
    function_name: str
    arguments: list[Any] = Field(default_factory=list)
    requested_mode: str = "server"
    priority: int = 50


@router.post("", response_model=JobSummary, status_code=status.HTTP_201_CREATED)
def submit_legacy_matlab_run(
    payload: LegacyMatlabRunRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Job:
    project = ensure_project_readable(db.get(Project, payload.project_id), current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project edit access required")
    job = Job(
        project_id=project.id,
        requested_mode=payload.requested_mode,
        priority=payload.priority,
        requested_by=current_user.user_key,
        params_json={
            "job_kind": "legacy_matlab",
            "routine_path": payload.routine_path,
            "function_name": payload.function_name,
            "arguments": payload.arguments,
        },
        status="queued",
    )
    db.add(job)
    db.flush()
    try:
        create_server_job_lock(db, project_id=project.id, job=job, owner=current_user,
            holder_host="detecdiv-client", write_scope="project_update", reason="legacy_matlab")
    except ProjectLockConflict as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Project is locked") from exc
    db.commit()
    db.refresh(job)
    return job
