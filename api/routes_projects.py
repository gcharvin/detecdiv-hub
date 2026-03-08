from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import Project, ProjectLocation
from api.schemas import ProjectDetail, ProjectSummary


router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("", response_model=list[ProjectSummary])
def list_projects(db: Session = Depends(get_db)) -> list[Project]:
    stmt = select(Project).order_by(Project.project_name.asc())
    return list(db.scalars(stmt))


@router.get("/{project_id}", response_model=ProjectDetail)
def get_project(project_id: UUID, db: Session = Depends(get_db)) -> Project:
    stmt = (
        select(Project)
        .options(joinedload(Project.locations).joinedload(ProjectLocation.storage_root))
        .where(Project.id == project_id)
    )
    project = db.scalars(stmt).unique().first()
    if project is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
    return project
