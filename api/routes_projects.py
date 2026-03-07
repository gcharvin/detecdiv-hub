from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import Project
from api.schemas import ProjectSummary


router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("", response_model=list[ProjectSummary])
def list_projects(db: Session = Depends(get_db)) -> list[Project]:
    stmt = select(Project).order_by(Project.project_name.asc())
    return list(db.scalars(stmt))

