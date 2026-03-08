from sqlalchemy import func, select
from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends

from api.db import get_db
from api.models import Project, ProjectGroup, ProjectNote, User
from api.schemas import DashboardHealthBucket, DashboardSummary
from api.services.users import get_current_user, project_access_filter


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/summary", response_model=DashboardSummary)
def get_dashboard_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DashboardSummary:
    accessible_stmt = select(Project).where(Project.status != "deleted").where(project_access_filter(current_user))
    accessible_projects = list(db.scalars(accessible_stmt))

    total_projects = len(accessible_projects)
    owned_projects = sum(1 for project in accessible_projects if project.owner_user_id == current_user.id)
    public_projects = sum(1 for project in accessible_projects if project.visibility == "public")
    shared_projects = max(total_projects - owned_projects - public_projects, 0)
    total_bytes = sum(int(project.total_bytes or 0) for project in accessible_projects)
    owned_bytes = sum(
        int(project.total_bytes or 0)
        for project in accessible_projects
        if project.owner_user_id == current_user.id
    )

    note_count = db.scalar(
        select(func.count(ProjectNote.id))
        .join(Project, Project.id == ProjectNote.project_id)
        .where(Project.status != "deleted")
        .where(project_access_filter(current_user))
    ) or 0

    group_count = db.scalar(
        select(func.count(ProjectGroup.id)).where(ProjectGroup.owner_user_id == current_user.id)
    ) or 0

    deleted_projects = db.scalar(
        select(func.count(Project.id)).where(Project.owner_user_id == current_user.id, Project.status == "deleted")
    ) or 0

    health_map: dict[str, dict[str, int]] = {}
    for project in accessible_projects:
        bucket = health_map.setdefault(
            project.health_status,
            {"project_count": 0, "total_bytes": 0},
        )
        bucket["project_count"] += 1
        bucket["total_bytes"] += int(project.total_bytes or 0)

    health = [
        DashboardHealthBucket(
            health_status=status_name,
            project_count=values["project_count"],
            total_bytes=values["total_bytes"],
        )
        for status_name, values in sorted(health_map.items(), key=lambda item: item[0])
    ]

    return DashboardSummary(
        user=current_user,
        total_projects=total_projects,
        owned_projects=owned_projects,
        shared_projects=shared_projects,
        public_projects=public_projects,
        total_bytes=total_bytes,
        owned_bytes=owned_bytes,
        note_count=note_count,
        group_count=group_count,
        deleted_projects=deleted_projects,
        health=health,
    )
