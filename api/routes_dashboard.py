from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload

from fastapi import APIRouter, Depends

from api.db import get_db
from api.models import AcquisitionSession, Job, Project, ProjectGroup, ProjectNote, User
from api.schemas import (
    AcquisitionSessionSummary,
    DashboardAcquisitionItem,
    DashboardActivity,
    DashboardHealthBucket,
    DashboardJobItem,
    DashboardSummary,
)
from api.services.users import get_current_user, project_access_filter


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/activity", response_model=DashboardActivity)
def get_dashboard_activity(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> DashboardActivity:
    job_base = (
        select(Job)
        .options(joinedload(Job.project), joinedload(Job.raw_dataset))
        .where(Job.requested_by == current_user.user_key)
    )
    active_jobs = list(
        db.scalars(
            job_base
            .where(Job.status.in_(("queued", "running")))
            .order_by(Job.priority.asc(), Job.created_at.desc())
        ).unique()
    )
    recent_jobs = list(
        db.scalars(
            job_base
            .where(Job.status.not_in(("queued", "running")))
            .order_by(func.coalesce(Job.finished_at, Job.updated_at, Job.created_at).desc())
            .limit(12)
        ).unique()
    )

    active_acquisitions = list(
        db.scalars(
            select(AcquisitionSession)
            .options(
                joinedload(AcquisitionSession.owner),
                joinedload(AcquisitionSession.landing_storage_root),
            )
            .where(
                AcquisitionSession.owner_user_id == current_user.id,
                AcquisitionSession.status.in_(("acquiring", "transferring")),
            )
            .order_by(func.coalesce(AcquisitionSession.last_seen_at, AcquisitionSession.updated_at).desc())
        ).unique()
    )
    recent_failed_acquisitions = list(
        db.scalars(
            select(AcquisitionSession)
            .options(
                joinedload(AcquisitionSession.owner),
                joinedload(AcquisitionSession.landing_storage_root),
            )
            .where(
                AcquisitionSession.owner_user_id == current_user.id,
                AcquisitionSession.status == "failed",
            )
            .order_by(
                func.coalesce(
                    AcquisitionSession.completed_at,
                    AcquisitionSession.last_seen_at,
                    AcquisitionSession.updated_at,
                    AcquisitionSession.created_at,
                ).desc()
            )
            .limit(12)
        ).unique()
    )

    return DashboardActivity(
        active_jobs=[dashboard_job_item(job) for job in active_jobs],
        recent_jobs=[dashboard_job_item(job) for job in recent_jobs],
        active_acquisitions=[AcquisitionSessionSummary.model_validate(item) for item in active_acquisitions],
        recent_failed_acquisitions=[dashboard_acquisition_item(item) for item in recent_failed_acquisitions],
    )


def dashboard_acquisition_item(acquisition: AcquisitionSession) -> DashboardAcquisitionItem:
    summary = AcquisitionSessionSummary.model_validate(acquisition)
    return DashboardAcquisitionItem.model_validate(
        {
            **summary.model_dump(),
            "mda_progress": acquisition_mda_progress(acquisition),
        }
    )


def acquisition_mda_progress(acquisition: AcquisitionSession):
    direct_progress = getattr(acquisition, "mda_progress", None)
    if direct_progress is not None:
        return direct_progress
    for payload in (
        acquisition.result_json,
        acquisition.metadata_json,
        acquisition.acquisition_params_json,
    ):
        progress = find_mda_progress(payload)
        if progress is not None:
            return progress
    return None


def find_mda_progress(payload):
    if isinstance(payload, dict):
        if payload.get("mda_progress") is not None:
            return payload["mda_progress"]
        for value in payload.values():
            progress = find_mda_progress(value)
            if progress is not None:
                return progress
    elif isinstance(payload, list):
        for value in payload:
            progress = find_mda_progress(value)
            if progress is not None:
                return progress
    return None


def dashboard_job_item(job: Job) -> DashboardJobItem:
    params = job.params_json or {}
    job_kind = str(params.get("job_kind") or "background")
    resource_name = None
    if job.project is not None:
        resource_name = job.project.project_name
    elif job.raw_dataset is not None:
        resource_name = job.raw_dataset.acquisition_label
    return DashboardJobItem(
        id=job.id,
        status=job.status,
        job_kind=job_kind,
        resource_name=resource_name,
        project_id=job.project_id,
        raw_dataset_id=job.raw_dataset_id,
        requested_mode=job.requested_mode,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        updated_at=job.updated_at,
        error_text=job.error_text,
    )


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
