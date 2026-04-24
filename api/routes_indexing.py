from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.config import get_settings
from api.db import get_db
from api.models import User
from api.schemas import (
    IndexJobDetail,
    IndexJobLaunchResponse,
    IndexJobSummary,
    IndexRequest,
    IndexResponse,
)
from api.services.indexing_jobs import (
    create_indexing_job,
    enqueue_indexing_worker_job,
    get_indexing_job_for_user,
    list_indexing_jobs_for_user,
)
from api.services.project_indexing import index_project_root
from api.services.users import get_current_user


router = APIRouter(prefix="/indexing", tags=["indexing"])


@router.post("/jobs", response_model=IndexJobLaunchResponse, status_code=status.HTTP_202_ACCEPTED)
def create_job(
    payload: IndexRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IndexJobLaunchResponse:
    if payload.source_kind != "project_root":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only source_kind=project_root is implemented.",
        )

    job = create_indexing_job(db, payload=payload, current_user=current_user)
    enqueue_indexing_worker_job(db, indexing_job=job, current_user=current_user)
    db.commit()
    job = get_indexing_job_for_user(db, job_id=job.id, current_user=current_user)
    if job is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to reload job.")
    return IndexJobLaunchResponse(
        status="queued",
        launch_mode="worker",
        job=IndexJobSummary.model_validate(job),
        message="Indexing job accepted and queued for worker execution.",
    )


@router.get("/jobs", response_model=list[IndexJobSummary])
def list_jobs(
    limit: int = 20,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[IndexJobSummary]:
    jobs = list_indexing_jobs_for_user(db, current_user=current_user, limit=min(max(limit, 1), 100))
    return [IndexJobSummary.model_validate(job) for job in jobs]


@router.get("/jobs/{job_id}", response_model=IndexJobDetail)
def get_job(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IndexJobDetail:
    job = get_indexing_job_for_user(db, job_id=job_id, current_user=current_user)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Indexing job not found.")
    return IndexJobDetail.model_validate(job)


@router.post("", response_model=IndexResponse)
def request_index(
    payload: IndexRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IndexResponse:
    settings = get_settings()
    if payload.source_kind != "project_root":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only source_kind=project_root is implemented.",
        )

    try:
        result = index_project_root(
            db,
            root_path=payload.source_path,
            storage_root_name=payload.storage_root_name,
            host_scope=payload.host_scope,
            root_type=payload.root_type,
            owner_user_key=payload.owner_user_key or current_user.user_key or settings.default_user_key,
            visibility=payload.visibility,
            clear_existing_for_root=payload.clear_existing_for_root,
        )
        db.commit()
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception:
        db.rollback()
        raise

    return IndexResponse(
        status="completed",
        source_kind=payload.source_kind,
        source_path=result.root_path,
        storage_root_name=result.storage_root_name,
        owner_user_key=result.owner_user_key,
        visibility=result.visibility,
        total_projects=result.total_projects,
        scanned_projects=result.scanned_projects,
        indexed_projects=result.indexed_projects,
        failed_projects=result.failed_projects,
        deleted_projects=result.deleted_projects,
        indexed_pipelines=result.indexed_pipelines,
        failed_pipelines=result.failed_pipelines,
        stale_cleanup_skipped=result.stale_cleanup_skipped,
        message="Project root indexed successfully.",
    )
