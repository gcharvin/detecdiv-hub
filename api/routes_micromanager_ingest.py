from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import MicroManagerIngestRun, User
from api.schemas import (
    MicroManagerIngestAutomaticConfig,
    MicroManagerIngestAutomaticStatus,
    MicroManagerIngestRunRequest,
    MicroManagerIngestRunSummary,
)
from api.services.micromanager_ingest import (
    automatic_micromanager_ingest_config,
    execute_micromanager_ingest_run,
    latest_micromanager_ingest_run,
    list_micromanager_ingest_runs,
    release_micromanager_ingest_lock,
    resolve_micromanager_ingest_user,
    try_acquire_micromanager_ingest_lock,
)
from api.services.users import get_current_user


router = APIRouter(prefix="/micromanager-ingest", tags=["micromanager-ingest"])


def ensure_micromanager_admin(current_user: User) -> None:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required for Micro-Manager ingestion controls",
        )


@router.get("/status", response_model=MicroManagerIngestAutomaticStatus)
def get_micromanager_ingest_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MicroManagerIngestAutomaticStatus:
    ensure_micromanager_admin(current_user)
    config = automatic_micromanager_ingest_config()
    last_run = latest_micromanager_ingest_run(db)
    recent_runs = list_micromanager_ingest_runs(db, limit=10)
    return MicroManagerIngestAutomaticStatus(
        config=MicroManagerIngestAutomaticConfig.model_validate(config.to_json()),
        last_run=micromanager_ingest_run_summary_view(last_run) if last_run is not None else None,
        recent_runs=[micromanager_ingest_run_summary_view(run) for run in recent_runs],
    )


@router.post("/run", response_model=MicroManagerIngestRunSummary)
def run_micromanager_ingest_now(
    payload: MicroManagerIngestRunRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MicroManagerIngestRunSummary:
    ensure_micromanager_admin(current_user)
    if not try_acquire_micromanager_ingest_lock(db):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Micro-Manager ingestion is already running on another worker or request",
        )

    try:
        config = automatic_micromanager_ingest_config()
        result = execute_micromanager_ingest_run(
            db,
            config=config,
            triggered_by_user=current_user,
            trigger_mode="manual",
            report_only=payload.report_only,
        )
        db.commit()
        db.refresh(result.run)
        return micromanager_ingest_run_summary_view(result.run)
    except Exception:
        db.rollback()
        raise
    finally:
        release_micromanager_ingest_lock(db)


def micromanager_ingest_run_summary_view(run: MicroManagerIngestRun) -> MicroManagerIngestRunSummary:
    return MicroManagerIngestRunSummary.model_validate(
        {
            "id": run.id,
            "trigger_mode": run.trigger_mode,
            "status": run.status,
            "report_only": run.report_only,
            "candidate_count": run.candidate_count,
            "ingested_count": run.ingested_count,
            "experiment_count": run.experiment_count,
            "skipped_count": run.skipped_count,
            "error_text": run.error_text,
            "config_json": run.config_json or {},
            "result_json": run.result_json or {},
            "triggered_by": run.triggered_by,
            "created_at": run.created_at,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
        }
    )
