from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import MicroManagerIngestRun, RawDataset, User
from api.schemas import (
    MicroManagerIngestAutomaticConfig,
    MicroManagerIngestAutomaticStatus,
    MicroManagerIngestCandidateSummary,
    MicroManagerLandingRootSummary,
    MicroManagerIngestRunRequest,
    MicroManagerIngestRunSummary,
)
from api.services.micromanager_ingest import (
    MicroManagerLandingRootData,
    automatic_micromanager_ingest_config,
    build_micromanager_raw_key,
    discover_micromanager_candidates,
    execute_micromanager_ingest_run,
    latest_micromanager_ingest_run,
    list_user_micromanager_landing_roots,
    list_micromanager_ingest_runs,
    release_micromanager_ingest_lock,
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
    landing_roots = micromanager_landing_roots_for_user(db, config=config, current_user=current_user)
    root_summaries = [micromanager_landing_root_summary(root) for root in landing_roots]
    candidate_preview = preview_micromanager_landing_candidates(
        db,
        roots=landing_roots,
        config=config,
        max_candidates=config.max_datasets,
    )
    last_run = latest_micromanager_ingest_run(db)
    recent_runs = list_micromanager_ingest_runs(db, limit=10)
    return MicroManagerIngestAutomaticStatus(
        config=MicroManagerIngestAutomaticConfig.model_validate(config.to_json()),
        checked_at=datetime.now(timezone.utc),
        default_landing_root=next((root for root in root_summaries if root.is_default), root_summaries[0] if root_summaries else None),
        landing_roots=root_summaries,
        candidate_preview=candidate_preview,
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
        landing_roots = micromanager_landing_roots_for_user(db, config=config, current_user=current_user)
        selected_roots = select_micromanager_landing_roots(landing_roots, payload.landing_root_key)
        if payload.landing_root_key and not selected_roots:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Selected landing root is not available")
        if payload.landing_root_override:
            config = replace(
                config,
                landing_root=payload.landing_root_override,
                landing_roots=None,
            )
        elif selected_roots:
            config = replace(
                config,
                landing_root=selected_roots[0].path if len(selected_roots) == 1 else config.landing_root,
                landing_roots=selected_roots,
            )
        if payload.storage_root_name_override:
            config = replace(
                config,
                storage_root_name=(payload.storage_root_name_override or config.storage_root_name),
            )
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


def micromanager_landing_roots_for_user(
    session: Session,
    *,
    config,
    current_user: User,
) -> list[MicroManagerLandingRootData]:
    user_roots = list_user_micromanager_landing_roots(session, default_user_key=current_user.user_key)
    roots: list[MicroManagerLandingRootData] = []
    roots.extend(user_roots)
    if config.landing_root:
        has_default = any(root.is_default for root in roots)
        roots.append(
            MicroManagerLandingRootData(
                root_key="configured",
                label="Legacy configured landing root",
                path=config.landing_root,
                source="configured",
                is_default=not has_default,
            )
        )
    if roots and not any(root.is_default for root in roots):
        roots[0].is_default = True
    return roots


def micromanager_landing_root_summary(root: MicroManagerLandingRootData) -> MicroManagerLandingRootSummary:
    path = Path(root.path).expanduser()
    exists = path.exists()
    accessible = exists and path.is_dir()
    status_text = "ready" if accessible else ("missing" if not exists else "not_directory")
    return MicroManagerLandingRootSummary.model_validate(
        {
            **root.to_json(),
            "exists": exists,
            "accessible": accessible,
            "status": status_text,
        }
    )


def select_micromanager_landing_roots(
    roots: list[MicroManagerLandingRootData],
    root_key: str | None,
) -> list[MicroManagerLandingRootData]:
    key = str(root_key or "").strip()
    if key == "all_user":
        return [root for root in roots if root.source == "user_home"]
    if key:
        return [root for root in roots if root.root_key == key]
    default_root = next((root for root in roots if root.is_default), None)
    return [default_root] if default_root is not None else roots[:1]


def preview_micromanager_landing_candidates(
    session: Session,
    *,
    roots: list[MicroManagerLandingRootData],
    config,
    max_candidates: int,
) -> list[MicroManagerIngestCandidateSummary]:
    previews: list[MicroManagerIngestCandidateSummary] = []
    for root in roots:
        if len(previews) >= max_candidates:
            break
        root_path = Path(root.path).expanduser()
        if not root_path.exists() or not root_path.is_dir():
            continue
        candidates = discover_micromanager_candidates(
            landing_root=root_path.resolve(),
            settle_seconds=config.settle_seconds,
            grouping_window_hours=config.grouping_window_hours,
            max_datasets=max(1, max_candidates - len(previews)),
            owner_user_key=root.user_key,
            landing_root_key=root.root_key,
        )
        for candidate in candidates:
            external_key = build_micromanager_raw_key(candidate.dataset_dir, candidate.relative_path)
            raw_dataset = session.scalars(select(RawDataset).where(RawDataset.external_key == external_key)).first()
            previews.append(
                MicroManagerIngestCandidateSummary.model_validate(
                    {
                        "landing_root_key": candidate.landing_root_key,
                        "landing_root": candidate.landing_root or str(root_path),
                        "relative_path": candidate.relative_path,
                        "dataset_path": str(candidate.dataset_dir),
                        "acquisition_label": candidate.acquisition_label,
                        "owner_user_key": candidate.owner_user_key or root.user_key,
                        "last_modified_at": candidate.last_modified_at,
                        "file_count": candidate.file_count,
                        "completeness_status": candidate.completeness_status,
                        "is_ingested": raw_dataset is not None,
                        "raw_dataset_id": raw_dataset.id if raw_dataset is not None else None,
                    }
                )
            )
            if len(previews) >= max_candidates:
                break
    return previews


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
