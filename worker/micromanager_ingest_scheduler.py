from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.models import ExecutionTarget, Job
from api.services.micromanager_ingest import (
    MicroManagerLandingRootData,
    automatic_micromanager_ingest_config,
    execute_micromanager_ingest_run,
    latest_micromanager_ingest_run_timestamp,
    list_user_micromanager_landing_roots,
    release_micromanager_ingest_lock,
    resolve_micromanager_ingest_user,
    try_acquire_micromanager_ingest_lock,
)


LOGGER = logging.getLogger("detecdiv-hub-worker")


def should_run_micromanager_ingest(*, last_run_at: datetime | None, now: datetime | None = None) -> bool:
    config = automatic_micromanager_ingest_config(get_settings())
    if not config.enabled:
        return False
    now = now or datetime.now(timezone.utc)
    if last_run_at is None:
        return True
    interval = timedelta(minutes=max(1, config.interval_minutes))
    return now - last_run_at >= interval


def effective_last_run_at(session: Session, *, last_run_at: datetime | None) -> datetime | None:
    latest_run_at = latest_micromanager_ingest_run_timestamp(session)
    if last_run_at is None:
        return latest_run_at
    if latest_run_at is None:
        return last_run_at
    return max(last_run_at, latest_run_at)


def run_micromanager_ingest_if_due(
    session: Session,
    *,
    last_run_at: datetime | None,
    target: ExecutionTarget | None = None,
) -> datetime | None:
    reference_run_at = effective_last_run_at(session, last_run_at=last_run_at)
    if not should_run_micromanager_ingest(last_run_at=reference_run_at):
        return reference_run_at

    existing_job = session.scalars(
        select(Job)
        .where(
            Job.params_json["job_kind"].as_string() == "micromanager_ingest",
            Job.status.in_(("queued", "running", "cancelling")),
        )
        .limit(1)
    ).first()
    if existing_job is not None:
        return datetime.now(timezone.utc)

    if not try_acquire_micromanager_ingest_lock(session):
        session.rollback()
        LOGGER.info("Micro-Manager ingest skipped because another worker holds the lock")
        return datetime.now(timezone.utc)

    try:
        config = automatic_micromanager_ingest_config(get_settings())
        current_user = resolve_micromanager_ingest_user(session, user_key=config.run_as_user_key)
        session.add(
            Job(
                execution_target_id=target.id if target is not None else None,
                requested_mode="server",
                priority=90,
                requested_by=current_user.user_key,
                requested_from_host="micromanager_ingest_scheduler",
                params_json={"job_kind": "micromanager_ingest", "trigger_mode": "scheduled"},
                status="queued",
            )
        )
        session.flush()
        LOGGER.info("Queued scheduled Micro-Manager ingestion on target %s", target.target_key if target else "unassigned")
        return datetime.now(timezone.utc)
    finally:
        release_micromanager_ingest_lock(session)


def execute_micromanager_ingest_job(session: Session, *, job: Job) -> dict:
    if not try_acquire_micromanager_ingest_lock(session):
        raise RuntimeError("Micro-Manager ingestion is already running on another worker or request")

    config = automatic_micromanager_ingest_config(get_settings())
    landing_roots = list_user_micromanager_landing_roots(session)
    if config.landing_root:
        landing_roots.append(
            MicroManagerLandingRootData(
                root_key="configured",
                label="Legacy configured landing root",
                path=config.landing_root,
                source="configured",
            )
        )
    if landing_roots:
        config = replace(config, landing_roots=landing_roots)
    current_user = resolve_micromanager_ingest_user(session, user_key=config.run_as_user_key)
    result = execute_micromanager_ingest_run(
        session,
        config=config,
        triggered_by_user=current_user,
        trigger_mode=str((job.params_json or {}).get("trigger_mode") or "scheduled"),
        report_only=False,
    )
    LOGGER.info(
        "Micro-Manager ingest job %s completed: candidates=%s ingested=%s experiments=%s skipped=%s",
        job.id,
        result.candidate_count,
        result.ingested_count,
        result.experiment_count,
        result.skipped_count,
    )
    return {
        "job_kind": "micromanager_ingest",
        "ingest_run_id": str(result.run.id),
        "candidate_count": result.candidate_count,
        "ingested_count": result.ingested_count,
        "experiment_count": result.experiment_count,
        "skipped_count": result.skipped_count,
        "queued_job_ids": result.queued_job_ids,
        "report_only": result.report_only,
    }
