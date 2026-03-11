from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from api.config import get_settings
from api.services.micromanager_ingest import (
    automatic_micromanager_ingest_config,
    execute_micromanager_ingest_run,
    latest_micromanager_ingest_run_timestamp,
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
    if last_run_at is None:
        return latest_micromanager_ingest_run_timestamp(session)
    return last_run_at


def run_micromanager_ingest_if_due(session: Session, *, last_run_at: datetime | None) -> datetime | None:
    reference_run_at = effective_last_run_at(session, last_run_at=last_run_at)
    if not should_run_micromanager_ingest(last_run_at=reference_run_at):
        return reference_run_at

    if not try_acquire_micromanager_ingest_lock(session):
        LOGGER.info("Micro-Manager ingest skipped because another worker holds the lock")
        return reference_run_at

    try:
        config = automatic_micromanager_ingest_config(get_settings())
        current_user = resolve_micromanager_ingest_user(session, user_key=config.run_as_user_key)
        result = execute_micromanager_ingest_run(
            session,
            config=config,
            triggered_by_user=current_user,
            trigger_mode="scheduled",
            report_only=False,
        )
        LOGGER.info(
            "Micro-Manager ingest run completed: candidates=%s ingested=%s experiments=%s skipped=%s",
            result.candidate_count,
            result.ingested_count,
            result.experiment_count,
            result.skipped_count,
        )
        return result.run.finished_at or datetime.now(timezone.utc)
    finally:
        release_micromanager_ingest_lock(session)
