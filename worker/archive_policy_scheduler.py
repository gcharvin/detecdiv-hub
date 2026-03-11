from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from api.config import get_settings
from api.services.archive_policy import (
    automatic_archive_policy_config,
    execute_archive_policy_run,
    latest_archive_policy_run_timestamp,
    release_archive_policy_lock,
    resolve_archive_policy_user,
    try_acquire_archive_policy_lock,
)


LOGGER = logging.getLogger("detecdiv-hub-worker")


def should_run_archive_policy(*, last_run_at: datetime | None, now: datetime | None = None) -> bool:
    config = automatic_archive_policy_config(get_settings())
    if not config.enabled:
        return False
    now = now or datetime.now(timezone.utc)
    if last_run_at is None:
        return True
    interval = timedelta(minutes=max(1, config.interval_minutes))
    return now - last_run_at >= interval


def effective_last_run_at(session: Session, *, last_run_at: datetime | None) -> datetime | None:
    if last_run_at is None:
        return latest_archive_policy_run_timestamp(session)
    return last_run_at

def run_archive_policy_if_due(session: Session, *, last_run_at: datetime | None) -> datetime | None:
    reference_run_at = effective_last_run_at(session, last_run_at=last_run_at)
    if not should_run_archive_policy(last_run_at=reference_run_at):
        return reference_run_at

    if not try_acquire_archive_policy_lock(session):
        LOGGER.info("Archive policy run skipped because another worker holds the lock")
        return reference_run_at

    try:
        config = automatic_archive_policy_config(get_settings())
        current_user = resolve_archive_policy_user(session, user_key=config.run_as_user_key)
        result = execute_archive_policy_run(
            session,
            config=config,
            triggered_by_user=current_user,
            trigger_mode="scheduled",
            report_only=False,
        )
        LOGGER.info(
            "Archive policy run completed: candidates=%s queued=%s skipped=%s reclaimable_bytes=%s",
            result.candidate_count,
            result.queued_count,
            result.skipped_count,
            result.total_reclaimable_bytes,
        )
        return result.run.finished_at or datetime.now(timezone.utc)
    finally:
        release_archive_policy_lock(session)
