from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from api.services.backup_policy import (
    execute_backup_policy_run,
    latest_backup_run_timestamp,
    resolve_backup_policy_user,
    try_acquire_backup_policy_lock,
)
from api.services.backup_settings import resolve_backup_runtime_config

LOGGER = logging.getLogger("detecdiv-hub-worker")


def should_run_backup(*, last_run_at: datetime | None, now: datetime | None = None) -> bool:
    # config is loaded inside a session; this is a lightweight pre-check
    return True  # actual enabled/interval check is done inside run_backup_if_due


def run_backup_if_due(session: Session, *, last_run_at: datetime | None) -> datetime | None:
    config = resolve_backup_runtime_config(session)
    if not config.backup_enabled:
        return last_run_at

    now = datetime.now(timezone.utc)
    reference = last_run_at or latest_backup_run_timestamp(session)
    if reference is not None:
        interval = timedelta(minutes=max(1, config.backup_interval_minutes))
        if now - reference < interval:
            return reference

    if not try_acquire_backup_policy_lock(session):
        LOGGER.info("Backup run skipped — another worker holds the lock")
        return reference

    try:
        user = resolve_backup_policy_user(session, user_key=config.backup_run_as_user_key)
        result = execute_backup_policy_run(
            session,
            config=config,
            triggered_by_user=user,
            trigger_mode="scheduled",
        )
        LOGGER.info(
            "Backup policy run done: raw=%s queued, proj=%s queued",
            result.raw_datasets_backed_up,
            result.projects_backed_up,
        )
        return result.run.finished_at or now
    except Exception:
        LOGGER.exception("Backup policy run failed")
        return reference
