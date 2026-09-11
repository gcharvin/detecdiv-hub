from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import UUID

from api.config import get_settings
from api.db import SessionLocal
from api.models import Job, User
from api.services.external_credentials import (
    decrypt_user_credential_token,
    get_user_credential,
)
from api.services.external_eln_clients import LabguruClient
from api.services.labguru_yeast_strains import sync_labguru_yeast_strains

LOGGER = logging.getLogger(__name__)


def run_labguru_yeast_sync_job(job_id: UUID) -> None:
    try:
        with SessionLocal() as session:
            job = session.get(Job, job_id)
            if job is None:
                return
            params = dict(job.params_json or {})
            settings = get_settings()
            token = labguru_token_for_sync_job(session, params=params)
            client = LabguruClient(
                base_url=settings.labguru_base_url,
                token=token,
                timeout_seconds=30,
            )
            result = sync_labguru_yeast_strains(
                session,
                client=client,
                collection_name=str(
                    params.get("collection_name") or settings.labguru_yeast_collection_name
                ),
                since=parse_sync_since(params.get("since")),
                progress_callback=lambda progress: update_labguru_yeast_sync_progress(
                    job_id,
                    progress,
                ),
            )
            now = datetime.now(UTC)
            result["execution_source"] = "api"
            job.status = "done"
            job.resolved_mode = "api"
            job.result_json = result
            job.error_text = None
            job.heartbeat_at = now
            job.finished_at = now
            job.updated_at = now
            session.commit()
    except Exception as exc:  # pragma: no cover - defensive around background execution
        LOGGER.exception("Labguru Yeast strains synchronization %s failed", job_id)
        mark_labguru_yeast_sync_failed(job_id, error_text=str(exc))


def labguru_token_for_sync_job(session, *, params: dict) -> str:
    settings = get_settings()
    credential_user_id = params.get("credential_user_id")
    if credential_user_id:
        user = session.get(User, UUID(str(credential_user_id)))
        if user is None:
            raise ValueError("The Labguru credential owner no longer exists")
        credential = get_user_credential(session, user=user, system_key="labguru")
        if credential is None:
            raise ValueError("The Labguru credential was removed before synchronization started")
        return decrypt_user_credential_token(credential)
    if settings.labguru_enabled and settings.labguru_token.strip():
        return settings.labguru_token
    raise ValueError("No Labguru credential is available to the API")


def update_labguru_yeast_sync_progress(job_id: UUID, progress: dict) -> None:
    try:
        with SessionLocal() as session:
            job = session.get(Job, job_id)
            if job is None or job.status != "running":
                return
            now = datetime.now(UTC)
            job.result_json = {
                "progress": {
                    **progress,
                    "execution_source": "api",
                    "updated_at": now.isoformat(),
                }
            }
            job.heartbeat_at = now
            job.updated_at = now
            session.commit()
    except Exception:  # pragma: no cover - progress must not abort the synchronization
        LOGGER.exception("Could not update Labguru Yeast sync progress for %s", job_id)


def mark_labguru_yeast_sync_failed(job_id: UUID, *, error_text: str) -> None:
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        now = datetime.now(UTC)
        job.status = "failed"
        job.resolved_mode = "api"
        job.error_text = error_text
        job.heartbeat_at = now
        job.finished_at = now
        job.updated_at = now
        session.commit()


def parse_sync_since(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
