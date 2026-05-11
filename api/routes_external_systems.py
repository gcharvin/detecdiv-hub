from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.config import get_settings
from api.db import get_db
from api.models import ExternalExperimentRecord, ExternalUserRecord, Job, User
from api.schemas import (
    ExternalExperimentRecordSummary,
    ExternalSystemStatus,
    ExternalSystemSyncQueueResult,
    ExternalSystemSyncRequest,
    ExternalUserRecordSummary,
)
from api.services.external_eln import external_system_status, search_external_experiments
from api.services.external_eln_clients import normalize_system_key
from api.services.users import get_current_user


router = APIRouter(prefix="/external-systems", tags=["external-systems"])


@router.get("/{system_key}/status", response_model=ExternalSystemStatus)
def get_external_system_status(
    system_key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExternalSystemStatus:
    _ = current_user
    normalized = normalize_or_400(system_key)
    enabled, configured = external_system_config_state(normalized)
    return external_system_status(db, system_key=normalized, enabled=enabled, configured=configured)


@router.post("/{system_key}/sync", response_model=ExternalSystemSyncQueueResult, status_code=status.HTTP_201_CREATED)
def queue_external_system_sync(
    system_key: str,
    payload: ExternalSystemSyncRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExternalSystemSyncQueueResult:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="External system sync requires admin role")
    normalized = normalize_or_400(system_key)
    enabled, configured = external_system_config_state(normalized)
    if not enabled or not configured:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{normalized} connector is not configured")
    job = Job(
        requested_mode="server",
        priority=payload.priority,
        requested_by=current_user.user_key,
        params_json={"job_kind": "external_eln_sync", "system_key": normalized},
        status="queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return ExternalSystemSyncQueueResult(
        system_key=normalized,
        job_id=job.id,
        status=job.status,
        message=f"Queued {normalized} sync job.",
    )


@router.get("/{system_key}/experiments", response_model=list[ExternalExperimentRecordSummary])
def list_external_experiments(
    system_key: str,
    search: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[ExternalExperimentRecord]:
    _ = current_user
    normalized = normalize_or_400(system_key)
    return search_external_experiments(db, system_key=normalized, search=search, limit=limit)


@router.get("/{system_key}/users", response_model=list[ExternalUserRecordSummary])
def list_external_users(
    system_key: str,
    match_status: str | None = None,
    limit: int = 500,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[ExternalUserRecord]:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User admin required")
    normalized = normalize_or_400(system_key)
    stmt = (
        select(ExternalUserRecord)
        .options(joinedload(ExternalUserRecord.matched_user))
        .where(ExternalUserRecord.system_key == normalized)
        .order_by(ExternalUserRecord.display_name.asc())
        .limit(min(max(limit, 1), 1000))
    )
    if match_status:
        stmt = stmt.where(ExternalUserRecord.match_status == match_status)
    return list(db.scalars(stmt))


@router.patch("/{system_key}/users/{external_user_record_id}", response_model=ExternalUserRecordSummary)
def update_external_user_match(
    system_key: str,
    external_user_record_id: UUID,
    matched_user_id: UUID | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ExternalUserRecord:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User admin required")
    normalized = normalize_or_400(system_key)
    record = db.scalars(
        select(ExternalUserRecord)
        .options(joinedload(ExternalUserRecord.matched_user))
        .where(ExternalUserRecord.id == external_user_record_id, ExternalUserRecord.system_key == normalized)
    ).first()
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="External user record not found")
    if matched_user_id is None:
        record.matched_user_id = None
        record.match_status = "pending"
    else:
        user = db.get(User, matched_user_id)
        if user is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Hub user not found")
        record.matched_user_id = user.id
        record.match_status = "matched"
    db.commit()
    db.refresh(record)
    return record


def normalize_or_400(system_key: str) -> str:
    try:
        return normalize_system_key(system_key)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


def external_system_config_state(system_key: str) -> tuple[bool, bool]:
    settings = get_settings()
    if system_key == "labguru":
        return settings.labguru_enabled, bool(settings.labguru_base_url.strip() and settings.labguru_token.strip())
    if system_key == "elabftw":
        return settings.elabftw_enabled, bool(settings.elabftw_base_url.strip() and settings.elabftw_token.strip())
    return False, False
