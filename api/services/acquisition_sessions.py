from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import PurePosixPath

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.models import AcquisitionSession, ExperimentProject, RawDataset, StorageRoot, User
from api.services.project_indexing import slugify
from api.services.users import ensure_experiment_readable, ensure_raw_dataset_readable


ACTIVE_ACQUISITION_STATUSES = {"draft", "acquiring", "transferring"}
TERMINAL_ACQUISITION_STATUSES = {"completed", "failed", "cancelled", "indexed"}
VALID_ACQUISITION_STATUSES = ACTIVE_ACQUISITION_STATUSES | TERMINAL_ACQUISITION_STATUSES


def build_session_key(acquisition_label: str) -> str:
    label = slugify(acquisition_label or "acquisition") or "acquisition"
    return f"acq_{label}_{uuid.uuid4().hex[:10]}"


def suggested_landing_relative_path(*, user: User, acquisition_label: str, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    user_token = slugify(user.user_key or "unknown") or "unknown"
    label_token = slugify(acquisition_label or "acquisition") or "acquisition"
    return f"acquisitions/{user_token}/{now:%Y%m%d}/{label_token}_{uuid.uuid4().hex[:8]}"


def normalize_landing_relative_path(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).replace("\\", "/").strip()
    if not normalized:
        return None
    if normalized.startswith("/"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Landing path must be relative")
    normalized = normalized.rstrip("/")
    if ":" in normalized:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Landing path must be relative")
    path = PurePosixPath(normalized)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Landing path must be a safe relative path")
    return path.as_posix()


def resolve_storage_root(session: Session, storage_root_name: str | None) -> StorageRoot | None:
    name = str(storage_root_name or "").strip()
    if not name:
        return None
    root = session.scalars(select(StorageRoot).where(StorageRoot.name == name)).first()
    if root is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Landing storage root not found")
    return root


def ensure_valid_acquisition_status(value: str) -> str:
    status_value = str(value or "").strip().lower()
    if status_value not in VALID_ACQUISITION_STATUSES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid acquisition status: {value}")
    return status_value


def merge_json(existing: dict | None, update: dict | None) -> dict:
    merged = dict(existing or {})
    if update:
        merged.update(update)
    return merged


def load_acquisition_session(session: Session, acquisition_session_id) -> AcquisitionSession | None:
    return session.scalars(
        select(AcquisitionSession)
        .options(
            joinedload(AcquisitionSession.owner),
            joinedload(AcquisitionSession.landing_storage_root),
            joinedload(AcquisitionSession.raw_dataset),
            joinedload(AcquisitionSession.experiment_project),
        )
        .where(AcquisitionSession.id == acquisition_session_id)
    ).first()


def ensure_acquisition_session_readable(
    acquisition_session: AcquisitionSession | None,
    current_user: User,
) -> AcquisitionSession:
    if acquisition_session is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Acquisition session not found")
    if current_user.role in {"admin", "service"}:
        return acquisition_session
    if acquisition_session.owner_user_id == current_user.id:
        return acquisition_session
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Acquisition session not found")


def ensure_acquisition_session_editable(acquisition_session: AcquisitionSession, current_user: User) -> None:
    if current_user.role in {"admin", "service"}:
        return
    if acquisition_session.owner_user_id == current_user.id:
        return
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Acquisition session is not editable")


def resolve_optional_experiment(session: Session, experiment_project_id, current_user: User) -> ExperimentProject | None:
    if experiment_project_id is None:
        return None
    experiment = session.get(ExperimentProject, experiment_project_id)
    return ensure_experiment_readable(experiment, current_user)


def resolve_optional_raw_dataset(session: Session, raw_dataset_id, current_user: User) -> RawDataset | None:
    if raw_dataset_id is None:
        return None
    raw_dataset = session.get(RawDataset, raw_dataset_id)
    return ensure_raw_dataset_readable(raw_dataset, current_user)
