from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.config import get_settings
from api.db import get_db
from api.models import AcquisitionSession, ExperimentProject, User
from api.schemas import (
    AcquisitionSessionComplete,
    AcquisitionSessionCreate,
    AcquisitionSessionHeartbeat,
    AcquisitionSessionLabguruExperimentRequest,
    AcquisitionSessionLabguruExperimentResult,
    AcquisitionSessionSummary,
    AcquisitionSessionUpdate,
)
from api.services.external_credentials import (
    decrypt_user_credential_token,
    get_user_credential,
    test_user_credential,
)
from api.services.external_eln import (
    external_link_summary_from_publication,
    linked_experiment_summary_view,
    upsert_external_experiment_record,
    upsert_external_publication_link,
)
from api.services.external_eln_clients import LabguruClient
from api.services.acquisition_sessions import (
    build_session_key,
    ensure_acquisition_session_editable,
    ensure_acquisition_session_readable,
    ensure_valid_acquisition_status,
    load_acquisition_session,
    merge_json,
    normalize_landing_relative_path,
    resolve_optional_experiment,
    resolve_optional_raw_dataset,
    resolve_storage_root,
    suggested_landing_relative_path,
)
from api.services.users import get_current_user


router = APIRouter(prefix="/acquisition-sessions", tags=["acquisition-sessions"])


@router.get("", response_model=list[AcquisitionSessionSummary])
def list_acquisition_sessions(
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[AcquisitionSessionSummary]:
    stmt = (
        select(AcquisitionSession)
        .options(
            joinedload(AcquisitionSession.owner),
            joinedload(AcquisitionSession.landing_storage_root),
        )
        .order_by(AcquisitionSession.created_at.desc())
        .limit(min(max(limit, 1), 500))
    )
    if current_user.role not in {"admin", "service"}:
        stmt = stmt.where(AcquisitionSession.owner_user_id == current_user.id)
    if status_filter:
        stmt = stmt.where(AcquisitionSession.status == ensure_valid_acquisition_status(status_filter))
    return [acquisition_session_view(session_record) for session_record in db.scalars(stmt).unique()]


@router.post("", response_model=AcquisitionSessionSummary, status_code=status.HTTP_201_CREATED)
def create_acquisition_session(
    payload: AcquisitionSessionCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AcquisitionSessionSummary:
    landing_root = resolve_storage_root(db, payload.landing_storage_root_name)
    experiment = resolve_optional_experiment(db, payload.experiment_project_id, current_user)
    landing_relative_path = normalize_landing_relative_path(payload.landing_relative_path)
    if landing_root is not None and landing_relative_path is None:
        landing_relative_path = suggested_landing_relative_path(
            user=current_user,
            acquisition_label=payload.acquisition_label,
        )
    session_key = str(payload.session_key or "").strip() or build_session_key(payload.acquisition_label)
    acquisition_session = AcquisitionSession(
        owner_user_id=current_user.id,
        experiment_project_id=experiment.id if experiment is not None else None,
        session_key=session_key,
        acquisition_label=payload.acquisition_label,
        microscope_name=payload.microscope_name,
        status=ensure_valid_acquisition_status(payload.status),
        landing_storage_root_id=landing_root.id if landing_root is not None else None,
        landing_relative_path=landing_relative_path,
        local_spool_path=payload.local_spool_path,
        metadata_json=merge_json({"source_system": "pymmcore-plus"}, payload.metadata_json),
        acquisition_params_json=payload.acquisition_params_json,
    )
    db.add(acquisition_session)
    db.commit()
    return get_acquisition_session(acquisition_session.id, db=db, current_user=current_user)


@router.get("/{acquisition_session_id}", response_model=AcquisitionSessionSummary)
def get_acquisition_session(
    acquisition_session_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AcquisitionSessionSummary:
    acquisition_session = ensure_acquisition_session_readable(
        load_acquisition_session(db, acquisition_session_id),
        current_user,
    )
    return acquisition_session_view(acquisition_session)


@router.patch("/{acquisition_session_id}", response_model=AcquisitionSessionSummary)
def update_acquisition_session(
    acquisition_session_id: UUID,
    payload: AcquisitionSessionUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AcquisitionSessionSummary:
    acquisition_session = ensure_acquisition_session_readable(
        load_acquisition_session(db, acquisition_session_id),
        current_user,
    )
    ensure_acquisition_session_editable(acquisition_session, current_user)

    if payload.acquisition_label is not None:
        acquisition_session.acquisition_label = payload.acquisition_label
    if payload.microscope_name is not None:
        acquisition_session.microscope_name = payload.microscope_name
    if payload.status is not None:
        acquisition_session.status = ensure_valid_acquisition_status(payload.status)
    if payload.landing_storage_root_name is not None:
        landing_root = resolve_storage_root(db, payload.landing_storage_root_name)
        acquisition_session.landing_storage_root_id = landing_root.id if landing_root is not None else None
    if payload.landing_relative_path is not None:
        acquisition_session.landing_relative_path = normalize_landing_relative_path(payload.landing_relative_path)
    if payload.local_spool_path is not None:
        acquisition_session.local_spool_path = payload.local_spool_path
    if payload.experiment_project_id is not None:
        experiment = resolve_optional_experiment(db, payload.experiment_project_id, current_user)
        acquisition_session.experiment_project_id = experiment.id if experiment is not None else None
    if payload.raw_dataset_id is not None:
        raw_dataset = resolve_optional_raw_dataset(db, payload.raw_dataset_id, current_user)
        acquisition_session.raw_dataset_id = raw_dataset.id if raw_dataset is not None else None
    if payload.metadata_json is not None:
        acquisition_session.metadata_json = merge_json(acquisition_session.metadata_json, payload.metadata_json)
    if payload.acquisition_params_json is not None:
        acquisition_session.acquisition_params_json = merge_json(
            acquisition_session.acquisition_params_json,
            payload.acquisition_params_json,
        )

    db.commit()
    return get_acquisition_session(acquisition_session_id, db=db, current_user=current_user)


@router.post("/{acquisition_session_id}/heartbeat", response_model=AcquisitionSessionSummary)
def heartbeat_acquisition_session(
    acquisition_session_id: UUID,
    payload: AcquisitionSessionHeartbeat,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AcquisitionSessionSummary:
    acquisition_session = ensure_acquisition_session_readable(
        load_acquisition_session(db, acquisition_session_id),
        current_user,
    )
    ensure_acquisition_session_editable(acquisition_session, current_user)
    now = datetime.now(timezone.utc)
    acquisition_session.status = ensure_valid_acquisition_status(payload.status)
    acquisition_session.last_seen_at = now
    if acquisition_session.started_at is None and acquisition_session.status in {"acquiring", "transferring"}:
        acquisition_session.started_at = now
    if payload.progress_percent is not None:
        acquisition_session.progress_percent = payload.progress_percent
    if payload.transfer_status is not None:
        acquisition_session.transfer_status = payload.transfer_status
    if payload.metadata_json is not None:
        acquisition_session.metadata_json = merge_json(acquisition_session.metadata_json, payload.metadata_json)
    if payload.acquisition_params_json is not None:
        acquisition_session.acquisition_params_json = merge_json(
            acquisition_session.acquisition_params_json,
            payload.acquisition_params_json,
        )
    if payload.result_json is not None:
        acquisition_session.result_json = merge_json(acquisition_session.result_json, payload.result_json)
    db.commit()
    return get_acquisition_session(acquisition_session_id, db=db, current_user=current_user)


@router.post("/{acquisition_session_id}/complete", response_model=AcquisitionSessionSummary)
def complete_acquisition_session(
    acquisition_session_id: UUID,
    payload: AcquisitionSessionComplete,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AcquisitionSessionSummary:
    acquisition_session = ensure_acquisition_session_readable(
        load_acquisition_session(db, acquisition_session_id),
        current_user,
    )
    ensure_acquisition_session_editable(acquisition_session, current_user)

    if payload.status not in {"completed", "failed", "cancelled"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Completion status must be completed, failed, or cancelled",
        )
    if payload.raw_dataset_id is not None:
        raw_dataset = resolve_optional_raw_dataset(db, payload.raw_dataset_id, current_user)
        acquisition_session.raw_dataset_id = raw_dataset.id
    if payload.experiment_project_id is not None:
        experiment = resolve_optional_experiment(db, payload.experiment_project_id, current_user)
        acquisition_session.experiment_project_id = experiment.id
    now = datetime.now(timezone.utc)
    acquisition_session.status = payload.status
    acquisition_session.completed_at = now
    acquisition_session.last_seen_at = now
    acquisition_session.progress_percent = payload.progress_percent
    if payload.transfer_status is not None:
        acquisition_session.transfer_status = payload.transfer_status
    if payload.metadata_json is not None:
        acquisition_session.metadata_json = merge_json(acquisition_session.metadata_json, payload.metadata_json)
    if payload.acquisition_params_json is not None:
        acquisition_session.acquisition_params_json = merge_json(
            acquisition_session.acquisition_params_json,
            payload.acquisition_params_json,
        )
    if payload.result_json is not None:
        acquisition_session.result_json = merge_json(acquisition_session.result_json, payload.result_json)
    acquisition_session.error_text = payload.error_text
    db.commit()
    return get_acquisition_session(acquisition_session_id, db=db, current_user=current_user)


@router.post(
    "/{acquisition_session_id}/labguru-experiment",
    response_model=AcquisitionSessionLabguruExperimentResult,
    status_code=status.HTTP_201_CREATED,
)
def create_labguru_experiment_for_acquisition_session(
    acquisition_session_id: UUID,
    payload: AcquisitionSessionLabguruExperimentRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AcquisitionSessionLabguruExperimentResult:
    acquisition_session = ensure_acquisition_session_readable(
        load_acquisition_session(db, acquisition_session_id),
        current_user,
    )
    ensure_acquisition_session_editable(acquisition_session, current_user)
    title = payload.title.strip()
    if not title:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Labguru experiment title is required")

    credential = get_user_credential(db, user=current_user, system_key="labguru")
    if credential is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No Labguru token stored for this user")
    credential_test = test_user_credential(db, user=current_user, system_key="labguru")
    if credential_test.status != "connected":
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Labguru token is not valid: {credential_test.message}",
        )
    token = decrypt_user_credential_token(credential)
    settings = get_settings()
    client = LabguruClient(base_url=settings.labguru_base_url, token=token, timeout_seconds=30)
    try:
        external_experiment = client.create_experiment(
            title=title,
            description=payload.description,
            procedure=payload.procedure,
            conditions=payload.conditions,
            project_id=payload.project_id,
            folder_id=payload.folder_id,
        )
    except Exception as exc:
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Labguru experiment creation failed: {exc}",
        ) from exc

    synced_at = datetime.now(timezone.utc)
    external_record = upsert_external_experiment_record(
        db,
        system_key="labguru",
        experiment=external_experiment,
        synced_at=synced_at,
    )

    experiment = None
    if acquisition_session.experiment_project_id is not None:
        experiment = db.get(ExperimentProject, acquisition_session.experiment_project_id)
    if experiment is None:
        experiment = db.scalars(
            select(ExperimentProject).where(ExperimentProject.experiment_key == f"labguru:{external_record.external_id}")
        ).first()
    if experiment is None:
        experiment = ExperimentProject(
            owner_user_id=current_user.id,
            experiment_key=f"labguru:{external_record.external_id}",
            title=external_record.title,
            visibility="private",
            status="indexed",
            summary=payload.description,
            started_at=synced_at,
            last_indexed_at=synced_at,
            metadata_json={
                "source": "detecdiv_acquisition_widget",
                "external_source": "labguru",
                "external_id": external_record.external_id,
            },
        )
        db.add(experiment)
        db.flush()

    acquisition_session.experiment_project_id = experiment.id
    metadata = merge_json(
        acquisition_session.metadata_json,
        {
            "labguru": {
                "enabled": True,
                "title": title,
                "description": payload.description,
                "procedure": payload.procedure,
                "conditions": payload.conditions,
                "project_id": payload.project_id,
                "project_name": payload.project_name,
                "folder_id": payload.folder_id,
                "folder_name": payload.folder_name,
                "notes": payload.notes,
                "metadata_json": payload.metadata_json,
                "external_id": external_record.external_id,
                "external_url": external_record.external_url,
            }
        },
    )
    acquisition_session.metadata_json = metadata
    acquisition_session.result_json = merge_json(
        acquisition_session.result_json,
        {
            "labguru": {
                "created": True,
                "external_id": external_record.external_id,
                "external_url": external_record.external_url,
            }
        },
    )
    publication = upsert_external_publication_link(
        db,
        experiment=experiment,
        system_key="labguru",
        external_record=external_record,
    )
    db.commit()
    db.refresh(acquisition_session)
    db.refresh(experiment)
    db.refresh(publication)
    return AcquisitionSessionLabguruExperimentResult(
        acquisition_session=acquisition_session_view(acquisition_session),
        experiment_project=linked_experiment_summary_view(experiment),
        external_link=external_link_summary_from_publication(publication),
    )


def acquisition_session_view(acquisition_session: AcquisitionSession) -> AcquisitionSessionSummary:
    return AcquisitionSessionSummary.model_validate(
        {
            "id": acquisition_session.id,
            "owner": acquisition_session.owner,
            "raw_dataset_id": acquisition_session.raw_dataset_id,
            "experiment_project_id": acquisition_session.experiment_project_id,
            "session_key": acquisition_session.session_key,
            "acquisition_label": acquisition_session.acquisition_label,
            "microscope_name": acquisition_session.microscope_name,
            "status": acquisition_session.status,
            "landing_storage_root": acquisition_session.landing_storage_root,
            "landing_relative_path": acquisition_session.landing_relative_path,
            "local_spool_path": acquisition_session.local_spool_path,
            "transfer_status": acquisition_session.transfer_status,
            "progress_percent": acquisition_session.progress_percent,
            "metadata_json": acquisition_session.metadata_json,
            "acquisition_params_json": acquisition_session.acquisition_params_json,
            "result_json": acquisition_session.result_json,
            "error_text": acquisition_session.error_text,
            "started_at": acquisition_session.started_at,
            "completed_at": acquisition_session.completed_at,
            "last_seen_at": acquisition_session.last_seen_at,
            "created_at": acquisition_session.created_at,
            "updated_at": acquisition_session.updated_at,
        }
    )
