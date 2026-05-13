from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, joinedload

from api.models import (
    ExperimentProject,
    ExperimentRawLink,
    ExternalExperimentRecord,
    ExternalPublicationRecord,
    ExternalUserRecord,
    RawDataset,
    User,
)
from api.schemas import (
    ExternalLinkSummary,
    ExternalSystemStatus,
    ExternalSystemSyncResult,
    LinkedExperimentSummary,
    PublicationRecordSummary,
)
from api.services.external_eln_clients import (
    ExternalElnClient,
    ExternalElnExperiment,
    ExternalElnUser,
    build_external_eln_client,
    extract_labguru_text_sections,
    normalize_name,
    normalize_system_key,
)


def sync_external_eln_system(
    session: Session,
    *,
    system_key: str,
    client: ExternalElnClient | None = None,
) -> ExternalSystemSyncResult:
    normalized = normalize_system_key(system_key)
    client = client or build_external_eln_client(normalized)
    synced_at = datetime.now(timezone.utc)
    experiments = client.list_experiments()
    for experiment in experiments:
        upsert_external_experiment_record(session, system_key=normalized, experiment=experiment, synced_at=synced_at)

    users = client.list_users_or_observed_members()
    matched_user_count = 0
    pending_user_count = 0
    for user in users:
        record = upsert_external_user_record(session, system_key=normalized, user=user, synced_at=synced_at)
        if record.match_status == "matched":
            matched_user_count += 1
        else:
            pending_user_count += 1

    return ExternalSystemSyncResult(
        system_key=normalized,
        experiment_count=len(experiments),
        user_count=len(users),
        matched_user_count=matched_user_count,
        pending_user_count=pending_user_count,
    )


def upsert_external_experiment_record(
    session: Session,
    *,
    system_key: str,
    experiment: ExternalElnExperiment,
    synced_at: datetime,
) -> ExternalExperimentRecord:
    record = session.scalars(
        select(ExternalExperimentRecord).where(
            ExternalExperimentRecord.system_key == system_key,
            ExternalExperimentRecord.external_id == experiment.external_id,
        )
    ).first()
    if record is None:
        record = ExternalExperimentRecord(
            system_key=system_key,
            external_id=experiment.external_id,
        )
        session.add(record)
    record.title = experiment.title
    record.external_url = experiment.external_url
    record.owner_name = experiment.owner_name
    record.started_at = experiment.started_at
    record.updated_external_at = experiment.updated_external_at
    record.payload_json = experiment.payload_json
    record.last_synced_at = synced_at
    record.updated_at = synced_at
    session.flush()
    return record


def upsert_external_user_record(
    session: Session,
    *,
    system_key: str,
    user: ExternalElnUser,
    synced_at: datetime,
) -> ExternalUserRecord:
    record = session.scalars(
        select(ExternalUserRecord).where(
            ExternalUserRecord.system_key == system_key,
            ExternalUserRecord.external_id == user.external_id,
        )
    ).first()
    if record is None:
        record = ExternalUserRecord(
            system_key=system_key,
            external_id=user.external_id,
        )
        session.add(record)
    record.display_name = user.display_name
    record.email = user.email
    record.payload_json = user.payload_json
    matched_user, status = match_external_user_by_name(session, user.display_name)
    record.matched_user_id = matched_user.id if matched_user is not None else None
    record.match_status = status
    record.last_synced_at = synced_at
    record.updated_at = synced_at
    session.flush()
    return record


def match_external_user_by_name(session: Session, display_name: str) -> tuple[User | None, str]:
    users = session.scalars(select(User).where(User.is_active.is_(True))).all()
    return select_unique_user_match(users, display_name)


def select_unique_user_match(users: list[User], display_name: str) -> tuple[User | None, str]:
    normalized = normalize_name(display_name)
    if not normalized:
        return None, "pending"
    candidates = [
        user
        for user in users
        if normalize_name(user.display_name) == normalized or normalize_name(user.user_key) == normalized
    ]
    if len(candidates) == 1:
        return candidates[0], "matched"
    if len(candidates) > 1:
        return None, "ambiguous"
    return None, "pending"


def external_system_status(session: Session, *, system_key: str, enabled: bool, configured: bool) -> ExternalSystemStatus:
    normalized = normalize_system_key(system_key)
    experiment_count = session.scalar(
        select(func.count(ExternalExperimentRecord.id)).where(ExternalExperimentRecord.system_key == normalized)
    )
    user_count = session.scalar(
        select(func.count(ExternalUserRecord.id)).where(ExternalUserRecord.system_key == normalized)
    )
    linked_experiment_count = session.scalar(
        select(func.count(ExternalPublicationRecord.id)).where(
            ExternalPublicationRecord.system_key == normalized,
            ExternalPublicationRecord.status == "linked",
        )
    )
    latest_sync_at = session.scalar(
        select(func.max(ExternalExperimentRecord.last_synced_at)).where(
            ExternalExperimentRecord.system_key == normalized
        )
    )
    return ExternalSystemStatus(
        system_key=normalized,
        enabled=enabled,
        configured=configured,
        experiment_count=int(experiment_count or 0),
        user_count=int(user_count or 0),
        linked_experiment_count=int(linked_experiment_count or 0),
        latest_sync_at=latest_sync_at,
    )


def search_external_experiments(
    session: Session,
    *,
    system_key: str,
    search: str | None,
    limit: int,
) -> list[ExternalExperimentRecord]:
    normalized = normalize_system_key(system_key)
    stmt = (
        select(ExternalExperimentRecord)
        .where(ExternalExperimentRecord.system_key == normalized)
        .order_by(ExternalExperimentRecord.updated_external_at.desc().nullslast(), ExternalExperimentRecord.title.asc())
        .limit(min(max(limit, 1), 500))
    )
    if search:
        pattern = f"%{search.strip()}%"
        stmt = stmt.where(
            or_(
                ExternalExperimentRecord.title.ilike(pattern),
                ExternalExperimentRecord.external_id.ilike(pattern),
                ExternalExperimentRecord.owner_name.ilike(pattern),
            )
        )
    return list(session.scalars(stmt))


def link_raw_dataset_to_external_experiment(
    session: Session,
    *,
    raw_dataset: RawDataset,
    system_key: str,
    external_experiment_id: str,
) -> tuple[ExperimentProject, ExternalPublicationRecord]:
    normalized = normalize_system_key(system_key)
    external_id = str(external_experiment_id or "").strip()
    if not external_id:
        raise ValueError("external_experiment_id is required")

    external_record = session.scalars(
        select(ExternalExperimentRecord).where(
            ExternalExperimentRecord.system_key == normalized,
            ExternalExperimentRecord.external_id == external_id,
        )
    ).first()
    if external_record is None:
        raise LookupError(f"{normalized} experiment {external_id} is not in the local sync cache")
    external_record = refresh_external_record_detail(session, system_key=normalized, external_record=external_record)

    experiment = first_experiment_for_raw_dataset(raw_dataset)
    if experiment is None:
        experiment_key = f"{normalized}:{external_id}"
        experiment = session.scalars(
            select(ExperimentProject).where(ExperimentProject.experiment_key == experiment_key)
        ).first()
        if experiment is None:
            experiment = ExperimentProject(
                owner_user_id=raw_dataset.owner_user_id,
                experiment_key=experiment_key,
                title=external_record.title,
                visibility=raw_dataset.visibility,
                status="indexed",
                summary=None,
                started_at=external_record.started_at,
                metadata_json={
                    "external_source": normalized,
                    "external_id": external_id,
                    "source_mode": "external_eln_link",
                },
            )
            session.add(experiment)
            session.flush()
        attach_raw_dataset_to_experiment(session, raw_dataset=raw_dataset, experiment=experiment)
    publication = upsert_external_publication_link(
        session,
        experiment=experiment,
        system_key=normalized,
        external_record=external_record,
    )
    session.flush()
    return experiment, publication


def attach_raw_dataset_to_experiment(
    session: Session,
    *,
    raw_dataset: RawDataset,
    experiment: ExperimentProject,
) -> ExperimentRawLink:
    for link in raw_dataset.experiment_links or []:
        if link.experiment_project_id == experiment.id:
            return link
    link = ExperimentRawLink(
        experiment_project_id=experiment.id,
        raw_dataset_id=raw_dataset.id,
        link_type="acquisition",
    )
    session.add(link)
    session.flush()
    return link


def first_experiment_for_raw_dataset(raw_dataset: RawDataset) -> ExperimentProject | None:
    links = sorted(
        raw_dataset.experiment_links or [],
        key=lambda link: link.created_at or datetime.min.replace(tzinfo=timezone.utc),
    )
    for link in links:
        if link.experiment_project is not None:
            return link.experiment_project
    return None


def upsert_external_publication_link(
    session: Session,
    *,
    experiment: ExperimentProject,
    system_key: str,
    external_record: ExternalExperimentRecord,
) -> ExternalPublicationRecord:
    publication = session.scalars(
        select(ExternalPublicationRecord).where(
            ExternalPublicationRecord.experiment_project_id == experiment.id,
            ExternalPublicationRecord.system_key == system_key,
        )
    ).first()
    if publication is None:
        publication = ExternalPublicationRecord(
            experiment_project_id=experiment.id,
            system_key=system_key,
        )
        session.add(publication)
    publication.status = "linked"
    publication.external_id = external_record.external_id
    publication.external_url = external_record.external_url
    text_sections = external_record_text_sections(external_record)
    publication.payload_json = {
        "link_mode": "external_eln_cache",
        "external_title": external_record.title,
        "owner_name": external_record.owner_name,
        "labguru_text": text_sections,
        "last_synced_at": external_record.last_synced_at.isoformat()
        if external_record.last_synced_at is not None
        else None,
    }
    publication.error_text = None
    publication.updated_at = datetime.now(timezone.utc)
    session.flush()
    return publication


def refresh_external_record_detail(
    session: Session,
    *,
    system_key: str,
    external_record: ExternalExperimentRecord,
) -> ExternalExperimentRecord:
    if system_key != "labguru":
        return external_record
    try:
        client = build_external_eln_client(system_key)
        detailed = client.get_experiment(external_record.external_id)
    except Exception:
        return external_record
    return upsert_external_experiment_record(
        session,
        system_key=system_key,
        experiment=detailed,
        synced_at=datetime.now(timezone.utc),
    )


def external_record_text_sections(external_record: ExternalExperimentRecord) -> dict[str, str]:
    if external_record.system_key == "labguru":
        return extract_labguru_text_sections(external_record.payload_json or {})
    return {}


def linked_experiment_summary_view(experiment: ExperimentProject) -> LinkedExperimentSummary:
    publication_records = [
        publication_record_summary_view(record)
        for record in sorted(
            experiment.publication_records or [],
            key=lambda item: (item.system_key.lower(), item.created_at or datetime.min.replace(tzinfo=timezone.utc)),
        )
    ]
    return LinkedExperimentSummary.model_validate(
        {
            "id": experiment.id,
            "experiment_key": experiment.experiment_key,
            "title": experiment.title,
            "visibility": experiment.visibility,
            "status": experiment.status,
            "summary": experiment.summary,
            "total_raw_bytes": experiment.total_raw_bytes,
            "total_derived_bytes": experiment.total_derived_bytes,
            "raw_dataset_count": len(experiment.raw_links or []),
            "analysis_project_count": len(
                [project for project in experiment.analysis_projects or [] if project.status != "deleted"]
            ),
            "metadata_json": experiment.metadata_json or {},
            "owner": experiment.owner,
            "started_at": experiment.started_at,
            "ended_at": experiment.ended_at,
            "last_indexed_at": experiment.last_indexed_at,
            "created_at": experiment.created_at,
            "updated_at": experiment.updated_at,
            "publication_records": publication_records,
            "external_links": [external_link_summary_from_publication(record) for record in publication_records],
        }
    )


def publication_record_summary_view(record: ExternalPublicationRecord) -> PublicationRecordSummary:
    return PublicationRecordSummary.model_validate(
        {
            "id": record.id,
            "system_key": record.system_key,
            "status": record.status,
            "external_id": record.external_id,
            "external_url": record.external_url,
            "payload_json": record.payload_json,
            "error_text": record.error_text,
            "last_attempt_at": record.last_attempt_at,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }
    )


def external_link_summary_from_publication(record: PublicationRecordSummary | ExternalPublicationRecord) -> ExternalLinkSummary:
    payload = getattr(record, "payload_json", None) or {}
    return ExternalLinkSummary(
        system_key=record.system_key,
        status=record.status,
        external_id=record.external_id,
        external_url=record.external_url,
        title=payload.get("external_title") if isinstance(payload, dict) else None,
        payload_json=payload if isinstance(payload, dict) else {},
    )


def load_raw_dataset_for_external_link(session: Session, raw_dataset_id: UUID) -> RawDataset | None:
    return session.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.owner),
            joinedload(RawDataset.experiment_links)
            .joinedload(ExperimentRawLink.experiment_project)
            .joinedload(ExperimentProject.publication_records),
        )
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
