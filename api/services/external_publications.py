from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.models import ExperimentProject, ExternalPublicationRecord


def ensure_publication_records(
    session: Session,
    *,
    experiment: ExperimentProject,
    system_keys: list[str] | None = None,
) -> list[ExternalPublicationRecord]:
    targets = system_keys or default_publication_targets()
    records: list[ExternalPublicationRecord] = []
    for system_key in targets:
        existing = session.scalars(
            select(ExternalPublicationRecord).where(
                ExternalPublicationRecord.experiment_project_id == experiment.id,
                ExternalPublicationRecord.system_key == system_key,
            )
        ).first()
        if existing is None:
            existing = ExternalPublicationRecord(
                experiment_project_id=experiment.id,
                system_key=system_key,
                status="pending",
                payload_json=build_publication_payload(experiment, system_key=system_key),
            )
            session.add(existing)
            session.flush()
        records.append(existing)
    return records


def default_publication_targets() -> list[str]:
    raw_value = get_settings().default_publication_targets
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def build_publication_payload(experiment: ExperimentProject, *, system_key: str) -> dict:
    return {
        "system_key": system_key,
        "experiment_key": experiment.experiment_key,
        "title": experiment.title,
        "summary": experiment.summary,
        "status": experiment.status,
        "metadata_json": experiment.metadata_json or {},
    }
