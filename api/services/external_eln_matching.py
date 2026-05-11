from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, joinedload

from api.models import (
    ExperimentProject,
    ExperimentRawLink,
    ExternalExperimentRecord,
    ExternalMatchCandidate,
    RawDataset,
    User,
)
from api.schemas import (
    ExternalExperimentRecordSummary,
    ExternalMatchCandidateGenerateResult,
    ExternalMatchCandidateSummary,
    RawDatasetSummary,
)
from api.services.external_eln import link_raw_dataset_to_external_experiment
from api.services.external_eln_clients import normalize_name, normalize_system_key


REVIEWED_CANDIDATE_STATUSES = {"accepted", "rejected"}


@dataclass(slots=True)
class CandidateScore:
    external_record: ExternalExperimentRecord
    score: float
    evidence: dict


def generate_external_match_candidates(
    session: Session,
    *,
    system_key: str,
    max_candidates_per_dataset: int = 5,
    min_score: float = 0.45,
    limit_raw_datasets: int | None = None,
    include_linked: bool = False,
    reset_proposed: bool = True,
) -> ExternalMatchCandidateGenerateResult:
    normalized = normalize_system_key(system_key)
    max_candidates = min(max(int(max_candidates_per_dataset), 1), 25)
    threshold = min(max(float(min_score), 0.0), 1.0)
    if reset_proposed:
        session.execute(
            delete(ExternalMatchCandidate).where(
                ExternalMatchCandidate.system_key == normalized,
                ExternalMatchCandidate.status == "proposed",
            )
        )
        session.flush()

    raw_stmt = (
        select(RawDataset)
        .options(
            joinedload(RawDataset.owner),
            joinedload(RawDataset.locations),
            joinedload(RawDataset.experiment_links)
            .joinedload(ExperimentRawLink.experiment_project)
            .joinedload(ExperimentProject.publication_records),
        )
        .order_by(RawDataset.started_at.desc().nullslast(), RawDataset.created_at.desc())
    )
    if limit_raw_datasets is not None:
        raw_stmt = raw_stmt.limit(min(max(int(limit_raw_datasets), 1), 5000))
    raw_datasets = list(session.scalars(raw_stmt).unique())
    external_records = list(
        session.scalars(
            select(ExternalExperimentRecord)
            .where(ExternalExperimentRecord.system_key == normalized)
            .order_by(ExternalExperimentRecord.started_at.desc().nullslast(), ExternalExperimentRecord.title.asc())
        )
    )
    existing_by_key = {
        (candidate.raw_dataset_id, candidate.external_experiment_record_id): candidate
        for candidate in session.scalars(
            select(ExternalMatchCandidate).where(ExternalMatchCandidate.system_key == normalized)
        )
    }

    candidate_count = 0
    created_count = 0
    updated_count = 0
    preserved_reviewed_count = 0
    now = datetime.now(timezone.utc)

    for raw_dataset in raw_datasets:
        if not include_linked and raw_dataset_has_external_link(raw_dataset, normalized):
            continue
        scored = score_external_records_for_raw_dataset(raw_dataset, external_records)
        qualifying = [item for item in scored if item.score >= threshold][:max_candidates]
        candidate_count += len(qualifying)
        for rank, scored_candidate in enumerate(qualifying, start=1):
            key = (raw_dataset.id, scored_candidate.external_record.id)
            candidate = existing_by_key.get(key)
            if candidate is not None and candidate.status in REVIEWED_CANDIDATE_STATUSES:
                preserved_reviewed_count += 1
                continue
            if candidate is None:
                candidate = ExternalMatchCandidate(
                    raw_dataset_id=raw_dataset.id,
                    system_key=normalized,
                    external_experiment_record_id=scored_candidate.external_record.id,
                    external_id=scored_candidate.external_record.external_id,
                )
                session.add(candidate)
                existing_by_key[key] = candidate
                created_count += 1
            else:
                updated_count += 1
            candidate.external_id = scored_candidate.external_record.external_id
            candidate.score = scored_candidate.score
            candidate.candidate_rank = rank
            candidate.status = "proposed"
            candidate.match_method = "deterministic_v1"
            candidate.evidence_json = scored_candidate.evidence
            candidate.reviewed_by_user_id = None
            candidate.reviewed_at = None
            candidate.updated_at = now

    session.flush()
    return ExternalMatchCandidateGenerateResult(
        system_key=normalized,
        raw_dataset_count=len(raw_datasets),
        external_experiment_count=len(external_records),
        candidate_count=candidate_count,
        created_count=created_count,
        updated_count=updated_count,
        preserved_reviewed_count=preserved_reviewed_count,
    )


def score_external_records_for_raw_dataset(
    raw_dataset: RawDataset,
    external_records: list[ExternalExperimentRecord],
) -> list[CandidateScore]:
    scored = [
        score_external_record_for_raw_dataset(raw_dataset, external_record)
        for external_record in external_records
    ]
    scored = [item for item in scored if item.score > 0]
    return sorted(
        scored,
        key=lambda item: (
            -item.score,
            item.external_record.started_at.isoformat() if item.external_record.started_at else "",
            item.external_record.title,
        ),
    )


def score_external_record_for_raw_dataset(
    raw_dataset: RawDataset,
    external_record: ExternalExperimentRecord,
) -> CandidateScore:
    raw_label = raw_dataset.acquisition_label or ""
    raw_text = " ".join(
        [
            raw_label,
            raw_dataset.microscope_name or "",
            " ".join(location.relative_path or "" for location in raw_dataset.locations or []),
        ]
    )
    external_text = " ".join(
        [
            external_record.title or "",
            external_record.owner_name or "",
            external_payload_text(external_record),
        ]
    )
    raw_label_norm = compact_match_text(raw_label)
    external_title_norm = compact_match_text(external_record.title)
    raw_tokens = token_set(raw_text)
    external_tokens = token_set(external_text)
    token_overlap = jaccard(raw_tokens, external_tokens)
    sequence_ratio = SequenceMatcher(None, raw_label_norm, external_title_norm).ratio() if raw_label_norm and external_title_norm else 0.0

    score = max(0.65 * sequence_ratio, 0.70 * token_overlap)
    label_signal = "weak_text_similarity"
    if raw_label_norm and external_title_norm and raw_label_norm == external_title_norm:
        score = max(score, 1.0)
        label_signal = "exact_label_title"
    elif raw_label_norm and external_title_norm and (raw_label_norm in external_title_norm or external_title_norm in raw_label_norm):
        if min(len(raw_label_norm), len(external_title_norm)) >= 8:
            score = max(score, 0.82)
            label_signal = "label_title_contains"

    raw_date = extract_date_key(raw_label) or extract_date_key(raw_text)
    external_date = extract_date_key(external_record.title) or extract_date_key(external_payload_text(external_record))
    date_match = bool(raw_date and external_date and raw_date == external_date)
    if date_match:
        score += 0.10

    owner_match = False
    if raw_dataset.owner is not None and external_record.owner_name:
        owner_match = normalize_name(raw_dataset.owner.display_name) == normalize_name(external_record.owner_name)
        if owner_match:
            score += 0.06

    score = round(min(score, 1.0), 4)
    evidence = {
        "label_signal": label_signal,
        "raw_label": raw_label,
        "external_title": external_record.title,
        "sequence_ratio": round(sequence_ratio, 4),
        "token_overlap": round(token_overlap, 4),
        "raw_date": raw_date,
        "external_date": external_date,
        "date_match": date_match,
        "owner_match": owner_match,
        "raw_owner": raw_dataset.owner.display_name if raw_dataset.owner is not None else None,
        "external_owner": external_record.owner_name,
        "raw_locations": [location.relative_path for location in raw_dataset.locations or []],
    }
    return CandidateScore(external_record=external_record, score=score, evidence=evidence)


def list_external_match_candidates(
    session: Session,
    *,
    system_key: str,
    status: str | None = None,
    limit: int = 100,
) -> list[ExternalMatchCandidate]:
    normalized = normalize_system_key(system_key)
    stmt = (
        select(ExternalMatchCandidate)
        .options(
            joinedload(ExternalMatchCandidate.raw_dataset).joinedload(RawDataset.owner),
            joinedload(ExternalMatchCandidate.external_experiment_record),
            joinedload(ExternalMatchCandidate.reviewed_by),
        )
        .where(ExternalMatchCandidate.system_key == normalized)
        .order_by(
            ExternalMatchCandidate.status.asc(),
            ExternalMatchCandidate.score.desc(),
            ExternalMatchCandidate.created_at.desc(),
        )
        .limit(min(max(limit, 1), 1000))
    )
    if status:
        stmt = stmt.where(ExternalMatchCandidate.status == status)
    return list(session.scalars(stmt).unique())


def review_external_match_candidate(
    session: Session,
    *,
    candidate_id: UUID,
    system_key: str,
    action: str,
    reviewed_by: User,
    note: str | None = None,
) -> tuple[ExternalMatchCandidate, bool, object | None, object | None]:
    normalized = normalize_system_key(system_key)
    candidate = load_external_match_candidate(session, candidate_id=candidate_id, system_key=normalized)
    normalized_action = str(action or "").strip().lower()
    now = datetime.now(timezone.utc)
    evidence = dict(candidate.evidence_json or {})
    if note:
        evidence["review_note"] = note

    linked = False
    experiment = None
    publication = None
    if normalized_action in {"accept", "accepted"}:
        raw_dataset = load_raw_dataset_for_candidate_link(session, candidate.raw_dataset_id)
        experiment, publication = link_raw_dataset_to_external_experiment(
            session,
            raw_dataset=raw_dataset,
            system_key=normalized,
            external_experiment_id=candidate.external_id,
        )
        candidate.status = "accepted"
        linked = True
    elif normalized_action in {"reject", "rejected"}:
        candidate.status = "rejected"
    elif normalized_action in {"reset", "proposed"}:
        candidate.status = "proposed"
        candidate.reviewed_by_user_id = None
        candidate.reviewed_at = None
        candidate.evidence_json = evidence
        candidate.updated_at = now
        session.flush()
        return candidate, False, None, None
    else:
        raise ValueError("action must be accept, reject, or reset")

    candidate.reviewed_by_user_id = reviewed_by.id
    candidate.reviewed_at = now
    candidate.evidence_json = evidence
    candidate.updated_at = now
    session.flush()
    return candidate, linked, experiment, publication


def load_external_match_candidate(
    session: Session,
    *,
    candidate_id: UUID,
    system_key: str,
) -> ExternalMatchCandidate:
    candidate = session.scalars(
        select(ExternalMatchCandidate)
        .options(
            joinedload(ExternalMatchCandidate.raw_dataset).joinedload(RawDataset.owner),
            joinedload(ExternalMatchCandidate.external_experiment_record),
            joinedload(ExternalMatchCandidate.reviewed_by),
        )
        .where(ExternalMatchCandidate.id == candidate_id, ExternalMatchCandidate.system_key == system_key)
    ).first()
    if candidate is None:
        raise LookupError("External match candidate not found")
    return candidate


def load_raw_dataset_for_candidate_link(session: Session, raw_dataset_id: UUID) -> RawDataset:
    raw_dataset = session.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.owner),
            joinedload(RawDataset.experiment_links)
            .joinedload(ExperimentRawLink.experiment_project)
            .joinedload(ExperimentProject.publication_records),
        )
        .where(RawDataset.id == raw_dataset_id)
    ).unique().first()
    if raw_dataset is None:
        raise LookupError("Raw dataset not found")
    return raw_dataset


def external_match_candidate_summary(candidate: ExternalMatchCandidate) -> ExternalMatchCandidateSummary:
    return ExternalMatchCandidateSummary.model_validate(
        {
            "id": candidate.id,
            "system_key": candidate.system_key,
            "external_id": candidate.external_id,
            "score": candidate.score,
            "candidate_rank": candidate.candidate_rank,
            "status": candidate.status,
            "match_method": candidate.match_method,
            "evidence_json": candidate.evidence_json or {},
            "raw_dataset": raw_dataset_summary(candidate.raw_dataset),
            "external_experiment": external_experiment_summary(candidate.external_experiment_record),
            "reviewed_by": candidate.reviewed_by,
            "reviewed_at": candidate.reviewed_at,
            "created_at": candidate.created_at,
            "updated_at": candidate.updated_at,
        }
    )


def raw_dataset_summary(raw_dataset: RawDataset) -> RawDatasetSummary:
    return RawDatasetSummary.model_validate(
        {
            "id": raw_dataset.id,
            "external_key": raw_dataset.external_key,
            "microscope_name": raw_dataset.microscope_name,
            "acquisition_label": raw_dataset.acquisition_label,
            "data_format": raw_dataset.data_format,
            "visibility": raw_dataset.visibility,
            "status": raw_dataset.status,
            "completeness_status": raw_dataset.completeness_status,
            "lifecycle_tier": raw_dataset.lifecycle_tier,
            "archive_status": raw_dataset.archive_status,
            "archive_uri": raw_dataset.archive_uri,
            "archive_compression": raw_dataset.archive_compression,
            "display_settings_uri": raw_dataset.display_settings_uri,
            "reclaimable_bytes": raw_dataset.reclaimable_bytes,
            "last_accessed_at": raw_dataset.last_accessed_at,
            "total_bytes": raw_dataset.total_bytes,
            "metadata_json": raw_dataset.metadata_json or {},
            "owner": raw_dataset.owner,
            "created_at": raw_dataset.created_at,
            "updated_at": raw_dataset.updated_at,
            "backup_status": raw_dataset.backup_status,
            "backup_excluded": raw_dataset.backup_excluded,
            "last_backup_at": raw_dataset.last_backup_at,
        }
    )


def external_experiment_summary(external_record: ExternalExperimentRecord) -> ExternalExperimentRecordSummary:
    return ExternalExperimentRecordSummary.model_validate(external_record)


def raw_dataset_has_external_link(raw_dataset: RawDataset, system_key: str) -> bool:
    for link in raw_dataset.experiment_links or []:
        experiment = link.experiment_project
        if experiment is None:
            continue
        for publication in experiment.publication_records or []:
            if publication.system_key == system_key and publication.status == "linked":
                return True
    return False


def compact_match_text(value: object) -> str:
    text = normalize_ascii(value)
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def token_set(value: object) -> set[str]:
    text = normalize_ascii(value).lower()
    return {token for token in re.split(r"[^a-z0-9]+", text) if len(token) >= 2}


def normalize_ascii(value: object) -> str:
    text = str(value or "")
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(character for character in normalized if not unicodedata.combining(character))


def jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def extract_date_key(value: object) -> str | None:
    text = str(value or "")
    match = re.search(r"(20\d{2}|19\d{2})[-_ ]?([01]\d)[-_ ]?([0-3]\d)", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = re.search(r"(?<!\d)([0-3]\d)[-_ ]?([01]\d)[-_ ]?(\d{2})(?!\d)", text)
    if match:
        day, month, year = match.groups()
        return f"20{year}-{month}-{day}"
    return None


def external_payload_text(external_record: ExternalExperimentRecord) -> str:
    payload = external_record.payload_json or {}
    parts: list[str] = []
    for key in ("project", "milestone", "tags", "description", "description_for_elastic"):
        value = payload.get(key)
        if isinstance(value, dict):
            parts.extend(str(value.get(item) or "") for item in ("name", "title"))
        elif isinstance(value, list):
            parts.extend(str(item.get("name") or item.get("title") or item) if isinstance(item, dict) else str(item) for item in value)
        elif value:
            parts.append(str(value))
    return " ".join(parts)
