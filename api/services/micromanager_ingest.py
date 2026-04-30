from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.orm import Session, joinedload

from api.config import Settings, get_settings
from api.models import ExperimentProject, Job, MicroManagerIngestRun, Pipeline, RawDataset, User
from api.services.micromanager_metadata import (
    extract_acquisition_dimensions,
    find_micromanager_display_settings_path,
    read_micromanager_metadata,
)
from api.services.external_publications import ensure_publication_records
from api.services.project_indexing import slugify
from api.services.raw_dataset_ingest import ingest_raw_dataset_from_directory
from api.services.users import get_or_create_user


MICROMANAGER_INGEST_LOCK_KEY = 2026031102
MICROMANAGER_METADATA_FILES = {
    "metadata.txt",
    "acquisitionmetadata.txt",
    "acquisition metadata.txt",
    "displaysettings.txt",
    "display settings.txt",
}
MICROMANAGER_DATA_SUFFIXES = {
    ".ome.tif",
    ".ome.tiff",
    ".tif",
    ".tiff",
    ".nd2",
    ".czi",
    ".lif",
    ".ims",
}
MICROMANAGER_INDEX_FILES = {
    "ndtiff.index",
}


@dataclass
class MicroManagerIngestConfigData:
    enabled: bool
    interval_minutes: int
    run_as_user_key: str
    landing_root: str | None
    storage_root_name: str | None
    host_scope: str
    visibility: str
    settle_seconds: int
    max_datasets: int
    grouping_window_hours: int
    post_ingest_pipeline_key: str | None
    post_ingest_requested_mode: str
    post_ingest_priority: int

    def to_json(self) -> dict:
        return {
            "enabled": self.enabled,
            "interval_minutes": self.interval_minutes,
            "run_as_user_key": self.run_as_user_key,
            "landing_root": self.landing_root,
            "storage_root_name": self.storage_root_name,
            "host_scope": self.host_scope,
            "visibility": self.visibility,
            "settle_seconds": self.settle_seconds,
            "max_datasets": self.max_datasets,
            "grouping_window_hours": self.grouping_window_hours,
            "post_ingest_pipeline_key": self.post_ingest_pipeline_key,
            "post_ingest_requested_mode": self.post_ingest_requested_mode,
            "post_ingest_priority": self.post_ingest_priority,
        }


@dataclass
class MicroManagerDatasetCandidate:
    dataset_dir: Path
    relative_path: str
    acquisition_label: str
    microscope_name: str | None
    session_label: str
    session_date: datetime | None
    group_key: str
    group_label: str
    last_modified_at: datetime
    file_count: int
    metadata_json: dict
    completeness_status: str


@dataclass
class MicroManagerIngestRunExecutionData:
    run: MicroManagerIngestRun
    candidate_count: int
    ingested_count: int
    experiment_count: int
    skipped_count: int
    candidate_paths: list[str]
    queued_job_ids: list[str]
    report_only: bool


def automatic_micromanager_ingest_config(settings: Settings | None = None) -> MicroManagerIngestConfigData:
    settings = settings or get_settings()
    return MicroManagerIngestConfigData(
        enabled=settings.micromanager_ingest_enabled,
        interval_minutes=max(1, settings.micromanager_ingest_interval_minutes),
        run_as_user_key=settings.micromanager_ingest_run_as_user_key,
        landing_root=settings.micromanager_ingest_root or None,
        storage_root_name=settings.micromanager_ingest_storage_root_name or None,
        host_scope=settings.micromanager_ingest_host_scope,
        visibility=settings.micromanager_ingest_visibility,
        settle_seconds=max(0, settings.micromanager_ingest_settle_seconds),
        max_datasets=max(1, settings.micromanager_ingest_max_datasets),
        grouping_window_hours=max(1, settings.micromanager_ingest_grouping_window_hours),
        post_ingest_pipeline_key=settings.micromanager_post_ingest_pipeline_key or None,
        post_ingest_requested_mode=settings.micromanager_post_ingest_requested_mode,
        post_ingest_priority=max(1, settings.micromanager_post_ingest_priority),
    )


def try_acquire_micromanager_ingest_lock(session: Session) -> bool:
    return bool(
        session.execute(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": MICROMANAGER_INGEST_LOCK_KEY}).scalar()
    )


def release_micromanager_ingest_lock(session: Session) -> None:
    _ = session


def latest_micromanager_ingest_run_timestamp(session: Session) -> datetime | None:
    stmt = (
        select(MicroManagerIngestRun)
        .order_by(
            MicroManagerIngestRun.finished_at.desc().nullslast(),
            MicroManagerIngestRun.started_at.desc().nullslast(),
            MicroManagerIngestRun.created_at.desc(),
        )
        .limit(1)
    )
    run = session.scalars(stmt).first()
    if run is None:
        return None
    return run.finished_at or run.started_at or run.created_at


def list_micromanager_ingest_runs(session: Session, *, limit: int = 10) -> list[MicroManagerIngestRun]:
    stmt = (
        select(MicroManagerIngestRun)
        .options(joinedload(MicroManagerIngestRun.triggered_by))
        .order_by(MicroManagerIngestRun.created_at.desc())
        .limit(max(1, min(limit, 50)))
    )
    return list(session.scalars(stmt).unique())


def latest_micromanager_ingest_run(session: Session) -> MicroManagerIngestRun | None:
    runs = list_micromanager_ingest_runs(session, limit=1)
    return runs[0] if runs else None


def resolve_micromanager_ingest_user(session: Session, *, user_key: str) -> User:
    return get_or_create_user(session, user_key=user_key, display_name=user_key, role="service")


def discover_micromanager_candidates(
    *,
    landing_root: Path,
    settle_seconds: int,
    grouping_window_hours: int,
    max_datasets: int,
) -> list[MicroManagerDatasetCandidate]:
    now = datetime.now(timezone.utc)
    candidates: list[MicroManagerDatasetCandidate] = []
    seen_paths: set[Path] = set()

    roots_to_scan = [landing_root, *[path for path in landing_root.iterdir() if path.is_dir()]]
    for path in roots_to_scan:
        if len(candidates) >= max_datasets:
            break
        dataset_dir = classify_micromanager_dataset_dir(path)
        if dataset_dir is None:
            continue
        dataset_dir = dataset_dir.resolve()
        if dataset_dir in seen_paths:
            continue
        seen_paths.add(dataset_dir)

        last_modified_at, file_count = latest_dataset_activity(dataset_dir)
        if last_modified_at is None:
            continue
        if now - last_modified_at < timedelta(seconds=max(0, settle_seconds)):
            continue

        metadata_json = read_micromanager_metadata(dataset_dir)
        microscope_name = extract_microscope_name(metadata_json)
        acquisition_label = extract_acquisition_label(dataset_dir, metadata_json)
        session_label = extract_session_label(dataset_dir, metadata_json, acquisition_label=acquisition_label)
        session_date = extract_session_datetime(metadata_json) or last_modified_at
        dimensions = extract_acquisition_dimensions(metadata_json)
        display_settings_path = find_micromanager_display_settings_path(dataset_dir)
        group_key, group_label = build_experiment_grouping(
            relative_path=str(dataset_dir.relative_to(landing_root)),
            session_label=session_label,
            session_date=session_date,
            grouping_window_hours=grouping_window_hours,
        )
        completeness_status = "complete"
        candidates.append(
            MicroManagerDatasetCandidate(
                dataset_dir=dataset_dir,
                relative_path=str(dataset_dir.relative_to(landing_root)),
                acquisition_label=acquisition_label,
                microscope_name=microscope_name,
                session_label=session_label,
                session_date=session_date,
                group_key=group_key,
                group_label=group_label,
                last_modified_at=last_modified_at,
                file_count=file_count,
                metadata_json={
                    "source": "micromanager_ingest",
                    "relative_path": str(dataset_dir.relative_to(landing_root)),
                    "file_count": file_count,
                    "last_modified_at": last_modified_at.isoformat(),
                    "session_label": session_label,
                    "session_date": session_date.isoformat() if session_date else None,
                    "group_key": group_key,
                    "group_label": group_label,
                    "dimensions": dimensions,
                    "display_settings_uri": str(display_settings_path) if display_settings_path is not None else None,
                },
                completeness_status=completeness_status,
            )
        )
    return candidates


def classify_micromanager_dataset_dir(path: Path) -> Path | None:
    if not path.exists() or not path.is_dir():
        return None

    try:
        entries = list(path.iterdir())
    except OSError:
        return None

    file_names = {entry.name.lower() for entry in entries if entry.is_file()}
    if file_names.intersection(MICROMANAGER_METADATA_FILES | MICROMANAGER_INDEX_FILES):
        return path
    if any(has_micromanager_data_suffix(entry.name) for entry in entries if entry.is_file()):
        return path

    for child in entries:
        if not child.is_dir():
            continue
        try:
            child_entries = list(child.iterdir())
        except OSError:
            continue
        child_file_names = {entry.name.lower() for entry in child_entries if entry.is_file()}
        if child_file_names.intersection(MICROMANAGER_METADATA_FILES | MICROMANAGER_INDEX_FILES):
            return child
        if any(has_micromanager_data_suffix(entry.name) for entry in child_entries if entry.is_file()):
            return child
    return None


def has_micromanager_data_suffix(file_name: str) -> bool:
    lower = file_name.lower()
    return any(lower.endswith(suffix) for suffix in MICROMANAGER_DATA_SUFFIXES)


def latest_dataset_activity(dataset_dir: Path) -> tuple[datetime | None, int]:
    latest_timestamp: datetime | None = None
    file_count = 0
    try:
        for file_path in dataset_dir.rglob("*"):
            if not file_path.is_file():
                continue
            file_count += 1
            modified_at = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
            if latest_timestamp is None or modified_at > latest_timestamp:
                latest_timestamp = modified_at
    except OSError:
        return None, 0
    return latest_timestamp, file_count


def extract_microscope_name(metadata_json: dict) -> str | None:
    summary = metadata_json.get("Summary") if isinstance(metadata_json, dict) else None
    if isinstance(summary, dict):
        for key in ("MicroManagerVersion", "Prefix", "ComputerName", "Microscope"):
            value = summary.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    for key in ("microscope", "microscope_name", "system"):
        value = metadata_json.get(key) if isinstance(metadata_json, dict) else None
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def extract_acquisition_label(dataset_dir: Path, metadata_json: dict) -> str:
    summary = metadata_json.get("Summary") if isinstance(metadata_json, dict) else None
    if isinstance(summary, dict):
        for key in ("Prefix", "Comment"):
            value = summary.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return dataset_dir.name


def extract_session_label(dataset_dir: Path, metadata_json: dict, *, acquisition_label: str) -> str:
    summary = metadata_json.get("Summary") if isinstance(metadata_json, dict) else None
    if isinstance(summary, dict):
        for key in ("Comment", "Prefix"):
            value = summary.get(key)
            if isinstance(value, str) and value.strip():
                return normalize_session_label(value)
    return normalize_session_label(acquisition_label or dataset_dir.name)


def normalize_session_label(value: str) -> str:
    compact = re.sub(r"[_\\-]?\\d+$", "", value.strip())
    compact = re.sub(r"\\s+", " ", compact)
    compact = compact.strip(" _-")
    return compact or value.strip()


def extract_session_datetime(metadata_json: dict) -> datetime | None:
    summary = metadata_json.get("Summary") if isinstance(metadata_json, dict) else None
    if isinstance(summary, dict):
        for key in ("StartTime", "Date", "DateTime", "Time"):
            parsed = parse_datetime_guess(summary.get(key))
            if parsed is not None:
                return parsed
    return None
def parse_datetime_guess(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    text_value = str(value).strip()
    if not text_value:
        return None
    candidates = (
        text_value.replace("Z", "+00:00"),
        text_value,
    )
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    for fmt in (
        "%a %b %d %H:%M:%S %Z %Y",
        "%a %b %d %H:%M:%S %Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d-%b-%Y %H:%M:%S",
    ):
        try:
            parsed = datetime.strptime(text_value, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def build_experiment_grouping(
    *,
    relative_path: str,
    session_label: str,
    session_date: datetime | None,
    grouping_window_hours: int,
) -> tuple[str, str]:
    relative_parent = str(Path(relative_path).parent)
    parent_token = "" if relative_parent in {"", "."} else slugify(relative_parent)
    session_token = slugify(session_label or "session")
    if session_date is None:
        bucket_label = "undated"
    else:
        bucket_start = session_date.replace(minute=0, second=0, microsecond=0)
        bucket_start = bucket_start - timedelta(hours=bucket_start.hour % max(1, grouping_window_hours))
        bucket_label = bucket_start.strftime("%Y%m%dT%H00")
    grouping_identity = "::".join(part for part in (parent_token, session_token, bucket_label) if part)
    grouping_label = session_label or Path(relative_path).name
    if relative_parent not in {"", "."}:
        grouping_label = f"{grouping_label} | {relative_parent}"
    if session_date is not None:
        grouping_label = f"{grouping_label} ({session_date.strftime('%Y-%m-%d')})"
    return grouping_identity, grouping_label


def build_micromanager_raw_key(dataset_dir: Path, relative_path: str) -> str:
    suffix = hashlib.sha1(f"{dataset_dir}|{relative_path}".encode("utf-8")).hexdigest()[:12]
    return f"mm_{slugify(dataset_dir.name)}_{suffix}"


def build_micromanager_experiment_key(group_key: str, group_label: str) -> str:
    suffix = hashlib.sha1(group_key.encode("utf-8")).hexdigest()[:10]
    return f"exp_mm_{slugify(group_label)}_{suffix}"


def ensure_micromanager_experiment(
    session: Session,
    *,
    owner: User,
    visibility: str,
    candidate: MicroManagerDatasetCandidate,
) -> tuple[ExperimentProject, bool]:
    experiment_key = build_micromanager_experiment_key(candidate.group_key, candidate.group_label)
    experiment = session.scalars(select(ExperimentProject).where(ExperimentProject.experiment_key == experiment_key)).first()
    created = False
    if experiment is None:
        experiment = ExperimentProject(
            owner_user_id=owner.id,
            experiment_key=experiment_key,
            title=candidate.group_label,
            visibility=visibility,
            status="indexed",
            summary="Auto-created from Micro-Manager ingestion.",
            started_at=candidate.session_date or candidate.last_modified_at,
            ended_at=candidate.last_modified_at,
            last_indexed_at=datetime.now(timezone.utc),
            metadata_json={
                "source": "micromanager_ingest",
                "group_key": candidate.group_key,
                "group_label": candidate.group_label,
                "session_label": candidate.session_label,
                "relative_paths": [candidate.relative_path],
            },
        )
        session.add(experiment)
        session.flush()
        ensure_publication_records(session, experiment=experiment)
        created = True
    else:
        experiment.owner_user_id = owner.id
        experiment.visibility = visibility
        experiment.status = "indexed"
        experiment.last_indexed_at = datetime.now(timezone.utc)
        merged_metadata = dict(experiment.metadata_json or {})
        relative_paths = list(merged_metadata.get("relative_paths") or [])
        if candidate.relative_path not in relative_paths:
            relative_paths.append(candidate.relative_path)
        merged_metadata.update(
            {
                "source": "micromanager_ingest",
                "group_key": candidate.group_key,
                "group_label": candidate.group_label,
                "session_label": candidate.session_label,
                "relative_paths": sorted(relative_paths),
            }
        )
        experiment.metadata_json = merged_metadata
        experiment.started_at = min(
            value for value in [experiment.started_at, candidate.session_date, candidate.last_modified_at] if value is not None
        )
        experiment.ended_at = max(
            value for value in [experiment.ended_at, candidate.last_modified_at] if value is not None
        )
    return experiment, created


def queue_post_ingest_pipeline_job(
    session: Session,
    *,
    config: MicroManagerIngestConfigData,
    triggered_by_user: User,
    raw_dataset: RawDataset,
    experiment: ExperimentProject,
) -> str | None:
    if not config.post_ingest_pipeline_key:
        return None

    pipeline = session.scalars(select(Pipeline).where(Pipeline.pipeline_key == config.post_ingest_pipeline_key)).first()
    if pipeline is None:
        return None
    input_kind = (pipeline.metadata_json or {}).get("input_kind")
    if input_kind not in (None, "", "raw", "raw_dataset"):
        return None

    existing_job = session.scalars(
        select(Job)
        .where(Job.raw_dataset_id == raw_dataset.id, Job.pipeline_id == pipeline.id)
        .order_by(Job.created_at.desc())
        .limit(1)
    ).first()
    if existing_job is not None:
        return None

    job = Job(
        raw_dataset_id=raw_dataset.id,
        pipeline_id=pipeline.id,
        requested_mode=config.post_ingest_requested_mode,
        priority=config.post_ingest_priority,
        requested_by=triggered_by_user.user_key,
        requested_from_host="micromanager_ingest",
        params_json={
            "job_kind": "micromanager_post_ingest",
            "source": "micromanager_ingest",
            "experiment_project_id": str(experiment.id),
            "experiment_key": experiment.experiment_key,
        },
        status="queued",
    )
    session.add(job)
    session.flush()
    return str(job.id)


def execute_micromanager_ingest_run(
    session: Session,
    *,
    config: MicroManagerIngestConfigData,
    triggered_by_user: User,
    trigger_mode: str,
    report_only: bool,
) -> MicroManagerIngestRunExecutionData:
    started_at = datetime.now(timezone.utc)
    run = MicroManagerIngestRun(
        triggered_by_user_id=triggered_by_user.id,
        trigger_mode=trigger_mode,
        status="running",
        report_only=report_only,
        config_json=config.to_json(),
        result_json={},
        started_at=started_at,
    )
    session.add(run)
    session.flush()

    try:
        if not config.landing_root:
            raise ValueError("Micro-Manager ingest root is not configured")

        landing_root = Path(config.landing_root).expanduser().resolve()
        if not landing_root.exists() or not landing_root.is_dir():
            raise ValueError(f"Micro-Manager ingest root does not exist or is not a directory: {landing_root}")

        candidates = discover_micromanager_candidates(
            landing_root=landing_root,
            settle_seconds=config.settle_seconds,
            grouping_window_hours=config.grouping_window_hours,
            max_datasets=config.max_datasets,
        )

        ingested_count = 0
        experiment_count = 0
        skipped_count = 0
        candidate_paths: list[str] = []
        ingested_dataset_ids: list[str] = []
        experiment_ids: list[str] = []
        queued_job_ids: list[str] = []

        if not report_only:
            for candidate in candidates:
                candidate_paths.append(str(candidate.dataset_dir))
                experiment, experiment_created = ensure_micromanager_experiment(
                    session,
                    owner=triggered_by_user,
                    visibility=config.visibility,
                    candidate=candidate,
                )
                raw_dataset = ingest_raw_dataset_from_directory(
                    session,
                    owner=triggered_by_user,
                    visibility=config.visibility,
                    storage_root_name=config.storage_root_name,
                    host_scope=config.host_scope,
                    root_type="raw_root",
                    root_path=landing_root,
                    dataset_dir=candidate.dataset_dir,
                    preferred_experiment=experiment,
                    source_label="micromanager_ingest",
                    source_metadata=candidate.metadata_json,
                    acquisition_label=candidate.acquisition_label,
                    microscope_name=candidate.microscope_name,
                    external_key=build_micromanager_raw_key(candidate.dataset_dir, candidate.relative_path),
                    status="indexed",
                    completeness_status=candidate.completeness_status,
                    started_at=candidate.session_date or candidate.last_modified_at,
                    ended_at=candidate.last_modified_at,
                )
                queued_job_id = queue_post_ingest_pipeline_job(
                    session,
                    config=config,
                    triggered_by_user=triggered_by_user,
                    raw_dataset=raw_dataset,
                    experiment=experiment,
                )
                ingested_count += 1
                if experiment_created:
                    experiment_count += 1
                ingested_dataset_ids.append(str(raw_dataset.id))
                if str(experiment.id) not in experiment_ids:
                    experiment_ids.append(str(experiment.id))
                if queued_job_id:
                    queued_job_ids.append(queued_job_id)
        else:
            candidate_paths = [str(candidate.dataset_dir) for candidate in candidates]
            queued_job_ids = []

        run.status = "done"
        run.candidate_count = len(candidates)
        run.ingested_count = ingested_count
        run.experiment_count = experiment_count
        run.skipped_count = skipped_count
        run.result_json = {
            "candidate_paths": candidate_paths,
            "ingested_dataset_ids": ingested_dataset_ids,
            "experiment_ids": experiment_ids,
            "candidate_count": len(candidates),
            "ingested_count": ingested_count,
            "experiment_count": experiment_count,
            "skipped_count": skipped_count,
            "queued_job_ids": queued_job_ids,
        }
        run.finished_at = datetime.now(timezone.utc)
        session.flush()
        return MicroManagerIngestRunExecutionData(
            run=run,
            candidate_count=len(candidates),
            ingested_count=ingested_count,
            experiment_count=experiment_count,
            skipped_count=skipped_count,
            candidate_paths=candidate_paths,
            queued_job_ids=queued_job_ids,
            report_only=report_only,
        )
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = datetime.now(timezone.utc)
        session.flush()
        raise
