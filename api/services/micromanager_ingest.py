from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.orm import Session, joinedload

from api.config import Settings, get_settings
from api.models import ExperimentProject, MicroManagerIngestRun, RawDataset, User
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
        }


@dataclass
class MicroManagerDatasetCandidate:
    dataset_dir: Path
    relative_path: str
    acquisition_label: str
    microscope_name: str | None
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
        completeness_status = "complete"
        candidates.append(
            MicroManagerDatasetCandidate(
                dataset_dir=dataset_dir,
                relative_path=str(dataset_dir.relative_to(landing_root)),
                acquisition_label=acquisition_label,
                microscope_name=microscope_name,
                last_modified_at=last_modified_at,
                file_count=file_count,
                metadata_json={
                    "source": "micromanager_ingest",
                    "relative_path": str(dataset_dir.relative_to(landing_root)),
                    "file_count": file_count,
                    "last_modified_at": last_modified_at.isoformat(),
                    "micromanager_metadata": metadata_json,
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


def read_micromanager_metadata(dataset_dir: Path) -> dict:
    for file_name in ("metadata.txt", "Metadata.txt", "Acquisition metadata.txt", "AcquisitionMetadata.txt"):
        metadata_path = dataset_dir / file_name
        if not metadata_path.exists() or not metadata_path.is_file():
            continue
        try:
            text_value = metadata_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            try:
                text_value = metadata_path.read_text(encoding="latin-1")
            except OSError:
                continue
        except OSError:
            continue

        stripped = text_value.strip()
        if not stripped:
            return {}
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {"raw_text": stripped[:2000]}
    return {}


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


def build_micromanager_raw_key(dataset_dir: Path, relative_path: str) -> str:
    suffix = hashlib.sha1(f"{dataset_dir}|{relative_path}".encode("utf-8")).hexdigest()[:12]
    return f"mm_{slugify(dataset_dir.name)}_{suffix}"


def build_micromanager_experiment_key(relative_path: str, acquisition_label: str) -> str:
    label = relative_path or acquisition_label
    suffix = hashlib.sha1(label.encode("utf-8")).hexdigest()[:10]
    return f"exp_mm_{slugify(acquisition_label)}_{suffix}"


def ensure_micromanager_experiment(
    session: Session,
    *,
    owner: User,
    visibility: str,
    candidate: MicroManagerDatasetCandidate,
) -> tuple[ExperimentProject, bool]:
    experiment_key = build_micromanager_experiment_key(candidate.relative_path, candidate.acquisition_label)
    experiment = session.scalars(select(ExperimentProject).where(ExperimentProject.experiment_key == experiment_key)).first()
    created = False
    if experiment is None:
        experiment = ExperimentProject(
            owner_user_id=owner.id,
            experiment_key=experiment_key,
            title=candidate.acquisition_label,
            visibility=visibility,
            status="indexed",
            summary="Auto-created from Micro-Manager ingestion.",
            started_at=candidate.last_modified_at,
            ended_at=candidate.last_modified_at,
            last_indexed_at=datetime.now(timezone.utc),
            metadata_json={
                "source": "micromanager_ingest",
                "relative_path": candidate.relative_path,
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
        merged_metadata.update({"source": "micromanager_ingest", "relative_path": candidate.relative_path})
        experiment.metadata_json = merged_metadata
    return experiment, created


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
            max_datasets=config.max_datasets,
        )

        ingested_count = 0
        experiment_count = 0
        skipped_count = 0
        candidate_paths: list[str] = []
        ingested_dataset_ids: list[str] = []
        experiment_ids: list[str] = []

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
                    started_at=candidate.last_modified_at,
                    ended_at=candidate.last_modified_at,
                )
                ingested_count += 1
                if experiment_created:
                    experiment_count += 1
                ingested_dataset_ids.append(str(raw_dataset.id))
                experiment_ids.append(str(experiment.id))
        else:
            candidate_paths = [str(candidate.dataset_dir) for candidate in candidates]

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
            report_only=report_only,
        )
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = datetime.now(timezone.utc)
        session.flush()
        raise
