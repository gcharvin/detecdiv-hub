from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.orm import Session, joinedload

from api.config import Settings, get_settings
from api.models import (
    AcquisitionSession,
    ExperimentProject,
    Job,
    MicroManagerIngestRun,
    Pipeline,
    RawDataset,
    User,
    UserStorageAccount,
)
from api.services.acquisition_sessions import merge_json
from api.services.micromanager_metadata import (
    extract_acquisition_dimensions,
    find_micromanager_display_settings_path,
    read_micromanager_metadata,
)
from api.services.external_publications import ensure_publication_records
from api.services.project_indexing import iter_orphan_raw_candidates, looks_like_raw_dataset_dir, slugify
from api.services.raw_dataset_ingest import ingest_raw_dataset_from_directory
from api.services.users import get_or_create_user


MICROMANAGER_INGEST_LOCK_KEY = 2026031102
DETECDIV_ACQUISITION_MANIFEST_FILE = "detecdiv_acquisition_manifest.json"
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
    landing_roots: list["MicroManagerLandingRootData"] | None = None

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
            "landing_roots": [root.to_json() for root in self.landing_roots or []],
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
    owner_user_key: str | None = None
    landing_root: str | None = None
    landing_root_key: str | None = None


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


@dataclass
class PromotedDatasetPath:
    dataset_dir: Path
    root_path: Path
    storage_root_name: str | None
    metadata_json: dict


@dataclass
class MicroManagerLandingRootData:
    root_key: str
    label: str
    path: str
    source: str
    user_key: str | None = None
    is_default: bool = False

    def to_json(self) -> dict:
        return {
            "root_key": self.root_key,
            "label": self.label,
            "path": self.path,
            "source": self.source,
            "user_key": self.user_key,
            "is_default": self.is_default,
        }


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


def account_landing_root_path(account: UserStorageAccount) -> Path | None:
    if account.home_storage_root is None or not account.home_relative_path:
        return None
    home_root = Path(account.home_storage_root.path_prefix).expanduser()
    return home_root / str(account.home_relative_path).strip("/") / "landing"


def list_user_micromanager_landing_roots(
    session: Session,
    *,
    default_user_key: str | None = None,
    include_inactive_users: bool = False,
) -> list[MicroManagerLandingRootData]:
    stmt = (
        select(UserStorageAccount)
        .join(UserStorageAccount.user)
        .options(
            joinedload(UserStorageAccount.user),
            joinedload(UserStorageAccount.home_storage_root),
            joinedload(UserStorageAccount.provider),
        )
        .order_by(User.user_key.asc(), UserStorageAccount.updated_at.desc())
    )
    if not include_inactive_users:
        stmt = stmt.where(User.is_active.is_(True))
    accounts = list(session.scalars(stmt).unique())
    roots: list[MicroManagerLandingRootData] = []
    seen_users: set[str] = set()
    normalized_default = slugify(default_user_key or "")
    for account in accounts:
        user = account.user
        if user is None:
            continue
        user_key = str(user.user_key or "").strip()
        if not user_key:
            continue
        normalized_user = slugify(user_key)
        if normalized_user in seen_users:
            continue
        if str(account.provisioning_status or "").strip().lower() not in {"ready", "provider_user_ready"}:
            continue
        provider_kind = str(getattr(account.provider, "provider_kind", "") or "").strip().lower()
        if provider_kind not in {"posix_mount", "synology_dsm"}:
            continue
        path = account_landing_root_path(account)
        if path is None:
            continue
        seen_users.add(normalized_user)
        roots.append(
            MicroManagerLandingRootData(
                root_key=f"user:{normalized_user}",
                label=f"{user.display_name or user_key} landing",
                path=str(path),
                source="user_home",
                user_key=user_key,
                is_default=normalized_user == normalized_default,
            )
        )
    return roots


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
    owner_user_key: str | None = None,
    landing_root_key: str | None = None,
) -> list[MicroManagerDatasetCandidate]:
    now = datetime.now(timezone.utc)
    candidates: list[MicroManagerDatasetCandidate] = []
    seen_paths: set[Path] = set()

    paths_to_scan = []
    root_dataset = classify_micromanager_dataset_dir(landing_root)
    if root_dataset is not None:
        paths_to_scan.append(root_dataset)
    paths_to_scan.extend(iter_orphan_raw_candidates(landing_root, project_dirs=[]))

    for path in paths_to_scan:
        if len(candidates) >= max_datasets:
            break
        dataset_dir = classify_micromanager_dataset_dir(path) or path
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
        manifest_json = read_detecdiv_acquisition_manifest(dataset_dir, landing_root=landing_root)
        manifest_positions = manifest_json.get("positions") if isinstance(manifest_json, dict) else None
        manifest_position_annotations = manifest_json.get("position_annotations") if isinstance(manifest_json, dict) else None
        manifest_labguru = manifest_json.get("labguru") if isinstance(manifest_json, dict) else None
        manifest_mda_settings = manifest_json.get("mda_settings_json") if isinstance(manifest_json, dict) else None
        manifest_mda_summary = manifest_json.get("mda_summary") if isinstance(manifest_json, dict) else None
        microscope_name = extract_microscope_name(metadata_json)
        if not microscope_name:
            microscope_name = manifest_string(manifest_json, "microscope_name")
        acquisition_label = extract_acquisition_label(dataset_dir, metadata_json)
        manifest_acquisition_label = manifest_string(manifest_json, "acquisition_label")
        if manifest_acquisition_label:
            acquisition_label = manifest_acquisition_label
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
        relative_path = str(dataset_dir.relative_to(landing_root))
        inferred_owner_user_key = owner_user_key or manifest_string(manifest_json, "user_key") or infer_owner_user_key_from_landing_relative_path(relative_path)
        candidates.append(
            MicroManagerDatasetCandidate(
                dataset_dir=dataset_dir,
                relative_path=relative_path,
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
                    "relative_path": relative_path,
                    "file_count": file_count,
                    "last_modified_at": last_modified_at.isoformat(),
                    "session_label": session_label,
                    "session_date": session_date.isoformat() if session_date else None,
                    "group_key": group_key,
                    "group_label": group_label,
                    "landing_root": str(landing_root),
                    "landing_root_key": landing_root_key,
                    "dimensions": dimensions,
                    "display_settings_uri": str(display_settings_path) if display_settings_path is not None else None,
                    "detecdiv_acquisition_manifest": manifest_json,
                    "mda_settings_json": manifest_mda_settings if isinstance(manifest_mda_settings, dict) else {},
                    "mda_summary": manifest_mda_summary if isinstance(manifest_mda_summary, dict) else {},
                    "positions": manifest_positions if isinstance(manifest_positions, list) else [],
                    "position_annotations": (
                        manifest_position_annotations if isinstance(manifest_position_annotations, list) else []
                    ),
                    "labguru": manifest_labguru if isinstance(manifest_labguru, dict) else {},
                },
                completeness_status=completeness_status,
                owner_user_key=inferred_owner_user_key,
                landing_root=str(landing_root),
                landing_root_key=landing_root_key,
            )
        )
    return candidates


def infer_owner_user_key_from_landing_relative_path(relative_path: str) -> str | None:
    normalized = relative_path.replace("\\", "/").strip("/")
    parts = [part for part in normalized.split("/") if part]
    if len(parts) >= 2 and parts[0].lower() == "acquisitions":
        return slugify(parts[1])
    return None


def classify_micromanager_dataset_dir(path: Path) -> Path | None:
    if not path.exists() or not path.is_dir():
        return None

    if looks_like_raw_dataset_dir(path):
        return path

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
        if looks_like_raw_dataset_dir(child):
            return child
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


def read_detecdiv_acquisition_manifest(dataset_dir: Path, *, landing_root: Path) -> dict:
    for path in [dataset_dir, *dataset_dir.parents]:
        manifest_path = path / DETECDIV_ACQUISITION_MANIFEST_FILE
        if manifest_path.is_file():
            try:
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return {}
            if isinstance(payload, dict):
                return {
                    **payload,
                    "manifest_path": str(manifest_path),
                }
            return {}
        if path == landing_root:
            break
    return {}


def manifest_string(manifest_json: dict, key: str) -> str | None:
    value = manifest_json.get(key) if isinstance(manifest_json, dict) else None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


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


def promote_micromanager_candidate_to_user_home(
    session: Session,
    *,
    owner: User,
    candidate: MicroManagerDatasetCandidate,
    landing_root: Path,
    fallback_storage_root_name: str | None,
) -> PromotedDatasetPath:
    landing_dataset_dir = candidate.dataset_dir
    source_metadata = dict(candidate.metadata_json or {})
    promotion_metadata: dict[str, object] = {
        "source": "landing_zone_promotion",
        "original_dataset_path": str(landing_dataset_dir),
        "original_landing_root": str(landing_root),
        "original_landing_relative_path": candidate.relative_path,
    }

    account = session.scalars(
        select(UserStorageAccount)
        .options(
            joinedload(UserStorageAccount.home_storage_root),
            joinedload(UserStorageAccount.provider),
        )
        .where(UserStorageAccount.user_id == owner.id)
        .order_by(UserStorageAccount.updated_at.desc())
    ).first()
    if account is None:
        return PromotedDatasetPath(
            dataset_dir=landing_dataset_dir,
            root_path=landing_root,
            storage_root_name=fallback_storage_root_name,
            metadata_json={
                **source_metadata,
                "landing_zone_promotion": {
                    **promotion_metadata,
                    "status": "skipped",
                    "reason": "owner_has_no_storage_account",
                },
            },
        )
    if account.home_storage_root is None or not account.home_relative_path:
        return PromotedDatasetPath(
            dataset_dir=landing_dataset_dir,
            root_path=landing_root,
            storage_root_name=fallback_storage_root_name,
            metadata_json={
                **source_metadata,
                "landing_zone_promotion": {
                    **promotion_metadata,
                    "status": "skipped",
                    "reason": "owner_storage_account_has_no_home_path",
                    "storage_account_id": str(account.id),
                },
            },
        )
    provider_kind = str(getattr(account.provider, "provider_kind", "") or "").strip().lower()
    if provider_kind not in {"posix_mount", "synology_dsm"}:
        return PromotedDatasetPath(
            dataset_dir=landing_dataset_dir,
            root_path=landing_root,
            storage_root_name=fallback_storage_root_name,
            metadata_json={
                **source_metadata,
                "landing_zone_promotion": {
                    **promotion_metadata,
                    "status": "skipped",
                    "reason": "owner_storage_provider_is_not_mount_backed",
                    "storage_account_id": str(account.id),
                    "provider_kind": provider_kind,
                },
            },
        )
    if str(account.provisioning_status or "").strip().lower() not in {"ready", "provider_user_ready"}:
        return PromotedDatasetPath(
            dataset_dir=landing_dataset_dir,
            root_path=landing_root,
            storage_root_name=fallback_storage_root_name,
            metadata_json={
                **source_metadata,
                "landing_zone_promotion": {
                    **promotion_metadata,
                    "status": "skipped",
                    "reason": "owner_storage_account_not_ready",
                    "storage_account_id": str(account.id),
                    "provisioning_status": account.provisioning_status,
                },
            },
        )

    home_root = Path(account.home_storage_root.path_prefix).expanduser().resolve()
    home_path = (home_root / str(account.home_relative_path).strip("/")).resolve()
    if not home_path.exists() or not home_path.is_dir():
        return PromotedDatasetPath(
            dataset_dir=landing_dataset_dir,
            root_path=landing_root,
            storage_root_name=fallback_storage_root_name,
            metadata_json={
                **source_metadata,
                "landing_zone_promotion": {
                    **promotion_metadata,
                    "status": "skipped",
                    "reason": "owner_home_path_not_accessible",
                    "storage_account_id": str(account.id),
                    "home_path": str(home_path),
                },
            },
        )
    try:
        home_path.relative_to(home_root)
    except ValueError:
        return PromotedDatasetPath(
            dataset_dir=landing_dataset_dir,
            root_path=landing_root,
            storage_root_name=fallback_storage_root_name,
            metadata_json={
                **source_metadata,
                "landing_zone_promotion": {
                    **promotion_metadata,
                    "status": "skipped",
                    "reason": "owner_home_path_escapes_storage_root",
                    "storage_account_id": str(account.id),
                },
            },
        )

    dataset_date = candidate.session_date or candidate.last_modified_at
    date_bucket = dataset_date.strftime("%Y%m%d") if dataset_date is not None else "undated"
    raw_root = home_path / "raw"
    destination_parent = raw_root / date_bucket
    destination_dir = destination_parent / landing_dataset_dir.name

    try:
        raw_root.mkdir(parents=True, exist_ok=True)
        destination_parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return PromotedDatasetPath(
            dataset_dir=landing_dataset_dir,
            root_path=landing_root,
            storage_root_name=fallback_storage_root_name,
            metadata_json={
                **source_metadata,
                "landing_zone_promotion": {
                    **promotion_metadata,
                    "status": "skipped",
                    "reason": "destination_parent_not_writable",
                    "error": str(exc),
                    "storage_account_id": str(account.id),
                    "destination_parent": str(destination_parent),
                },
            },
        )

    if destination_dir.exists():
        if destination_dir.resolve() == landing_dataset_dir.resolve():
            status = "already_promoted"
        else:
            return PromotedDatasetPath(
                dataset_dir=landing_dataset_dir,
                root_path=landing_root,
                storage_root_name=fallback_storage_root_name,
                metadata_json={
                    **source_metadata,
                    "landing_zone_promotion": {
                        **promotion_metadata,
                        "status": "skipped",
                        "reason": "destination_exists",
                        "storage_account_id": str(account.id),
                        "destination_path": str(destination_dir),
                    },
                },
            )
    else:
        try:
            same_device = landing_dataset_dir.stat().st_dev == destination_parent.stat().st_dev
        except OSError as exc:
            return PromotedDatasetPath(
                dataset_dir=landing_dataset_dir,
                root_path=landing_root,
                storage_root_name=fallback_storage_root_name,
                metadata_json={
                    **source_metadata,
                    "landing_zone_promotion": {
                        **promotion_metadata,
                        "status": "skipped",
                        "reason": "device_check_failed",
                        "error": str(exc),
                        "storage_account_id": str(account.id),
                        "destination_path": str(destination_dir),
                    },
                },
            )
        if not same_device:
            return PromotedDatasetPath(
                dataset_dir=landing_dataset_dir,
                root_path=landing_root,
                storage_root_name=fallback_storage_root_name,
                metadata_json={
                    **source_metadata,
                    "landing_zone_promotion": {
                        **promotion_metadata,
                        "status": "skipped",
                        "reason": "different_filesystem",
                        "storage_account_id": str(account.id),
                        "destination_path": str(destination_dir),
                    },
                },
            )
        landing_dataset_dir.rename(destination_dir)
        status = "promoted"

    promoted_metadata = {
        **source_metadata,
        "landing_zone_promotion": {
            **promotion_metadata,
            "status": status,
            "storage_account_id": str(account.id),
            "home_storage_root_name": account.home_storage_root.name,
            "home_relative_path": account.home_relative_path,
            "destination_path": str(destination_dir),
            "destination_root": str(raw_root),
            "destination_relative_path": str(destination_dir.relative_to(raw_root)),
        },
    }
    return PromotedDatasetPath(
        dataset_dir=destination_dir,
        root_path=raw_root,
        storage_root_name=f"raw-{slugify(owner.user_key)}",
        metadata_json=promoted_metadata,
    )


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


def acquisition_session_for_candidate(
    session: Session,
    candidate: MicroManagerDatasetCandidate,
) -> AcquisitionSession | None:
    manifest = candidate.metadata_json.get("detecdiv_acquisition_manifest")
    if not isinstance(manifest, dict):
        return None

    acquisition_session_id = manifest_string(manifest, "acquisition_session_id")
    if acquisition_session_id:
        try:
            acquisition_session = session.scalars(
                select(AcquisitionSession)
                .options(joinedload(AcquisitionSession.experiment_project))
                .where(AcquisitionSession.id == UUID(acquisition_session_id))
            ).first()
        except ValueError:
            acquisition_session = None
        if acquisition_session is not None:
            return acquisition_session

    acquisition_session_key = manifest_string(manifest, "acquisition_session_key")
    if acquisition_session_key:
        acquisition_session = session.scalars(
            select(AcquisitionSession)
            .options(joinedload(AcquisitionSession.experiment_project))
            .where(AcquisitionSession.session_key == acquisition_session_key)
        ).first()
        if acquisition_session is not None:
            return acquisition_session

    landing_relative_path = manifest_string(manifest, "landing_relative_path")
    if landing_relative_path:
        normalized_landing_path = landing_relative_path.strip().strip("/").replace("\\", "/")
        return session.scalars(
            select(AcquisitionSession)
            .options(joinedload(AcquisitionSession.experiment_project))
            .where(AcquisitionSession.landing_relative_path == normalized_landing_path)
            .order_by(AcquisitionSession.created_at.desc())
        ).first()
    return None


def mark_acquisition_session_ingested(
    acquisition_session: AcquisitionSession | None,
    *,
    raw_dataset: RawDataset,
    experiment: ExperimentProject,
    candidate: MicroManagerDatasetCandidate,
) -> None:
    if acquisition_session is None:
        return
    acquisition_session.raw_dataset_id = raw_dataset.id
    acquisition_session.experiment_project_id = experiment.id
    acquisition_session.result_json = merge_json(
        acquisition_session.result_json,
        {
            "raw_dataset_id": str(raw_dataset.id),
            "experiment_project_id": str(experiment.id),
            "micromanager_ingest": {
                "linked": True,
                "relative_path": candidate.relative_path,
                "raw_dataset_external_key": raw_dataset.external_key,
            },
        },
    )


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
        landing_roots = list(config.landing_roots or [])
        if not landing_roots and config.landing_root:
            landing_roots.append(
                MicroManagerLandingRootData(
                    root_key="configured",
                    label="Configured landing root",
                    path=config.landing_root,
                    source="configured",
                )
            )
        if not landing_roots:
            raise ValueError("Micro-Manager ingest root is not configured")

        candidates: list[tuple[MicroManagerDatasetCandidate, Path, str | None]] = []
        scanned_roots: list[dict] = []
        for landing_root_config in landing_roots:
            if len(candidates) >= config.max_datasets:
                break
            landing_root = Path(landing_root_config.path).expanduser().resolve()
            root_record = landing_root_config.to_json()
            root_record["resolved_path"] = str(landing_root)
            if not landing_root.exists() or not landing_root.is_dir():
                root_record["status"] = "inaccessible"
                scanned_roots.append(root_record)
                continue
            root_record["status"] = "scanned"
            remaining_limit = max(1, config.max_datasets - len(candidates))
            root_candidates = discover_micromanager_candidates(
                landing_root=landing_root,
                settle_seconds=config.settle_seconds,
                grouping_window_hours=config.grouping_window_hours,
                max_datasets=remaining_limit,
                owner_user_key=landing_root_config.user_key,
                landing_root_key=landing_root_config.root_key,
            )
            root_record["candidate_count"] = len(root_candidates)
            scanned_roots.append(root_record)
            candidates.extend((candidate, landing_root, landing_root_config.root_key) for candidate in root_candidates)

        ingested_count = 0
        experiment_count = 0
        skipped_count = 0
        candidate_paths: list[str] = []
        ingested_dataset_ids: list[str] = []
        experiment_ids: list[str] = []
        queued_job_ids: list[str] = []

        if not report_only:
            for candidate, landing_root, _landing_root_key in candidates:
                candidate_paths.append(str(candidate.dataset_dir))
                candidate_owner = (
                    get_or_create_user(
                        session,
                        user_key=candidate.owner_user_key,
                        display_name=candidate.owner_user_key,
                    )
                    if candidate.owner_user_key
                    else triggered_by_user
                )
                acquisition_session = acquisition_session_for_candidate(session, candidate)
                if acquisition_session is not None and acquisition_session.experiment_project is not None:
                    experiment = acquisition_session.experiment_project
                    experiment_created = False
                else:
                    experiment, experiment_created = ensure_micromanager_experiment(
                        session,
                        owner=candidate_owner,
                        visibility=config.visibility,
                        candidate=candidate,
                    )
                promoted_path = promote_micromanager_candidate_to_user_home(
                    session,
                    owner=candidate_owner,
                    candidate=candidate,
                    landing_root=landing_root,
                    fallback_storage_root_name=config.storage_root_name,
                )
                raw_dataset = ingest_raw_dataset_from_directory(
                    session,
                    owner=candidate_owner,
                    visibility=config.visibility,
                    storage_root_name=promoted_path.storage_root_name,
                    host_scope=config.host_scope,
                    root_type="raw_root",
                    root_path=promoted_path.root_path,
                    dataset_dir=promoted_path.dataset_dir,
                    preferred_experiment=experiment,
                    source_label="micromanager_ingest",
                    source_metadata=promoted_path.metadata_json,
                    acquisition_label=candidate.acquisition_label,
                    microscope_name=candidate.microscope_name,
                    external_key=build_micromanager_raw_key(candidate.dataset_dir, candidate.relative_path),
                    status="indexed",
                    completeness_status=candidate.completeness_status,
                    started_at=candidate.session_date or candidate.last_modified_at,
                    ended_at=candidate.last_modified_at,
                )
                mark_acquisition_session_ingested(
                    acquisition_session,
                    raw_dataset=raw_dataset,
                    experiment=experiment,
                    candidate=candidate,
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
            candidate_paths = [str(candidate.dataset_dir) for candidate, _landing_root, _landing_root_key in candidates]
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
            "scanned_roots": scanned_roots,
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
