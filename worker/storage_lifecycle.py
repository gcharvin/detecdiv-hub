from __future__ import annotations

import hashlib
import re
import shutil
import tarfile
import zipfile
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.config import get_settings
from api.models import Artifact, Job, RawDataset, RawDatasetLocation, StorageLifecycleEvent, User
from api.services.raw_dataset_lifecycle import (
    complete_raw_dataset_archive,
    complete_raw_dataset_restore,
    fail_raw_dataset_lifecycle_job,
    pick_preferred_raw_location,
    resolve_raw_location_path,
)


def execute_storage_lifecycle_job(session: Session, *, job: Job) -> dict:
    job_kind = (job.params_json or {}).get("job_kind")
    if job_kind == "archive_raw_dataset":
        return execute_raw_dataset_archive(session, job=job)
    if job_kind == "restore_raw_dataset":
        return execute_raw_dataset_restore(session, job=job)
    raise ValueError(f"Unsupported storage lifecycle job kind: {job_kind}")


def finalize_storage_lifecycle_failure(session: Session, *, job: Job, error_text: str) -> None:
    job_kind = (job.params_json or {}).get("job_kind")
    if job_kind not in {"archive_raw_dataset", "restore_raw_dataset"}:
        return

    raw_dataset = load_raw_dataset_for_job(session, job=job)
    requested_by_user = resolve_requested_by_user(session, job=job)
    if job_kind == "archive_raw_dataset":
        fail_raw_dataset_lifecycle_job(
            session,
            raw_dataset=raw_dataset,
            requested_by_user=requested_by_user,
            event_kind="archive_failed",
            archive_status="archive_failed",
            error_text=error_text,
        )
    else:
        fail_raw_dataset_lifecycle_job(
            session,
            raw_dataset=raw_dataset,
            requested_by_user=requested_by_user,
            event_kind="restore_failed",
            archive_status="restore_failed",
            error_text=error_text,
        )


def execute_raw_dataset_archive(session: Session, *, job: Job) -> dict:
    settings = get_settings()
    raw_dataset = load_raw_dataset_for_job(session, job=job)
    requested_by_user = resolve_requested_by_user(session, job=job)
    source_location = pick_preferred_raw_location(raw_dataset)
    source_path = resolve_raw_location_path(source_location)
    if not source_path.exists():
        raise FileNotFoundError(f"Raw dataset path does not exist: {source_path}")

    compression = ((job.params_json or {}).get("archive_compression") or raw_dataset.archive_compression or settings.default_archive_compression).strip()
    archive_path = resolve_archive_path(
        raw_dataset=raw_dataset,
        source_path=source_path,
        archive_uri=(job.params_json or {}).get("archive_uri") or raw_dataset.archive_uri,
        compression=compression,
        default_archive_root=settings.default_archive_root,
    )
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    if archive_path.exists():
        raise FileExistsError(f"Archive destination already exists: {archive_path}")

    create_archive(source_path=source_path, archive_path=archive_path, compression=compression)
    archive_sha256 = compute_sha256(archive_path)
    archive_bytes = archive_path.stat().st_size

    source_deleted = bool((job.params_json or {}).get("mark_archived"))
    if source_deleted:
        delete_source_path(source_path)

    artifact = Artifact(
        job_id=job.id,
        artifact_kind="raw_dataset_archive",
        uri=str(archive_path),
        metadata_json={
            "compression": compression,
            "sha256": archive_sha256,
            "archive_bytes": archive_bytes,
            "source_path": str(source_path),
            "source_deleted": source_deleted,
        },
    )
    session.add(artifact)

    result_json = {
        "job_kind": "archive_raw_dataset",
        "raw_dataset_id": str(raw_dataset.id),
        "source_path": str(source_path),
        "archive_uri": str(archive_path),
        "archive_compression": compression,
        "archive_bytes": archive_bytes,
        "archive_sha256": archive_sha256,
        "source_deleted": source_deleted,
    }
    complete_raw_dataset_archive(
        session,
        raw_dataset=raw_dataset,
        requested_by_user=requested_by_user,
        archive_uri=str(archive_path),
        archive_compression=compression,
        source_deleted=source_deleted,
        result_json=result_json,
    )
    session.flush()
    return result_json


def execute_raw_dataset_restore(session: Session, *, job: Job) -> dict:
    raw_dataset = load_raw_dataset_for_job(session, job=job)
    requested_by_user = resolve_requested_by_user(session, job=job)
    source_location = pick_preferred_raw_location(raw_dataset)
    target_path = resolve_raw_location_path(source_location)
    archive_uri = (job.params_json or {}).get("archive_uri") or raw_dataset.archive_uri
    if not archive_uri:
        raise ValueError(f"Raw dataset {raw_dataset.id} has no archive_uri to restore from")

    archive_path = Path(archive_uri)
    if not archive_path.exists():
        raise FileNotFoundError(f"Archive file does not exist: {archive_path}")

    restored_from_archive = False
    if not target_path.exists():
        target_path.parent.mkdir(parents=True, exist_ok=True)
        extract_archive(archive_path=archive_path, destination_parent=target_path.parent)
        restored_from_archive = True

    result_json = {
        "job_kind": "restore_raw_dataset",
        "raw_dataset_id": str(raw_dataset.id),
        "archive_uri": str(archive_path),
        "target_path": str(target_path),
        "restored_from_archive": restored_from_archive,
    }
    complete_raw_dataset_restore(
        session,
        raw_dataset=raw_dataset,
        requested_by_user=requested_by_user,
        result_json=result_json,
    )
    session.flush()
    return result_json


def load_raw_dataset_for_job(session: Session, *, job: Job) -> RawDataset:
    if job.raw_dataset_id is None:
        raise ValueError(f"Job {job.id} does not reference a raw dataset")
    stmt = (
        select(RawDataset)
        .options(
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.lifecycle_events).joinedload(StorageLifecycleEvent.requested_by),
        )
        .where(RawDataset.id == job.raw_dataset_id)
    )
    raw_dataset = session.scalars(stmt).unique().first()
    if raw_dataset is None:
        raise ValueError(f"Raw dataset {job.raw_dataset_id} not found")
    return raw_dataset


def resolve_requested_by_user(session: Session, *, job: Job) -> User | None:
    if not job.requested_by:
        return None
    stmt = select(User).where(User.user_key == job.requested_by).limit(1)
    return session.scalars(stmt).first()


def resolve_archive_path(
    *,
    raw_dataset: RawDataset,
    source_path: Path,
    archive_uri: str | None,
    compression: str,
    default_archive_root: str,
) -> Path:
    compression = compression.lower()
    extension = archive_extension(compression)
    safe_label = slugify(raw_dataset.external_key or raw_dataset.acquisition_label or source_path.name)
    default_name = f"{safe_label}-{raw_dataset.id}{extension}"

    if archive_uri:
        candidate = Path(archive_uri)
        if archive_uri.endswith(extension):
            return candidate
        if archive_uri.endswith(".zip") or archive_uri.endswith(".tar.gz"):
            return candidate
        return candidate / default_name

    if not default_archive_root:
        raise ValueError("No archive destination configured; set archive_uri or DETECDIV_HUB_DEFAULT_ARCHIVE_ROOT")
    return Path(default_archive_root) / default_name


def archive_extension(compression: str) -> str:
    normalized = compression.lower()
    if normalized == "zip":
        return ".zip"
    if normalized in {"tar.gz", "tgz"}:
        return ".tar.gz"
    raise ValueError(f"Unsupported archive compression: {compression}")


def create_archive(*, source_path: Path, archive_path: Path, compression: str) -> None:
    normalized = compression.lower()
    if normalized == "zip":
        write_zip_archive(source_path=source_path, archive_path=archive_path)
        return
    if normalized in {"tar.gz", "tgz"}:
        write_tar_gz_archive(source_path=source_path, archive_path=archive_path)
        return
    raise ValueError(f"Unsupported archive compression: {compression}")


def extract_archive(*, archive_path: Path, destination_parent: Path) -> None:
    archive_name = archive_path.name.lower()
    if archive_name.endswith(".zip"):
        with zipfile.ZipFile(archive_path, mode="r") as archive:
            archive.extractall(destination_parent)
        return
    if archive_name.endswith(".tar.gz") or archive_name.endswith(".tgz"):
        with tarfile.open(archive_path, mode="r:gz") as archive:
            archive.extractall(destination_parent)
        return
    raise ValueError(f"Unsupported archive format for restore: {archive_path}")


def write_zip_archive(*, source_path: Path, archive_path: Path) -> None:
    with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        if source_path.is_dir():
            for child in sorted(source_path.rglob("*")):
                if child.is_dir():
                    continue
                archive.write(child, arcname=child.relative_to(source_path.parent))
            return
        archive.write(source_path, arcname=source_path.name)


def write_tar_gz_archive(*, source_path: Path, archive_path: Path) -> None:
    with tarfile.open(archive_path, mode="w:gz", compresslevel=9) as archive:
        archive.add(source_path, arcname=source_path.name)


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def delete_source_path(source_path: Path) -> None:
    if source_path.is_dir():
        shutil.rmtree(source_path)
        return
    source_path.unlink()


def slugify(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    normalized = normalized.strip("-._")
    return normalized or "raw-dataset"
