from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.models import Job, StorageProvider, StorageRoot, UserStorageAccount
from api.services.user_home_storage import normalize_home_relative_path, record_provisioning_event


DEFAULT_USER_HOME_SUBDIRECTORIES = ["landing", "projects", "raw", "artifacts", "exports"]


def execute_user_home_storage_job(session: Session, *, job: Job) -> dict:
    job_kind = (job.params_json or {}).get("job_kind")
    if job_kind == "prepare_user_home_storage":
        return execute_prepare_user_home_storage(session, job=job)
    raise ValueError(f"Unsupported user home storage job kind: {job_kind}")


def finalize_user_home_storage_failure(session: Session, *, job: Job, error_text: str) -> None:
    if (job.params_json or {}).get("job_kind") != "prepare_user_home_storage":
        return
    account = load_user_storage_account_for_job(session, job=job)
    account.provisioning_status = "failed"
    record_provisioning_event(
        session,
        user=account.user,
        provider=account.provider,
        storage_account=account,
        event_kind="home_prepare_failed",
        status_text="failed",
        message=error_text,
        metadata_json={"job_id": str(job.id)},
    )


def execute_prepare_user_home_storage(session: Session, *, job: Job) -> dict:
    account = load_user_storage_account_for_job(session, job=job)
    create_directories = bool((job.params_json or {}).get("create_directories", True))
    subdirectories = normalize_subdirectories((job.params_json or {}).get("subdirectories"))
    home_path = resolve_user_home_path(account)

    if create_directories:
        home_path.mkdir(parents=True, exist_ok=True)
        for subdirectory in subdirectories:
            (home_path / subdirectory).mkdir(parents=True, exist_ok=True)

    if not home_path.exists() or not home_path.is_dir():
        raise FileNotFoundError(f"User home path is not accessible: {home_path}")

    prepared_paths = [str(home_path / subdirectory) for subdirectory in subdirectories if (home_path / subdirectory).is_dir()]
    now = datetime.now(timezone.utc)
    account.provisioning_status = "ready"
    account.last_synced_at = now
    account.metadata_json = {
        **dict(account.metadata_json or {}),
        "last_prepared_path": str(home_path),
        "last_prepared_at": now.isoformat(),
        "prepared_subdirectories": subdirectories,
    }
    record_provisioning_event(
        session,
        user=account.user,
        provider=account.provider,
        storage_account=account,
        event_kind="home_prepared",
        status_text="ready",
        message="User home storage path is ready.",
        metadata_json={
            "job_id": str(job.id),
            "home_path": str(home_path),
            "prepared_paths": prepared_paths,
            "create_directories": create_directories,
        },
    )
    session.flush()
    return {
        "job_kind": "prepare_user_home_storage",
        "storage_account_id": str(account.id),
        "user_id": str(account.user_id),
        "provider_key": account.provider.provider_key,
        "provider_kind": account.provider.provider_kind,
        "home_path": str(home_path),
        "prepared_paths": prepared_paths,
        "create_directories": create_directories,
        "status": "ready",
    }


def load_user_storage_account_for_job(session: Session, *, job: Job) -> UserStorageAccount:
    account_id = (job.params_json or {}).get("storage_account_id")
    if not account_id:
        raise ValueError(f"Job {job.id} is missing storage_account_id")
    stmt = (
        select(UserStorageAccount)
        .options(
            joinedload(UserStorageAccount.user),
            joinedload(UserStorageAccount.provider),
            joinedload(UserStorageAccount.home_storage_root),
        )
        .where(UserStorageAccount.id == UUID(str(account_id)))
    )
    account = session.scalars(stmt).unique().first()
    if account is None:
        raise ValueError(f"User storage account {account_id} not found")
    return account


def resolve_user_home_path(account: UserStorageAccount) -> Path:
    if account.provider is None:
        raise ValueError(f"User storage account {account.id} has no storage provider")
    if account.home_storage_root is None:
        raise ValueError(f"User storage account {account.id} has no home storage root")
    if account.provider.provider_kind not in {"posix_mount", "synology_dsm"}:
        raise ValueError(f"Provider kind {account.provider.provider_kind} is not mount-backed")
    return resolve_storage_root_relative_path(account.home_storage_root, account.home_relative_path)


def resolve_storage_root_relative_path(storage_root: StorageRoot, relative_path: str | None) -> Path:
    if not relative_path:
        raise ValueError("Home relative path is required")
    normalized = normalize_home_relative_path(relative_path)
    root_path = Path(storage_root.path_prefix).expanduser().resolve()
    if not root_path.exists() or not root_path.is_dir():
        raise FileNotFoundError(f"Storage root path is not accessible on this worker: {root_path}")
    target_path = (root_path / normalized).resolve()
    try:
        target_path.relative_to(root_path)
    except ValueError as exc:
        raise ValueError("Home path escapes the configured storage root") from exc
    return target_path


def normalize_subdirectories(value) -> list[str]:
    if value is None:
        raw_items = DEFAULT_USER_HOME_SUBDIRECTORIES
    elif isinstance(value, list):
        raw_items = value
    else:
        raise ValueError("subdirectories must be a list")

    normalized: list[str] = []
    for item in raw_items:
        text = normalize_home_relative_path(str(item))
        if "/" in text:
            raise ValueError("User home subdirectories must be direct children")
        if text not in normalized:
            normalized.append(text)
    return normalized
