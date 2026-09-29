from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.models import (
    Project,
    RawDataset,
    StorageProvider,
    StorageProvisioningEvent,
    StorageQuotaSnapshot,
    StorageRoot,
    User,
    UserStorageAccount,
)


VALID_PROVIDER_KINDS = {"posix_mount", "synology_dsm", "smb_share", "nfs"}
VALID_QUOTA_MODES = {"none", "measured_only", "provider_enforced"}


def normalize_provider_key(value: str) -> str:
    key = re.sub(r"[^a-z0-9_-]+", "-", str(value or "").strip().lower()).strip("-")
    if not key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provider key is required")
    return key


def normalize_provider_kind(value: str) -> str:
    kind = str(value or "").strip().lower()
    if kind not in VALID_PROVIDER_KINDS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported storage provider kind: {kind}")
    return kind


def normalize_quota_mode(value: str) -> str:
    mode = str(value or "").strip().lower()
    if mode not in VALID_QUOTA_MODES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported quota mode: {mode}")
    return mode


def storage_safe_user_key(value: str) -> str:
    key = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip()).strip("._-")
    if not key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provider user key is required")
    return key


def normalize_home_relative_path(value: str) -> str:
    raw_input = str(value or "").strip().replace("\\", "/")
    if raw_input.startswith("/"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Home relative path must be relative")
    raw = raw_input.strip("/")
    if not raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Home relative path is required")
    if ":" in raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Home relative path must be relative")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Home relative path must stay under the storage root")
    return str(path)


def default_home_relative_path(provider_user_key: str) -> str:
    return normalize_home_relative_path(f"{storage_safe_user_key(provider_user_key)}/DetecdivHub")


def has_user_scoped_synology_mount(
    *,
    provider: StorageProvider,
    storage_root: StorageRoot,
    home_path: Path,
    provider_user_key: str,
) -> bool:
    """Require Synology writes to pass through a per-home mount below its NAS root.

    A mount at the aggregate ``/homes`` or ``/homes2`` root is not sufficient:
    it may be authenticated as an administrator and charge writes to that
    account instead of the home owner. A nested CIFS mount must use the
    provider's configured per-user ``home_mount_source`` and the DSM identity
    linked to this Hub account.
    """
    if str(provider.provider_kind or "").strip().lower() != "synology_dsm":
        return True
    config = dict(provider.config_json or {})
    expected_source = str(config.get("home_mount_source") or "").strip()
    expected_username = str(provider_user_key or "").strip()
    if not provider.mount_root or not expected_source or not expected_username:
        return False

    try:
        provider_root = Path(provider.mount_root).expanduser().resolve()
        storage_root_path = Path(storage_root.path_prefix).expanduser().resolve()
        target_path = Path(home_path).expanduser().resolve()
    except (OSError, RuntimeError, ValueError):
        return False

    try:
        mountinfo_text = Path("/proc/self/mountinfo").read_text(encoding="utf-8")
    except OSError:
        return False
    return synology_home_mount_matches_mountinfo(
        provider_root=provider_root,
        storage_root_path=storage_root_path,
        target_path=target_path,
        expected_source=expected_source,
        expected_username=expected_username,
        mountinfo_text=mountinfo_text,
    )


def synology_home_mount_matches_mountinfo(
    *,
    provider_root: Path,
    storage_root_path: Path,
    target_path: Path,
    expected_source: str,
    expected_username: str,
    mountinfo_text: str,
) -> bool:
    """Check that the most specific mount serving a home is its owner's CIFS home."""
    try:
        provider_root = Path(provider_root).resolve()
        storage_root_path = Path(storage_root_path).resolve()
        target_path = Path(target_path).resolve()
        storage_root_path.relative_to(provider_root)
        target_path.relative_to(storage_root_path)
    except (OSError, RuntimeError, ValueError):
        return False

    matching_mounts: list[tuple[int, Path, str, str, str]] = []
    for line in mountinfo_text.splitlines():
        try:
            left, right = line.split(" - ", 1)
            left_fields = left.split()
            right_fields = right.split()
            if len(left_fields) < 5 or len(right_fields) < 3:
                continue
            mount_path_text = re.sub(
                r"\\([0-7]{3})",
                lambda match: chr(int(match.group(1), 8)),
                left_fields[4],
            )
            mount_path = Path(mount_path_text).resolve()
            target_path.relative_to(mount_path)
        except (OSError, RuntimeError, ValueError):
            continue

        mount_type, source, super_options = right_fields[:3]
        options = {
            option.split("=", 1)[0]: option.split("=", 1)[1]
            for option in super_options.split(",")
            if "=" in option
        }
        matching_mounts.append(
            (len(mount_path.parts), mount_path, mount_type, source, options.get("username", ""))
        )

    if not matching_mounts:
        return False
    _, mount_path, mount_type, source, username = max(matching_mounts, key=lambda item: item[0])
    try:
        relative_mount_path = mount_path.relative_to(provider_root)
    except ValueError:
        return False

    # Strict descent excludes the broad aggregate `homes` share itself.
    return (
        bool(relative_mount_path.parts)
        and mount_type == "cifs"
        and source == expected_source
        and username == expected_username
    )


def resolve_provider(session: Session, provider_key: str) -> StorageProvider:
    key = normalize_provider_key(provider_key)
    provider = session.scalars(select(StorageProvider).where(StorageProvider.provider_key == key)).first()
    if provider is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Storage provider not found")
    return provider


def resolve_storage_root(session: Session, storage_root_id: int | None) -> StorageRoot | None:
    if storage_root_id is None:
        return None
    storage_root = session.get(StorageRoot, storage_root_id)
    if storage_root is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Home storage root not found")
    return storage_root


def create_storage_provider(
    session: Session,
    *,
    provider_key: str,
    display_name: str,
    provider_kind: str,
    mount_root: str | None,
    quota_mode: str,
    is_active: bool,
    capabilities_json: dict,
    config_json: dict,
) -> StorageProvider:
    key = normalize_provider_key(provider_key)
    if session.scalars(select(StorageProvider).where(StorageProvider.provider_key == key)).first() is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Storage provider already exists")
    provider = StorageProvider(
        provider_key=key,
        display_name=display_name.strip() or key,
        provider_kind=normalize_provider_kind(provider_kind),
        mount_root=mount_root.strip() if mount_root else None,
        quota_mode=normalize_quota_mode(quota_mode),
        is_active=is_active,
        capabilities_json=capabilities_json or {},
        config_json=config_json or {},
    )
    session.add(provider)
    session.flush()
    return provider


def upsert_user_storage_account(
    session: Session,
    *,
    user: User,
    provider: StorageProvider,
    provider_user_key: str | None,
    home_storage_root_id: int | None,
    home_relative_path: str | None,
    quota_bytes: int | None,
    quota_status: str | None = None,
    provisioning_status: str | None = None,
    metadata_json: dict | None = None,
) -> UserStorageAccount:
    normalized_provider_user_key = storage_safe_user_key(provider_user_key or user.user_key)
    relative_path = normalize_home_relative_path(home_relative_path) if home_relative_path else default_home_relative_path(normalized_provider_user_key)
    home_storage_root = resolve_storage_root(session, home_storage_root_id)
    existing = session.scalars(
        select(UserStorageAccount).where(
            UserStorageAccount.user_id == user.id,
            UserStorageAccount.provider_id == provider.id,
        )
    ).first()
    provider_user_owner = session.scalars(
        select(UserStorageAccount).where(
            UserStorageAccount.provider_id == provider.id,
            UserStorageAccount.provider_user_key == normalized_provider_user_key,
        )
    ).first()
    if provider_user_owner is not None and existing is not None and provider_user_owner.id != existing.id:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Provider user key is already linked")
    if provider_user_owner is not None and existing is None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Provider user key is already linked")

    if existing is None:
        existing = UserStorageAccount(
            user_id=user.id,
            provider_id=provider.id,
            provider_user_key=normalized_provider_user_key,
            home_storage_root_id=home_storage_root.id if home_storage_root is not None else None,
            home_relative_path=relative_path,
            quota_bytes=quota_bytes,
            quota_status=quota_status or "unknown",
            provisioning_status=provisioning_status or "planned",
            metadata_json=metadata_json or {},
        )
        session.add(existing)
        session.flush()
        return existing

    existing.provider_user_key = normalized_provider_user_key
    existing.home_storage_root_id = home_storage_root.id if home_storage_root is not None else None
    existing.home_relative_path = relative_path
    existing.quota_bytes = quota_bytes
    if quota_status is not None:
        existing.quota_status = quota_status
    if provisioning_status is not None:
        existing.provisioning_status = provisioning_status
    if metadata_json is not None:
        merged = dict(existing.metadata_json or {})
        merged.update(metadata_json)
        existing.metadata_json = merged
    session.flush()
    return existing


def record_provisioning_event(
    session: Session,
    *,
    user: User | None,
    provider: StorageProvider | None,
    storage_account: UserStorageAccount | None,
    event_kind: str,
    status_text: str,
    message: str | None = None,
    metadata_json: dict | None = None,
) -> StorageProvisioningEvent:
    event = StorageProvisioningEvent(
        user_id=user.id if user is not None else None,
        provider_id=provider.id if provider is not None else None,
        storage_account_id=storage_account.id if storage_account is not None else None,
        event_kind=event_kind,
        status=status_text,
        message=message,
        metadata_json=metadata_json or {},
    )
    session.add(event)
    session.flush()
    return event


def provision_user_home_account(
    session: Session,
    *,
    user: User,
    provider: StorageProvider,
    provider_user_key: str | None,
    home_storage_root_id: int | None,
    home_relative_path: str | None,
    quota_bytes: int | None,
) -> UserStorageAccount:
    account = upsert_user_storage_account(
        session,
        user=user,
        provider=provider,
        provider_user_key=provider_user_key,
        home_storage_root_id=home_storage_root_id,
        home_relative_path=home_relative_path,
        quota_bytes=quota_bytes,
        quota_status="desired" if quota_bytes is not None else "unknown",
        provisioning_status="planned",
        metadata_json={"layout": "per_user_home"},
    )
    record_provisioning_event(
        session,
        user=user,
        provider=provider,
        storage_account=account,
        event_kind="home_account_planned",
        status_text="planned",
        message="User home storage account recorded. Filesystem/provider actions must run through a worker-backed step.",
        metadata_json={
            "provider_kind": provider.provider_kind,
            "quota_mode": provider.quota_mode,
            "home_relative_path": account.home_relative_path,
        },
    )
    return account


def create_hub_usage_snapshot(session: Session, *, account: UserStorageAccount) -> StorageQuotaSnapshot:
    project_bytes = session.scalar(
        select(func.coalesce(func.sum(Project.total_bytes), 0)).where(
            Project.owner_user_id == account.user_id,
            Project.status != "deleted",
        )
    )
    raw_bytes = session.scalar(
        select(func.coalesce(func.sum(RawDataset.total_bytes), 0)).where(RawDataset.owner_user_id == account.user_id)
    )
    artifact_bytes = estimate_artifact_bytes(session, user_id=account.user_id)
    snapshot = StorageQuotaSnapshot(
        user_id=account.user_id,
        provider_id=account.provider_id,
        storage_account_id=account.id,
        provider_user_key=account.provider_user_key,
        quota_bytes=account.quota_bytes,
        provider_used_bytes=None,
        hub_project_bytes=int(project_bytes or 0),
        hub_raw_bytes=int(raw_bytes or 0),
        hub_artifact_bytes=int(artifact_bytes or 0),
        measured_at=datetime.now(timezone.utc),
        metadata_json={"source": "hub_indexed_usage"},
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def estimate_artifact_bytes(session: Session, *, user_id: UUID) -> int:
    _ = session, user_id
    # Artifacts are not yet owned directly. Keep this explicit until artifact
    # storage locations carry enough ownership metadata to roll them up safely.
    return 0
