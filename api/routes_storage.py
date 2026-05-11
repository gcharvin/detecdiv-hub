from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import (
    StorageProvider,
    StorageProvisioningEvent,
    StorageQuotaSnapshot,
    User,
    UserStorageAccount,
)
from api.schemas import (
    StorageProviderCreate,
    StorageProviderSummary,
    StorageProviderUpdate,
    StorageProvisioningEventSummary,
    StorageQuotaSnapshotSummary,
    UserHomeProvisionRequest,
    UserStorageAccountCreate,
    UserStorageAccountSummary,
    UserStorageAccountUpdate,
)
from api.services.user_home_storage import (
    create_hub_usage_snapshot,
    create_storage_provider,
    normalize_provider_kind,
    normalize_provider_key,
    normalize_quota_mode,
    provision_user_home_account,
    record_provisioning_event,
    resolve_provider,
    upsert_user_storage_account,
)
from api.services.users import get_current_user


router = APIRouter(prefix="/storage", tags=["storage"])


def require_storage_admin(current_user: User) -> None:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Storage admin role required")


def account_options():
    return (
        joinedload(UserStorageAccount.user),
        joinedload(UserStorageAccount.provider),
        joinedload(UserStorageAccount.home_storage_root),
    )


def snapshot_options():
    return (
        joinedload(StorageQuotaSnapshot.user),
        joinedload(StorageQuotaSnapshot.provider),
        joinedload(StorageQuotaSnapshot.storage_account).joinedload(UserStorageAccount.user),
        joinedload(StorageQuotaSnapshot.storage_account).joinedload(UserStorageAccount.provider),
        joinedload(StorageQuotaSnapshot.storage_account).joinedload(UserStorageAccount.home_storage_root),
    )


def event_options():
    return (
        joinedload(StorageProvisioningEvent.user),
        joinedload(StorageProvisioningEvent.provider),
        joinedload(StorageProvisioningEvent.storage_account).joinedload(UserStorageAccount.user),
        joinedload(StorageProvisioningEvent.storage_account).joinedload(UserStorageAccount.provider),
        joinedload(StorageProvisioningEvent.storage_account).joinedload(UserStorageAccount.home_storage_root),
    )


def load_account(session: Session, account_id: UUID) -> UserStorageAccount:
    account = session.scalars(select(UserStorageAccount).options(*account_options()).where(UserStorageAccount.id == account_id)).first()
    if account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User storage account not found")
    return account


@router.get("/providers", response_model=list[StorageProviderSummary])
def list_storage_providers(
    active_only: bool = True,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[StorageProvider]:
    _ = current_user
    stmt = select(StorageProvider).order_by(StorageProvider.provider_key.asc())
    if active_only:
        stmt = stmt.where(StorageProvider.is_active.is_(True))
    return list(db.scalars(stmt))


@router.post("/providers", response_model=StorageProviderSummary, status_code=status.HTTP_201_CREATED)
def create_provider(
    payload: StorageProviderCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StorageProvider:
    require_storage_admin(current_user)
    provider = create_storage_provider(
        db,
        provider_key=payload.provider_key,
        display_name=payload.display_name,
        provider_kind=payload.provider_kind,
        mount_root=payload.mount_root,
        quota_mode=payload.quota_mode,
        is_active=payload.is_active,
        capabilities_json=payload.capabilities_json,
        config_json=payload.config_json,
    )
    db.commit()
    db.refresh(provider)
    return provider


@router.patch("/providers/{provider_key}", response_model=StorageProviderSummary)
def update_provider(
    provider_key: str,
    payload: StorageProviderUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StorageProvider:
    require_storage_admin(current_user)
    provider = resolve_provider(db, provider_key)
    if payload.display_name is not None:
        provider.display_name = payload.display_name
    if payload.provider_kind is not None:
        provider.provider_kind = normalize_provider_kind(payload.provider_kind)
    if payload.mount_root is not None:
        provider.mount_root = payload.mount_root
    if payload.quota_mode is not None:
        provider.quota_mode = normalize_quota_mode(payload.quota_mode)
    if payload.is_active is not None:
        provider.is_active = payload.is_active
    if payload.capabilities_json is not None:
        provider.capabilities_json = payload.capabilities_json
    if payload.config_json is not None:
        provider.config_json = payload.config_json
    db.commit()
    db.refresh(provider)
    return provider


@router.get("/user-accounts", response_model=list[UserStorageAccountSummary])
def list_user_storage_accounts(
    user_id: UUID | None = None,
    provider_key: str | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[UserStorageAccount]:
    stmt = select(UserStorageAccount).options(*account_options()).order_by(UserStorageAccount.created_at.desc())
    if current_user.role not in {"admin", "service"}:
        stmt = stmt.where(UserStorageAccount.user_id == current_user.id)
    elif user_id is not None:
        stmt = stmt.where(UserStorageAccount.user_id == user_id)
    if provider_key:
        stmt = stmt.join(StorageProvider).where(StorageProvider.provider_key == normalize_provider_key(provider_key))
    return list(db.scalars(stmt).unique())


@router.post("/user-accounts", response_model=UserStorageAccountSummary, status_code=status.HTTP_201_CREATED)
def create_user_storage_account(
    payload: UserStorageAccountCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> UserStorageAccount:
    require_storage_admin(current_user)
    user = db.get(User, payload.user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    provider = resolve_provider(db, payload.provider_key)
    account = upsert_user_storage_account(
        db,
        user=user,
        provider=provider,
        provider_user_key=payload.provider_user_key,
        home_storage_root_id=payload.home_storage_root_id,
        home_relative_path=payload.home_relative_path,
        quota_bytes=payload.quota_bytes,
        quota_status=payload.quota_status,
        provisioning_status=payload.provisioning_status,
        metadata_json=payload.metadata_json,
    )
    record_provisioning_event(
        db,
        user=user,
        provider=provider,
        storage_account=account,
        event_kind="storage_account_upserted",
        status_text=account.provisioning_status,
        message="User storage account was created or updated in the hub catalog.",
    )
    db.commit()
    return load_account(db, account.id)


@router.patch("/user-accounts/{account_id}", response_model=UserStorageAccountSummary)
def update_user_storage_account(
    account_id: UUID,
    payload: UserStorageAccountUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> UserStorageAccount:
    require_storage_admin(current_user)
    account = db.scalars(select(UserStorageAccount).where(UserStorageAccount.id == account_id)).first()
    if account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User storage account not found")
    provider = db.get(StorageProvider, account.provider_id)
    user = db.get(User, account.user_id)
    if provider is None or user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Storage account is missing linked records")
    account = upsert_user_storage_account(
        db,
        user=user,
        provider=provider,
        provider_user_key=payload.provider_user_key or account.provider_user_key,
        home_storage_root_id=payload.home_storage_root_id if payload.home_storage_root_id is not None else account.home_storage_root_id,
        home_relative_path=payload.home_relative_path or account.home_relative_path,
        quota_bytes=payload.quota_bytes if payload.quota_bytes is not None else account.quota_bytes,
        quota_status=payload.quota_status,
        provisioning_status=payload.provisioning_status,
        metadata_json=payload.metadata_json,
    )
    record_provisioning_event(
        db,
        user=user,
        provider=provider,
        storage_account=account,
        event_kind="storage_account_updated",
        status_text=account.provisioning_status,
    )
    db.commit()
    return load_account(db, account.id)


@router.post("/users/{user_id}/home-account", response_model=UserStorageAccountSummary, status_code=status.HTTP_201_CREATED)
def provision_user_home_storage(
    user_id: UUID,
    payload: UserHomeProvisionRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> UserStorageAccount:
    require_storage_admin(current_user)
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    try:
        provider = resolve_provider(db, payload.provider_key)
    except HTTPException:
        if not payload.create_missing_provider:
            raise
        provider = create_storage_provider(
            db,
            provider_key=payload.provider_key,
            display_name=payload.provider_key,
            provider_kind="posix_mount",
            mount_root=None,
            quota_mode="measured_only",
            is_active=True,
            capabilities_json={"can_validate_home": False, "can_set_quota": False, "can_read_quota": False},
            config_json={},
        )
    account = provision_user_home_account(
        db,
        user=user,
        provider=provider,
        provider_user_key=payload.provider_user_key,
        home_storage_root_id=payload.home_storage_root_id,
        home_relative_path=payload.home_relative_path,
        quota_bytes=payload.quota_bytes,
    )
    db.commit()
    return load_account(db, account.id)


@router.post("/user-accounts/{account_id}/quota-snapshots", response_model=StorageQuotaSnapshotSummary, status_code=status.HTTP_201_CREATED)
def create_quota_snapshot(
    account_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StorageQuotaSnapshot:
    account = db.scalars(select(UserStorageAccount).where(UserStorageAccount.id == account_id)).first()
    if account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User storage account not found")
    if current_user.role not in {"admin", "service"} and account.user_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Storage account owner required")
    snapshot = create_hub_usage_snapshot(db, account=account)
    db.commit()
    return db.scalars(select(StorageQuotaSnapshot).options(*snapshot_options()).where(StorageQuotaSnapshot.id == snapshot.id)).first()


@router.get("/quota-snapshots", response_model=list[StorageQuotaSnapshotSummary])
def list_quota_snapshots(
    user_id: UUID | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[StorageQuotaSnapshot]:
    stmt = select(StorageQuotaSnapshot).options(*snapshot_options()).order_by(StorageQuotaSnapshot.measured_at.desc())
    if current_user.role not in {"admin", "service"}:
        stmt = stmt.where(StorageQuotaSnapshot.user_id == current_user.id)
    elif user_id is not None:
        stmt = stmt.where(StorageQuotaSnapshot.user_id == user_id)
    stmt = stmt.limit(min(max(limit, 1), 500))
    return list(db.scalars(stmt).unique())


@router.get("/provisioning-events", response_model=list[StorageProvisioningEventSummary])
def list_provisioning_events(
    user_id: UUID | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[StorageProvisioningEvent]:
    stmt = select(StorageProvisioningEvent).options(*event_options()).order_by(StorageProvisioningEvent.created_at.desc())
    if current_user.role not in {"admin", "service"}:
        stmt = stmt.where(StorageProvisioningEvent.user_id == current_user.id)
    elif user_id is not None:
        stmt = stmt.where(StorageProvisioningEvent.user_id == user_id)
    stmt = stmt.limit(min(max(limit, 1), 500))
    return list(db.scalars(stmt).unique())
