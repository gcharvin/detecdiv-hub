from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.config import get_settings
from api.db import get_db
from api.models import (
    ExecutionTarget,
    Job,
    MiscStorageItem,
    StorageProvider,
    StorageProvisioningEvent,
    StorageQuotaSnapshot,
    User,
    UserStorageAccount,
)
from api.schemas import (
    IndexJobLaunchResponse,
    IndexJobSummary,
    IndexRequest,
    StorageProviderCreate,
    StorageProviderSummary,
    StorageProviderUpdate,
    StorageProvisioningEventSummary,
    StorageQuotaSnapshotSummary,
    SynologyDsmApiProbeRequest,
    SynologyDsmApiProbeResponse,
    SynologyDsmDiscoveryResponse,
    SynologyDsmEnsureUserRequest,
    SynologyDsmEnsureUserResponse,
    SynologyDsmLoginCheckResponse,
    SynologyQuotaUpdateRequest,
    SynologyQuotaUpdateResponse,
    SynologyDsmUserHomeResponse,
    SynologyDsmUserListResponse,
    SynologyDsmUserQuotaResponse,
    SynologyDsmUserSummary,
    UserHomePrepareRequest,
    UserHomePrepareResponse,
    UserHomeProvisionRequest,
    MiscStorageExploreChildrenRequest,
    MiscStorageIndexProjectsRequest,
    MiscStorageInventoryRequest,
    MiscStorageInventoryResponse,
    MiscStorageItemSummary,
    MiscStorageItemUpdate,
    UserStorageAccountCreate,
    UserStorageAccountSummary,
    UserStorageAccountUpdate,
)
from api.services.indexing_jobs import create_indexing_job, enqueue_indexing_worker_job, get_indexing_job_for_user
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
from api.services.storage_providers.synology_dsm import (
    SynologyDsmClient,
    SynologyDsmError,
    parse_user_quota_payload,
    summarize_discovered_capabilities,
)
from api.services.storage_providers.synology_ssh import SynologySshClient, SynologySshError
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


def misc_item_options():
    return (
        joinedload(MiscStorageItem.owner),
        joinedload(MiscStorageItem.storage_root),
    )


def misc_item_absolute_path(item: MiscStorageItem) -> str:
    metadata = item.metadata_json if isinstance(item.metadata_json, dict) else {}
    absolute_path = str(metadata.get("absolute_path") or "").strip()
    if absolute_path:
        return absolute_path
    return str((Path(item.storage_root.path_prefix) / (item.relative_path or "")).resolve())


def resolve_storage_worker_target(
    session: Session,
    *,
    execution_target_id: UUID | None = None,
    execution_target_key: str | None = None,
) -> ExecutionTarget | None:
    if execution_target_id is not None:
        target = session.get(ExecutionTarget, execution_target_id)
        if target is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Execution target not found")
        return target

    target_key = str(execution_target_key or get_settings().indexing_target_key or get_settings().worker_target_key or "").strip()
    if not target_key:
        return None
    target = session.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key == target_key)).first()
    if target is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Execution target '{target_key}' not found")
    return target


def load_account(session: Session, account_id: UUID) -> UserStorageAccount:
    account = session.scalars(select(UserStorageAccount).options(*account_options()).where(UserStorageAccount.id == account_id)).first()
    if account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User storage account not found")
    return account


@router.get("/misc-items", response_model=list[MiscStorageItemSummary])
def list_misc_storage_items(
    user_id: UUID | None = None,
    owner_user_key: str | None = None,
    category: str | None = None,
    status_filter: str | None = None,
    min_size_bytes: int | None = None,
    limit: int = 200,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[MiscStorageItem]:
    stmt = select(MiscStorageItem).options(*misc_item_options()).order_by(MiscStorageItem.total_bytes.desc())
    if current_user.role not in {"admin", "service"}:
        stmt = stmt.where(MiscStorageItem.owner_user_id == current_user.id)
    elif user_id is not None:
        stmt = stmt.where(MiscStorageItem.owner_user_id == user_id)
    elif owner_user_key:
        stmt = stmt.join(User, MiscStorageItem.owner_user_id == User.id).where(User.user_key == owner_user_key)
    if category:
        stmt = stmt.where(MiscStorageItem.category == category)
    if status_filter:
        stmt = stmt.where(MiscStorageItem.status == status_filter)
    else:
        stmt = stmt.where(MiscStorageItem.status.notin_(("cataloged", "deleted")))
    if min_size_bytes is not None:
        stmt = stmt.where(MiscStorageItem.total_bytes >= max(int(min_size_bytes), 0))
    stmt = stmt.limit(min(max(int(limit), 1), 1000))
    return list(db.scalars(stmt).unique())


@router.patch("/misc-items/{item_id}", response_model=MiscStorageItemSummary)
def update_misc_storage_item(
    item_id: UUID,
    payload: MiscStorageItemUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MiscStorageItem:
    require_storage_admin(current_user)
    item = db.scalars(select(MiscStorageItem).options(*misc_item_options()).where(MiscStorageItem.id == item_id)).first()
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Misc storage item not found")
    if payload.owner_user_id is not None:
        if db.get(User, payload.owner_user_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Owner user not found")
        item.owner_user_id = payload.owner_user_id
    if payload.category is not None:
        item.category = payload.category
    if payload.status is not None:
        item.status = payload.status
    if payload.visibility is not None:
        item.visibility = payload.visibility
    if payload.lifecycle_tier is not None:
        item.lifecycle_tier = payload.lifecycle_tier
    if payload.archive_status is not None:
        item.archive_status = payload.archive_status
    if payload.archive_uri is not None:
        item.archive_uri = payload.archive_uri
    if payload.backup_status is not None:
        item.backup_status = payload.backup_status
    if payload.backup_excluded is not None:
        item.backup_excluded = payload.backup_excluded
    if payload.notes is not None:
        item.notes = payload.notes
    if payload.metadata_json is not None:
        item.metadata_json = {**dict(item.metadata_json or {}), **payload.metadata_json}
    db.commit()
    return db.scalars(select(MiscStorageItem).options(*misc_item_options()).where(MiscStorageItem.id == item.id)).first()


@router.post("/misc-items/{item_id}/inventory-children/jobs", response_model=MiscStorageInventoryResponse, status_code=status.HTTP_202_ACCEPTED)
def queue_misc_storage_children_inventory_job(
    item_id: UUID,
    payload: MiscStorageExploreChildrenRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MiscStorageInventoryResponse:
    require_storage_admin(current_user)
    item = db.scalars(select(MiscStorageItem).options(*misc_item_options()).where(MiscStorageItem.id == item_id)).first()
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Misc storage item not found")
    target = resolve_storage_worker_target(
        db,
        execution_target_id=payload.execution_target_id,
        execution_target_key=payload.execution_target_key,
    )
    source_path = misc_item_absolute_path(item)
    job = Job(
        execution_target_id=target.id if target is not None else None,
        requested_mode=payload.requested_mode,
        priority=payload.priority,
        requested_by=current_user.user_key,
        requested_from_host="api-storage",
        params_json={
            "job_kind": "misc_storage_inventory",
            "source_path": source_path,
            "storage_root_id": item.storage_root_id,
            "parent_item_id": str(item.id),
            "storage_root_name": item.storage_root.name,
            "host_scope": item.storage_root.host_scope,
            "root_type": item.storage_root.root_type,
            "owner_user_key": item.owner.user_key if item.owner is not None else None,
            "visibility": item.visibility,
            "min_size_bytes": payload.min_size_bytes,
            "max_depth": max(1, min(int(payload.max_depth or 1), 2)),
            "du_timeout_sec": payload.du_timeout_sec,
            "include_cataloged": payload.include_cataloged,
            "metadata_json": {
                **dict(payload.metadata_json or {}),
                "launched_from_misc_item_id": str(item.id),
                "launched_from_misc_path": item.relative_path,
            },
        },
    )
    db.add(job)
    db.commit()
    return MiscStorageInventoryResponse(
        status="queued",
        job_id=job.id,
        scanned_count=0,
        indexed_count=0,
        skipped_count=0,
        timeout_count=0,
        message=f"Queued child inventory for {item.relative_path}.",
    )


@router.post("/misc-items/{item_id}/index-projects/jobs", response_model=IndexJobLaunchResponse, status_code=status.HTTP_202_ACCEPTED)
def queue_misc_storage_project_index_job(
    item_id: UUID,
    payload: MiscStorageIndexProjectsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IndexJobLaunchResponse:
    require_storage_admin(current_user)
    item = db.scalars(select(MiscStorageItem).options(*misc_item_options()).where(MiscStorageItem.id == item_id)).first()
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Misc storage item not found")

    source_path = misc_item_absolute_path(item)
    owner_user_key = payload.owner_user_key or (item.owner.user_key if item.owner is not None else None)
    request = IndexRequest(
        source_kind="project_root",
        source_path=source_path,
        storage_root_name=payload.storage_root_name,
        host_scope="server",
        root_type="project_root",
        owner_user_key=owner_user_key,
        visibility=payload.visibility,
        clear_existing_for_root=payload.clear_existing_for_root,
        scan_orphan_raw=payload.scan_orphan_raw,
        queue_previews=payload.queue_previews,
        execution_target_id=payload.execution_target_id,
        execution_target_key=payload.execution_target_key,
        metadata_json={
            **dict(payload.metadata_json or {}),
            "launched_from_misc_item_id": str(item.id),
            "launched_from_misc_path": item.relative_path,
        },
    )
    try:
        indexing_job = create_indexing_job(db, payload=request, current_user=current_user)
        enqueue_indexing_worker_job(db, indexing_job=indexing_job, current_user=current_user)
        item.status = "project_index_queued"
        item.metadata_json = {**dict(item.metadata_json or {}), "project_index_job_id": str(indexing_job.id)}
        db.commit()
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception:
        db.rollback()
        raise

    indexing_job = get_indexing_job_for_user(db, job_id=indexing_job.id, current_user=current_user)
    if indexing_job is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to reload indexing job")
    return IndexJobLaunchResponse(
        status="queued",
        launch_mode="worker",
        job=IndexJobSummary.model_validate(indexing_job),
        message="Indexing job accepted and queued for worker execution.",
    )


@router.post("/misc-inventory/jobs", response_model=MiscStorageInventoryResponse, status_code=status.HTTP_202_ACCEPTED)
def queue_misc_storage_inventory_job(
    payload: MiscStorageInventoryRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MiscStorageInventoryResponse:
    require_storage_admin(current_user)
    target = resolve_storage_worker_target(
        db,
        execution_target_id=payload.execution_target_id,
        execution_target_key=payload.execution_target_key,
    )
    job = Job(
        execution_target_id=target.id if target is not None else None,
        requested_mode=payload.requested_mode,
        priority=payload.priority,
        requested_by=current_user.user_key,
        requested_from_host="api-storage",
        params_json={
            "job_kind": "misc_storage_inventory",
            "source_path": payload.source_path,
            "storage_root_id": payload.storage_root_id,
            "parent_item_id": str(payload.parent_item_id) if payload.parent_item_id is not None else None,
            "storage_root_name": payload.storage_root_name,
            "host_scope": payload.host_scope,
            "root_type": payload.root_type,
            "owner_user_key": payload.owner_user_key,
            "visibility": payload.visibility,
            "min_size_bytes": payload.min_size_bytes,
            "max_depth": payload.max_depth,
            "du_timeout_sec": payload.du_timeout_sec,
            "include_cataloged": payload.include_cataloged,
            "metadata_json": payload.metadata_json,
        },
        status="queued",
    )
    db.add(job)
    db.commit()
    return MiscStorageInventoryResponse(
        status="queued",
        job_id=job.id,
        source_path=payload.source_path,
        message="Misc storage inventory job queued for worker execution.",
    )


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


@router.post("/providers/{provider_key}/synology/discover", response_model=SynologyDsmDiscoveryResponse)
def discover_synology_provider(
    provider_key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SynologyDsmDiscoveryResponse:
    require_storage_admin(current_user)
    provider = resolve_provider(db, provider_key)
    if provider.provider_kind != "synology_dsm":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provider is not a Synology DSM provider")
    client = SynologyDsmClient()
    try:
        api_info = client.discover()
    except SynologyDsmError as exc:
        return SynologyDsmDiscoveryResponse(
            provider=StorageProviderSummary.model_validate(provider),
            configured=client.is_configured(),
            success=False,
            message=str(exc),
        )
    capabilities = summarize_discovered_capabilities(api_info)
    provider.capabilities_json = capabilities
    provider.config_json = {
        **dict(provider.config_json or {}),
        "discovery_source": "synology_dsm",
    }
    db.commit()
    db.refresh(provider)
    return SynologyDsmDiscoveryResponse(
        provider=StorageProviderSummary.model_validate(provider),
        configured=client.is_configured(),
        success=True,
        api_info=api_info,
        capabilities_json=capabilities,
    )


@router.post("/providers/{provider_key}/synology/login-check", response_model=SynologyDsmLoginCheckResponse)
def check_synology_login(
    provider_key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SynologyDsmLoginCheckResponse:
    require_storage_admin(current_user)
    provider = resolve_provider(db, provider_key)
    if provider.provider_kind != "synology_dsm":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provider is not a Synology DSM provider")
    client = SynologyDsmClient()
    try:
        result = client.login_check()
    except SynologyDsmError as exc:
        return SynologyDsmLoginCheckResponse(
            provider=StorageProviderSummary.model_validate(provider),
            configured=client.is_configured(),
            success=False,
            message=str(exc),
        )
    return SynologyDsmLoginCheckResponse(
        provider=StorageProviderSummary.model_validate(provider),
        configured=client.is_configured(),
        success=True,
        session=result.get("session"),
        sid_received=bool(result.get("sid_received")),
        discovered_apis=list(result.get("discovered_apis") or []),
    )


@router.post("/providers/{provider_key}/synology/probe", response_model=SynologyDsmApiProbeResponse)
def probe_synology_api(
    provider_key: str,
    payload: SynologyDsmApiProbeRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SynologyDsmApiProbeResponse:
    require_storage_admin(current_user)
    provider = resolve_provider(db, provider_key)
    if provider.provider_kind != "synology_dsm":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provider is not a Synology DSM provider")
    client = SynologyDsmClient()
    try:
        result = client.call_discovered_api(
            api_name=payload.api_name,
            method=payload.method,
            params=payload.params,
            version=payload.version,
            login=payload.login,
        )
    except SynologyDsmError as exc:
        return SynologyDsmApiProbeResponse(
            provider=StorageProviderSummary.model_validate(provider),
            configured=client.is_configured(),
            success=False,
            message=str(exc),
        )
    finally:
        client.logout()
    return SynologyDsmApiProbeResponse(
        provider=StorageProviderSummary.model_validate(provider),
        configured=client.is_configured(),
        success=True,
        payload=result,
    )


@router.get("/providers/{provider_key}/synology/users", response_model=SynologyDsmUserListResponse)
def list_synology_users(
    provider_key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SynologyDsmUserListResponse:
    require_storage_admin(current_user)
    provider = resolve_provider(db, provider_key)
    if provider.provider_kind != "synology_dsm":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provider is not a Synology DSM provider")
    client = SynologyDsmClient()
    try:
        users = client.list_users()
    except SynologyDsmError as exc:
        return SynologyDsmUserListResponse(
            provider=StorageProviderSummary.model_validate(provider),
            configured=client.is_configured(),
            success=False,
            message=str(exc),
        )
    summaries = [
        SynologyDsmUserSummary(
            name=str(user.get("name") or ""),
            raw_keys=sorted(str(key) for key in user.keys()),
        )
        for user in users
        if user.get("name")
    ]
    return SynologyDsmUserListResponse(
        provider=StorageProviderSummary.model_validate(provider),
        configured=client.is_configured(),
        success=True,
        users=summaries,
        total=len(summaries),
    )


@router.get("/providers/{provider_key}/synology/user-home", response_model=SynologyDsmUserHomeResponse)
def get_synology_user_home_settings(
    provider_key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SynologyDsmUserHomeResponse:
    require_storage_admin(current_user)
    provider = resolve_provider(db, provider_key)
    if provider.provider_kind != "synology_dsm":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provider is not a Synology DSM provider")
    client = SynologyDsmClient()
    try:
        settings = client.get_user_home_settings()
    except SynologyDsmError as exc:
        return SynologyDsmUserHomeResponse(
            provider=StorageProviderSummary.model_validate(provider),
            configured=client.is_configured(),
            success=False,
            message=str(exc),
        )
    return SynologyDsmUserHomeResponse(
        provider=StorageProviderSummary.model_validate(provider),
        configured=client.is_configured(),
        success=True,
        enable=settings.get("enable") if isinstance(settings.get("enable"), bool) else None,
        location=str(settings.get("location") or "") or None,
        raw_settings=settings,
    )


@router.get("/user-accounts/{account_id}/synology/quota", response_model=SynologyDsmUserQuotaResponse)
def get_synology_user_quota_for_account(
    account_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SynologyDsmUserQuotaResponse:
    require_storage_admin(current_user)
    account = load_account(db, account_id)
    if account.provider.provider_kind != "synology_dsm":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Storage account is not linked to a Synology DSM provider")
    client = SynologyDsmClient()
    try:
        payload = client.get_user_quota(account.provider_user_key)
    except SynologyDsmError as exc:
        return SynologyDsmUserQuotaResponse(
            provider=StorageProviderSummary.model_validate(account.provider),
            configured=client.is_configured(),
            success=False,
            provider_user_key=account.provider_user_key,
            message=str(exc),
        )
    parsed = parse_user_quota_payload(payload)
    provider_quota_bytes = parsed.get("quota_bytes")
    provider_reported = provider_quota_bytes is not None
    effective_quota_bytes = provider_quota_bytes if provider_reported else account.quota_bytes
    quota_source = "provider" if provider_reported else "hub_desired"
    message = None
    if not provider_reported and account.quota_bytes is not None:
        message = (
            "Synology DSM accepted the quota lookup but did not return a quota entry. "
            "Using the hub desired quota for the effective value."
        )
    return SynologyDsmUserQuotaResponse(
        provider=StorageProviderSummary.model_validate(account.provider),
        configured=client.is_configured(),
        success=True,
        provider_user_key=account.provider_user_key,
        quota_bytes=provider_quota_bytes,
        used_bytes=parsed.get("used_bytes"),
        desired_quota_bytes=account.quota_bytes,
        effective_quota_bytes=effective_quota_bytes,
        provider_reported=provider_reported,
        quota_source=quota_source,
        entry_count=int(parsed.get("entry_count") or 0),
        raw_quota=payload,
        message=message,
    )


@router.post("/user-accounts/{account_id}/synology/quota", response_model=SynologyQuotaUpdateResponse)
def update_synology_user_quota_for_account(
    account_id: UUID,
    payload: SynologyQuotaUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SynologyQuotaUpdateResponse:
    require_storage_admin(current_user)
    account = load_account(db, account_id)
    if account.provider.provider_kind != "synology_dsm":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Storage account is not linked to a Synology DSM provider")

    quota_bytes = payload.quota_bytes
    if quota_bytes is not None and quota_bytes <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Quota must be greater than zero")

    account.quota_bytes = quota_bytes
    account.quota_status = "desired" if quota_bytes is not None else "unknown"
    provider_applied = False
    method = None
    message = "Quota target recorded in the hub."
    if quota_bytes is not None:
        try:
            SynologySshClient().set_user_quota(
                user_name=account.provider_user_key,
                quota_bytes=quota_bytes,
            )
            provider_applied = True
            method = "ssh_synoquota"
            account.quota_status = "applied"
            message = "Quota target recorded in the hub and applied on Synology via SSH."
        except SynologySshError as exc:
            account.quota_status = "apply_failed"
            message = f"Quota target recorded in the hub; Synology apply failed: {exc}"
    record_provisioning_event(
        db,
        user=account.user,
        provider=account.provider,
        storage_account=account,
        event_kind="synology_quota_updated",
        status_text=account.quota_status,
        message=message,
        metadata_json={"quota_bytes": quota_bytes, "provider_applied": provider_applied, "method": method},
    )
    db.commit()
    account = load_account(db, account.id)
    return SynologyQuotaUpdateResponse(
        account=UserStorageAccountSummary.model_validate(account),
        provider_user_key=account.provider_user_key,
        requested_quota_bytes=quota_bytes,
        provider_applied=provider_applied,
        method=method,
        message=message,
    )


@router.post("/user-accounts/{account_id}/synology/ensure-user", response_model=SynologyDsmEnsureUserResponse)
def ensure_synology_user_for_account(
    account_id: UUID,
    payload: SynologyDsmEnsureUserRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SynologyDsmEnsureUserResponse:
    require_storage_admin(current_user)
    account = load_account(db, account_id)
    if account.provider.provider_kind != "synology_dsm":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Storage account is not linked to a Synology DSM provider")

    client = SynologyDsmClient()
    try:
        raw_user = client.get_user(account.provider_user_key)
    except SynologyDsmError as exc:
        return SynologyDsmEnsureUserResponse(
            provider=StorageProviderSummary.model_validate(account.provider),
            account=UserStorageAccountSummary.model_validate(account),
            configured=client.is_configured(),
            success=False,
            provider_user_key=account.provider_user_key,
            message=str(exc),
        )

    exists_before = raw_user is not None
    created = False
    creation_method = None
    dsm_api_create_error_code = None
    if raw_user is None:
        if not payload.create_missing:
            now = datetime.now(timezone.utc)
            account.last_synced_at = now
            account.provisioning_status = "provider_user_missing"
            account.metadata_json = {
                **dict(account.metadata_json or {}),
                "synology_user_exists": False,
                "synology_user_last_checked_at": now.isoformat(),
            }
            record_provisioning_event(
                db,
                user=account.user,
                provider=account.provider,
                storage_account=account,
                event_kind="synology_user_missing",
                status_text="missing",
                message="Synology user does not exist and create_missing was false.",
            )
            db.commit()
            return SynologyDsmEnsureUserResponse(
                provider=StorageProviderSummary.model_validate(account.provider),
                account=UserStorageAccountSummary.model_validate(load_account(db, account.id)),
                configured=client.is_configured(),
                success=False,
                provider_user_key=account.provider_user_key,
                exists_before=False,
                exists_after=False,
                message="Synology user does not exist. Retry with create_missing=true and an initial password to create it.",
            )
        if not payload.initial_password:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Initial password is required to create a Synology user")
        try:
            client.create_user(
                user_name=account.provider_user_key,
                initial_password=payload.initial_password,
                display_name=payload.display_name or account.user.display_name,
                email=payload.email or account.user.email,
                groups=payload.groups or None,
            )
            raw_user = client.get_user(account.provider_user_key)
            created = True
            creation_method = "dsm_api"
        except SynologyDsmError as exc:
            dsm_api_create_error_code = exc.code
            ssh_client = SynologySshClient()
            if not ssh_client.is_configured():
                now = datetime.now(timezone.utc)
                account.last_synced_at = now
                account.provisioning_status = "provider_user_failed"
                account.metadata_json = {
                    **dict(account.metadata_json or {}),
                    "synology_user_exists": False,
                    "synology_user_last_checked_at": now.isoformat(),
                    "synology_user_create_error_code": exc.code,
                    "synology_user_create_method": "dsm_api",
                    "synology_ssh_fallback_configured": False,
                }
                record_provisioning_event(
                    db,
                    user=account.user,
                    provider=account.provider,
                    storage_account=account,
                    event_kind="synology_user_create_failed",
                    status_text="failed",
                    message=f"{exc}; Synology SSH fallback is not configured.",
                )
                db.commit()
                return SynologyDsmEnsureUserResponse(
                    provider=StorageProviderSummary.model_validate(account.provider),
                    account=UserStorageAccountSummary.model_validate(load_account(db, account.id)),
                    configured=client.is_configured(),
                    success=False,
                    provider_user_key=account.provider_user_key,
                    exists_before=False,
                    created=False,
                    exists_after=False,
                    message=f"{exc}; Synology SSH fallback is not configured.",
                )
            try:
                ssh_client.create_user(
                    user_name=account.provider_user_key,
                    initial_password=payload.initial_password,
                    display_name=payload.display_name or account.user.display_name,
                    email=payload.email or account.user.email,
                )
                raw_user = client.get_user(account.provider_user_key)
                created = raw_user is not None
                creation_method = "ssh_synouser"
                if raw_user is None:
                    raise SynologySshError("Synology SSH command completed but DSM API did not verify the new user")
            except SynologySshError as ssh_exc:
                now = datetime.now(timezone.utc)
                account.last_synced_at = now
                account.provisioning_status = "provider_user_failed"
                account.metadata_json = {
                    **dict(account.metadata_json or {}),
                    "synology_user_exists": False,
                    "synology_user_last_checked_at": now.isoformat(),
                    "synology_user_create_error_code": exc.code,
                    "synology_user_create_method": "ssh_synouser",
                    "synology_ssh_fallback_configured": True,
                }
                record_provisioning_event(
                    db,
                    user=account.user,
                    provider=account.provider,
                    storage_account=account,
                    event_kind="synology_user_create_failed",
                    status_text="failed",
                    message=f"DSM API failed: {exc}; SSH fallback failed: {ssh_exc}",
                )
                db.commit()
                return SynologyDsmEnsureUserResponse(
                    provider=StorageProviderSummary.model_validate(account.provider),
                    account=UserStorageAccountSummary.model_validate(load_account(db, account.id)),
                    configured=client.is_configured(),
                    success=False,
                    provider_user_key=account.provider_user_key,
                    exists_before=False,
                    created=False,
                    exists_after=False,
                    message=f"DSM API failed: {exc}; SSH fallback failed: {ssh_exc}",
                )

    exists_after = raw_user is not None
    now = datetime.now(timezone.utc)
    account.last_synced_at = now
    account.provisioning_status = "provider_user_ready" if account.provisioning_status != "ready" else account.provisioning_status
    account.metadata_json = {
        **dict(account.metadata_json or {}),
        "synology_user_exists": exists_after,
        "synology_user_last_checked_at": now.isoformat(),
        "synology_user_created_by_hub": bool(created or (account.metadata_json or {}).get("synology_user_created_by_hub")),
        "synology_user_create_method": creation_method or (account.metadata_json or {}).get("synology_user_create_method"),
        "synology_dsm_api_create_error_code": dsm_api_create_error_code,
        "synology_user_create_error_code": None,
    }
    record_provisioning_event(
        db,
        user=account.user,
        provider=account.provider,
        storage_account=account,
        event_kind="synology_user_created" if created else "synology_user_verified",
        status_text="ready" if exists_after else "missing",
        message=f"Synology user was created by the hub via {creation_method}." if created else "Synology user exists.",
        metadata_json={"provider_user_key": account.provider_user_key, "creation_method": creation_method},
    )
    db.commit()
    account = load_account(db, account.id)
    return SynologyDsmEnsureUserResponse(
        provider=StorageProviderSummary.model_validate(account.provider),
        account=UserStorageAccountSummary.model_validate(account),
        configured=client.is_configured(),
        success=exists_after,
        provider_user_key=account.provider_user_key,
        exists_before=exists_before,
        created=created,
        creation_method=creation_method,
        exists_after=exists_after,
        raw_user=raw_user or {},
        message=f"Synology user was created via {creation_method}." if created else "Synology user exists.",
    )


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


@router.post("/user-accounts/{account_id}/prepare", response_model=UserHomePrepareResponse, status_code=status.HTTP_201_CREATED)
def queue_user_home_prepare_job(
    account_id: UUID,
    payload: UserHomePrepareRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> UserHomePrepareResponse:
    require_storage_admin(current_user)
    account = load_account(db, account_id)
    job = Job(
        requested_mode=payload.requested_mode,
        priority=payload.priority,
        requested_by=current_user.user_key,
        params_json={
            "job_kind": "prepare_user_home_storage",
            "storage_account_id": str(account.id),
            "create_directories": payload.create_directories,
            "subdirectories": payload.subdirectories,
        },
        status="queued",
    )
    account.provisioning_status = "queued"
    db.add(job)
    record_provisioning_event(
        db,
        user=account.user,
        provider=account.provider,
        storage_account=account,
        event_kind="home_prepare_queued",
        status_text="queued",
        message="User home preparation job queued for worker execution.",
        metadata_json={"job_kind": "prepare_user_home_storage"},
    )
    db.commit()
    account = load_account(db, account.id)
    return UserHomePrepareResponse(
        job_id=job.id,
        account=UserStorageAccountSummary.model_validate(account),
        detail="User home preparation job queued.",
    )


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
