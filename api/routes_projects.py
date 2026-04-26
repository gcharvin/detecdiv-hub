from pathlib import Path
from datetime import timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import or_, select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import (
    Job,
    Project,
    ProjectAcl,
    ProjectGroup,
    ProjectGroupMember,
    ProjectLock,
    ProjectLocation,
    ProjectRawLink,
    ProjectNote,
    RawDataset,
    StorageRoot,
    User,
)
from api.schemas import (
    UserCreate,
    ProjectAclCreate,
    ProjectAclSummary,
    ProjectDetail,
    ProjectDeletionPreview,
    ProjectDeletionRequest,
    ProjectDeletionResult,
    ProjectGroupCreate,
    ProjectGroupDetail,
    ProjectGroupSummary,
    ProjectEditLeaseRequest,
    ProjectLocationSummary,
    ProjectLockStatus,
    ProjectLockSummary,
    ProjectNoteCreate,
    ProjectNoteSummary,
    ProjectSummary,
    ProjectRawPreviewQueueRequest,
    ProjectRawPreviewQueueResult,
    ProjectRawPreviewBulkQueueRequest,
    ProjectRawPreviewBulkQueueResult,
    RawDatasetSummary,
    ProjectUpdate,
    StorageRootBrowseEntry,
    StorageRootBrowseResponse,
    StorageRootSummary,
    UserBulkUpsertRequest,
    UserBulkUpsertResponse,
    UserSummary,
    UserUpdate,
)
from api.services.auth import set_user_password
from api.services.path_resolution import compose_storage_path
from api.services.project_deletion import build_deletion_preview, execute_project_deletion, record_deletion_preview
from api.services.project_locks import (
    ProjectLockConflict,
    acquire_client_edit_lease,
    project_lock_status,
    release_project_lock,
    utcnow,
)
from api.services.users import ensure_project_readable, get_current_user, get_or_create_user, project_access_filter, user_can_edit_project


router = APIRouter(prefix="/projects", tags=["projects"])
groups_router = APIRouter(prefix="/project-groups", tags=["project-groups"])
users_router = APIRouter(prefix="/users", tags=["users"])
storage_roots_router = APIRouter(prefix="/storage-roots", tags=["storage-roots"])


@router.get("", response_model=list[ProjectSummary])
def list_projects(
    group_id: UUID | None = None,
    owned_only: bool = False,
    search: str | None = None,
    owner_key: str | None = None,
    storage_root_name: str | None = None,
    visibility: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[Project]:
    stmt = (
        select(Project)
        .options(joinedload(Project.owner), joinedload(Project.locations).joinedload(ProjectLocation.storage_root))
        .where(Project.status != "deleted")
        .where(project_access_filter(current_user))
        .order_by(Project.project_name.asc())
    )
    if owned_only:
        stmt = stmt.where(Project.owner_user_id == current_user.id)
    if group_id is not None:
        stmt = (
            stmt.join(ProjectGroupMember)
            .join(ProjectGroup)
            .where(
                ProjectGroupMember.group_id == group_id,
                ProjectGroup.id == group_id,
                ProjectGroup.owner_user_id == current_user.id,
            )
        )
    if search:
        pattern = f"%{search.strip()}%"
        stmt = stmt.where(
            or_(
                Project.project_name.ilike(pattern),
                Project.project_key.ilike(pattern),
            )
        )
    if owner_key:
        stmt = stmt.join(User, Project.owner_user_id == User.id).where(User.user_key == owner_key)
    if storage_root_name:
        stmt = (
            stmt.join(ProjectLocation, ProjectLocation.project_id == Project.id)
            .join(StorageRoot, StorageRoot.id == ProjectLocation.storage_root_id)
            .where(StorageRoot.name == storage_root_name)
        )
    if visibility:
        stmt = stmt.where(Project.visibility == visibility)
    stmt = stmt.limit(min(max(limit, 1), 500))
    return [project_summary_view(project) for project in db.scalars(stmt).unique()]


@router.get("/{project_id}", response_model=ProjectDetail)
def get_project(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectDetail:
    stmt = (
        select(Project)
        .options(
            joinedload(Project.owner),
            joinedload(Project.acl_entries),
            joinedload(Project.locations).joinedload(ProjectLocation.storage_root),
            joinedload(Project.raw_links).joinedload(ProjectRawLink.raw_dataset).joinedload(RawDataset.owner),
        )
        .where(Project.id == project_id, Project.status != "deleted")
    )
    project = db.scalars(stmt).unique().first()
    return project_detail_view(ensure_project_readable(project, current_user))


@router.patch("/{project_id}", response_model=ProjectDetail)
def update_project(
    project_id: UUID,
    payload: ProjectUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectDetail:
    stmt = (
        select(Project)
        .options(
            joinedload(Project.owner),
            joinedload(Project.acl_entries),
            joinedload(Project.locations).joinedload(ProjectLocation.storage_root),
            joinedload(Project.raw_links).joinedload(ProjectRawLink.raw_dataset).joinedload(RawDataset.owner),
        )
        .where(Project.id == project_id, Project.status != "deleted")
    )
    project = db.scalars(stmt).unique().first()
    project = ensure_project_readable(project, current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project is not editable")

    if payload.visibility is not None:
        project.visibility = payload.visibility
    if payload.owner_user_key:
        new_owner = get_or_create_user(db, user_key=payload.owner_user_key, display_name=payload.owner_user_key)
        project.owner_user_id = new_owner.id
    if payload.metadata_json:
        merged = dict(project.metadata_json or {})
        merged.update(payload.metadata_json)
        project.metadata_json = merged

    db.commit()
    return get_project(project_id=project_id, db=db, current_user=current_user)


@router.post("/{project_id}/preview-videos/queue", response_model=ProjectRawPreviewQueueResult, status_code=status.HTTP_201_CREATED)
def queue_project_raw_preview_videos(
    project_id: UUID,
    payload: ProjectRawPreviewQueueRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectRawPreviewQueueResult:
    project = db.scalars(
        select(Project)
        .options(
            joinedload(Project.owner),
            joinedload(Project.acl_entries),
            joinedload(Project.raw_links).joinedload(ProjectRawLink.raw_dataset).joinedload(RawDataset.positions),
        )
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project is not editable")

    queued_job_ids: list[UUID] = []
    raw_dataset_ids: list[UUID] = []
    skipped_count = 0
    for link in project.raw_links or []:
        raw_dataset = link.raw_dataset
        if raw_dataset is None:
            skipped_count += 1
            continue
        queued = enqueue_raw_preview_job_for_dataset(
            db,
            current_user_key=current_user.user_key,
            project_id=project.id,
            raw_dataset=raw_dataset,
            force=payload.force,
            requested_mode=payload.requested_mode,
            priority=payload.priority,
            params_json=payload.params_json or {},
        )
        if queued is None:
            skipped_count += 1
            continue
        raw_dataset_ids.append(raw_dataset.id)
        queued_job_ids.append(queued)

    db.commit()
    return ProjectRawPreviewQueueResult(
        project_id=project.id,
        queued_count=len(queued_job_ids),
        skipped_count=skipped_count,
        raw_dataset_ids=raw_dataset_ids,
        queued_job_ids=queued_job_ids,
        message=f"Queued {len(queued_job_ids)} raw preview dataset job(s) for project {project.project_name}.",
    )


@router.post("/preview-videos/queue-bulk", response_model=ProjectRawPreviewBulkQueueResult, status_code=status.HTTP_201_CREATED)
def queue_bulk_project_raw_preview_videos(
    payload: ProjectRawPreviewBulkQueueRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectRawPreviewBulkQueueResult:
    if not payload.project_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="project_ids is required")

    unique_project_ids = list(dict.fromkeys(payload.project_ids))
    queued_job_ids: list[UUID] = []
    raw_dataset_ids: list[UUID] = []
    skipped_project_ids: list[UUID] = []
    seen_raw_datasets: set[UUID] = set()
    skipped_count = 0

    for project_id in unique_project_ids:
        project = db.scalars(
            select(Project)
            .options(
                joinedload(Project.owner),
                joinedload(Project.acl_entries),
                joinedload(Project.raw_links).joinedload(ProjectRawLink.raw_dataset).joinedload(RawDataset.positions),
            )
            .where(Project.id == project_id, Project.status != "deleted")
        ).unique().first()
        if project is None:
            skipped_project_ids.append(project_id)
            skipped_count += 1
            continue
        try:
            project = ensure_project_readable(project, current_user)
        except HTTPException:
            skipped_project_ids.append(project_id)
            skipped_count += 1
            continue
        if not user_can_edit_project(project, current_user):
            skipped_project_ids.append(project_id)
            skipped_count += 1
            continue

        for link in project.raw_links or []:
            raw_dataset = link.raw_dataset
            if raw_dataset is None:
                skipped_count += 1
                continue
            if raw_dataset.id in seen_raw_datasets:
                skipped_count += 1
                continue
            seen_raw_datasets.add(raw_dataset.id)

            queued = enqueue_raw_preview_job_for_dataset(
                db,
                current_user_key=current_user.user_key,
                project_id=project.id,
                raw_dataset=raw_dataset,
                force=payload.force,
                requested_mode=payload.requested_mode,
                priority=payload.priority,
                params_json=payload.params_json or {},
            )
            if queued is None:
                skipped_count += 1
                continue
            raw_dataset_ids.append(raw_dataset.id)
            queued_job_ids.append(queued)

    db.commit()
    return ProjectRawPreviewBulkQueueResult(
        project_count=len(unique_project_ids),
        queued_count=len(queued_job_ids),
        skipped_count=skipped_count,
        raw_dataset_ids=raw_dataset_ids,
        queued_job_ids=queued_job_ids,
        skipped_project_ids=skipped_project_ids,
        message=f"Queued {len(queued_job_ids)} raw preview dataset job(s) from {len(unique_project_ids)} project(s).",
    )


@router.post("/{project_id}/deletion-preview", response_model=ProjectDeletionPreview)
def preview_project_deletion(
    project_id: UUID,
    payload: ProjectDeletionRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectDeletionPreview:
    project = db.scalars(
        select(Project)
        .options(
            joinedload(Project.owner),
            joinedload(Project.acl_entries),
            joinedload(Project.locations).joinedload(ProjectLocation.storage_root),
        )
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project is not deletable")

    preview = build_deletion_preview(
        db,
        project=project,
        delete_project_files=payload.delete_project_files,
        delete_linked_raw_data=payload.delete_linked_raw_data,
    )
    record_deletion_preview(db, preview=preview, requested_by_user=current_user)
    db.commit()
    return ProjectDeletionPreview(
        project_id=project.id,
        project_name=project.project_name,
        delete_project_files=payload.delete_project_files,
        delete_linked_raw_data=payload.delete_linked_raw_data,
        reclaimable_bytes=preview.reclaimable_bytes,
        preview_json=preview.preview_json,
    )


@router.delete("/{project_id}", response_model=ProjectDeletionResult)
def delete_project(
    project_id: UUID,
    delete_project_files: bool = False,
    delete_linked_raw_data: bool = False,
    confirm: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectDeletionResult:
    if not confirm:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Deletion requires confirm=true")

    project = db.scalars(
        select(Project)
        .options(
            joinedload(Project.owner),
            joinedload(Project.acl_entries),
            joinedload(Project.locations).joinedload(ProjectLocation.storage_root),
        )
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project is not deletable")

    preview = build_deletion_preview(
        db,
        project=project,
        delete_project_files=delete_project_files,
        delete_linked_raw_data=delete_linked_raw_data,
    )
    event = execute_project_deletion(db, preview=preview, requested_by_user=current_user)
    db.commit()
    return ProjectDeletionResult(
        event_id=event.id,
        project_id=project_id,
        status=event.status,
        reclaimable_bytes=event.reclaimable_bytes,
        result_json=event.result_json,
    )


@router.get("/{project_id}/locks", response_model=ProjectLockStatus)
def get_project_lock_status(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectLockStatus:
    project = db.scalars(
        select(Project)
        .options(joinedload(Project.owner), joinedload(Project.acl_entries))
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    return ProjectLockStatus.model_validate(project_lock_status(db, project_id=project.id))


@router.post("/{project_id}/leases", response_model=ProjectLockSummary, status_code=status.HTTP_201_CREATED)
def acquire_project_edit_lease(
    project_id: UUID,
    payload: ProjectEditLeaseRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectLock:
    project = db.scalars(
        select(Project)
        .options(joinedload(Project.owner), joinedload(Project.acl_entries))
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project is not editable")
    try:
        lease = acquire_client_edit_lease(
            db,
            project_id=project.id,
            owner=current_user,
            holder_key=payload.holder_key,
            holder_host=payload.holder_host,
            ttl_seconds=payload.ttl_seconds,
            write_scope=payload.write_scope,
            reason=payload.reason,
            metadata_json=payload.metadata_json,
        )
    except ProjectLockConflict as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Project is locked",
                "locks": [ProjectLockSummary.model_validate(lock).model_dump(mode="json") for lock in exc.locks],
            },
        ) from exc
    db.commit()
    db.refresh(lease)
    return db.scalars(
        select(ProjectLock).options(joinedload(ProjectLock.owner)).where(ProjectLock.id == lease.id)
    ).first()


@router.post("/{project_id}/leases/{lock_id}/heartbeat", response_model=ProjectLockSummary)
def heartbeat_project_edit_lease(
    project_id: UUID,
    lock_id: UUID,
    payload: ProjectEditLeaseRequest | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectLock:
    project = db.scalars(
        select(Project)
        .options(joinedload(Project.owner), joinedload(Project.acl_entries))
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    lease = db.scalars(
        select(ProjectLock)
        .options(joinedload(ProjectLock.owner))
        .where(ProjectLock.id == lock_id, ProjectLock.project_id == project.id, ProjectLock.status == "active")
    ).first()
    if lease is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Active project lease not found")
    if lease.owner_user_id != current_user.id and current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Lease owner required")

    now = utcnow()
    ttl_seconds = payload.ttl_seconds if payload is not None else 300
    lease.heartbeat_at = now
    lease.expires_at = now + timedelta(seconds=ttl_seconds)
    lease.updated_at = now
    db.commit()
    db.refresh(lease)
    return lease


@router.delete("/{project_id}/leases/{lock_id}", response_model=ProjectLockSummary)
def release_project_edit_lease(
    project_id: UUID,
    lock_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectLock:
    project = db.scalars(
        select(Project)
        .options(joinedload(Project.owner), joinedload(Project.acl_entries))
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    lease = db.scalars(
        select(ProjectLock)
        .options(joinedload(ProjectLock.owner))
        .where(ProjectLock.id == lock_id, ProjectLock.project_id == project.id)
    ).first()
    if lease is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project lease not found")
    if lease.owner_user_id != current_user.id and current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Lease owner required")
    if lease.status == "active":
        release_project_lock(db, lock=lease)
    db.commit()
    db.refresh(lease)
    return lease


@router.get("/{project_id}/acl", response_model=list[ProjectAclSummary])
def list_project_acl(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[ProjectAcl]:
    project = db.scalars(
        select(Project)
        .options(joinedload(Project.acl_entries).joinedload(ProjectAcl.user))
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project ACL is not editable")
    return list(project.acl_entries)


@router.post("/{project_id}/acl", response_model=ProjectAclSummary)
def create_project_acl(
    project_id: UUID,
    payload: ProjectAclCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectAcl:
    project = db.scalars(
        select(Project)
        .options(joinedload(Project.acl_entries))
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project ACL is not editable")

    target_user = get_or_create_user(db, user_key=payload.user_key, display_name=payload.user_key)
    acl = db.scalars(
        select(ProjectAcl).where(ProjectAcl.project_id == project_id, ProjectAcl.user_id == target_user.id)
    ).first()
    if acl is None:
        acl = ProjectAcl(project_id=project_id, user_id=target_user.id, access_level=payload.access_level)
        db.add(acl)
    else:
        acl.access_level = payload.access_level
    db.commit()
    db.refresh(acl)
    return db.scalars(
        select(ProjectAcl).options(joinedload(ProjectAcl.user)).where(ProjectAcl.id == acl.id)
    ).first()


@router.get("/{project_id}/notes", response_model=list[ProjectNoteSummary])
def list_project_notes(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[ProjectNote]:
    project = db.scalars(
        select(Project)
        .options(joinedload(Project.acl_entries))
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    ensure_project_readable(project, current_user)

    stmt = (
        select(ProjectNote)
        .options(joinedload(ProjectNote.author))
        .where(ProjectNote.project_id == project_id)
        .order_by(ProjectNote.is_pinned.desc(), ProjectNote.updated_at.desc())
    )
    return list(db.scalars(stmt))


@router.post("/{project_id}/notes", response_model=ProjectNoteSummary)
def create_project_note(
    project_id: UUID,
    payload: ProjectNoteCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectNote:
    project = db.scalars(
        select(Project)
        .options(joinedload(Project.acl_entries))
        .where(Project.id == project_id)
    ).unique().first()
    project = ensure_project_readable(project, current_user)
    if not user_can_edit_project(project, current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Project is not editable")

    note = ProjectNote(
        project_id=project.id,
        author_user_id=current_user.id,
        note_text=payload.note_text,
        is_pinned=payload.is_pinned,
    )
    db.add(note)
    db.commit()
    db.refresh(note)
    return note


@groups_router.get("", response_model=list[ProjectGroupSummary])
def list_project_groups(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[ProjectGroup]:
    stmt = (
        select(ProjectGroup)
        .options(joinedload(ProjectGroup.owner))
        .where(ProjectGroup.owner_user_id == current_user.id)
        .order_by(ProjectGroup.display_name.asc())
    )
    return list(db.scalars(stmt))


@groups_router.post("", response_model=ProjectGroupSummary)
def create_project_group(
    payload: ProjectGroupCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectGroup:
    group = ProjectGroup(
        owner_user_id=current_user.id,
        group_key=payload.group_key,
        display_name=payload.display_name,
        description=payload.description,
        metadata_json=payload.metadata_json,
    )
    db.add(group)
    db.commit()
    db.refresh(group)
    return group


@groups_router.get("/{group_id}", response_model=ProjectGroupDetail)
def get_project_group(
    group_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectGroupDetail:
    group = db.scalars(
        select(ProjectGroup)
        .options(joinedload(ProjectGroup.owner))
        .where(ProjectGroup.id == group_id, ProjectGroup.owner_user_id == current_user.id)
    ).first()
    if group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project group not found")

    projects = list(
        db.scalars(
            select(Project)
            .join(ProjectGroupMember)
            .options(joinedload(Project.owner))
            .where(ProjectGroupMember.group_id == group_id, Project.status != "deleted")
            .order_by(Project.project_name.asc())
        ).unique()
    )
    detail = ProjectGroupDetail.model_validate(group)
    detail.projects = [project_summary_view(project) for project in projects]
    detail.project_count = len(projects)
    return detail


@groups_router.post("/{group_id}/projects/{project_id}", response_model=ProjectGroupDetail)
def add_project_to_group(
    group_id: UUID,
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ProjectGroupDetail:
    group = db.scalars(
        select(ProjectGroup)
        .where(ProjectGroup.id == group_id, ProjectGroup.owner_user_id == current_user.id)
    ).first()
    if group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project group not found")

    project = db.scalars(
        select(Project)
        .options(joinedload(Project.acl_entries), joinedload(Project.owner))
        .where(Project.id == project_id, Project.status != "deleted")
    ).unique().first()
    project = ensure_project_readable(project, current_user)

    existing = db.scalars(
        select(ProjectGroupMember).where(
            ProjectGroupMember.group_id == group_id,
            ProjectGroupMember.project_id == project_id,
        )
    ).first()
    if existing is None:
        db.add(ProjectGroupMember(group_id=group_id, project_id=project_id))
        db.commit()

    return get_project_group(group_id=group_id, db=db, current_user=current_user)


@users_router.get("/me", response_model=UserSummary)
def get_me(current_user: User = Depends(get_current_user)) -> User:
    return current_user


@users_router.get("", response_model=list[UserSummary])
def list_users(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[User]:
    if current_user.role not in {"admin", "service"}:
        return [current_user]
    return list(db.scalars(select(User).where(User.is_active.is_(True)).order_by(User.display_name.asc())))


@users_router.post("", response_model=UserSummary, status_code=status.HTTP_201_CREATED)
def create_user(
    payload: UserCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> User:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User admin required")
    existing = db.scalars(select(User).where(User.user_key == payload.user_key)).first()
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists")
    user = User(
        user_key=payload.user_key,
        display_name=payload.display_name,
        email=payload.email,
        role=payload.role,
        is_active=payload.is_active,
        metadata_json=payload.metadata_json,
    )
    db.add(user)
    db.flush()
    if payload.password:
        set_user_password(db, user=user, password=payload.password)
    db.commit()
    db.refresh(user)
    return user


@users_router.post("/bulk", response_model=UserBulkUpsertResponse)
def bulk_upsert_users(
    payload: UserBulkUpsertRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> UserBulkUpsertResponse:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User admin required")
    if not payload.users:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No users provided")

    created_count = 0
    updated_count = 0
    touched_users: list[User] = []
    seen_user_keys: set[str] = set()

    for item in payload.users:
        user_key = item.user_key.strip()
        if not user_key:
            continue
        if user_key in seen_user_keys:
            continue
        seen_user_keys.add(user_key)

        user = db.scalars(select(User).where(User.user_key == user_key)).first()
        if user is None:
            user = User(
                user_key=user_key,
                display_name=(item.display_name or user_key).strip(),
                email=item.email,
                role=item.role,
                is_active=item.is_active,
                metadata_json=item.metadata_json,
            )
            db.add(user)
            db.flush()
            created_count += 1
        else:
            user.display_name = (item.display_name or user.display_name or user_key).strip()
            user.email = item.email
            user.role = item.role
            user.is_active = item.is_active
            merged_metadata = dict(user.metadata_json or {})
            merged_metadata.update(item.metadata_json or {})
            user.metadata_json = merged_metadata
            updated_count += 1

        if item.password:
            set_user_password(db, user=user, password=item.password)
        touched_users.append(user)

    db.commit()
    for user in touched_users:
        db.refresh(user)
    return UserBulkUpsertResponse(
        created_count=created_count,
        updated_count=updated_count,
        users=touched_users,
    )


@users_router.patch("/{user_id}", response_model=UserSummary)
def update_user(
    user_id: UUID,
    payload: UserUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> User:
    if current_user.role not in {"admin", "service"} and current_user.id != user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User admin required")
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    if payload.display_name is not None:
        user.display_name = payload.display_name
    if payload.email is not None:
        user.email = payload.email
    if payload.role is not None and current_user.role in {"admin", "service"}:
        user.role = payload.role
    if payload.is_active is not None and current_user.role in {"admin", "service"}:
        user.is_active = payload.is_active
    if payload.metadata_json is not None:
        merged = dict(user.metadata_json or {})
        merged.update(payload.metadata_json)
        user.metadata_json = merged
    if payload.password:
        set_user_password(db, user=user, password=payload.password)

    db.commit()
    db.refresh(user)
    return user


@storage_roots_router.get("", response_model=list[StorageRootSummary])
def list_storage_roots(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[StorageRoot]:
    _ = current_user
    stmt = select(StorageRoot).order_by(StorageRoot.name.asc())
    return list(db.scalars(stmt))


@storage_roots_router.get("/{storage_root_id}/browse", response_model=StorageRootBrowseResponse)
def browse_storage_root(
    storage_root_id: int,
    relative_path: str = "",
    limit: int = 200,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StorageRootBrowseResponse:
    _ = current_user
    storage_root = db.get(StorageRoot, storage_root_id)
    if storage_root is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Storage root not found")

    root_path = Path(storage_root.path_prefix).expanduser().resolve()
    if not root_path.exists() or not root_path.is_dir():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Storage root path is not accessible on this host: {root_path}",
        )

    relative = relative_path.strip().replace("\\", "/").strip("/")
    candidate_path = (root_path / relative).resolve() if relative else root_path
    try:
        candidate_path.relative_to(root_path)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Requested path escapes storage root") from exc

    if not candidate_path.exists() or not candidate_path.is_dir():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Directory not found under storage root")

    try:
        child_dirs = sorted((entry for entry in candidate_path.iterdir() if entry.is_dir()), key=lambda entry: entry.name.lower())
    except OSError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to read directory contents: {exc}",
        ) from exc

    directories = [
        StorageRootBrowseEntry(
            name=entry.name,
            relative_path="" if entry == root_path else str(entry.relative_to(root_path)).replace("\\", "/"),
            absolute_path=str(entry),
        )
        for entry in child_dirs[: min(max(limit, 1), 500)]
    ]

    current_relative = "" if candidate_path == root_path else str(candidate_path.relative_to(root_path)).replace("\\", "/")
    parent_relative: str | None
    if candidate_path == root_path:
        parent_relative = None
    else:
        parent = candidate_path.parent
        parent_relative = "" if parent == root_path else str(parent.relative_to(root_path)).replace("\\", "/")

    return StorageRootBrowseResponse(
        storage_root=storage_root,
        current_relative_path=current_relative,
        current_absolute_path=str(candidate_path),
        parent_relative_path=parent_relative,
        directories=directories,
    )


def project_summary_view(project: Project) -> ProjectSummary:
    return ProjectSummary.model_validate(
        {
            "id": project.id,
            "experiment_project_id": project.experiment_project_id,
            "project_key": project.project_key,
            "project_name": project.project_name,
            "status": project.status,
            "health_status": project.health_status,
            "visibility": project.visibility,
            "fov_count": project.fov_count,
            "roi_count": project.roi_count,
            "classifier_count": project.classifier_count,
            "processor_count": project.processor_count,
            "pipeline_run_count": project.pipeline_run_count,
            "available_raw_count": project.available_raw_count,
            "missing_raw_count": project.missing_raw_count,
            "run_json_count": project.run_json_count,
            "h5_count": project.h5_count,
            "h5_bytes": project.h5_bytes,
            "latest_run_status": project.latest_run_status,
            "latest_run_at": project.latest_run_at,
            "project_mat_bytes": project.project_mat_bytes,
            "project_dir_bytes": project.project_dir_bytes,
            "estimated_raw_bytes": project.estimated_raw_bytes,
            "total_bytes": project.total_bytes,
            "metadata_json": summarize_metadata(project.metadata_json),
            "owner": project.owner,
            "created_at": project.created_at,
            "updated_at": project.updated_at,
        }
    )


def raw_dataset_summary_view(raw_dataset: RawDataset) -> RawDatasetSummary:
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
            "reclaimable_bytes": raw_dataset.reclaimable_bytes,
            "last_accessed_at": raw_dataset.last_accessed_at,
            "total_bytes": raw_dataset.total_bytes,
            "metadata_json": raw_dataset.metadata_json or {},
            "owner": raw_dataset.owner,
            "created_at": raw_dataset.created_at,
            "updated_at": raw_dataset.updated_at,
        }
    )


def summarize_metadata(metadata_json: dict | None) -> dict:
    metadata = dict(metadata_json or {})
    inventory = metadata.get("inventory")
    if not isinstance(inventory, dict):
        return metadata

    trimmed_inventory = dict(inventory)
    for key in ("pipeline_runs", "classifier_runs", "processor_runs"):
        records = inventory.get(key)
        if isinstance(records, list):
            trimmed_inventory[f"{key}_count"] = len(records)
            trimmed_inventory.pop(key, None)
    metadata["inventory"] = trimmed_inventory
    return metadata


def raw_dataset_needs_preview(raw_dataset: RawDataset) -> bool:
    positions = list(raw_dataset.positions or [])
    if not positions:
        return True
    return any(position.preview_artifact_id is None for position in positions)


def enqueue_raw_preview_job_for_dataset(
    db: Session,
    *,
    current_user_key: str,
    project_id: UUID,
    raw_dataset: RawDataset,
    force: bool,
    requested_mode: str,
    priority: int,
    params_json: dict,
) -> UUID | None:
    if not force and not raw_dataset_needs_preview(raw_dataset):
        return None

    job_params = dict(params_json or {})
    job_params.update(
        {
            "job_kind": "raw_preview_video",
            "force": force,
            "position_id": None,
            "position_key": None,
            "scope": "dataset",
            "project_id": str(project_id),
        }
    )
    job = Job(
        project_id=project_id,
        raw_dataset_id=raw_dataset.id,
        requested_mode=requested_mode,
        priority=priority,
        requested_by=current_user_key,
        params_json=job_params,
        status="queued",
    )
    db.add(job)
    db.flush()
    return job.id


def project_detail_view(project: Project) -> ProjectDetail:
    detail = ProjectDetail.model_validate(project_summary_view(project).model_dump())
    detail.locations = [project_location_summary_view(location) for location in project.locations or []]
    detail.raw_datasets = [
        raw_dataset_summary_view(link.raw_dataset)
        for link in sorted(project.raw_links or [], key=lambda value: value.created_at or project.created_at)
        if link.raw_dataset is not None
    ]
    return detail


def project_location_summary_view(location: ProjectLocation) -> ProjectLocationSummary:
    project_mat_path = ""
    project_dir_path = ""
    if location.storage_root is not None:
        project_mat_path = compose_storage_path(
            location.storage_root.path_prefix,
            location.relative_path,
            location.project_file_name,
        )
        if location.project_file_name:
            project_dir_path = compose_storage_path(
                location.storage_root.path_prefix,
                location.relative_path,
                Path(location.project_file_name).stem,
            )

    return ProjectLocationSummary.model_validate(
        {
            "id": location.id,
            "relative_path": location.relative_path,
            "project_file_name": location.project_file_name,
            "project_mat_path": project_mat_path or None,
            "project_dir_path": project_dir_path or None,
            "access_mode": location.access_mode,
            "is_preferred": location.is_preferred,
            "storage_root": location.storage_root,
        }
    )
