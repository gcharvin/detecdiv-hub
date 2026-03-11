from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import or_, select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import (
    Project,
    ProjectAcl,
    ProjectDeletionEvent,
    ProjectGroup,
    ProjectGroupMember,
    ProjectLocation,
    ProjectNote,
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
    ProjectNoteCreate,
    ProjectNoteSummary,
    ProjectSummary,
    ProjectUpdate,
    StorageRootSummary,
    UserSummary,
    UserUpdate,
)
from api.services.auth import set_user_password
from api.services.project_deletion import build_deletion_preview, execute_project_deletion, record_deletion_preview
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
) -> Project:
    stmt = (
        select(Project)
        .options(
            joinedload(Project.owner),
            joinedload(Project.acl_entries),
            joinedload(Project.locations).joinedload(ProjectLocation.storage_root),
        )
        .where(Project.id == project_id, Project.status != "deleted")
    )
    project = db.scalars(stmt).unique().first()
    return ensure_project_readable(project, current_user)


@router.patch("/{project_id}", response_model=ProjectDetail)
def update_project(
    project_id: UUID,
    payload: ProjectUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Project:
    stmt = (
        select(Project)
        .options(
            joinedload(Project.owner),
            joinedload(Project.acl_entries),
            joinedload(Project.locations).joinedload(ProjectLocation.storage_root),
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
