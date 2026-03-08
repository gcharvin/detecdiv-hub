from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
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
    User,
)
from api.schemas import (
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
    UserSummary,
)
from api.services.project_deletion import build_deletion_preview, execute_project_deletion, record_deletion_preview
from api.services.users import ensure_project_readable, get_current_user, get_or_create_user, project_access_filter, user_can_edit_project


router = APIRouter(prefix="/projects", tags=["projects"])
groups_router = APIRouter(prefix="/project-groups", tags=["project-groups"])
users_router = APIRouter(prefix="/users", tags=["users"])


@router.get("", response_model=list[ProjectSummary])
def list_projects(
    group_id: UUID | None = None,
    owned_only: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[Project]:
    stmt = (
        select(Project)
        .options(joinedload(Project.owner))
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
    return list(db.scalars(stmt).unique())


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
    detail.projects = [ProjectSummary.model_validate(project) for project in projects]
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
