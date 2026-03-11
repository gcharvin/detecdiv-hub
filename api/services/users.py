from __future__ import annotations

from fastapi import Depends, Header, HTTPException, Query, Request, status
from sqlalchemy import exists, literal, or_, select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.db import get_db
from api.models import ExperimentProject, Project, ProjectAcl, User
from api.services.auth import get_user_by_session_token


def get_or_create_user(
    session: Session,
    *,
    user_key: str,
    display_name: str | None = None,
    role: str = "user",
) -> User:
    user = session.scalars(select(User).where(User.user_key == user_key)).first()
    if user is None:
        user = User(
            user_key=user_key,
            display_name=display_name or user_key,
            role=role,
            is_active=True,
            metadata_json={},
        )
        session.add(user)
        session.flush()
    return user


def get_current_user(
    request: Request,
    user_key: str | None = Query(default=None),
    x_detecdiv_user: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> User:
    settings = get_settings()
    bearer_token = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()

    cookie_token = None
    if request is not None:
        cookie_token = request.cookies.get(settings.session_cookie_name)
    session_user = get_user_by_session_token(db, bearer_token or cookie_token)
    if session_user is not None:
        return session_user

    effective_user_key = None
    if settings.allow_legacy_user_key_auth:
        effective_user_key = user_key or x_detecdiv_user or settings.default_user_key
    if not effective_user_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing user identity")

    user = db.scalars(
        select(User).where(User.user_key == effective_user_key, User.is_active.is_(True))
    ).first()
    if user is None:
        if not settings.auto_provision_users:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unknown user")
        user = get_or_create_user(db, user_key=effective_user_key, display_name=effective_user_key)
        db.flush()
    return user


def project_access_filter(user: User):
    if user.role in {"admin", "service"}:
        return literal(True)

    acl_exists = exists(
        select(ProjectAcl.id).where(
            ProjectAcl.project_id == Project.id,
            ProjectAcl.user_id == user.id,
        )
    )
    return or_(
        Project.visibility == "public",
        Project.owner_user_id == user.id,
        acl_exists,
    )


def ensure_project_readable(project: Project | None, user: User) -> Project:
    if project is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
    if user.role in {"admin", "service"}:
        return project
    if project.visibility == "public":
        return project
    if project.owner_user_id == user.id:
        return project
    for acl in project.acl_entries:
        if acl.user_id == user.id:
            return project
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")


def user_can_edit_project(project: Project, user: User) -> bool:
    if user.role in {"admin", "service"}:
        return True
    if project.owner_user_id == user.id:
        return True
    for acl in project.acl_entries:
        if acl.user_id == user.id and acl.access_level in {"editor", "owner"}:
            return True
    return False


def experiment_access_filter(user: User):
    if user.role in {"admin", "service"}:
        return literal(True)
    return or_(
        ExperimentProject.visibility == "public",
        ExperimentProject.owner_user_id == user.id,
    )


def ensure_experiment_readable(experiment: ExperimentProject | None, user: User) -> ExperimentProject:
    if experiment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment not found")
    if user.role in {"admin", "service"}:
        return experiment
    if experiment.visibility == "public":
        return experiment
    if experiment.owner_user_id == user.id:
        return experiment
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Experiment not found")


def user_can_edit_experiment(experiment: ExperimentProject, user: User) -> bool:
    if user.role in {"admin", "service"}:
        return True
    return experiment.owner_user_id == user.id
