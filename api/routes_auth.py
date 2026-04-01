from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.db import get_db
from api.models import User
from api.schemas import AuthLoginRequest, AuthLoginResponse, AuthSessionResponse, UserSessionSummary, UserSummary
from api.services.auth import (
    get_user_by_session_token,
    issue_user_session,
    list_active_sessions,
    revoke_session_token,
    revoke_user_session,
    verify_password,
)
from api.services.users import get_current_identity, get_current_user


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=AuthLoginResponse)
def login(
    payload: AuthLoginRequest,
    response: Response,
    db: Session = Depends(get_db),
) -> AuthLoginResponse:
    user = db.scalars(select(User).where(User.user_key == payload.user_key, User.is_active.is_(True))).first()
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    user_session, token = issue_user_session(db, user=user, client_label=payload.client_label)
    db.commit()
    settings = get_settings()
    response.set_cookie(
        key=settings.session_cookie_name,
        value=token,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=int(settings.session_duration_hours) * 3600,
    )
    return AuthLoginResponse(
        user=UserSummary.model_validate(user),
        session_token=token,
        expires_at=user_session.expires_at,
    )


@router.post("/logout")
def logout(
    request: Request,
    response: Response,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    _ = current_user
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    elif request is not None:
        settings = get_settings()
        token = request.cookies.get(settings.session_cookie_name)
    revoke_session_token(db, token)
    db.commit()
    settings = get_settings()
    response.delete_cookie(settings.session_cookie_name)
    return {"status": "logged_out"}


@router.get("/session", response_model=AuthSessionResponse)
def get_session(
    current_identity: tuple[User, str] = Depends(get_current_identity),
) -> AuthSessionResponse:
    current_user, auth_mode = current_identity
    return AuthSessionResponse(
        authenticated=True,
        auth_mode=auth_mode,
        user=UserSummary.model_validate(current_user),
        expires_at=None,
    )


@router.get("/legacy-session", response_model=AuthSessionResponse)
def get_legacy_session(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> AuthSessionResponse:
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    user = get_user_by_session_token(db, token)
    if user is None:
        return AuthSessionResponse(authenticated=False, auth_mode="none", user=None, expires_at=None)
    return AuthSessionResponse(
        authenticated=True,
        auth_mode="session",
        user=UserSummary.model_validate(user),
        expires_at=None,
    )


@router.get("/sessions", response_model=list[UserSessionSummary])
def list_sessions(
    all_users: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[UserSessionSummary]:
    sessions = list_active_sessions(db, acting_user=current_user, include_all=all_users)
    return [
        UserSessionSummary.model_validate(
            {
                "id": session.id,
                "status": session.status,
                "client_label": session.client_label,
                "last_seen_at": session.last_seen_at,
                "expires_at": session.expires_at,
                "created_at": session.created_at,
                "user": session.user,
            }
        )
        for session in sessions
    ]


@router.delete("/sessions/{session_id}")
def delete_session(
    session_id: UUID,
    request: Request,
    response: Response,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    session = revoke_user_session(db, session_id=session_id, acting_user=current_user)
    if session is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")

    settings = get_settings()
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    elif request is not None:
        token = request.cookies.get(settings.session_cookie_name)

    if token is not None and get_user_by_session_token(db, token) is None:
        response.delete_cookie(settings.session_cookie_name)
    db.commit()
    return {"status": "revoked", "session_id": str(session_id)}
