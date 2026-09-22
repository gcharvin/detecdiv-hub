from typing import Annotated

import requests
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.db import get_db
from api.models import ExecutionTarget, Job, User
from api.schemas import (
    AssistantChatRequest,
    AssistantChatResponse,
    AssistantControlRequest,
    AssistantStatusResponse,
    JobSummary,
)
from api.services.assistant_control import (
    ASSISTANT_CONTROL_JOB_KIND,
    get_assistant_desired_state,
    set_assistant_desired_state,
)
from api.services.qwen_assistant import AssistantUnavailableError, ask_qwen
from api.services.users import get_current_user

router = APIRouter(prefix="/assistant", tags=["assistant"])


@router.get("/status", response_model=AssistantStatusResponse)
def get_assistant_status(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Session = Depends(get_db),
) -> AssistantStatusResponse:
    settings = get_settings()
    configured = bool(settings.assistant_qwen_base_url and settings.assistant_qwen_model)
    desired_state = get_assistant_desired_state(db)
    pending_control = db.scalar(
        select(Job.id)
        .where(Job.status.in_(("queued", "running")))
        .where(Job.params_json["job_kind"].as_string() == ASSISTANT_CONTROL_JOB_KIND)
        .limit(1)
    )
    running = _qwen_is_running(settings) if configured else False
    if not settings.assistant_enabled:
        message = "Assistant désactivé dans la configuration."
    elif not configured:
        message = "Le service Qwen n’est pas configuré."
    elif pending_control and desired_state == "running" and not running:
        message = "Démarrage de Qwen en cours."
    elif pending_control and desired_state == "stopped" and running:
        message = "Arrêt de Qwen en cours."
    elif desired_state == "stopped" and not running:
        message = "Modèle arrêté par l’administration."
    elif not running:
        message = "Modèle indisponible. Un administrateur peut le relancer."
    else:
        message = "Qwen est disponible."
    return AssistantStatusResponse(
        enabled=settings.assistant_enabled,
        configured=configured,
        running=running,
        desired_state=desired_state,
        control_pending=bool(pending_control),
        can_control=current_user.role in {"admin", "service"},
        model=settings.assistant_qwen_model or None,
        message=message,
    )


@router.post("/control", response_model=JobSummary, status_code=status.HTTP_202_ACCEPTED)
def control_assistant_service(
    payload: AssistantControlRequest,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Session = Depends(get_db),
) -> Job:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Administrator role required")

    settings = get_settings()
    if not settings.assistant_qwen_base_url or not settings.assistant_qwen_model:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Qwen is not configured")

    pending = db.scalar(
        select(Job.id)
        .where(Job.status.in_(("queued", "running")))
        .where(Job.params_json["job_kind"].as_string() == ASSISTANT_CONTROL_JOB_KIND)
        .limit(1)
    )
    if pending:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A Qwen control request is already in progress")

    target_key = str(settings.indexing_target_key or settings.worker_target_key or "").strip()
    if not target_key:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="No compute worker target is configured")
    target = db.scalar(select(ExecutionTarget).where(ExecutionTarget.target_key == target_key))
    if target is None or not target.supports_gpu:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="The configured worker target has no GPU")

    desired_state = "running" if payload.action == "start" else "stopped"
    set_assistant_desired_state(db, state=desired_state, requested_by=current_user.user_key)
    job = Job(
        execution_target_id=target.id,
        requested_mode="server",
        priority=0,
        requested_by=current_user.user_key,
        requested_from_host="assistant-admin-ui",
        params_json={"job_kind": ASSISTANT_CONTROL_JOB_KIND, "action": payload.action},
        status="queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


@router.post("/chat", response_model=AssistantChatResponse)
def chat_with_assistant(
    payload: AssistantChatRequest,
    current_user: Annotated[User, Depends(get_current_user)],
) -> AssistantChatResponse:
    _ = current_user
    settings = get_settings()
    try:
        reply = ask_qwen(settings=settings, message=payload.message.strip(), mode=payload.mode)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except AssistantUnavailableError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    return AssistantChatResponse(answer=reply.answer, model=reply.model)


def _qwen_is_running(settings) -> bool:
    try:
        response = requests.get(
            f"{settings.assistant_qwen_base_url.rstrip('/')}/models",
            timeout=min(settings.assistant_request_timeout_sec, 3),
        )
        response.raise_for_status()
        models = response.json().get("data", [])
    except (requests.RequestException, TypeError, ValueError):
        return False
    return any(item.get("id") == settings.assistant_qwen_model for item in models if isinstance(item, dict))
