from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from api.config import get_settings
from api.models import User
from api.schemas import AssistantChatRequest, AssistantChatResponse, AssistantStatusResponse
from api.services.qwen_assistant import AssistantUnavailableError, ask_qwen
from api.services.users import get_current_user

router = APIRouter(prefix="/assistant", tags=["assistant"])


@router.get("/status", response_model=AssistantStatusResponse)
def get_assistant_status(
    current_user: Annotated[User, Depends(get_current_user)],
) -> AssistantStatusResponse:
    _ = current_user
    settings = get_settings()
    configured = bool(settings.assistant_qwen_base_url and settings.assistant_qwen_model)
    if not settings.assistant_enabled:
        message = "Assistant disabled by administrator."
    elif not configured:
        message = "Assistant awaiting local Qwen service configuration."
    else:
        message = "Assistant ready."
    return AssistantStatusResponse(
        enabled=settings.assistant_enabled,
        configured=configured,
        model=settings.assistant_qwen_model or None,
        message=message,
    )


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
