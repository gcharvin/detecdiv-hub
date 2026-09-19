from __future__ import annotations

from dataclasses import dataclass

import requests

from api.config import Settings


class AssistantUnavailableError(RuntimeError):
    """Raised when the optional local Qwen inference service is unavailable."""


@dataclass(frozen=True)
class AssistantReply:
    answer: str
    model: str


def _system_prompt(mode: str) -> str:
    base = (
        "You are the DetecDiv Hub assistant. Answer accurately and concisely. "
        "You have no access to files, databases, terminals, jobs, or external tools. "
        "Never claim that you inspected Hub data or launched a calculation."
    )
    if mode == "translation":
        return base + " Translate the user's text faithfully; preserve scientific notation, paths, and identifiers."
    return base


def _endpoint(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/chat/completions"


def ask_qwen(*, settings: Settings, message: str, mode: str) -> AssistantReply:
    if not settings.assistant_enabled:
        raise AssistantUnavailableError("The Hub assistant is disabled by configuration.")
    if not settings.assistant_qwen_base_url or not settings.assistant_qwen_model:
        raise AssistantUnavailableError("The local Qwen service has not been configured yet.")

    if len(message) > settings.assistant_max_message_chars:
        raise ValueError("Message exceeds the configured assistant limit.")

    try:
        response = requests.post(
            _endpoint(settings.assistant_qwen_base_url),
            json={
                "model": settings.assistant_qwen_model,
                "messages": [
                    {"role": "system", "content": _system_prompt(mode)},
                    {"role": "user", "content": message},
                ],
                "temperature": 0.2 if mode == "translation" else 0.5,
                "max_tokens": settings.assistant_max_response_tokens,
                # Qwen3 enables a long reasoning trace by default.  The Hub
                # interface is a concise assistant, so request its direct
                # response mode; this is supported by its vLLM chat template.
                "chat_template_kwargs": {"enable_thinking": False},
            },
            timeout=settings.assistant_request_timeout_sec,
        )
        response.raise_for_status()
        payload = response.json()
        answer = payload["choices"][0]["message"]["content"].strip()
    except (requests.RequestException, KeyError, IndexError, TypeError, ValueError) as exc:
        raise AssistantUnavailableError("The local Qwen service is unavailable or returned an invalid response.") from exc

    if not answer:
        raise AssistantUnavailableError("The local Qwen service returned an empty response.")
    return AssistantReply(answer=answer, model=str(payload.get("model") or settings.assistant_qwen_model))
