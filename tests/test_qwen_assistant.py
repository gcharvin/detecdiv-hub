from types import SimpleNamespace

import pytest

from api.services.qwen_assistant import AssistantUnavailableError, ask_qwen


def settings(**overrides):
    values = {
        "assistant_enabled": True,
        "assistant_qwen_base_url": "http://qwen.internal/v1",
        "assistant_qwen_model": "Qwen/Qwen3.5-14B",
        "assistant_request_timeout_sec": 10.0,
        "assistant_max_message_chars": 100,
        "assistant_max_response_tokens": 256,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_ask_qwen_uses_openai_compatible_chat_endpoint(monkeypatch):
    calls = []

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"model": "Qwen/test", "choices": [{"message": {"content": " Bonjour "}}]}

    def fake_post(*args, **kwargs):
        calls.append((args, kwargs))
        return Response()

    monkeypatch.setattr("api.services.qwen_assistant.requests.post", fake_post)

    reply = ask_qwen(settings=settings(), message="Salut", mode="translation")

    assert reply.answer == "Bonjour"
    assert reply.model == "Qwen/test"
    assert calls[0][0] == ("http://qwen.internal/v1/chat/completions",)
    assert calls[0][1]["json"]["messages"][1] == {"role": "user", "content": "Salut"}


def test_ask_qwen_rejects_unconfigured_service():
    with pytest.raises(AssistantUnavailableError):
        ask_qwen(settings=settings(assistant_qwen_base_url=""), message="Salut", mode="chat")


def test_ask_qwen_never_exposes_tools_to_the_model(monkeypatch):
    captured = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "Réponse"}}]}

    def fake_post(*_args, **kwargs):
        captured.update(kwargs["json"])
        return Response()

    monkeypatch.setattr("api.services.qwen_assistant.requests.post", fake_post)

    ask_qwen(settings=settings(), message="Liste les fichiers", mode="chat")

    assert "tools" not in captured
    assert "no access to files" in captured["messages"][0]["content"]
