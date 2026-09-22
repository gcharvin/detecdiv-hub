from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from api.models import SystemSetting


ASSISTANT_CONTROL_SETTING_KEY = "qwen_assistant_control"
ASSISTANT_CONTROL_JOB_KIND = "assistant_service_control"


def get_assistant_desired_state(session: Session) -> str:
    setting = session.get(SystemSetting, ASSISTANT_CONTROL_SETTING_KEY)
    state = str((setting.value_json or {}).get("desired_state") or "running").lower() if setting else "running"
    return state if state in {"running", "stopped"} else "running"


def set_assistant_desired_state(session: Session, *, state: str, requested_by: str | None) -> None:
    if state not in {"running", "stopped"}:
        raise ValueError("Assistant desired state must be running or stopped.")
    payload = {
        "desired_state": state,
        "requested_by": requested_by,
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    setting = session.get(SystemSetting, ASSISTANT_CONTROL_SETTING_KEY)
    if setting is None:
        session.add(SystemSetting(key=ASSISTANT_CONTROL_SETTING_KEY, value_json=payload))
    else:
        setting.value_json = payload
