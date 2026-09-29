"""Persist a full DetecDiv commit in each new MATLAB job."""
import re

from fastapi import HTTPException

from sqlalchemy import select
from api.models import ExecutionTarget, SystemSetting

SETTING_KEY = "matlab_code_release"


def validate_commit(value: str) -> str:
    value = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("DetecDiv code_commit must be a full 40-character Git commit SHA.")
    return value


def default_commit(session) -> str:
    record = session.get(SystemSetting, SETTING_KEY)
    value = (record.value_json or {}).get("code_commit") if record else None
    return validate_commit(value) if value else ""


def pin_execution(session, execution: dict) -> dict:
    result = dict(execution or {})
    try:
        value = result.get("code_commit") or default_commit(session)
        if value:
            result["code_commit"] = validate_commit(value)
        else:
            raise HTTPException(status_code=503, detail="Publish a MATLAB code release before submitting a new job.")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return result


def versioned_target(session, requested_id=None):
    """Keep pinned jobs away from pollers that still use the old launcher."""
    if requested_id is not None:
        target = session.get(ExecutionTarget, requested_id)
        if target and (target.metadata_json or {}).get("matlab_code_isolation_ready") is True:
            return target.id
        raise HTTPException(status_code=503, detail="This target has not activated MATLAB code isolation yet.")
    targets = session.scalars(select(ExecutionTarget).order_by(ExecutionTarget.target_key)).all()
    for target in targets:
        if target.status == "online" and (target.metadata_json or {}).get("matlab_code_isolation_ready") is True:
            return target.id
    raise HTTPException(status_code=503, detail="No execution target has activated MATLAB code isolation yet.")
