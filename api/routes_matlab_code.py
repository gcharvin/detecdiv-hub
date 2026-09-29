"""Publish the default DetecDiv version without restarting workers."""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.db import get_db
from api.models import SystemSetting, User
from api.services.matlab_code_versions import SETTING_KEY, default_commit, validate_commit
from api.services.users import get_current_user

router = APIRouter(prefix="/matlab-code", tags=["matlab-code"])


class MatlabCodeRelease(BaseModel):
    code_commit: str


@router.get("/release")
def get_release(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    return {"code_commit": default_commit(db)}


@router.put("/release")
def publish_release(payload: MatlabCodeRelease, db: Session = Depends(get_db),
                    current_user: User = Depends(get_current_user)):
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=403, detail="MATLAB code release admin required")
    try:
        commit = validate_commit(payload.code_commit)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    record = db.get(SystemSetting, SETTING_KEY)
    if record is None:
        record = SystemSetting(key=SETTING_KEY)
        db.add(record)
    record.value_json = {"code_commit": commit, "published_by": current_user.user_key}
    db.commit()
    return {"code_commit": commit}
