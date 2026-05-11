from fastapi import HTTPException

from api.models import User
from api.services.acquisition_sessions import (
    ensure_valid_acquisition_status,
    normalize_landing_relative_path,
    suggested_landing_relative_path,
)


def make_user() -> User:
    return User(user_key="Alice Smith", display_name="Alice Smith", role="user", is_active=True)


def test_normalize_landing_relative_path_accepts_safe_paths() -> None:
    assert normalize_landing_relative_path("session_1/Pos0") == "session_1/Pos0"
    assert normalize_landing_relative_path(r"session_1\Pos0") == "session_1/Pos0"
    assert normalize_landing_relative_path(" session_1/Pos0/ ") == "session_1/Pos0"


def test_normalize_landing_relative_path_rejects_unsafe_paths() -> None:
    for value in ("/absolute", "../outside", "session/../outside", "C:/data/session"):
        try:
            normalize_landing_relative_path(value)
        except HTTPException as exc:
            assert exc.status_code == 400
        else:
            raise AssertionError(f"Expected {value!r} to be rejected")


def test_suggested_landing_relative_path_is_hub_relative() -> None:
    path = suggested_landing_relative_path(user=make_user(), acquisition_label="YAM 853 Switch")

    assert path.startswith("acquisitions/alice_smith/")
    assert "yam_853_switch" in path
    assert "\\" not in path
    assert ":" not in path


def test_acquisition_status_validation() -> None:
    assert ensure_valid_acquisition_status("Acquiring") == "acquiring"
    try:
        ensure_valid_acquisition_status("unknown")
    except HTTPException as exc:
        assert exc.status_code == 400
    else:
        raise AssertionError("Expected invalid acquisition status to be rejected")
