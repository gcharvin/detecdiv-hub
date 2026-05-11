from fastapi import HTTPException

from api.services.user_home_storage import (
    default_home_relative_path,
    normalize_home_relative_path,
    normalize_provider_key,
    normalize_quota_mode,
    storage_safe_user_key,
)


def test_normalize_provider_key_is_stable() -> None:
    assert normalize_provider_key(" Synology Main ") == "synology-main"
    assert normalize_provider_key("homes_provider") == "homes_provider"


def test_normalize_home_relative_path_accepts_safe_paths() -> None:
    assert normalize_home_relative_path("florian/DetecDiv") == "florian/DetecDiv"
    assert normalize_home_relative_path(r"florian\DetecDiv\projects") == "florian/DetecDiv/projects"
    assert normalize_home_relative_path(" florian/DetecDiv/ ") == "florian/DetecDiv"


def test_normalize_home_relative_path_rejects_unsafe_paths() -> None:
    for value in ("/absolute", "../florian", "florian/../alice", "C:/homes/florian", ""):
        try:
            normalize_home_relative_path(value)
        except HTTPException as exc:
            assert exc.status_code == 400
        else:
            raise AssertionError(f"Expected {value!r} to be rejected")


def test_default_home_relative_path_uses_storage_safe_user_key() -> None:
    assert storage_safe_user_key("Alice Smith") == "Alice_Smith"
    assert default_home_relative_path("Alice Smith") == "Alice_Smith/DetecDiv"


def test_quota_mode_guard() -> None:
    assert normalize_quota_mode(" Provider_Enforced ") == "provider_enforced"
    try:
        normalize_quota_mode("synology_only")
    except HTTPException as exc:
        assert exc.status_code == 400
    else:
        raise AssertionError("Expected invalid quota mode to be rejected")
