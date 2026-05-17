from fastapi import HTTPException

from api.services.user_home_storage import (
    default_home_relative_path,
    normalize_home_relative_path,
    normalize_provider_key,
    normalize_quota_mode,
    storage_safe_user_key,
)
from api.models import StorageRoot
from worker.user_home_storage import normalize_subdirectories, resolve_storage_root_relative_path


def test_normalize_provider_key_is_stable() -> None:
    assert normalize_provider_key(" Synology Main ") == "synology-main"
    assert normalize_provider_key("homes_provider") == "homes_provider"


def test_normalize_home_relative_path_accepts_safe_paths() -> None:
    assert normalize_home_relative_path("florian/DetecdivHub") == "florian/DetecdivHub"
    assert normalize_home_relative_path(r"florian\DetecdivHub\projects") == "florian/DetecdivHub/projects"
    assert normalize_home_relative_path(" florian/DetecdivHub/ ") == "florian/DetecdivHub"


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
    assert default_home_relative_path("Alice Smith") == "Alice_Smith/DetecdivHub"


def test_quota_mode_guard() -> None:
    assert normalize_quota_mode(" Provider_Enforced ") == "provider_enforced"
    try:
        normalize_quota_mode("synology_only")
    except HTTPException as exc:
        assert exc.status_code == 400
    else:
        raise AssertionError("Expected invalid quota mode to be rejected")


def test_resolve_storage_root_relative_path_stays_under_root(tmp_path) -> None:
    root = StorageRoot(name="user-homes", root_type="user_home_root", host_scope="test", path_prefix=str(tmp_path))

    resolved = resolve_storage_root_relative_path(root, "alice/DetecdivHub")

    assert resolved == (tmp_path / "alice" / "DetecdivHub").resolve()


def test_resolve_storage_root_relative_path_requires_existing_root(tmp_path) -> None:
    missing = tmp_path / "missing"
    root = StorageRoot(name="user-homes", root_type="user_home_root", host_scope="test", path_prefix=str(missing))

    try:
        resolve_storage_root_relative_path(root, "alice/DetecdivHub")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Expected missing storage root to be rejected")


def test_normalize_subdirectories_rejects_nested_or_unsafe_values() -> None:
    assert normalize_subdirectories(["projects", "raw", "raw"]) == ["projects", "raw"]
    for value in (["../outside"], ["nested/path"]):
        try:
            normalize_subdirectories(value)
        except (HTTPException, ValueError):
            pass
        else:
            raise AssertionError(f"Expected {value!r} to be rejected")
