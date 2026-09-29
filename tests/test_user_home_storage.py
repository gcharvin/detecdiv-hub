from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api.services.user_home_storage import (
    default_home_relative_path,
    normalize_home_relative_path,
    normalize_provider_key,
    normalize_quota_mode,
    storage_safe_user_key,
    synology_home_mount_matches_mountinfo,
)
from api.models import StorageRoot
from worker.user_home_storage import normalize_subdirectories, resolve_storage_root_relative_path, resolve_user_home_path


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


def test_synology_home_prepare_rejects_unmounted_root(tmp_path) -> None:
    root = StorageRoot(name="homes2", root_type="user_home_root", host_scope="test", path_prefix=str(tmp_path))
    provider = SimpleNamespace(
        provider_key="synology-secondary",
        provider_kind="synology_dsm",
        mount_root=str(tmp_path),
        config_json={"home_mount_source": "//10.20.8.250/home"},
    )
    account = SimpleNamespace(
        id="test",
        provider=provider,
        provider_user_key="maliavko",
        home_storage_root=root,
        home_relative_path="maliavko/DetecdivHub",
    )

    with pytest.raises(RuntimeError, match="no user-scoped home mount"):
        resolve_user_home_path(account)
    assert not (tmp_path / "maliavko").exists()


def test_synology_home_accepts_matching_per_user_mount_but_not_aggregate_mount(tmp_path) -> None:
    provider_root = tmp_path / "homes"
    user_home = provider_root / "maliavko"
    hub_home = user_home / "DetecdivHub"
    hub_home.mkdir(parents=True)
    mountinfo = "\n".join(
        (
            f"10 1 0:32 / {provider_root} rw - cifs //10.20.8.250/homes rw,vers=3.0,username=Fred",
            f"11 10 0:33 / {user_home} rw - cifs //10.20.8.250/home rw,vers=3.0,username=maliavko",
        )
    )
    common = {
        "provider_root": provider_root,
        "storage_root_path": provider_root,
        "target_path": hub_home,
        "expected_source": "//10.20.8.250/home",
        "expected_username": "maliavko",
    }

    assert synology_home_mount_matches_mountinfo(**common, mountinfo_text=mountinfo)
    aggregate_only = mountinfo.splitlines()[0]
    assert not synology_home_mount_matches_mountinfo(**common, mountinfo_text=aggregate_only)


def test_synology_home_rejects_wrong_user_or_nas_mount(tmp_path) -> None:
    provider_root = tmp_path / "homes"
    user_home = provider_root / "maliavko"
    hub_home = user_home / "DetecdivHub"
    hub_home.mkdir(parents=True)
    common = {
        "provider_root": provider_root,
        "storage_root_path": provider_root,
        "target_path": hub_home,
        "expected_source": "//10.20.8.250/home",
        "expected_username": "maliavko",
    }

    wrong_user = f"11 10 0:33 / {user_home} rw - cifs //10.20.8.250/home rw,vers=3.0,username=Fred"
    wrong_nas = f"11 10 0:33 / {user_home} rw - cifs //10.20.11.250/home rw,vers=3.0,username=maliavko"
    assert not synology_home_mount_matches_mountinfo(**common, mountinfo_text=wrong_user)
    assert not synology_home_mount_matches_mountinfo(**common, mountinfo_text=wrong_nas)


def test_synology_home_rejects_storage_root_outside_provider_namespace(tmp_path) -> None:
    provider_root = tmp_path / "homes"
    data_root = tmp_path / "data" / "Gilles"
    data_root.mkdir(parents=True)
    root = StorageRoot(
        name="legacy-user-home",
        root_type="user_home_root",
        host_scope="test",
        path_prefix=str(data_root),
    )
    provider = SimpleNamespace(provider_key="synology-main", provider_kind="synology_dsm", mount_root=str(provider_root))
    assert not synology_home_mount_matches_mountinfo(
        provider_root=Path(provider.mount_root),
        storage_root_path=Path(root.path_prefix),
        target_path=data_root / "DetecdivHub",
        expected_source="//10.20.11.250/home",
        expected_username="Gilles",
        mountinfo_text="99 1 0:1 / / rw - ext4 /dev/root rw",
    )


def test_normalize_subdirectories_rejects_nested_or_unsafe_values() -> None:
    assert normalize_subdirectories(["projects", "raw", "raw"]) == ["projects", "raw"]
    for value in (["../outside"], ["nested/path"]):
        try:
            normalize_subdirectories(value)
        except (HTTPException, ValueError):
            pass
        else:
            raise AssertionError(f"Expected {value!r} to be rejected")
