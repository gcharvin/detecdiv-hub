from pathlib import Path

from api.models import User
from api.services.users import can_access_admin_portal
from scripts.provision_users_from_data_dirs import is_first_name_folder, normalize_user_key


def make_user(*, role: str = "user", admin_portal_access: bool = False) -> User:
    return User(
        user_key="tester",
        display_name="Tester",
        role=role,
        is_active=True,
        admin_portal_access=admin_portal_access,
    )


def test_admin_portal_access_helper() -> None:
    assert can_access_admin_portal(make_user(role="admin"))
    assert can_access_admin_portal(make_user(role="service"))
    assert can_access_admin_portal(make_user(admin_portal_access=True))
    assert not can_access_admin_portal(make_user())


def test_first_name_folder_heuristic() -> None:
    assert is_first_name_folder(Path("Florian"))
    assert is_first_name_folder(Path("Abhilasha"))
    assert not is_first_name_folder(Path("common_labo4"))
    assert not is_first_name_folder(Path("Collab-Theo_Basile"))
    assert not is_first_name_folder(Path("basile2"))


def test_normalize_user_key() -> None:
    assert normalize_user_key("Florian") == "florian"
    assert normalize_user_key("Anais") == "anais"
