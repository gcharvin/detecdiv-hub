from types import SimpleNamespace

from api.jupyterhub_auth import allowed_user, lookup_user, normalize_username, user_is_admin, user_to_auth_state


def test_normalize_username_strips_whitespace() -> None:
    assert normalize_username("  alice  ") == "alice"


def test_user_is_admin_maps_roles_and_portal_access() -> None:
    assert user_is_admin(SimpleNamespace(role="admin", admin_portal_access=False))
    assert user_is_admin(SimpleNamespace(role="service", admin_portal_access=False))
    assert user_is_admin(SimpleNamespace(role="user", admin_portal_access=True))
    assert not user_is_admin(SimpleNamespace(role="user", admin_portal_access=False))


def test_allowed_user_respects_active_flag_and_lab_status() -> None:
    assert allowed_user(SimpleNamespace(is_active=True, lab_status="yes"), allowed_lab_statuses=None)
    assert not allowed_user(SimpleNamespace(is_active=False, lab_status="yes"), allowed_lab_statuses=None)
    assert allowed_user(
        SimpleNamespace(is_active=True, lab_status="yes"),
        allowed_lab_statuses={"yes", "alumni"},
    )
    assert not allowed_user(
        SimpleNamespace(is_active=True, lab_status="alumni"),
        allowed_lab_statuses={"yes"},
    )


def test_user_to_auth_state_preserves_useful_fields() -> None:
    user = SimpleNamespace(
        user_key="alice",
        display_name="Alice",
        email="alice@example.org",
        role="user",
        is_active=True,
        admin_portal_access=False,
        lab_status="yes",
        default_path="/data/alice",
        metadata_json={"team": "lab"},
    )

    auth_state = user_to_auth_state(user)

    assert auth_state["user_key"] == "alice"
    assert auth_state["display_name"] == "Alice"
    assert auth_state["default_path"] == "/data/alice"
    assert auth_state["metadata"] == {"team": "lab"}
    assert auth_state["admin"] is False


def test_lookup_user_falls_back_to_case_insensitive_match() -> None:
    user = SimpleNamespace(user_key="alice")

    class FakeQuery:
        def __init__(self, result):
            self._result = result

        def first(self):
            return self._result

    class FakeSession:
        def __init__(self):
            self.calls = []

        def scalars(self, _statement):
            call_index = len(self.calls)
            self.calls.append(_statement)
            if call_index == 0:
                return FakeQuery(None)
            return FakeQuery(user)

    session = FakeSession()

    result = lookup_user(session, username="ALICE", case_insensitive_usernames=True)

    assert result is user
    assert len(session.calls) == 2
