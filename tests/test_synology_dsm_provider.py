from api.services.storage_providers.synology_dsm import (
    SynologyDsmClient,
    SynologyDsmError,
    choose_auth_version,
    choose_max_version,
    parse_user_quota_payload,
    summarize_discovered_capabilities,
)
from api.services.storage_providers.synology_ssh import build_synouser_add_command
from api.schemas import SynologyDsmUserQuotaResponse, StorageProviderSummary


def test_choose_auth_version_caps_at_recommended_version() -> None:
    assert choose_auth_version({"maxVersion": 7}) == 6
    assert choose_auth_version({"maxVersion": 6}) == 6
    assert choose_auth_version({"maxVersion": 3}) == 3
    assert choose_auth_version({}) == 6


def test_choose_max_version_defaults_to_one() -> None:
    assert choose_max_version({"maxVersion": 2}) == 2
    assert choose_max_version({}) == 1
    assert choose_max_version({"maxVersion": "bad"}) == 1


def test_summarize_discovered_capabilities_marks_optional_apis() -> None:
    capabilities = summarize_discovered_capabilities(
        {
            "SYNO.API.Info": {},
            "SYNO.API.Auth": {},
            "SYNO.Core.User": {},
            "SYNO.Core.Quota": {},
        }
    )

    assert capabilities["can_discover"] is True
    assert capabilities["can_login"] is True
    assert capabilities["can_manage_users"] is True
    assert capabilities["can_read_quota"] is True
    assert capabilities["can_set_quota"] is True
    assert capabilities["can_manage_homes"] is False


def test_parse_user_quota_payload_handles_empty_and_known_shapes() -> None:
    assert parse_user_quota_payload({"user_quota": []}) == {
        "entries": [],
        "entry_count": 0,
        "quota_bytes": None,
        "used_bytes": None,
    }

    parsed = parse_user_quota_payload(
        {
            "user_quota": [
                {
                    "volume_path": "/volume1",
                    "quota_byte": "1000",
                    "used_byte": 125,
                }
            ]
        }
    )

    assert parsed["entry_count"] == 1
    assert parsed["quota_bytes"] == 1000
    assert parsed["used_bytes"] == 125


def test_synology_quota_response_can_mark_hub_desired_fallback() -> None:
    response = SynologyDsmUserQuotaResponse(
        provider=StorageProviderSummary(
            id="1802bbf7-b5a9-4b6a-841b-d698a5801046",
            provider_key="synology-main",
            display_name="Synology main NAS",
            provider_kind="synology_dsm",
            mount_root="/homes",
            quota_mode="provider_enforced",
            is_active=True,
            capabilities_json={},
            config_json={},
        ),
        configured=True,
        success=True,
        provider_user_key="test_user",
        quota_bytes=None,
        desired_quota_bytes=107374182400,
        effective_quota_bytes=107374182400,
        provider_reported=False,
        quota_source="hub_desired",
        entry_count=0,
        message="Synology DSM did not return a quota entry.",
    )

    assert response.quota_bytes is None
    assert response.effective_quota_bytes == 107374182400
    assert response.provider_reported is False
    assert response.quota_source == "hub_desired"


def test_synology_get_user_matches_requested_name(monkeypatch) -> None:
    client = SynologyDsmClient()
    monkeypatch.setattr(client, "login", lambda: "sid")
    monkeypatch.setattr(client, "logout", lambda: None)

    def fake_call(**kwargs):
        assert kwargs["api_name"] == "SYNO.Core.User"
        assert kwargs["method"] == "get"
        assert kwargs["params"] == {"name": "test_user"}
        return {"data": {"users": [{"name": "other"}, {"name": "test_user", "uid": 1045}]}}

    monkeypatch.setattr(client, "call_discovered_api", fake_call)

    assert client.get_user("test_user") == {"name": "test_user", "uid": 1045}


def test_synology_get_user_treats_missing_user_code_as_absent(monkeypatch) -> None:
    client = SynologyDsmClient()
    monkeypatch.setattr(client, "login", lambda: "sid")
    monkeypatch.setattr(client, "logout", lambda: None)

    def fake_call(**kwargs):
        raise SynologyDsmError("missing", code=3106, payload={"error": {"code": 3106}})

    monkeypatch.setattr(client, "call_discovered_api", fake_call)

    assert client.get_user("missing_user") is None


def test_synology_create_user_sends_password_without_storing(monkeypatch) -> None:
    client = SynologyDsmClient()
    monkeypatch.setattr(client, "login", lambda: "sid")
    monkeypatch.setattr(client, "logout", lambda: None)
    captured = {}

    def fake_call(**kwargs):
        captured.update(kwargs)
        return {"data": {"created": True}}

    monkeypatch.setattr(client, "call_discovered_api", fake_call)

    result = client.create_user(
        user_name="new_user",
        initial_password="temporary-password",
        display_name="New User",
        email="new@example.test",
        groups=["users"],
    )

    assert result == {"created": True}
    assert captured["api_name"] == "SYNO.Core.User"
    assert captured["method"] == "create"
    assert captured["http_method"] == "post"
    assert captured["params"] == {
        "name": "new_user",
        "password": "temporary-password",
        "expire": "never",
        "cannot_chg_passwd": False,
        "passwd_never_expire": True,
        "notify_by_email": False,
        "send_password": False,
        "description": "New User",
        "email": "new@example.test",
        "groups": '["users"]',
    }


def test_build_synouser_add_command_quotes_user_fields() -> None:
    command = build_synouser_add_command(
        synouser_command="/usr/syno/sbin/synouser",
        user_name="alice.smith",
        initial_password="temporary password",
        display_name="Alice Smith",
        email="alice@example.test",
    )

    assert command == "/usr/syno/sbin/synouser --add alice.smith 'temporary password' 'Alice Smith' 0 alice@example.test 0"
