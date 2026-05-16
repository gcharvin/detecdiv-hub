from datetime import datetime, timedelta, timezone
from uuid import UUID

from api.models import ExternalExperimentRecord, ExternalUserCredential, RawDataset, User
from api.services.external_credentials import credential_status, decrypt_external_token, encrypt_external_token
from api.services.external_eln import select_unique_user_match
from api.services.external_eln_clients import (
    LabguruClient,
    extract_list_payload,
    extract_labguru_text_sections,
    html_to_text,
    labguru_experiment_from_payload,
    labguru_observed_users_from_payload,
    normalize_external_url,
    normalize_system_key,
    sanitize_http_error_message,
)
from api.services.external_eln_matching import (
    extract_date_key,
    raw_dataset_match_sort_key,
    score_external_record_for_raw_dataset,
)


def test_labguru_experiment_payload_builds_stable_record() -> None:
    payload = {
        "id": 277,
        "title": "280224_dhytnl_12rdnars",
        "updated_at": "2024-03-07 16:44",
        "owner": {"id": 29, "name": "Aleksandr Maliavko"},
    }

    record = labguru_experiment_from_payload(payload, base_url="https://cle.inserm.fr/")

    assert record.external_id == "277"
    assert record.title == "280224_dhytnl_12rdnars"
    assert record.external_url == "https://cle.inserm.fr/knowledge/experiments/277"
    assert record.owner_name == "Aleksandr Maliavko"
    assert record.payload_json == payload


def test_labguru_experiment_payload_absolutizes_relative_url() -> None:
    record = labguru_experiment_from_payload(
        {"id": 697, "title": "Example", "url": "/knowledge/experiments/697"},
        base_url="https://cle.inserm.fr",
    )

    assert record.external_url == "https://cle.inserm.fr/knowledge/experiments/697"


def test_normalize_external_url_keeps_absolute_url() -> None:
    assert (
        normalize_external_url("https://cle.inserm.fr/knowledge/experiments/697", base_url="https://cle.inserm.fr")
        == "https://cle.inserm.fr/knowledge/experiments/697"
    )


def test_labguru_text_sections_extract_text_element_html() -> None:
    payload = {
        "experiment_procedures": [
            {
                "experiment_procedure": {
                    "name": "Description",
                    "section_type": "description",
                    "elements": [
                        {"element_type": "text", "data": "<p>strain A<br>condition B</p>"},
                    ],
                }
            },
            {
                "experiment_procedure": {
                    "name": "Procedure",
                    "section_type": "procedure",
                    "elements": [
                        {"element_type": "text", "data": "<p>37C incubation</p>"},
                    ],
                }
            },
        ]
    }

    assert extract_labguru_text_sections(payload) == {
        "description": "strain A\ncondition B",
        "procedure": "37C incubation",
    }


def test_html_to_text_removes_basic_markup() -> None:
    assert html_to_text("<p>A&nbsp;B</p><p>C</p>") == "A B\nC"


def test_labguru_observed_users_extracts_member_payloads() -> None:
    users = labguru_observed_users_from_payload(
        {
            "owner": {"id": 29, "name": "Aleksandr Maliavko", "email": "alek@example.org"},
            "created_by": "Aleksandr Maliavko",
        }
    )

    by_id = {user.external_id: user for user in users}
    assert by_id["29"].display_name == "Aleksandr Maliavko"
    assert by_id["29"].email == "alek@example.org"
    assert by_id["name:aleksandr maliavko"].display_name == "Aleksandr Maliavko"


def test_external_credential_token_round_trips_encrypted() -> None:
    encrypted = encrypt_external_token("secret-token")

    assert encrypted != "secret-token"
    assert decrypt_external_token(encrypted) == "secret-token"


def test_external_credential_status_reports_expired() -> None:
    credential = ExternalUserCredential(
        system_key="labguru",
        user_id=UUID("00000000-0000-0000-0000-000000000000"),
        encrypted_token="x",
        status="connected",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    assert credential_status(credential) == "expired"


def test_user_matching_is_name_first_and_detects_ambiguity() -> None:
    first = User(user_key="alek", display_name="Aleksandr Maliavko", role="user", is_active=True)
    second = User(user_key="alek2", display_name="Aleksandr Maliavko", role="user", is_active=True)
    other = User(user_key="florian", display_name="Florian", role="user", is_active=True)

    matched, status = select_unique_user_match([first, other], " Aleksandr   Maliavko ")
    assert matched is first
    assert status == "matched_auto"

    matched, status = select_unique_user_match([first, second, other], "Aleksandr Maliavko")
    assert matched is None
    assert status == "ambiguous"

    matched, status = select_unique_user_match([other], "Aleksandr Maliavko")
    assert matched is None
    assert status == "pending"


def test_external_system_key_guard() -> None:
    assert normalize_system_key("Labguru") == "labguru"
    assert normalize_system_key("elabftw") == "elabftw"


def test_extract_list_payload_accepts_common_labguru_shapes() -> None:
    assert extract_list_payload([{"id": 1}]) == [{"id": 1}]
    assert extract_list_payload({"data": [{"id": 2}]}) == [{"id": 2}]
    assert extract_list_payload({"experiments": [{"id": 3}]}) == [{"id": 3}]


def test_labguru_client_creates_experiment_with_widget_fields(monkeypatch) -> None:
    calls = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "experiment": {
                    "id": 731,
                    "title": "2026_05_14_test",
                    "url": "/knowledge/experiments/731",
                }
            }

    def fake_post(url, *, json, params, timeout):
        calls.append({"url": url, "json": json, "params": params, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("api.services.external_eln_clients.requests.post", fake_post)
    client = LabguruClient(base_url="https://labguru.example.org", token="token-123")

    experiment = client.create_experiment(
        title="2026_05_14_test",
        description="Acquisition confirmed",
        procedure="1. Start MDA\n2. Confirm transfer",
    )

    assert experiment.external_id == "731"
    assert experiment.title == "2026_05_14_test"
    assert experiment.external_url == "https://labguru.example.org/knowledge/experiments/731"
    assert experiment.payload_json["detecdiv_widget_request"]["description"] == "Acquisition confirmed"
    assert calls == [
        {
            "url": "https://labguru.example.org/api/v2/experiments",
            "json": {
                "experiment": {
                    "title": "2026_05_14_test",
                    "name": "2026_05_14_test",
                    "description": "Acquisition confirmed",
                    "experiment_procedures_attributes": [
                        {
                            "name": "DetecDiv acquisition procedure",
                            "description": "1. Start MDA\n2. Confirm transfer",
                            "elements_attributes": [
                                {
                                    "element_type": "text",
                                    "data": "1. Start MDA<br>2. Confirm transfer",
                                }
                            ],
                        }
                    ],
                }
            },
            "params": {"token": "token-123"},
            "timeout": 30.0,
        }
    ]


def test_labguru_client_falls_back_to_legacy_item_experiment_payload(monkeypatch) -> None:
    calls = []

    class FakeResponse:
        def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
            self.status_code = status_code
            self.reason = "Bad Request" if status_code >= 400 else "OK"
            self._payload = payload or {}
            self.text = text
            self.url = "https://labguru.example.org/api/v1/experiments.json?token=secret-token"

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                import requests

                raise requests.HTTPError("bad request", response=self)

        def json(self) -> dict:
            return self._payload

    def fake_post(url, *, json, params, timeout):
        calls.append({"method": "POST", "url": url, "json": json, "params": params, "timeout": timeout})
        if url.endswith("/api/v2/experiments"):
            return FakeResponse(400, text="unsupported payload")
        if url.endswith("/api/v1/sections.json"):
            return FakeResponse(
                200,
                {
                    "id": 331,
                    "name": json["item"]["name"],
                    "container_id": json["item"]["container_id"],
                    "container_type": json["item"]["container_type"],
                },
            )
        if url.endswith("/api/v1/elements.json"):
            return FakeResponse(
                200,
                {
                    "id": 881,
                    "element_type": json["item"]["element_type"],
                    "data": json["item"]["data"],
                },
            )
        return FakeResponse(
            200,
            {
                "id": 732,
                "title": json["item"]["title"],
                "url": "/knowledge/experiments/732",
                "experiment_procedures": [
                    {
                        "experiment_procedure": {
                            "name": "Description",
                            "elements": [{"id": 771, "element_type": "text"}],
                        }
                    },
                    {
                        "experiment_procedure": {
                            "name": "Procedure",
                            "elements": [{"id": 772, "element_type": "text"}],
                        }
                    },
                    {
                        "experiment_procedure": {
                            "name": "Results",
                            "elements": [{"id": 773, "element_type": "text"}],
                        }
                    },
                ],
            },
        )

    def fake_put(url, *, json, params, timeout):
        calls.append({"method": "PUT", "url": url, "json": json, "params": params, "timeout": timeout})
        return FakeResponse(
            200,
            {
                "id": int(url.rsplit("/", 1)[-1].split(".", 1)[0]),
                "element_type": "text",
                "name": json["element"]["name"],
                "data": json["element"]["data"],
            },
        )

    monkeypatch.setattr("api.services.external_eln_clients.requests.post", fake_post)
    monkeypatch.setattr("api.services.external_eln_clients.requests.put", fake_put)
    client = LabguruClient(base_url="https://labguru.example.org", token="secret-token")

    experiment = client.create_experiment(
        title="2026_05_16_test",
        description="Acquisition confirmed",
        procedure="Run MDA",
        conditions="YPD",
        project_id="47",
        folder_id="235",
    )

    assert experiment.external_id == "732"
    assert calls[1]["url"] == "https://labguru.example.org/api/v1/experiments.json"
    assert calls[1]["params"] == {}
    assert calls[1]["json"]["token"] == "secret-token"
    item = calls[1]["json"]["item"]
    assert item["title"] == "2026_05_16_test"
    assert item["project_id"] == "47"
    assert item["milestone_id"] == "235"
    assert "name" not in item
    assert "folder_id" not in item
    assert "Acquisition confirmed" in item["description"]
    assert "Run MDA" in item["description"]
    assert "YPD" in item["description"]
    put_calls = [call for call in calls if call["method"] == "PUT"]
    assert [call["json"]["element"]["name"] for call in put_calls] == ["Description", "Procedure"]
    assert "Acquisition confirmed" in put_calls[0]["json"]["element"]["data"]
    assert "Run MDA" in put_calls[1]["json"]["element"]["data"]
    conditions_section = next(call for call in calls if call["url"].endswith("/api/v1/sections.json"))
    assert conditions_section["json"]["item"]["name"] == "Conditions"
    conditions_element = next(call for call in calls if call["url"].endswith("/api/v1/elements.json"))
    assert "YPD" in conditions_element["json"]["item"]["data"]
    assert experiment.payload_json["experiment_procedures"][-1]["experiment_procedure"]["name"] == "Conditions"


def test_labguru_http_error_sanitizer_redacts_token() -> None:
    import requests

    response = requests.Response()
    response.status_code = 400
    response.reason = "Bad Request"
    response.url = "https://cle.inserm.fr/api/v1/experiments.json?token=secret-token"
    response._content = b"token=secret-token invalid payload"
    exc = requests.HTTPError("bad", response=response)

    message = sanitize_http_error_message(exc)

    assert "secret-token" not in message
    assert "<redacted>" in message


def test_labguru_client_lists_project_milestones_as_folders(monkeypatch) -> None:
    calls = []

    class FakeResponse:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._payload

    def fake_get(url, *, json, params, timeout):
        calls.append({"url": url, "json": json, "params": params, "timeout": timeout})
        return FakeResponse(
            {
                "id": 3,
                "title": "C.albicans preliminary work",
                "milestones": [
                    {"id": 41, "title": "pilot movies"},
                    {"id": 42, "name": "segmentation checks"},
                ],
            }
        )

    monkeypatch.setattr("api.services.external_eln_clients.requests.get", fake_get)
    client = LabguruClient(base_url="https://labguru.example.org", token="token-123")

    folders = client.list_folders(project_id="3")

    assert [(folder.external_id, folder.name) for folder in folders] == [
        ("41", "pilot movies"),
        ("42", "segmentation checks"),
    ]
    assert calls[0]["url"] == "https://labguru.example.org/api/v1/projects/3.json"
    assert calls[0]["params"]["meta"] is True


def test_external_matching_scores_exact_dataset_label() -> None:
    raw = RawDataset(acquisition_label="2026_03_17_BUD4-NG-3xmAID_YAL18", visibility="private")
    raw.locations = []
    external = ExternalExperimentRecord(
        system_key="labguru",
        external_id="699",
        title="2026_03_17_BUD4-NG-3xmAID_YAL18",
        started_at=datetime(2026, 3, 17, tzinfo=timezone.utc),
    )

    scored = score_external_record_for_raw_dataset(raw, external)

    assert scored.score == 1.0
    assert scored.evidence["label_signal"] == "exact_label_title"
    assert scored.evidence["date_match"] is True


def test_external_matching_scores_partial_date_candidate_below_exact() -> None:
    raw = RawDataset(acquisition_label="2026_03_19_YAK109_Ferm53Spiral", visibility="private")
    raw.locations = []
    external = ExternalExperimentRecord(
        system_key="labguru",
        external_id="729",
        title="2026_03_19",
        started_at=datetime(2026, 3, 19, tzinfo=timezone.utc),
    )

    scored = score_external_record_for_raw_dataset(raw, external)

    assert 0.45 <= scored.score < 1.0
    assert scored.evidence["label_signal"] == "label_title_contains"
    assert scored.evidence["date_match"] is True


def test_external_matching_extracts_labguru_legacy_date_shapes() -> None:
    assert extract_date_key("2026_04_28_Scerev") == "2026-04-28"
    assert extract_date_key("200702_Gln1_uNS_5") == "2020-07-02"
    assert extract_date_key("01092023_m_array_1") == "2023-09-01"
    assert extract_date_key("280224_dhytnl_12rdnars") == "2024-02-28"
    assert extract_date_key("2025_30_10_ClassifDivDaughters_training") is None


def test_external_matching_sorts_raw_datasets_by_label_date_before_index_time() -> None:
    older_biology_recent_index = RawDataset(
        acquisition_label="200702_Gln1_uNS_5",
        visibility="private",
        started_at=datetime(2026, 5, 10, tzinfo=timezone.utc),
        created_at=datetime(2026, 5, 10, tzinfo=timezone.utc),
    )
    newer_biology_older_index = RawDataset(
        acquisition_label="2026_03_17_BUD4-NG-3xmAID_YAL18",
        visibility="private",
        started_at=datetime(2026, 3, 17, tzinfo=timezone.utc),
        created_at=datetime(2026, 3, 17, tzinfo=timezone.utc),
    )

    sorted_raw = sorted(
        [older_biology_recent_index, newer_biology_older_index],
        key=raw_dataset_match_sort_key,
        reverse=True,
    )

    assert sorted_raw[0] is newer_biology_older_index
