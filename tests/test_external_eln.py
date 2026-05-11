from datetime import datetime, timezone

from api.models import ExternalExperimentRecord, RawDataset, User
from api.services.external_eln import select_unique_user_match
from api.services.external_eln_clients import (
    extract_list_payload,
    labguru_experiment_from_payload,
    labguru_observed_users_from_payload,
    normalize_system_key,
)
from api.services.external_eln_matching import (
    extract_date_key,
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


def test_user_matching_is_name_first_and_detects_ambiguity() -> None:
    first = User(user_key="alek", display_name="Aleksandr Maliavko", role="user", is_active=True)
    second = User(user_key="alek2", display_name="Aleksandr Maliavko", role="user", is_active=True)
    other = User(user_key="florian", display_name="Florian", role="user", is_active=True)

    matched, status = select_unique_user_match([first, other], " Aleksandr   Maliavko ")
    assert matched is first
    assert status == "matched"

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
    assert extract_date_key("280224_dhytnl_12rdnars") == "2024-02-28"
