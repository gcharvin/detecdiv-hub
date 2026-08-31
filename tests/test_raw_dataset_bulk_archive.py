from uuid import uuid4

from api.models import RawDataset, User
from api.schemas import RawDatasetArchiveBulkRequest
from api.services.raw_dataset_lifecycle import RawDatasetLifecycleConflictError
import api.routes_raw_datasets as routes


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def unique(self):
        return self

    def __iter__(self):
        return iter(self.value)

    def first(self):
        return self.value


class _Session:
    def __init__(self, values):
        self.values = iter(values)
        self.commit_calls = 0

    def scalars(self, _statement):
        return _ScalarResult(next(self.values))

    def commit(self):
        self.commit_calls += 1


def test_bulk_archive_reports_skip_reasons(monkeypatch) -> None:
    raw_dataset_id = uuid4()
    missing_dataset_id = uuid4()
    raw_dataset = RawDataset(
        id=raw_dataset_id,
        acquisition_label="nested dataset",
        lifecycle_tier="hot",
        archive_status="none",
    )
    current_user = User(id=uuid4(), user_key="admin", display_name="Admin", role="admin", is_active=True)
    session = _Session([[raw_dataset]])

    monkeypatch.setattr(routes, "ensure_raw_dataset_readable", lambda value, _user: value)
    monkeypatch.setattr(routes, "user_can_edit_raw_dataset", lambda _raw, _user: True)
    monkeypatch.setattr(routes, "load_legacy_archive_bundle_inventory", lambda _db: object())

    def reject_archive(*_args, **_kwargs):
        raise RawDatasetLifecycleConflictError("Archive bundle preflight failed: nested raw dataset")

    monkeypatch.setattr(routes, "transition_raw_dataset_to_archive", reject_archive)

    result = routes.bulk_archive_raw_datasets(
        payload=RawDatasetArchiveBulkRequest(raw_dataset_ids=[raw_dataset_id, missing_dataset_id]),
        db=session,
        current_user=current_user,
    )

    assert result.requested_count == 2
    assert result.queued_count == 0
    assert result.skipped_count == 2
    assert result.skipped_raw_dataset_ids == [raw_dataset_id, missing_dataset_id]
    assert [detail.reason_code for detail in result.skipped_details] == ["lifecycle_conflict", "not_found"]
    assert result.skipped_details[0].acquisition_label == "nested dataset"
    assert "nested raw dataset" in result.skipped_details[0].reason
    assert session.commit_calls == 1


def test_bulk_archive_skips_dataset_that_is_already_cold(monkeypatch) -> None:
    raw_dataset_id = uuid4()
    raw_dataset = RawDataset(
        id=raw_dataset_id,
        acquisition_label="already archived",
        lifecycle_tier="cold",
        archive_status="archived",
    )
    current_user = User(id=uuid4(), user_key="admin", display_name="Admin", role="admin", is_active=True)
    session = _Session([[raw_dataset]])

    monkeypatch.setattr(routes, "ensure_raw_dataset_readable", lambda value, _user: value)
    monkeypatch.setattr(routes, "user_can_edit_raw_dataset", lambda _raw, _user: True)
    monkeypatch.setattr(routes, "load_legacy_archive_bundle_inventory", lambda _db: object())

    def unexpected_archive(*_args, **_kwargs):
        raise AssertionError("cold dataset must not be queued again")

    monkeypatch.setattr(routes, "transition_raw_dataset_to_archive", unexpected_archive)

    result = routes.bulk_archive_raw_datasets(
        payload=RawDatasetArchiveBulkRequest(raw_dataset_ids=[raw_dataset_id]),
        db=session,
        current_user=current_user,
    )

    assert result.queued_count == 0
    assert result.skipped_count == 1
    assert result.skipped_details[0].reason_code == "not_archive_eligible"
    assert session.commit_calls == 1
