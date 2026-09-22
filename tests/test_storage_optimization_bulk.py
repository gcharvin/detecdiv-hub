from uuid import uuid4

from api.models import Job, RawDataset, StorageOptimizationRun, User
from api.schemas import StorageOptimizationBulkRequest
import api.routes_raw_datasets as routes


class _ScalarResult:
    def __init__(self, values):
        self.values = values

    def __iter__(self):
        return iter(self.values)


class _Session:
    def __init__(self, values):
        self.values = values
        self.added = []
        self.flush_calls = 0
        self.commit_calls = 0

    def scalars(self, _statement):
        return _ScalarResult(self.values)

    def add(self, value):
        self.added.append(value)

    def flush(self):
        self.flush_calls += 1
        for value in self.added:
            if isinstance(value, StorageOptimizationRun) and value.id is None:
                value.id = uuid4()

    def commit(self):
        self.commit_calls += 1


def test_bulk_storage_optimization_queues_eligible_datasets_and_reports_skips(monkeypatch):
    eligible_id = uuid4()
    incomplete_id = uuid4()
    running_id = uuid4()
    missing_id = uuid4()
    eligible = RawDataset(
        id=eligible_id,
        acquisition_label="eligible",
        completeness_status="complete",
        storage_optimization_status="partial",
    )
    incomplete = RawDataset(
        id=incomplete_id,
        acquisition_label="incomplete",
        completeness_status="acquiring",
        storage_optimization_status="none",
    )
    running = RawDataset(
        id=running_id,
        acquisition_label="already running",
        completeness_status="complete",
        storage_optimization_status="running",
    )
    user = User(id=uuid4(), user_key="admin", display_name="Admin", role="admin", is_active=True)
    session = _Session([eligible, incomplete, running])
    monkeypatch.setattr(routes, "ensure_raw_dataset_readable", lambda value, _user: value)

    result = routes.queue_bulk_raw_dataset_storage_optimization(
        payload=StorageOptimizationBulkRequest(
            raw_dataset_ids=[eligible_id, incomplete_id, running_id, missing_id, eligible_id]
        ),
        db=session,
        current_user=user,
    )

    assert result.requested_count == 4
    assert result.queued_count == 1
    assert result.skipped_count == 3
    assert result.queued_raw_dataset_ids == [eligible_id]
    assert result.skipped_raw_dataset_ids == [incomplete_id, running_id, missing_id]
    assert [item.reason_code for item in result.skipped_details] == [
        "incomplete",
        "already_optimized_or_active",
        "not_found",
    ]
    runs = [item for item in session.added if isinstance(item, StorageOptimizationRun)]
    jobs = [item for item in session.added if isinstance(item, Job)]
    assert len(runs) == 1
    assert runs[0].raw_dataset_id == eligible_id
    assert runs[0].codec == "deflate"
    assert len(jobs) == 1
    assert jobs[0].raw_dataset_id == eligible_id
    assert jobs[0].params_json["job_kind"] == "storage_optimization_scan"
    assert jobs[0].params_json["storage_optimization_run_id"] == str(runs[0].id)
    assert session.commit_calls == 1
