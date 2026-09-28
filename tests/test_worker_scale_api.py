from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from api.routes_execution_targets import scale_execution_target_workers
from api.schemas import ExecutionTargetWorkerScaleRequest


def call(metadata, count=6):
    target = SimpleNamespace(id=uuid4(), display_name="Compute host", metadata_json=metadata)
    def scalars(statement):
        assert statement._for_update_arg is not None
        return SimpleNamespace(first=lambda: target)
    db = SimpleNamespace(scalars=scalars, commit=lambda: None, refresh=lambda x: None)
    response = scale_execution_target_workers(target.id, ExecutionTargetWorkerScaleRequest(worker_instances=count), db, SimpleNamespace(role="admin"))
    return target, response


def test_scale_records_request_for_compute_manager_and_drains_new_jobs():
    target, response = call({"worker_manager_enabled": True, "worker_manager_seen_at": datetime.now(timezone.utc).isoformat(), "worker_cpu_capacity": 36})
    assert target.metadata_json["worker_instances_desired"] == 6
    assert target.metadata_json["drain_new_jobs"]
    assert target.metadata_json["worker_scale_state"] == "requested"
    assert "after active jobs finish" in response.message


def test_offline_manager_does_not_start_workers_in_api_vm():
    with pytest.raises(HTTPException) as exc:
        call({})
    assert exc.value.status_code == 503


def test_worker_count_cannot_exceed_host_cpu_budget():
    with pytest.raises(HTTPException) as exc:
        call({"worker_manager_enabled": True, "worker_manager_seen_at": datetime.now(timezone.utc).isoformat(), "worker_cpu_capacity": 4}, count=6)
    assert exc.value.status_code == 422
