from types import SimpleNamespace

import pytest

from api.services.job_priority_settings import JobResourceRuntimeConfig, default_job_resource_profiles
from worker.job_resources import allocation_fits, allocation_fits_with_reservation, resolve_job_resource_allocation
from worker.memory_resources import memory_policy, requested_memory_mb, worker_allocation_fits


@pytest.fixture(autouse=True)
def host(monkeypatch):
    for key in ("WORKER_MEMORY_BUDGET_MB", "WORKER_MEMORY_LIMIT_MB", "HOST_MEMORY_RESERVE_MB", "HOST_MEMORY_HEADROOM_MB", "WORKER_CPU_LIMIT"):
        monkeypatch.delenv(f"DETECDIV_HUB_{key}", raising=False)
    monkeypatch.setattr("worker.memory_resources.psutil.virtual_memory", lambda: SimpleNamespace(total=128 * 1024**3, available=110 * 1024**3))


@pytest.fixture
def config():
    return JobResourceRuntimeConfig(default_job_resource_profiles(), 36, 0, 36)


@pytest.mark.parametrize("count", [1, 3, 6, 9, 12])
def test_variable_worker_count_never_increases_total_reserved_ram(monkeypatch, config, count):
    budget = 98304
    slot = budget // count
    monkeypatch.setenv("DETECDIV_HUB_WORKER_MEMORY_BUDGET_MB", str(budget))
    monkeypatch.setenv("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB", str(slot))
    policy = memory_policy()
    job = SimpleNamespace(params_json={"job_kind": "pipeline_run"})
    allocation = {"cpu_cores": 1, "memory_mb": requested_memory_mb(job, policy=policy), "disk_io_units": 0, "gpu_required": False}
    totals = {"cpu_cores": 0, "memory_mb": 0, "memory_capacity_mb": policy["capacity_mb"], "disk_io_units": 0}
    for _ in range(count):
        assert allocation_fits(allocation, totals=totals, config=config) == (True, None)
        totals["cpu_cores"] += 1
        totals["memory_mb"] += slot
    assert allocation_fits(allocation, totals=totals, config=config) == (False, "memory_capacity")


def test_target_cannot_expand_budget_past_physical_host_reserve(monkeypatch):
    monkeypatch.setenv("DETECDIV_HUB_HOST_MEMORY_RESERVE_MB", "32768")
    target = SimpleNamespace(metadata_json={"memory_capacity_mb": 999999})
    assert memory_policy(target)["capacity_mb"] == 98304


def test_large_request_waits_for_larger_worker_and_host_pressure_pauses_claims(monkeypatch):
    monkeypatch.setenv("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB", "16384")
    allocation = {"cpu_cores": 2, "memory_mb": 24576}
    assert worker_allocation_fits(allocation, policy=memory_policy()) == (False, "worker_memory_capacity")
    allocation["memory_mb"] = 8192
    monkeypatch.setenv("DETECDIV_HUB_WORKER_CPU_LIMIT", "1")
    assert worker_allocation_fits(allocation, policy=memory_policy()) == (False, "worker_cpu_capacity")
    monkeypatch.delenv("DETECDIV_HUB_WORKER_CPU_LIMIT")
    policy = {**memory_policy(), "available_mb": 512}
    assert worker_allocation_fits(allocation, policy=policy) == (False, "host_memory_pressure")


def test_priority_backfill_keeps_ram_for_waiting_job(config):
    totals = {"cpu_cores": 1, "memory_mb": 32768, "memory_capacity_mb": 65536, "disk_io_units": 0}
    small = {"cpu_cores": 1, "memory_mb": 8192, "disk_io_units": 0, "gpu_required": False}
    waiting = {**small, "memory_mb": 32768}
    assert allocation_fits(small, totals=totals, config=config)[0]
    assert not allocation_fits_with_reservation(small, reserved_allocation=waiting, totals=totals, config=config)


def test_explicit_memory_request_does_not_silently_shrink_to_worker_limit(monkeypatch):
    monkeypatch.setenv("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB", "16384")
    job = SimpleNamespace(params_json={"job_kind": "pipeline_run", "resources": {"memory_mb": 32768}})
    assert requested_memory_mb(job, policy=memory_policy()) == 32768
    job.params_json["resources"]["memory_mb"] = -1
    with pytest.raises(ValueError, match="positive integer"):
        requested_memory_mb(job, policy=memory_policy())


def test_more_small_cpu_workers_can_still_run_default_pipeline_profile(monkeypatch, config):
    monkeypatch.setenv("DETECDIV_HUB_WORKER_CPU_LIMIT", "3")
    monkeypatch.setenv("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB", "8192")
    monkeypatch.setattr("worker.job_resources.inspect_pipeline_gpu_requirements", lambda *a, **kw: {"status": "known", "gpu_required": False})
    job = SimpleNamespace(params_json={"job_kind": "pipeline_run"})
    allocation = resolve_job_resource_allocation(None, job=job, target=None, config=config)
    assert allocation["requested_cpu_cores"] == 8
    assert allocation["cpu_cores"] == 3
    assert allocation["memory_mb"] == 8192
    assert worker_allocation_fits(allocation, policy=memory_policy())[0]
