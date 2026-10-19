import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from api.services.job_priority_settings import (
    JobPriorityRuntimeConfig,
    JobResourceRuntimeConfig,
    default_job_resource_profiles,
)
from worker import autoscaling, dynamic_resources, job_resources, run_worker
from worker.memory_resources import memory_policy, worker_allocation_fits
from worker.roi_extraction_resources import inspect_roi_extraction, minimum_allocation


@pytest.fixture
def setup(monkeypatch, tmp_path):
    for key in ("WORKER_MEMORY_LIMIT_MB", "HOST_MEMORY_RESERVE_MB", "HOST_MEMORY_HEADROOM_MB",
                "WORKER_CPU_LIMIT"):
        monkeypatch.delenv(f"DETECDIV_HUB_{key}", raising=False)
    monkeypatch.setenv("DETECDIV_HUB_WORKER_DYNAMIC_RESOURCES", "1")
    monkeypatch.setenv("DETECDIV_HUB_WORKER_MEMORY_BUDGET_MB", "98304")
    monkeypatch.setenv("DETECDIV_HUB_WORKER_SWAP_BUDGET_MB", "6144")
    monkeypatch.setenv("DETECDIV_HUB_WORKER_CPU_BUDGET", "36")
    monkeypatch.setattr("worker.memory_resources.psutil.virtual_memory",
                        lambda: SimpleNamespace(total=128 * 1024**3, available=110 * 1024**3))
    node = {"id": "extract", "type": "roiExtract", "pkg": "roiExtract",
            "func": "roiExtract.process"}
    path = tmp_path / "pipeline.json"
    path.write_text(json.dumps({"nodes": [node]}), encoding="utf-8")
    target = SimpleNamespace(id=uuid4(), supports_gpu=True, metadata_json={})
    config = JobResourceRuntimeConfig(default_job_resource_profiles(), 36, 24576, 8)
    settings = SimpleNamespace(worker_path_mappings="[]", worker_target_key="compute",
                               worker_claim_unassigned_jobs=True, worker_job_kinds="",
                               worker_excluded_job_kinds="")
    monkeypatch.setattr(job_resources, "get_settings", lambda: settings)
    monkeypatch.setattr("worker.pipeline_run_executor.normalize_pipeline_run_payload",
                        lambda session, *, job: job.params_json)
    job = SimpleNamespace(id=uuid4(), execution_target_id=None, status="queued",
                          requested_mode="auto", started_at=None, params_json={
                              "job_kind": "pipeline_run",
                              "pipeline_ref": {"pipeline_json_path": str(path)},
                              "run_request": {},
                          })
    totals = {"cpu_cores": 0, "memory_mb": 0, "memory_capacity_mb": 78 * 1024,
              "disk_io_units": 0, "gpu_jobs": 0, "gpu_vram_mb": 0,
              "gpu_exclusive": False, "allocations": {}}
    return SimpleNamespace(job=job, target=target, config=config, settings=settings,
                           totals=totals, node=node, path=path)


def resolve(s, **kwargs):
    return job_resources.resolve_job_resource_allocation(
        None, job=s.job, target=s.target, config=s.config, totals=s.totals, **kwargs)


@pytest.mark.parametrize("busy_gib,expected_gib,fits", [
    (0, 48, True), (48, 30, True), (54, 24, True), (55, 24, False),
])
def test_auto_memory_uses_available_capacity_or_waits(setup, busy_gib, expected_gib, fits):
    s = setup
    s.totals["memory_mb"] = busy_gib * 1024
    before = json.dumps(s.job.params_json)
    allocation = resolve(s)
    assert allocation["memory_mb"] == expected_gib * 1024
    assert allocation["memory_min_mb"] == 24 * 1024
    assert allocation["memory_preferred_mb"] == 48 * 1024
    assert allocation["swap_limit_mb"] == allocation["memory_mb"] // 16
    assert job_resources.allocation_fits(allocation, totals=s.totals, config=s.config)[0] is fits
    assert not allocation["gpu_required"]
    assert json.dumps(s.job.params_json) == before  # Planning never changes a queued job.


def test_backfill_borrows_only_after_preserving_waiting_job_minimum(setup):
    s = setup
    s.totals["memory_mb"] = 24 * 1024
    waiting = minimum_allocation(resolve(s))
    assert waiting["memory_mb"] == 24 * 1024
    allocation = resolve(s, reserved_allocation=waiting)
    assert allocation["memory_mb"] == 30 * 1024
    assert job_resources.allocation_fits_with_reservation(
        allocation, totals=s.totals, reserved_allocation=waiting, config=s.config)


def test_actual_host_pressure_limits_surplus_and_can_prevent_start(setup, monkeypatch):
    s = setup
    monkeypatch.setenv("DETECDIV_HUB_HOST_MEMORY_HEADROOM_MB", "8192")
    monkeypatch.setattr("worker.memory_resources.psutil.virtual_memory",
                        lambda: SimpleNamespace(total=128 * 1024**3, available=38 * 1024**3))
    assert resolve(s)["memory_mb"] == 30 * 1024
    monkeypatch.setattr("worker.memory_resources.psutil.virtual_memory",
                        lambda: SimpleNamespace(total=128 * 1024**3, available=31 * 1024**3))
    allocation = resolve(s)
    assert worker_allocation_fits(allocation, policy=memory_policy(s.target)) == (
        False, "host_memory_pressure")


def test_profile_can_raise_safe_floor_but_is_never_silently_reduced(setup):
    s = setup
    for minimum in (32, 64):
        s.config.profiles["pipeline_run"] = replace(
            s.config.profiles["pipeline_run"], memory_mb=minimum * 1024)
        s.totals["memory_mb"] = 48 * 1024
        allocation = resolve(s)
        assert allocation["memory_mb"] == minimum * 1024
        assert not job_resources.allocation_fits(allocation, totals=s.totals, config=s.config)[0]


def test_explicit_request_and_active_legacy_allocation_remain_fixed(setup):
    s = setup
    legacy = job_resources.resolve_job_resource_allocation(
        None, job=s.job, target=s.target, config=s.config)
    assert legacy["memory_mb"] == 24576
    assert "memory_allocation_mode" not in legacy
    s.job.params_json["resources"] = {"memory_mb": 65536}
    s.totals["memory_mb"] = 48 * 1024
    allocation = resolve(s)
    assert allocation["memory_mb"] == 65536
    assert "memory_allocation_mode" not in allocation


def test_non_dynamic_worker_retains_its_fixed_limits(setup, monkeypatch):
    monkeypatch.setenv("DETECDIV_HUB_WORKER_DYNAMIC_RESOURCES", "0")
    monkeypatch.setenv("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB", "16384")
    allocation = resolve(setup)
    assert allocation["memory_mb"] == 16384
    assert "memory_allocation_mode" not in allocation


@pytest.mark.parametrize("extra", [
    {"id": "classifier", "type": "classifier", "pkg": "cellposeSAM"},
    {"id": "legacy", "type": "processor", "pkg": "externalLegacyProcessor"},
])
def test_mixed_pipeline_is_fixed_but_extraction_only_selection_is_automatic(setup, extra):
    s = setup
    s.path.write_text(json.dumps({"nodes": [s.node, extra]}), encoding="utf-8")
    assert "memory_allocation_mode" not in resolve(s)
    s.job.params_json["run_request"]["selected_nodes"] = ["extract"]
    assert resolve(s)["memory_mb"] == 49152


@pytest.mark.parametrize("change", ["missing", "invalid", "unknown_selection", "memory_cache",
                                    "ingest", "custom_func", "disabled"])
def test_uncertain_or_non_streaming_runs_do_not_opt_in(setup, change):
    s = setup
    if change == "missing":
        s.path.unlink()
    elif change == "invalid":
        s.path.write_text("not JSON", encoding="utf-8")
    elif change == "unknown_selection":
        s.job.params_json["run_request"]["selected_nodes"] = ["other"]
    elif change == "memory_cache":
        s.job.params_json["run_request"]["roi_cache_policy"] = "memory"
    elif change == "ingest":
        s.job.params_json["run_request"]["ingest_raw_dataset"] = True
    else:
        if change == "custom_func":
            s.node["func"] = "custom.extract"
        else:
            s.node["enabled"] = False
        s.path.write_text(json.dumps({"nodes": [s.node]}), encoding="utf-8")
    assert "memory_allocation_mode" not in resolve(s)


def test_disabled_classifier_does_not_disable_extraction_contract(setup):
    s = setup
    profile = inspect_roi_extraction({"nodes": [s.node, {
        "id": "classifier", "type": "classifier", "enabled": False,
    }]}, s.job.params_json)
    assert profile["eligible"]


class Session:
    def __init__(self, s, waiting, workers=()):
        self.s = s
        self.waiting = waiting
        self.workers = workers
        self.detached = []

    def scalars(self, stmt):
        table = stmt.column_descriptions[0]["entity"].__tablename__
        if table == "execution_targets":
            assert stmt._for_update_arg is not None
            return SimpleNamespace(one=lambda: self.s.target)
        if table == "worker_instances":
            return self.workers
        assert table == "jobs"
        return self.waiting

    def flush(self):
        pass

    def expunge(self, job):
        self.detached.append(job)


def wire_scheduler(monkeypatch, s, session):
    @contextmanager
    def scope():
        yield session
    monkeypatch.setattr(run_worker, "session_scope", scope)
    monkeypatch.setattr(run_worker, "get_settings", lambda: s.settings)
    monkeypatch.setattr(run_worker, "resolve_worker_target", lambda *a: s.target)
    monkeypatch.setattr(run_worker, "update_worker_target_state", lambda *a, **kw: None)
    # The native Windows deployment also checks host-specific code/path
    # compatibility. These fixtures exercise memory admission after that gate.
    monkeypatch.setattr(run_worker, "can_claim_automatic_pipeline",
                        lambda *a, **kw: (True, "compatible"), raising=False)
    for module in (run_worker, autoscaling):
        monkeypatch.setattr(module, "resolve_job_priority_runtime_config",
                            lambda *a: JobPriorityRuntimeConfig({"generic": 100, "pipeline_run": 10}))
        monkeypatch.setattr(module, "resolve_job_resource_runtime_config", lambda *a: s.config)
        monkeypatch.setattr(module, "active_resource_totals",
                            lambda *a, **kw: {**s.totals, "allocations": dict(s.totals["allocations"])})


@pytest.mark.parametrize("busy_gib,expected_gib", [(0, 48), (48, 30), (55, None)])
def test_real_claim_and_autoscaler_agree_on_elastic_admission(setup, monkeypatch, busy_gib, expected_gib):
    s = setup
    s.totals["memory_mb"] = busy_gib * 1024
    session = Session(s, [s.job])
    wire_scheduler(monkeypatch, s, session)
    plan = autoscaling.plan_workers(session, target=s.target, settings=s.settings, baseline=6)
    assert plan["admissible_queued_jobs"] == int(expected_gib is not None)
    claimed = run_worker.claim_next_job()
    if expected_gib is None:
        assert claimed is None
        assert s.job.status == "queued"
        assert "_hub_resource_allocation" not in s.job.params_json
        assert plan["blocked"] == {"memory_capacity": 1}
    else:
        assert claimed is s.job
        assert s.job.params_json["_hub_resource_allocation"]["memory_mb"] == expected_gib * 1024
        assert s.job.status == "running"
        assert session.detached == [s.job]


def test_second_claim_obeys_first_saved_allocation_and_stops_below_minimum(setup, monkeypatch):
    s = setup
    session = Session(s, [s.job])
    wire_scheduler(monkeypatch, s, session)
    first = run_worker.claim_next_job()
    saved = dict(first.params_json["_hub_resource_allocation"])
    s.totals.update(memory_mb=saved["memory_mb"], cpu_cores=8, disk_io_units=3,
                    allocations={str(first.id): saved})
    second = SimpleNamespace(**{**vars(s.job), "id": uuid4(), "status": "queued",
                                "params_json": {k: v for k, v in s.job.params_json.items()
                                                if k != "_hub_resource_allocation"}})
    session.waiting = [second]
    claimed = run_worker.claim_next_job()
    assert claimed.params_json["_hub_resource_allocation"]["memory_mb"] == 30 * 1024
    assert first.params_json["_hub_resource_allocation"] == saved


def test_autoscaler_accounts_for_each_planned_surplus(setup, monkeypatch):
    s = setup
    other = SimpleNamespace(**{**vars(s.job), "id": uuid4()})
    session = Session(s, [s.job, other])
    wire_scheduler(monkeypatch, s, session)
    plan = autoscaling.plan_workers(session, target=s.target, settings=s.settings, baseline=1)
    assert plan["admissible_queued_jobs"] == 2
    assert plan["desired"] == 2
    assert "_hub_resource_allocation" not in s.job.params_json


def test_blocked_extraction_reserves_only_its_floor_in_real_claim_loop(setup, monkeypatch):
    s = setup
    s.totals.update(cpu_cores=30, memory_mb=20 * 1024)
    lower = SimpleNamespace(id=uuid4(), execution_target_id=None,
                            params_json={"job_kind": "generic"}, status="queued")
    session = Session(s, [s.job, lower])
    wire_scheduler(monkeypatch, s, session)
    reservations = []
    original = job_resources.allocation_fits_with_reservation

    def record(allocation, **kwargs):
        reservations.append(kwargs["reserved_allocation"])
        return original(allocation, **kwargs)

    monkeypatch.setattr(run_worker, "allocation_fits_with_reservation", record)
    assert run_worker.claim_next_job() is None
    assert reservations[0]["memory_mb"] == 24 * 1024
    assert s.job.status == lower.status == "queued"


def test_chosen_memory_reaches_service_limits_and_child_environment(setup, monkeypatch):
    s = setup
    s.totals["memory_mb"] = 48 * 1024
    s.job.params_json["_hub_resource_allocation"] = resolve(s)
    monkeypatch.setenv("DETECDIV_HUB_WORKER_CONFIGURE_SCRIPT", "/allowed/configure-workers.sh")
    # apply_job_limits updates these in os.environ; register their restoration.
    monkeypatch.setenv("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB", "512")
    monkeypatch.setenv("DETECDIV_HUB_WORKER_CPU_LIMIT", "1")
    monkeypatch.setenv("DETECDIV_HUB_WORKER_SWAP_LIMIT_MB", "0")
    monkeypatch.setattr(dynamic_resources, "_applied", None)
    calls = []
    monkeypatch.setattr(dynamic_resources.subprocess, "run", lambda command, **kw: calls.append(command))
    dynamic_resources.apply_job_limits("@2", s.job)
    command = calls[0]
    assert command[command.index("--job-memory-mb") + 1] == "30720"
    assert command[command.index("--job-swap-mb") + 1] == "1920"
    from worker.matlab_code_checkout import job_environment
    monkeypatch.setattr("worker.matlab_code_checkout._short_matlab_temp_path",
                        lambda work_dir: s.path.parent / "matlab-alias" / "job")
    environment = job_environment(s.path.parent)
    assert environment["DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB"] == "30720"
    dynamic_resources.apply_job_limits("@2", None)
    assert calls[-1][calls[-1].index("--job-memory-mb") + 1] == "512"
