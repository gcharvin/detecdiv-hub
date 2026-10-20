import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from api.services.job_priority_settings import JobResourceRuntimeConfig, default_job_resource_profiles
from worker import job_resources as resources


def test_external_processor_gpu_override_is_reserved(tmp_path, monkeypatch):
    node = {"id": "xtra", "type": "processor", "pkg": "externalLegacyProcessor", "params": {}}
    pipeline = tmp_path / "pipeline.json"
    pipeline.write_text(json.dumps({"nodes": [node]}), encoding="utf-8")
    payload = {
        "pipeline_ref": {"pipeline_json_path": str(pipeline)},
        "run_request": {"selected_nodes": ["xtra"], "gpu": {"mode": "force_gpu"},
                        "node_params": [{"id": "xtra", "params": {"executionEnvironment": "gpu"}}]},
    }
    monkeypatch.setattr("worker.pipeline_run_executor.normalize_pipeline_run_payload",
                        lambda *args, **kwargs: payload)
    monkeypatch.setattr(resources, "get_settings", lambda: SimpleNamespace(worker_path_mappings="[]"))
    profile = resources.inspect_pipeline_gpu_requirements(None, job=object(),
                                                         target=SimpleNamespace(supports_gpu=True))
    assert profile["status"] == "known"
    assert profile["gpu_required"] is True
    assert profile["gpu_node_ids"] == ["xtra"]


def test_undeclared_or_cpu_processor_does_not_reserve_gpu():
    node = {"type": "processor", "pkg": "externalLegacyProcessor"}
    assert not resources.is_deep_learning_node(node, {})
    assert not resources.is_deep_learning_node(node, {"device": "cpu"})


@pytest.fixture
def legacy_run(tmp_path, monkeypatch):
    node = {"id": "extract", "type": "processor", "pkg": "saveCroppedImagesXConservative",
            "func": "saveCroppedImagesXConservative.process", "params": {}}
    pipeline = tmp_path / "pipeline.json"
    payload = {"job_kind": "pipeline_run",
               "pipeline_ref": {"pipeline_json_path": str(pipeline)},
               "run_request": {"selected_nodes": ["extract"], "gpu": {"mode": "gpu"}}}
    monkeypatch.setattr("worker.pipeline_run_executor.normalize_pipeline_run_payload",
                        lambda *args, **kwargs: payload)
    monkeypatch.setattr(resources, "get_settings", lambda: SimpleNamespace(worker_path_mappings="[]"))
    target = SimpleNamespace(supports_gpu=True, metadata_json={})

    def inspect():
        pipeline.write_text(json.dumps({"nodes": [node]}), encoding="utf-8")
        return resources.inspect_pipeline_gpu_requirements(None, job=object(), target=target)

    return SimpleNamespace(node=node, pipeline=pipeline, payload=payload, target=target, inspect=inspect)


@pytest.mark.parametrize("mode", ["gpu", "force_gpu", "module_default", "force_cpu", "disabled"])
def test_legacy_extractor_reserves_unconditional_gpu_without_deep_learning(legacy_run, mode):
    run = legacy_run
    run.payload["run_request"]["gpu"]["mode"] = mode
    profile = run.inspect()
    assert profile["status"] == "known"
    assert profile["gpu_required"] is True
    assert profile["gpu_node_ids"] == ["extract"]
    assert profile["deep_learning_node_ids"] == []
    assert profile["roi_extraction"]["eligible"] is False
    assert resources.is_deep_learning_node(run.node, {}) is False


@pytest.mark.parametrize("excluded", ["not_selected", "disabled"])
def test_inactive_legacy_extractor_does_not_reserve_gpu(legacy_run, excluded):
    run = legacy_run
    if excluded == "not_selected":
        run.payload["run_request"]["selected_nodes"] = ["other"]
    else:
        run.node["enabled"] = False
    profile = run.inspect()
    assert profile["selected_node_ids"] == []
    assert profile["gpu_node_ids"] == []
    assert profile["deep_learning_node_ids"] == []
    assert profile["gpu_required"] is False


@pytest.mark.parametrize("mode", ["gpu", "force_gpu", "module_default"])
def test_other_undeclared_processors_keep_cpu_admission(legacy_run, mode):
    run = legacy_run
    run.node.update(pkg="externalLegacyProcessor", func="externalLegacyProcessor.process")
    run.payload["run_request"]["gpu"]["mode"] = mode
    profile = run.inspect()
    assert profile["gpu_required"] is False
    assert profile["deep_learning_node_ids"] == []


def test_legacy_gpu_mode_alias_applies_to_declared_gpu_modules(legacy_run):
    run = legacy_run
    run.node.update(type="classifier", pkg="cellposeSAM", func="cellposeSAM.process")
    profile = run.inspect()
    assert profile["run_gpu_mode"] == "force_gpu"
    assert profile["deep_learning_node_ids"] == ["extract"]
    assert profile["gpu_required"] is True


@pytest.mark.parametrize("memory_mb", [None, 30720])
def test_legacy_gpu_allocation_uses_profile_vram_and_retains_fixed_memory(legacy_run, monkeypatch, memory_mb):
    run = legacy_run
    run.inspect()
    profiles = default_job_resource_profiles()
    profiles["pipeline_run"] = replace(profiles["pipeline_run"], gpu_vram_mb=8192)
    config = JobResourceRuntimeConfig(profiles, 36, 24576, 8)
    monkeypatch.setattr(resources, "memory_policy", lambda target: {
        "dynamic": True, "capacity_mb": 98304, "worker_limit_mb": 98304,
        "available_mb": 98304, "headroom_mb": 8192,
    })
    if memory_mb is not None:
        run.payload["resources"] = {"memory_mb": memory_mb}
    allocation = resources.resolve_job_resource_allocation(
        None, job=SimpleNamespace(params_json=run.payload), target=run.target, config=config,
        totals={"memory_mb": 0, "memory_capacity_mb": 98304})
    assert allocation["gpu_required"] is True
    assert allocation["gpu_vram_mb"] == 8192
    assert allocation["memory_mb"] == (memory_mb or 24576)
    assert "memory_allocation_mode" not in allocation
