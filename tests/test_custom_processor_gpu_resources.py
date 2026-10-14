import json
from types import SimpleNamespace

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
