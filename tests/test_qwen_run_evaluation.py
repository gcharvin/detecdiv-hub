import json
from types import SimpleNamespace

from scripts.evaluate_qwen_runs import (
    benchmark,
    compact,
    compare_progress,
    evidence,
    normalize_progress,
    redact,
)


def test_completed_frame_scope_is_not_global_job_completion():
    event = {"protocol": "detecdiv.progress.v1", "scope": "frame", "current": 100,
             "total": 100, "localValue": 1, "value": 0.23, "status": "running"}
    result = normalize_progress({"recent_stdout": ["@@DETECDIV_PROGRESS@@ " + json.dumps(event)]})
    assert result["latest_event"]["scope"] == "frame"
    assert result["latest_event"]["value"] == 0.23
    assert "global ETA not established" in result["fraction_semantics"]


def test_poll_and_heartbeat_updates_do_not_prove_progress():
    old = {"captured_at": "2026-10-14T08:00:00Z", "heartbeat_at": "08:00:00",
           "status": "running", "progress": {"sampled_at": "08:00:00", "recent_stdout": ["frame 1"]}}
    new = {**old, "captured_at": "2026-10-14T08:05:00Z", "heartbeat_at": "08:05:00",
           "progress": {**old["progress"], "sampled_at": "08:05:00"}}
    result = compare_progress(old, new)
    assert result["heartbeat_changed"]
    assert not result["output_changed"]
    assert "insufficient evidence" in result["interpretation"]
    new["progress"]["recent_stdout"] = ["frame 2"]
    assert compare_progress(old, new)["output_changed"]


def test_tool_cannot_read_jobs_outside_selected_corpus():
    assert "error" in evidence({"cases": {}}, "get_job_evidence", "other-user-job")


def test_busy_gpu_never_calls_qwen(tmp_path, monkeypatch):
    from scripts import evaluate_qwen_runs as evaluation
    (tmp_path / "corpus.json").write_text('{"cases":{}}', encoding="utf-8")
    monkeypatch.setattr(evaluation, "gpu_guard", lambda _: {
        "available": False, "gpu_jobs": [{"id": "active-job"}]})

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Qwen called despite active GPU reservation")

    monkeypatch.setattr(evaluation, "completion", forbidden)
    args = SimpleNamespace(output=tmp_path, label="int4", model="Qwen/test")
    assert benchmark(args) == 2
    result = json.loads((tmp_path / "results-int4.json").read_text())
    assert result["status"] == "blocked_gpu_busy"
    assert not result["cases"]


def test_resource_check_failure_is_recorded_without_inference(tmp_path, monkeypatch):
    from scripts import evaluate_qwen_runs as evaluation
    (tmp_path / "corpus.json").write_text('{"cases":{}}', encoding="utf-8")

    def unavailable(_args):
        raise RuntimeError("SSH unavailable")

    monkeypatch.setattr(evaluation, "gpu_guard", unavailable)
    args = SimpleNamespace(output=tmp_path, label="int4", model="Qwen/test")
    assert benchmark(args) == 2
    assert json.loads((tmp_path / "results-int4.json").read_text())["status"] == "blocked_resource_check"


def test_credentials_are_removed_from_structured_context():
    result = redact({"params": {"api_key": "private", "note": "Bearer private"},
                     "database": "postgresql://user:private@host/db"})
    assert "private" not in json.dumps(result)


def test_large_arrays_are_explicitly_summarized_instead_of_filling_context():
    result = compact({"frames": list(range(1, 4001))})
    assert result["frames"]["items_count"] == 4000
    assert result["frames"]["last_item"] == 4000
    assert result["frames"]["omitted"]
    assert len(json.dumps(result)) < 200
