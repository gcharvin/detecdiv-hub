import hashlib
import json
import sys
import time
from types import SimpleNamespace

import pytest

from scripts.evaluate_qwen_runs import (
    benchmark,
    compact,
    compact_evidence,
    compare_progress,
    evidence,
    gpu_guard,
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


def test_coexistence_requires_live_owner_lock_and_preserves_reserved_vram(monkeypatch):
    from scripts import evaluate_qwen_runs as evaluation
    active = [{"id": "first", "status": "running", "gpu_vram_mb": 8192},
              {"id": "second", "status": "running", "gpu_vram_mb": 8192}]
    admission = {"owner": "pilot", "status": "locked", "checked_at": time.time()}
    watcher = {"owner": "pilot", "manifest_owner": "pilot", "status": "watching",
               "lease_valid": True, "launcher_owned": True, "checked_at": time.time(),
               "service_mib": 5700, "max_service_mib": 5888}

    def evaluate():
        replies = iter([active, ["MATLAB, 2184 MiB"], admission, watcher])
        monkeypatch.setattr(evaluation, "remote", lambda *_args, **_kwargs: next(replies))
        return gpu_guard(SimpleNamespace(api_host="api", compute_host="compute", coexistence_owner="pilot"))

    assert evaluate()["available"]
    active.append({"id": "new", "status": "queued", "gpu_vram_mb": 8192})
    assert not evaluate()["available"]
    active.pop()
    admission["cancel_requested"] = True
    assert not evaluate()["available"]
    admission["cancel_requested"] = False
    watcher["launcher_owned"] = False
    assert not evaluate()["available"]
    watcher["launcher_owned"] = True
    active[0]["gpu_vram_mb"] = None
    assert not evaluate()["available"]


def test_completed_results_are_preserved_without_gpu_or_model_access(tmp_path, monkeypatch):
    from scripts import evaluate_qwen_runs as evaluation
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text('{"cases":{}}', encoding="utf-8")
    result_path = tmp_path / "results-int4.json"
    original = json.dumps({"status": "complete_needs_review", "model": "Qwen/test",
                           "corpus_sha256": hashlib.sha256(corpus_path.read_bytes()).hexdigest(),
                           "cases": [{"job_id": "already-tested", "answer": "preserved"}]})
    result_path.write_text(original, encoding="utf-8")

    def forbidden(_args):
        raise AssertionError("Completed run must not inspect GPU or overwrite results")

    monkeypatch.setattr(evaluation, "gpu_guard", forbidden)
    assert benchmark(SimpleNamespace(output=tmp_path, label="int4", model="Qwen/test")) == 0
    assert result_path.read_text(encoding="utf-8") == original


def test_reduced_context_preserves_error_signature_and_progress_scope():
    signature = "Index in position 4 exceeds array bounds. Index must not exceed 13."
    capture = {"status": "running", "captured_at": "2026-10-14T10:00:00Z", "error_text": signature,
               "run_request": {"node_params": [{"id": "tracking", "params": {"frames": list(range(4000))}}]},
               "progress": {"latest_event": {"scope": "frame", "current": 47, "total": 100,
                                             "localValue": 0.46, "value": 0.24}}}
    result = compact_evidence({"cases": {"job": {"captures": [capture]}}}, "get_job_evidence", "job")
    assert result["error_text"] == signature
    assert result["progress"]["latest_event"]["scope"] == "frame"
    assert result["progress"]["latest_event"]["value"] == 0.24
    assert result["node_params"][0]["params"]["frames"] == {"omitted": True}
    assert "omitted" in result["omission_notice"]


def test_watchdog_failed_state_reset_never_starts_production_model(tmp_path, monkeypatch):
    from scripts import qwen_pilot_watchdog as watchdog
    launcher = tmp_path / "launcher.sh"
    backup = tmp_path / "backup.sh"
    original = b"#!/bin/sh\nexec production-model\n"
    pilot = b"#!/bin/sh\nexec pilot-model\n"
    backup.write_bytes(original)
    launcher.write_bytes(pilot)
    (tmp_path / "stop").touch()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"owner": "pilot", "launcher": str(launcher),
        "backup": str(backup), "original_sha256": hashlib.sha256(original).hexdigest(),
        "pilot_sha256": hashlib.sha256(pilot).hexdigest(), "original_mode": 0o755,
        "expires_at": time.time() + 10}))
    monkeypatch.setattr(sys, "argv", ["watchdog", str(manifest)])
    monkeypatch.setattr(watchdog.signal, "signal", lambda *_args: None)

    def service(command, **_kwargs):
        if "start" in command:
            assert b"exec /bin/true" in launcher.read_bytes()
            assert b"production-model" not in launcher.read_bytes()
        return SimpleNamespace(returncode=0, stdout="failed\n")

    monkeypatch.setattr(watchdog.subprocess, "run", service)
    watchdog.main()
    assert launcher.read_bytes() == original
    assert json.loads((tmp_path / "watchdog-state.json").read_text())["launcher_restored"]


def test_main_plan_busy_gpu_never_acquires_admission_or_starts_service(tmp_path, monkeypatch):
    from scripts import run_qwen_coexistence_pilot as pilot
    (tmp_path / "corpus.json").write_text('{"cases":{}}')
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"candidates": [{"model": "Qwen/Qwen3-14B-AWQ",
        "revision": "a" * 40, "provisional_gpu_memory_utilization": 0.55}]}))
    monkeypatch.setattr(sys, "argv", ["pilot", "--output", str(tmp_path),
        "--model", "Qwen/Qwen3-14B-AWQ", "--main-plan", str(plan)])
    monkeypatch.setattr(pilot, "gpu_guard", lambda _args: {"available": False,
        "gpu_jobs": [{"id": "science", "status": "running", "gpu_vram_mb": 8192}]})

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Busy main arm must not change admission or a service")

    monkeypatch.setattr(pilot, "remote", forbidden)
    assert pilot.main() == 2
    assert not list(tmp_path.glob("coexistence-controller*.json"))


def test_main_plan_rejects_offload_arm_and_resume_rejects_changed_conditions(tmp_path):
    from scripts.run_qwen_coexistence_pilot import configure_main_plan, validate_resume
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"candidates": [{"model": "Qwen/Qwen3-8B-FP8",
        "revision": "b" * 40, "provisional_gpu_memory_utilization": 0.55}]}))
    args = SimpleNamespace(main_plan=plan, model="Qwen/Qwen3-8B-FP8", context_tokens=8192,
        kv_cache_mib=0, cpu_offload_gib=2.5, allocator_gc=0, disable_uva=False,
        max_new_tokens=1100, evidence_profile="standard", label="main")
    with pytest.raises(ValueError, match="separate supplementary arm"):
        configure_main_plan(args)
    args.cpu_offload_gib = 0
    configure_main_plan(args)
    assert args.revision == "b" * 40 and args.max_service_mib + 2048 < 24576
    previous = {"model": args.model, "corpus_sha256": "frozen", "cases": [{"job_id": "done"}],
                "inference_settings": {"context_tokens": 8192}}
    with pytest.raises(ValueError, match="preserve"):
        validate_resume(previous, args.model, "frozen", {"context_tokens": 4096})
    with pytest.raises(ValueError, match="corpus"):
        validate_resume(previous, args.model, "changed", {"context_tokens": 8192})


def test_new_scientific_request_preempts_during_inflight_inference(monkeypatch):
    import threading

    from scripts import run_qwen_coexistence_pilot as pilot
    args = SimpleNamespace(api_host="api", compute_host="compute")
    stopped = []
    monkeypatch.setattr(pilot, "remote", lambda *_args, **_kwargs: {
        "owner": "campaign", "status": "locked", "cancel_requested": True,
        "checked_at": time.time()})
    monkeypatch.setattr(pilot, "request_compute_stop", lambda _args, owner: stopped.append(owner))
    state = {}
    finished = threading.Event()
    pilot.watch_admission(args, "campaign", finished, state)
    assert stopped == ["campaign"] and state["preemption_reason"]
    assert not finished.is_set()  # Preemption does not wait for inference completion.
