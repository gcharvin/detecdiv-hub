import pytest

from scripts.monitor_pipeline_campaign import ensure_retry_allowed, retry_payload


def test_retry_preserves_selection_and_discards_previous_cancel_paths():
    source = {"run_request": {"selection": {"frames": [1, 2]}, "paths": {},
                             "control": {"cancel_token_path": "old.cancel", "cancel_policy": "cooperative"},
                             "node_params": [{"id": "xtra_full_channels_legacy", "params": {}}]},
              "execution": {"code_commit": "a"*40, "cancel_token_file": "old.cancel",
                            "result_json_path": "old-result.json"}}
    state = {"run_prefix": "recovery", "retry_count": 0, "project_folder": "/data/project",
             "selection": {"fovs": [1], "rois": [1], "frames": [], "channels": []}}
    result = retry_payload(source, state, classification_only=True, cut=88)
    assert result["run_request"]["selection"] == state["selection"]
    assert result["run_request"]["selected_nodes"] == ["xtra_full_channels_legacy"]
    assert result["run_request"]["node_params"][0]["params"]["executionEnvironment"] == "gpu"
    assert result["execution"] == {"code_commit": "a"*40, "allow_gui": False,
                                   "interactive": False, "save_project": True, "write_scope": "project_update"}
    assert result["run_request"]["control"] == {"cancel_policy": "cooperative"}
    assert source["execution"]["cancel_token_file"] == "old.cancel"


@pytest.mark.parametrize("status,result", [("running", ""), ("queued", ""),
                                           ("cancelling", ""), ("cancelled", ""),
                                           ("done", "cancelled"), ("done", "done")])
def test_automatic_retry_cannot_override_active_or_human_stopped_job(status, result):
    with pytest.raises(ValueError):
        ensure_retry_allowed(status, result, {"retry_count": 0, "max_retries": 3})


def test_diagnosed_failure_can_retry_with_a_bound():
    ensure_retry_allowed("failed", "failed", {"retry_count": 0, "max_retries": 3})
    with pytest.raises(ValueError):
        ensure_retry_allowed("failed", "failed", {"retry_count": 3, "max_retries": 3})


def test_repaired_commit_must_be_explicit_and_full():
    source = {"run_request": {"paths": {}, "node_params": []}, "execution": {"code_commit": "a"*40}}
    state = {"run_prefix": "recovery", "retry_count": 0, "project_folder": "/data/project",
             "selection": {"frames": []}, "retry_code_commit": "b"*40}
    result = retry_payload(source, state, classification_only=True, cut=88)
    assert result["execution"]["code_commit"] == "b"*40
    state["retry_code_commit"] = "b"*7
    with pytest.raises(ValueError):
        retry_payload(source, state, classification_only=True, cut=88)
