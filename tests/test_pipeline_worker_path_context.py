import json
from types import SimpleNamespace

import pytest

from worker import pipeline_run_executor as executor


@pytest.mark.parametrize("worker_mappings", [[], [{"source": "/data", "target": "//test-storage/DATA"}]])
def test_matlab_receives_trusted_worker_mappings_without_rewriting_client_identity(
    monkeypatch, worker_mappings
):
    client_mappings = [{"localRoot": "Z:\\", "remoteRoot": "/data"}]
    payload = {
        "project_ref": {},
        "pipeline_ref": {},
        "run_request": {"paths": {"path_mappings": client_mappings}},
        "execution": {"worker_path_mappings": [{"source": "/data", "target": "//untrusted/share"}]},
    }
    monkeypatch.setattr(
        executor, "get_settings", lambda: SimpleNamespace(
            matlab_repo_root="test-runtime", matlab_command="matlab",
            worker_path_mappings=json.dumps(worker_mappings),
        )
    )
    monkeypatch.setattr(executor, "normalize_pipeline_run_payload", lambda *args, **kwargs: payload)
    monkeypatch.setattr(executor, "resolve_pipeline_ref_for_server", lambda *args, **kwargs: None)
    monkeypatch.setattr(executor, "normalize_pipeline_ref_paths_for_posix", lambda **kwargs: kwargs["payload"])
    captured = {}

    class Prepared(Exception):
        pass

    def stop_after_prepare(session, *, job, payload):
        captured.update(payload)
        raise Prepared

    monkeypatch.setattr(executor, "persist_prepared_pipeline_run", stop_after_prepare)
    with pytest.raises(Prepared):
        executor.execute_pipeline_run_job(object(), job=object())

    assert captured["execution"]["worker_path_mappings"] == worker_mappings
    assert captured["run_request"]["paths"]["path_mappings"] == client_mappings
