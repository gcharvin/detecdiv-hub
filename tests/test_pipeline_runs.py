from __future__ import annotations

from uuid import uuid4

from api.models import User
from api.schemas import PipelineRunCreateRequest
from api.services.pipeline_runs import normalized_pipeline_run_params, pipeline_ref_has_source


def test_normalized_pipeline_run_params_marks_prepared_local_submission():
    project_id = uuid4()
    pipeline_id = uuid4()
    target_id = uuid4()
    user = User(id=uuid4(), user_key="alice", display_name="Alice")
    payload = PipelineRunCreateRequest(
        project_id=project_id,
        pipeline_id=pipeline_id,
        execution_target_id=target_id,
        requested_mode="server",
        run_request={"run_id": "run_001", "selected_nodes": ["classifier_1"]},
        execution={"allow_gui": False},
        client_context={"client_host": "LOCAL-PC"},
    )

    params = normalized_pipeline_run_params(
        payload,
        current_user=user,
        submitted_via="local_client",
    )

    assert params["job_kind"] == "pipeline_run"
    assert params["project_ref"]["project_id"] == str(project_id)
    assert params["pipeline_ref"]["pipeline_id"] == str(pipeline_id)
    assert params["execution"]["execution_target_id"] == str(target_id)
    assert params["execution"]["requested_mode"] == "server"
    assert params["execution"]["interactive"] is False
    assert params["execution"]["save_project"] is True
    assert params["client_context"]["submitted_via"] == "local_client"
    assert params["client_context"]["submitted_by_user_key"] == "alice"
    assert params["client_context"]["schema"] == "pipeline_run_contract_v1"


def test_pipeline_ref_has_source_accepts_registry_or_portable_bundle():
    assert pipeline_ref_has_source({"pipeline_ref": {"pipeline_id": str(uuid4())}})
    assert pipeline_ref_has_source({"pipeline_ref": {"pipeline_key": "segmentation_v2"}})
    assert pipeline_ref_has_source(
        {"pipeline_ref": {"pipeline_json_path": "/srv/project/pipeline.json"}}
    )
    assert not pipeline_ref_has_source({"pipeline_ref": {}})
