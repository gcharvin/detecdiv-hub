from __future__ import annotations

from copy import deepcopy
from uuid import uuid4

import pytest
from fastapi import HTTPException

from api.models import Project, User
from api.schemas import PipelineRunCreateRequest
from api.services import pipeline_runs


class SessionStub:
    def __init__(self, project):
        self.project = project
        self.added = []
        self.commits = 0

    def get(self, model, identity):
        assert model is Project and identity == self.project.id
        return self.project

    def add(self, value):
        self.added.append(value)

    def commit(self):
        self.commits += 1


@pytest.fixture
def request_context(monkeypatch):
    user = User(id=uuid4(), user_key="alice", display_name="Alice", role="user")
    project = Project(
        id=uuid4(),
        project_name="Example",
        owner_user_id=user.id,
        status="indexed",
        visibility="private",
        metadata_json={},
    )
    payload = PipelineRunCreateRequest(
        project_id=project.id,
        requested_mode="server",
        project_ref={"project_mat_path": "/srv/project.json"},
        pipeline_ref={"pipeline_json_path": "/srv/pipeline.json"},
        client_context={"client_host": "CLIENT"},
    )
    monkeypatch.setattr(pipeline_runs, "active_project_locks", lambda *args, **kwargs: [])
    return SessionStub(project), payload, user


@pytest.mark.parametrize(
    "defaults",
    [
        {"memory_mb": 30720},
        {"cpu_cores": 4},
        {"memory_mb": 30720, "cpu_cores": 8},
        {"memory_mb": 512, "cpu_cores": 1},
        {"memory_mb": 262144, "cpu_cores": 36},
    ],
)
def test_preflight_applies_validated_project_resource_defaults(request_context, defaults):
    session, payload, user = request_context
    session.project.metadata_json = {"pipeline_run_resources": defaults}

    result = pipeline_runs.preflight_pipeline_run_request(
        session, payload=payload, current_user=user
    )

    assert result.can_submit
    assert result.normalized_payload["resources"] == defaults
    assert result.normalized_payload["client_context"]["resource_defaults_source"] == "project"
    assert result.normalized_payload["client_context"]["client_host"] == "CLIENT"


@pytest.mark.parametrize(
    "metadata", [None, {}, {"notes": "Unrelated"}, {"pipeline_run_resources": {}}]
)
def test_preflight_without_project_defaults_keeps_payload_unchanged(request_context, metadata):
    session, payload, user = request_context
    session.project.metadata_json = metadata

    result = pipeline_runs.preflight_pipeline_run_request(
        session, payload=payload, current_user=user
    )

    assert result.can_submit
    assert "resources" not in result.normalized_payload
    assert result.normalized_payload == pipeline_runs.normalized_pipeline_run_params(
        payload, current_user=user, submitted_via="hub_web"
    )


def test_preflight_copies_defaults_without_mutating_project_or_request(request_context):
    session, payload, user = request_context
    defaults = {"memory_mb": 30720, "cpu_cores": 8}
    session.project.metadata_json = {
        "pipeline_run_resources": defaults,
        "notes": {"text": "Preserve this metadata"},
    }
    before_metadata = deepcopy(session.project.metadata_json)
    before_request = payload.model_dump()

    first = pipeline_runs.preflight_pipeline_run_request(
        session, payload=payload, current_user=user
    )
    first.normalized_payload["resources"]["memory_mb"] = 512
    first.normalized_payload["client_context"]["resource_defaults_source"] = "changed"
    second = pipeline_runs.preflight_pipeline_run_request(
        session, payload=payload, current_user=user
    )

    assert session.project.metadata_json == before_metadata
    assert payload.model_dump() == before_request
    assert second.normalized_payload["resources"] == defaults
    assert second.normalized_payload["client_context"]["resource_defaults_source"] == "project"


@pytest.mark.parametrize(
    "defaults",
    [
        None,
        [],
        30720,
        {"memory_mb": True},
        {"memory_mb": 511},
        {"memory_mb": 262145},
        {"memory_mb": 30720.0},
        {"memory_mb": "30720"},
        {"cpu_cores": False},
        {"cpu_cores": 0},
        {"cpu_cores": 37},
        {"cpu_cores": 4.0},
        {"cpu_cores": "4"},
        {"memory_mb": 30720, "gpu_vram_mb": 8192},
    ],
)
def test_invalid_project_defaults_fail_preflight_and_never_submit(request_context, defaults):
    session, payload, user = request_context
    session.project.metadata_json = {"pipeline_run_resources": defaults}

    result = pipeline_runs.preflight_pipeline_run_request(
        session, payload=payload, current_user=user
    )

    assert not result.can_submit
    assert "resources" not in result.normalized_payload
    assert "resource_defaults_source" not in result.normalized_payload["client_context"]
    assert [item.code for item in result.issues if item.severity == "error"] == [
        "invalid_project_pipeline_run_resources"
    ]
    with pytest.raises(HTTPException) as exc:
        pipeline_runs.create_pipeline_run_job(session, payload=payload, current_user=user)
    assert exc.value.status_code == 422
    assert session.added == []
    assert session.commits == 0
