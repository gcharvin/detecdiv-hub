from uuid import uuid4

import pytest

from api.models import Job, User
from api.routes_pipeline_runs import cancel_pipeline_run
from worker.pipeline_run_executor import PipelineRunCancelled, validate_pipeline_run_result


class SessionStub:
    def __init__(self, job):
        self.job = job

    def get(self, model, identity):
        assert identity == self.job.id
        return self.job

    def commit(self):
        pass

    def refresh(self, job):
        assert job is self.job


def job_fixture(status="running"):
    return Job(id=uuid4(), status=status, params_json={"job_kind": "pipeline_run"}, result_json={})


@pytest.mark.parametrize("status", ["cancelled", "canceled"])
def test_cancelled_matlab_result_with_zero_exit_is_not_success(status):
    job = job_fixture()
    with pytest.raises(PipelineRunCancelled, match=f"status {status}"):
        validate_pipeline_run_result(SessionStub(job), job=job, returncode=0,
                                     result_json={"status": status})


def test_pending_hub_cancellation_wins_over_matlab_done():
    job = job_fixture("cancelling")
    with pytest.raises(PipelineRunCancelled):
        validate_pipeline_run_result(SessionStub(job), job=job, returncode=0,
                                     result_json={"status": "done"})


@pytest.mark.parametrize("status", ["failed", "error", "incomplete"])
def test_unsuccessful_result_with_zero_exit_preserves_error(status):
    job = job_fixture()
    with pytest.raises(RuntimeError, match="missing predictions"):
        validate_pipeline_run_result(SessionStub(job), job=job, returncode=0,
                                     result_json={"status": status, "error": "missing predictions"})


def test_nonzero_exit_preserves_matlab_diagnostics():
    job = job_fixture()
    with pytest.raises(RuntimeError, match="memory exhausted"):
        validate_pipeline_run_result(SessionStub(job), job=job, returncode=1,
                                     result_json={}, stderr="memory exhausted")


@pytest.mark.parametrize("result", [{"status": "done"}, {}])
def test_success_and_legacy_resultless_success_remain_supported(result):
    job = job_fixture()
    validate_pipeline_run_result(SessionStub(job), job=job, returncode=0, result_json=result)


def test_cancel_request_records_authenticated_actor(monkeypatch):
    job = job_fixture()
    user = User(id=uuid4(), user_key="gilles", role="admin", display_name="Gilles")
    tokens = []
    monkeypatch.setattr("api.routes_pipeline_runs.write_cancel_token", lambda j: tokens.append(j.id))
    result = cancel_pipeline_run(job.id, db=SessionStub(job), current_user=user)
    assert result.status == "cancelling"
    assert result.result_json["cancel_requested_by"] == "gilles"
    assert result.result_json["cancel_requested_by_user_id"] == str(user.id)
    assert tokens == [job.id]
