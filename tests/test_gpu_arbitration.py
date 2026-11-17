from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from worker.gpu_arbitration import (
    GpuArbitrationError,
    execute_assistant_control_job,
    job_requires_gpu,
    pause_qwen_for_gpu_job,
    resume_qwen_if_gpu_is_idle,
)


def test_pipeline_job_uses_scheduler_gpu_reservation_even_for_cpu_mode():
    job = SimpleNamespace(
        params_json={"job_kind": "pipeline_run", "run_request": {"gpu": {"mode": "force_cpu"}},
                     "_hub_resource_allocation": {"gpu_required": True}},
        execution_target_id=None,
    )

    assert job_requires_gpu(None, job=job) is True


def test_pipeline_job_without_gpu_reservation_does_not_pause_assistant():
    job = SimpleNamespace(
        params_json={"job_kind": "pipeline_run", "run_request": {"gpu": {"mode": "force_gpu"}},
                     "_hub_resource_allocation": {"gpu_required": False}},
        execution_target_id=None,
    )

    assert job_requires_gpu(None, job=job) is False


def test_gpu_pause_uses_only_the_configured_systemd_service(monkeypatch):
    observed = []
    settings = SimpleNamespace(
        assistant_gpu_arbitration_enabled=True,
        assistant_sudo_command="/usr/bin/sudo",
        assistant_systemctl_command="/usr/bin/systemctl",
        assistant_qwen_service_name="detecdiv-qwen.service",
    )

    class Completed:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake_run(command, **_kwargs):
        observed.append(command)
        return Completed()

    monkeypatch.setattr("worker.gpu_arbitration.subprocess.run", fake_run)

    pause_qwen_for_gpu_job(settings=settings)

    assert observed == [["/usr/bin/sudo", "-n", "/usr/bin/systemctl", "stop", "detecdiv-qwen.service"]]


@pytest.mark.parametrize("linux_job_status", [None, "running", "cancelling"])
def test_qwen_arbitration_only_considers_jobs_on_its_execution_target(monkeypatch, linux_job_status):
    linux = SimpleNamespace(id=uuid4(), display_name="Linux GPU")
    windows_id = uuid4()
    gpu = {"job_kind": "pipeline_run", "_hub_resource_allocation": {"gpu_required": True}}
    jobs = [SimpleNamespace(execution_target_id=windows_id, status="running", params_json=gpu)]
    if linux_job_status:
        jobs.append(SimpleNamespace(execution_target_id=linux.id, status=linux_job_status, params_json=gpu))

    def active_jobs(statement):
        filters = statement.compile().params
        return [job for job in jobs if job.execution_target_id == filters["execution_target_id_1"]
                and job.status in filters["status_1"]]

    session = Mock()
    session.get.return_value = linux
    session.scalars.side_effect = active_jobs
    start = Mock()
    monkeypatch.setattr("worker.gpu_arbitration._run_systemctl", start)
    monkeypatch.setattr("worker.gpu_arbitration.get_assistant_desired_state", lambda _: "running")
    settings = SimpleNamespace(assistant_gpu_arbitration_enabled=True, assistant_qwen_service_name="qwen")
    control = SimpleNamespace(execution_target_id=linux.id,
                              params_json={"job_kind": "assistant_service_control", "action": "start"})
    if linux_job_status:
        with pytest.raises(GpuArbitrationError, match="Linux GPU"):
            execute_assistant_control_job(session, job=control, settings=settings)
    else:
        execute_assistant_control_job(session, job=control, settings=settings)
    resume_qwen_if_gpu_is_idle(session, settings=settings, target=linux)

    assert start.call_count == (0 if linux_job_status else 2)
    session.flush.assert_called_once()
