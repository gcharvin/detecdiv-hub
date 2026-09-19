from types import SimpleNamespace

from worker.gpu_arbitration import job_requires_gpu, pause_qwen_for_gpu_job


def test_pipeline_job_requires_gpu_when_mode_is_forced():
    job = SimpleNamespace(
        params_json={"job_kind": "pipeline_run", "run_request": {"gpu": {"mode": "force_gpu"}}},
        execution_target_id=None,
    )

    assert job_requires_gpu(None, job=job) is True


def test_pipeline_job_with_cpu_mode_does_not_require_gpu():
    job = SimpleNamespace(
        params_json={"job_kind": "pipeline_run", "run_request": {"gpu": {"mode": "force_cpu"}}},
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
