from datetime import datetime, timezone
from types import SimpleNamespace

from worker import resource_reporting


def test_visible_gpu_usage_filters_to_cuda_visible_devices(monkeypatch):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "1")
    output = "0, GPU-a, 1200, 10\n1, GPU-b, 8800, 95\n"
    monkeypatch.setattr(
        resource_reporting.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=output),
    )

    assert resource_reporting.visible_gpu_usage() == {
        "gpu_memory_used_mb": 8800,
        "gpu_utilization_percent": 95,
    }


def test_worker_snapshot_reports_gpu_use_separately_from_job_reservation(monkeypatch):
    monkeypatch.setattr(resource_reporting, "visible_gpu_memory_mb", lambda: 16_000)
    monkeypatch.setattr(
        resource_reporting,
        "visible_gpu_usage",
        lambda: {"gpu_memory_used_mb": 8_800, "gpu_utilization_percent": 95},
    )
    job = SimpleNamespace(params_json={
        "_hub_resource_allocation": {
            "gpu_required": True,
            "gpu_vram_mb": 4_000,
            "cpu_cores": 2,
        },
    })

    snapshot = resource_reporting.worker_resource_snapshot(
        available_cpu_count=8,
        current_job=job,
        now=datetime(2026, 9, 29, tzinfo=timezone.utc),
    )

    assert snapshot["gpu_memory_used_mb"] == 8_800
    assert snapshot["gpu_utilization_percent"] == 95
    assert snapshot["vram_allocated_mb"] == 4_000
    assert snapshot["gpu_shared_capacity_mb"] == 16_000
