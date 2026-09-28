"""Host and worker RAM budgets, independent of the number of worker instances."""
from __future__ import annotations

import os

import psutil


def positive_mb(value: object) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def memory_policy(target=None) -> dict[str, int | None]:
    metadata = (target.metadata_json or {}) if target is not None else {}
    host = psutil.virtual_memory()
    host_mb = host.total // (1024 * 1024)
    reserve_mb = positive_mb(os.getenv("DETECDIV_HUB_HOST_MEMORY_RESERVE_MB")) or min(24576, max(4096, host_mb // 5))
    host_capacity = max(0, host_mb - reserve_mb)
    configured = [positive_mb(metadata.get("memory_capacity_mb")), positive_mb(os.getenv("DETECDIV_HUB_WORKER_MEMORY_BUDGET_MB"))]
    capacity = min([host_capacity, *[value for value in configured if value is not None]])
    worker_limit = positive_mb(os.getenv("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB"))
    dynamic = os.getenv("DETECDIV_HUB_WORKER_DYNAMIC_RESOURCES") == "1"
    if dynamic:
        if metadata.get("worker_autoscale_enabled", True):
            count = positive_mb(metadata.get("worker_cpu_capacity")) or positive_mb(os.getenv("DETECDIV_HUB_WORKER_CPU_BUDGET")) or 36
        else:
            count = positive_mb(metadata.get("worker_instances_desired")) or positive_mb(os.getenv("DETECDIV_HUB_WORKER_INSTANCES")) or 1
        capacity = max(0, capacity - 512 * count)
        worker_limit = capacity
    return {
        "dynamic": dynamic,
        "capacity_mb": capacity,
        "worker_limit_mb": worker_limit,
        "available_mb": host.available // (1024 * 1024),
        "headroom_mb": positive_mb(os.getenv("DETECDIV_HUB_HOST_MEMORY_HEADROOM_MB")) or min(8192, max(1024, host_mb // 10)),
    }


def requested_memory_mb(job, *, policy: dict, default_mb: int | None = None) -> int:
    params = job.params_json or {}
    resources = params.get("resources") or {}
    if not isinstance(resources, dict):
        raise ValueError("Job resources must be an object")
    if "memory_mb" in resources:
        memory = positive_mb(resources["memory_mb"])
        if memory is None or (policy.get("dynamic") and memory < 512):
            raise ValueError("Job resources.memory_mb must be a positive integer (at least 512 MB for dynamic workers)")
        return memory
    if policy.get("dynamic"):
        return default_mb or (24576 if params.get("job_kind") in {"pipeline_run", "legacy_matlab"} else 1024)
    # A MATLAB pool can multiply image/model memory. Reserve its entire worker
    # slot by default, rather than underestimating each additional process.
    if params.get("job_kind") in {"pipeline_run", "legacy_matlab"}:
        return policy["worker_limit_mb"] or 24576
    return 1024


def worker_allocation_fits(allocation: dict, *, policy: dict) -> tuple[bool, str | None]:
    limit = policy["worker_limit_mb"]
    if limit is not None and allocation["memory_mb"] > limit:
        return False, "worker_memory_capacity"
    cpu_limit = positive_mb(os.getenv("DETECDIV_HUB_WORKER_CPU_BUDGET" if policy.get("dynamic") else "DETECDIV_HUB_WORKER_CPU_LIMIT"))
    if cpu_limit is not None and allocation["cpu_cores"] > cpu_limit:
        return False, "worker_cpu_capacity"
    if policy["available_mb"] < policy["headroom_mb"]:
        return False, "host_memory_pressure"
    return True, None
