"""Report worker limits and current GPU reservation without inventing quotas."""
from __future__ import annotations

import os
import subprocess
from functools import lru_cache


def positive_env(name: str) -> int | None:
    try:
        value = int(os.getenv(name, ""))
        return value if value > 0 else None
    except ValueError:
        return None


@lru_cache(maxsize=1)
def visible_gpu_memory_mb() -> int | None:
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,uuid,memory.total", "--format=csv,noheader,nounits"],
            check=True, capture_output=True, text=True, timeout=3,
        )
        visible = os.getenv("CUDA_VISIBLE_DEVICES")
        allowed = None if visible is None else {item.strip() for item in visible.split(",")}
        total = 0
        for line in result.stdout.splitlines():
            index, gpu_uuid, memory = [item.strip() for item in line.split(",")]
            if allowed is None or index in allowed or gpu_uuid in allowed:
                total += int(memory)
        return total
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def visible_gpu_usage() -> dict[str, int | None]:
    """Return device-wide NVIDIA memory use and utilization for visible GPUs."""
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,uuid,memory.used,utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            check=True, capture_output=True, text=True, timeout=3,
        )
        visible = os.getenv("CUDA_VISIBLE_DEVICES")
        allowed = None if visible is None else {item.strip() for item in visible.split(",")}
        memory_used = 0
        utilization_values: list[int] = []
        selected = 0
        for line in result.stdout.splitlines():
            index, gpu_uuid, used, utilization = [item.strip() for item in line.split(",")]
            if allowed is not None and index not in allowed and gpu_uuid not in allowed:
                continue
            memory_used += int(used)
            selected += 1
            if utilization.isdigit():
                utilization_values.append(int(utilization))
        if not selected:
            return {"gpu_memory_used_mb": None, "gpu_utilization_percent": None}
        utilization_percent = (
            round(sum(utilization_values) / len(utilization_values))
            if utilization_values else None
        )
        return {
            "gpu_memory_used_mb": memory_used,
            "gpu_utilization_percent": utilization_percent,
        }
    except (OSError, subprocess.SubprocessError, ValueError):
        return {"gpu_memory_used_mb": None, "gpu_utilization_percent": None}


def worker_resource_snapshot(*, available_cpu_count: int, current_job, now) -> dict:
    allocation = ((current_job.params_json or {}).get("_hub_resource_allocation") or {}) if current_job else {}
    gpu_required = bool(allocation.get("gpu_required"))
    gpu_capacity = visible_gpu_memory_mb()
    gpu_usage = visible_gpu_usage()
    reserved = int(allocation.get("gpu_vram_mb") or 0)
    exclusive = gpu_required and reserved == 0
    return {
        "cpu_allocated_cores": int(allocation.get("cpu_cores") or 0),
        "cpu_limit_cores": positive_env("DETECDIV_HUB_WORKER_CPU_LIMIT") or available_cpu_count,
        "ram_allocated_mb": int(allocation.get("memory_mb") or 0),
        "ram_limit_mb": positive_env("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB"),
        "swap_allocated_mb": positive_env("DETECDIV_HUB_WORKER_SWAP_LIMIT_MB"),
        "gpu_shared_capacity_mb": gpu_capacity,
        **gpu_usage,
        "vram_allocated_mb": (gpu_capacity if exclusive else reserved) if gpu_required else 0,
        "gpu_exclusive": exclusive,
        "resource_updated_at": now.isoformat(),
    }
