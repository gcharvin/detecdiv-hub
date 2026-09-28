"""Resize one service in place for its job, inside the shared compute budget."""
from __future__ import annotations

import getpass
import os
import re
import subprocess
import threading
from pathlib import Path

_lock = threading.Lock()
_applied = None


def enabled() -> bool:
    return os.getenv("DETECDIV_HUB_WORKER_DYNAMIC_RESOURCES") == "1"


def apply_job_limits(worker_instance: str, current_job) -> None:
    global _applied
    if not enabled():
        return
    instance = worker_instance.removeprefix("@")
    if not re.fullmatch(r"[1-9][0-9]*", instance):
        raise ValueError("Dynamic service limits require a numbered worker")
    allocation = ((current_job.params_json or {}).get("_hub_resource_allocation") or {}) if current_job else {}
    cpu = int(allocation.get("cpu_cores") or 1)
    memory = int(allocation.get("memory_mb") or 512)
    swap = int(allocation.get("swap_limit_mb") or 0) if current_job else 0
    wanted = (cpu, memory, swap)
    with _lock:
        if _applied == wanted:
            return
        helper = os.getenv("DETECDIV_HUB_WORKER_CONFIGURE_SCRIPT")
        if not helper:
            raise RuntimeError("Dynamic worker quota helper is not configured")
        subprocess.run([
            "sudo", "-n", "/bin/bash", helper,
            "--repo-root", str(Path(__file__).resolve().parents[1]),
            "--service-user", getpass.getuser(),
            "--job-worker-instance", instance, "--job-cpu-cores", str(cpu),
            "--job-memory-mb", str(memory), "--job-swap-mb", str(swap),
        ], check=True, capture_output=True, text=True, timeout=20)
        os.environ["DETECDIV_HUB_WORKER_CPU_LIMIT"] = str(cpu)
        os.environ["DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB"] = str(memory)
        os.environ["DETECDIV_HUB_WORKER_SWAP_LIMIT_MB"] = str(swap)
        _applied = wanted
