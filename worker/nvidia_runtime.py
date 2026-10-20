"""Use an operator-staged NVIDIA runtime matching the loaded Linux driver.

This only affects new compute children. It never installs a driver, changes
system libraries, or touches processes which already use the GPU.
"""
from __future__ import annotations

import re
from pathlib import Path


def matching_nvidia_environment(
    environment: dict[str, str], *, runtime_root: Path | None = None,
    version_file: Path = Path("/proc/driver/nvidia/version"),
) -> dict[str, str]:
    runtime_root = runtime_root or Path(__file__).resolve().parents[1] / ".nvidia-runtime"
    if not runtime_root.is_dir() or not version_file.is_file():
        return environment
    match = re.search(r"Kernel Module\s+(\d+\.\d+(?:\.\d+)?)", version_file.read_text())
    if not match:
        raise RuntimeError("Cannot identify the loaded NVIDIA driver for the staged runtime")
    runtime = runtime_root / match.group(1)
    if not runtime.exists():
        return environment
    library_dir = runtime / "usr/lib/x86_64-linux-gnu"
    for name in ("libcuda.so.1", "libnvidia-ml.so.1"):
        library = library_dir / name
        if not library.is_file() or not library.resolve().is_relative_to(runtime.resolve()):
            raise RuntimeError(f"Incomplete staged NVIDIA runtime: {name}")
    previous = environment.get("LD_LIBRARY_PATH", "")
    return {**environment,
            "LD_LIBRARY_PATH": str(library_dir) + (":" + previous if previous else ""),
            "DETECDIV_HUB_NVIDIA_RUNTIME_VERSION": match.group(1)}
