"""Linux watchdog for a temporary pilot using the managed Qwen service.

The owner manifest specifies the exact launcher and backup hashes. Stop and
restore only this pilot, on exit, pressure, a stop request, or lease expiration.
Workers retain their existing ability to stop detecdiv-qwen.service.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path


def save(path, data):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2), encoding="utf-8")
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    launcher = Path(manifest["launcher"])
    backup = Path(manifest["backup"])
    if hashlib.sha256(backup.read_bytes()).hexdigest() != manifest["original_sha256"]:
        raise RuntimeError("Backup hash mismatch")
    state_path = args.manifest.with_name("watchdog-state.json")
    stop_path = args.manifest.with_name("stop")
    state = {"owner": manifest["owner"], "pid": os.getpid(), "status": "watching",
             "peak_service_mib": 0, "samples": [], "started_at": time.time()}
    stopped = False

    def terminate(_number, _frame):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    observed_active = False
    try:
        while True:
            state["checked_at"] = time.time()
            if hashlib.sha256(launcher.read_bytes()).hexdigest() != manifest["pilot_sha256"]:
                state["reason"] = "launcher ownership changed"
                break
            if stopped or stop_path.exists() or time.time() > manifest["expires_at"]:
                state["reason"] = "stop requested or lease expired"
                break
            running = subprocess.run(["systemctl", "is-active", "detecdiv-qwen.service"],
                                     capture_output=True, text=True, check=False).stdout.strip()
            observed_active |= running == "active"
            if observed_active and running not in {"active", "activating"}:
                state["reason"] = "managed service stopped (possibly worker arbitration)"
                break
            result = subprocess.run(["nvidia-smi", "--query-compute-apps=pid,used_memory",
                                     "--format=csv,noheader,nounits"], capture_output=True,
                                    text=True, check=True, timeout=5)
            own_mb = 0
            unknown = []
            for line in result.stdout.splitlines():
                pid, memory = (part.strip() for part in line.split(","))
                try:
                    group = Path("/proc", pid, "cgroup").read_text()
                except FileNotFoundError:
                    continue
                if "detecdiv-qwen.service" in group:
                    own_mb += int(memory)
                elif "detecdiv-worker@" not in group:
                    unknown.append(pid)
            total = subprocess.run(["nvidia-smi", "--query-gpu=memory.used,memory.total",
                                    "--format=csv,noheader,nounits"], capture_output=True,
                                   text=True, check=True, timeout=5).stdout.strip()
            used, capacity = (int(part.strip()) for part in total.split(","))
            state["peak_service_mib"] = max(state["peak_service_mib"], own_mb)
            state["service_mib"] = own_mb
            state["free_mib"] = capacity - used
            state["samples"].append({"time": time.time(), "service_mib": own_mb,
                                     "total_used_mib": used})
            state["samples"] = state["samples"][-900:]
            save(state_path, state)
            if own_mb > manifest["max_service_mib"] or capacity - used < 2048 or unknown:
                state["reason"] = "VRAM limit, free margin, or unmanaged compute process"
                break
            time.sleep(1)
    finally:
        owned = hashlib.sha256(launcher.read_bytes()).hexdigest() == manifest["pilot_sha256"]
        if owned:
            stopped_service = subprocess.run(
                ["sudo", "-n", "/usr/bin/systemctl", "stop", "detecdiv-qwen.service"],
                capture_output=True, text=True, check=False, timeout=60)
            state["stop_exit_code"] = stopped_service.returncode
            if stopped_service.returncode == 0:
                status = subprocess.run(["systemctl", "is-active", "detecdiv-qwen.service"],
                                        capture_output=True, text=True, check=False).stdout.strip()
                if status == "failed":
                    # An interrupted startup can hit systemd's stop timeout.
                    # A harmless successful activation clears the failed state
                    # without loading the production model or altering the unit.
                    launcher.write_text("#!/usr/bin/env bash\nexec /bin/true\n", encoding="utf-8")
                    try:
                        subprocess.run(["sudo", "-n", "/usr/bin/systemctl", "start", "detecdiv-qwen.service"],
                                       capture_output=True, check=True, timeout=60)
                        subprocess.run(["sudo", "-n", "/usr/bin/systemctl", "stop", "detecdiv-qwen.service"],
                                       capture_output=True, check=True, timeout=60)
                    except (OSError, subprocess.SubprocessError) as exc:
                        state["reset_failed_error"] = str(exc)
                replacement = launcher.with_name(launcher.name + ".pilot-restore")
                replacement.write_bytes(backup.read_bytes())
                replacement.chmod(manifest["original_mode"])
                replacement.replace(launcher)
                state["launcher_restored"] = True
        state["status"] = "stopped"
        state["finished_at"] = time.time()
        save(state_path, state)


if __name__ == "__main__":
    main()
