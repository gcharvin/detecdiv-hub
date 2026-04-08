from pathlib import Path
import subprocess
from collections.abc import Callable


def build_matlab_batch_command(repo_root: str, entrypoint: str, *, matlab_command: str = "matlab") -> list[str]:
    command = [
        matlab_command,
        "-batch",
        f"cd('{Path(repo_root).as_posix()}'); {entrypoint}",
    ]
    return command


def run_matlab_command(
    command: list[str],
    *,
    heartbeat_callback: Callable[[], None] | None = None,
    heartbeat_interval_sec: float = 10.0,
) -> subprocess.CompletedProcess[str]:
    if heartbeat_callback is None:
        return subprocess.run(command, check=False, text=True, capture_output=True)

    process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = ""
    stderr = ""
    while True:
        try:
            stdout, stderr = process.communicate(timeout=heartbeat_interval_sec)
            break
        except subprocess.TimeoutExpired:
            heartbeat_callback()
    return subprocess.CompletedProcess(command, process.returncode or 0, stdout, stderr)
