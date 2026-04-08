from pathlib import Path
import subprocess
import time
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
    progress_callback: Callable[[], None] | None = None,
    heartbeat_interval_sec: float = 10.0,
    stdout_path: Path | None = None,
    stderr_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    if heartbeat_callback is None:
        return subprocess.run(command, check=False, text=True, capture_output=True)

    if stdout_path is None or stderr_path is None:
        process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = ""
        stderr = ""
        while True:
            try:
                stdout, stderr = process.communicate(timeout=heartbeat_interval_sec)
                break
            except subprocess.TimeoutExpired:
                heartbeat_callback()
                if progress_callback is not None:
                    progress_callback()
        return subprocess.CompletedProcess(command, process.returncode or 0, stdout, stderr)

    with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open("w", encoding="utf-8") as stderr_file:
        process = subprocess.Popen(command, text=True, stdout=stdout_file, stderr=stderr_file)
        while process.poll() is None:
            time.sleep(heartbeat_interval_sec)
            heartbeat_callback()
            if progress_callback is not None:
                progress_callback()

    stdout = stdout_path.read_text(encoding="utf-8", errors="replace") if stdout_path.is_file() else ""
    stderr = stderr_path.read_text(encoding="utf-8", errors="replace") if stderr_path.is_file() else ""
    return subprocess.CompletedProcess(command, process.returncode or 0, stdout, stderr)
