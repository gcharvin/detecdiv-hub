import subprocess
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

from worker.executors.job_process_scope import JobProcessScope


class MatlabCommandForcedStop(RuntimeError):
    pass


def build_matlab_batch_command(
    repo_root: str, entrypoint: str, *, matlab_command: str = "matlab"
) -> list[str]:
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
    env: dict[str, str] | None = None,
    cwd: str | None = None,
    force_stop_callback: Callable[[], bool] | None = None,
    started_callback: Callable[[int], None] | None = None,
) -> subprocess.CompletedProcess[str]:
    if force_stop_callback is not None:
        return run_contained_command(
            command,
            env=env,
            cwd=cwd,
            heartbeat_callback=heartbeat_callback,
            progress_callback=progress_callback,
            heartbeat_interval_sec=heartbeat_interval_sec,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            force_stop_callback=force_stop_callback,
            started_callback=started_callback,
        )
    if heartbeat_callback is None:
        return subprocess.run(
            command, check=False, text=True, capture_output=True, env=env, cwd=cwd
        )

    if stdout_path is None or stderr_path is None:
        process = subprocess.Popen(
            command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=cwd
        )
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

    with (
        stdout_path.open("w", encoding="utf-8") as stdout_file,
        stderr_path.open("w", encoding="utf-8") as stderr_file,
    ):
        process = subprocess.Popen(
            command, text=True, stdout=stdout_file, stderr=stderr_file, env=env, cwd=cwd
        )
        while process.poll() is None:
            time.sleep(heartbeat_interval_sec)
            heartbeat_callback()
            if progress_callback is not None:
                progress_callback()

    stdout = (
        stdout_path.read_text(encoding="utf-8", errors="replace") if stdout_path.is_file() else ""
    )
    stderr = (
        stderr_path.read_text(encoding="utf-8", errors="replace") if stderr_path.is_file() else ""
    )
    return subprocess.CompletedProcess(command, process.returncode or 0, stdout, stderr)


def run_contained_command(
    command,
    *,
    env,
    cwd,
    heartbeat_callback,
    progress_callback,
    heartbeat_interval_sec,
    stdout_path,
    stderr_path,
    force_stop_callback,
    started_callback,
):
    # File-backed output avoids pipe deadlocks, including when children retain
    # stdout after MATLAB itself exits. Retain caller-provided persistent logs.
    with tempfile.TemporaryDirectory(prefix="detecdiv-command-") as tmp:
        stdout_path = stdout_path or Path(tmp) / "stdout.log"
        stderr_path = stderr_path or Path(tmp) / "stderr.log"
        scope = JobProcessScope()
        try:
            with (
                stdout_path.open("w", encoding="utf-8") as out,
                stderr_path.open("w", encoding="utf-8") as err,
            ):
                # Also honor a request accepted between preparation and launch.
                if force_stop_callback():
                    raise MatlabCommandForcedStop("Job forcibly stopped before MATLAB launch.")
                process = scope.start(command, stdout=out, stderr=err, env=env, cwd=cwd)
                if started_callback:
                    started_callback(process.pid)
                next_heartbeat = time.monotonic() + heartbeat_interval_sec
                next_control = 0.0
                while scope.running():
                    now = time.monotonic()
                    if now >= next_control:
                        if force_stop_callback():
                            scope.kill()
                            raise MatlabCommandForcedStop(
                                "Job forcibly stopped by user; all job processes have exited."
                            )
                        next_control = now + 1.0
                    if now >= next_heartbeat:
                        if heartbeat_callback:
                            heartbeat_callback()
                        if progress_callback:
                            progress_callback()
                        next_heartbeat = now + heartbeat_interval_sec
                    time.sleep(0.1)
                process.wait()
        finally:
            # A callback/database error must not leave an uncontrolled MATLAB tree.
            scope.close()
        return subprocess.CompletedProcess(
            command,
            process.returncode,
            stdout_path.read_text(encoding="utf-8", errors="replace"),
            stderr_path.read_text(encoding="utf-8", errors="replace"),
        )
