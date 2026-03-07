from pathlib import Path
import subprocess


def build_matlab_batch_command(repo_root: str, entrypoint: str) -> list[str]:
    command = [
        "matlab",
        "-batch",
        f"cd('{Path(repo_root).as_posix()}'); {entrypoint}",
    ]
    return command


def run_matlab_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, text=True, capture_output=True)

