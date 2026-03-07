import subprocess


def run_python_module(module_name: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python", "-m", module_name],
        check=False,
        text=True,
        capture_output=True,
    )

