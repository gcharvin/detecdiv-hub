"""Separate Git working trees for MATLAB jobs; never pull the live checkout."""
from contextlib import contextmanager
from pathlib import Path
import os
import subprocess
import time

from api.services.matlab_code_versions import validate_commit


def git(repo: Path, *args: str) -> str:
    command = ["git", "-c", f"safe.directory={repo.as_posix()}", "-C", str(repo), *args]
    result = subprocess.run(command, capture_output=True, text=True, timeout=180,
                            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"})
    if result.returncode:
        raise RuntimeError(f"Git {args[0]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


@contextmanager
def preparation_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".prepare.lock").open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        deadline = time.monotonic() + 240
        while True:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("Timed out waiting for MATLAB checkout preparation lock.")
                time.sleep(0.2)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def prepare_checkout(repo_root: str, *, job_id, code_commit: str = "",
                     checkout_root: str = "", remote: str = "origin", branch: str = "unstable") -> tuple[str, str]:
    repo = Path(repo_root).resolve()
    root = Path(checkout_root).resolve() if checkout_root else repo.parent / (repo.name + "-jobs")
    if root == repo or repo in root.parents:
        raise ValueError("MATLAB job checkouts must live outside the source checkout.")
    # UUID validation keeps user-controlled identifiers out of filesystem paths.
    from uuid import UUID
    job_key = str(UUID(str(job_id)))
    with preparation_lock(root):
        commit = validate_commit(code_commit or git(repo, "rev-parse", "HEAD"))
        try:
            git(repo, "cat-file", "-e", commit + "^{commit}")
        except RuntimeError:
            git(repo, "fetch", "--no-tags", remote, branch)
            git(repo, "cat-file", "-e", commit + "^{commit}")
        target = root / (commit + "-" + job_key)
        if not target.exists():
            git(repo, "worktree", "add", "--detach", str(target), commit)
            git(repo, "worktree", "lock", "--reason", "Hub MATLAB job " + job_key, str(target))
        if git(target, "rev-parse", "HEAD") != commit:
            raise RuntimeError("MATLAB job checkout has an unexpected commit.")
        if git(target, "status", "--porcelain", "--untracked-files=no"):
            raise RuntimeError("MATLAB job checkout has modified tracked files; preserved for inspection.")
        # Submodules require explicit packaging; do not silently execute missing code.
        if (target / ".gitmodules").exists():
            raise RuntimeError("MATLAB job checkout contains submodules; configure an explicit runtime package.")
    return str(target), commit


def prepare_job_checkout(session, job, settings) -> tuple[str, str]:
    params = dict(job.params_json or {})
    execution = dict(params.get("execution") or {})
    repo_root, commit = prepare_checkout(
        settings.matlab_repo_root, job_id=job.id, code_commit=execution.get("code_commit", ""),
        checkout_root=getattr(settings, "matlab_job_checkout_root", ""),
        remote=getattr(settings, "matlab_git_remote", "origin"),
        branch=getattr(settings, "matlab_git_branch", "unstable"),
    )
    execution["code_commit"] = commit
    params["execution"] = execution
    job.params_json = params
    result = dict(job.result_json or {})
    result["worker_runtime"] = {"engine": "matlab", "repo_root": repo_root, "code_commit": commit}
    job.result_json = result
    session.commit()
    return repo_root, commit


if __name__ == "__main__":
    import argparse
    import json
    from uuid import uuid4
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--checkout-root", default="")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default="unstable")
    args = parser.parse_args()
    path, commit = prepare_checkout(args.repo_root, job_id=uuid4(), code_commit=args.commit,
                                   checkout_root=args.checkout_root, remote=args.remote, branch=args.branch)
    print(json.dumps({"repo_root": path, "code_commit": commit}))
