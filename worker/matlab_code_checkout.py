"""Read-only Git release cache and separate persistent MATLAB job directories."""
from contextlib import contextmanager
from pathlib import Path
import os
import subprocess
import time
import stat
import json
import base64
from datetime import datetime, timezone, timedelta

from api.services.matlab_code_versions import validate_commit


def git(repo: Path, *args: str) -> str:
    command = ["git", "-c", f"safe.directory={repo.as_posix()}", "-C", str(repo), *args]
    result = subprocess.run(command, capture_output=True, text=True, timeout=180,
                            env={**os.environ, "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0"})
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


def cache_root(repo_root, checkout_root=""):
    repo = Path(repo_root).resolve()
    root = Path(checkout_root).resolve() if checkout_root else repo.parent / (repo.name + "-jobs")
    if root == repo or repo in root.parents:
        raise ValueError("MATLAB cache must live outside the source checkout.")
    return repo, root


def protect_release(target):
    # Refuse links: their destinations would not be protected by this directory.
    paths = [target, *target.rglob("*")]
    if any(p.is_symlink() for p in paths):
        raise RuntimeError("Shared MATLAB releases cannot contain symbolic links.")
    if os.name == "nt":
        # NTFS read-only attributes do not prevent writes. Deny write/delete to
        # all accounts, including descendants, while retaining read. Owners/admins
        # can explicitly remove the ACL for GC; ordinary writes remain denied.
        # icacls adds SYNCHRONIZE to write-deny ACEs, which also blocks chdir.
        # .NET's Deny rule removes that bit, preserving read/traverse access.
        quoted = str(target).replace("'", "''")
        script = f"""$ErrorActionPreference='Stop'; $p='{quoted}';
$acl=Get-Acl -LiteralPath $p;
$sid=[System.Security.Principal.SecurityIdentifier]::new('S-1-1-0');
$rule=[System.Security.AccessControl.FileSystemAccessRule]::new($sid,
 [System.Security.AccessControl.FileSystemRights]0x10156,
 [System.Security.AccessControl.InheritanceFlags]'ContainerInherit,ObjectInherit',
 [System.Security.AccessControl.PropagationFlags]::None,
 [System.Security.AccessControl.AccessControlType]::Deny);
$acl.AddAccessRule($rule); Set-Acl -LiteralPath $p -AclObject $acl;
"""
        encoded = base64.b64encode(script.encode("utf-16-le")).decode()
        subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
                       check=True, capture_output=True, text=True)
    else:
        for path in reversed(paths):
            mode = path.stat().st_mode
            path.chmod(0o555 if path.is_dir() or mode & 0o111 else 0o444)


def unprotect_release(target):
    if os.name == "nt":
        subprocess.run(["icacls", str(target), "/remove:d", "*S-1-1-0", "/T", "/Q"],
                       check=True, capture_output=True, text=True)
    else:
        for path in [target, *target.rglob("*")]:
            if not path.is_symlink():
                path.chmod(path.stat().st_mode | stat.S_IWUSR)


def prepare_checkout(repo_root: str, *, job_id, code_commit: str = "",
                     checkout_root: str = "", remote: str = "origin", branch: str = "unstable") -> tuple[str, str]:
    repo, root = cache_root(repo_root, checkout_root)
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
        target = root / "releases" / commit
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            git(repo, "worktree", "add", "--detach", str(target), commit)
            git(repo, "worktree", "lock", "--reason", "Hub shared MATLAB release " + commit, str(target))
        if git(target, "rev-parse", "HEAD") != commit:
            raise RuntimeError("MATLAB job checkout has an unexpected commit.")
        if git(target, "status", "--porcelain", "--untracked-files=all", "--ignored"):
            raise RuntimeError("MATLAB shared release has modified or extra files; preserved for inspection.")
        # Submodules require explicit packaging; do not silently execute missing code.
        if (target / ".gitmodules").exists():
            raise RuntimeError("MATLAB job checkout contains submodules; configure an explicit runtime package.")
        protection = root / "protection" / (commit + ".json")
        if not protection.exists():
            protect_release(target)
            protection.parent.mkdir(exist_ok=True)
            protection.write_text(json.dumps({"schema": "readonly-release-v1", "path": str(target)}), encoding="utf-8")
        (root / "usage").mkdir(exist_ok=True)
        (root / "usage" / commit).touch()
    return str(target), commit


def cleanup_cache(session, settings, *, apply=False, retention_days=30):
    """Conservative GC: failed/cancelled jobs remain resumable indefinitely.

    Serialized with claims via the target row and with worktree preparation via
    the cache lock. Historical per-job worktrees are eligible by the same rules.
    Job work directories/logs are retained; this GC only reclaims code copies.
    """
    from sqlalchemy import select
    from api.models import Job, ExecutionTarget, SystemSetting
    repo, root = cache_root(settings.matlab_repo_root,
                            getattr(settings, "matlab_job_checkout_root", ""))
    if retention_days < 1:
        raise ValueError("Retention must be at least one day.")
    # All target rows: unassigned jobs can be claimed on either machine.
    session.scalars(select(ExecutionTarget).order_by(ExecutionTarget.id).with_for_update()).all()
    protected = set()
    paths_in_use = set()
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    release = session.get(SystemSetting, "matlab_code_release")
    if release:
        protected.add((release.value_json or {}).get("code_commit"))
    for job in session.scalars(select(Job).where(Job.status != "done")).all():
        if (job.params_json or {}).get("job_kind") not in {"pipeline_run", "legacy_matlab"}:
            continue
        execution = (job.params_json or {}).get("execution") or {}
        runtime = (job.result_json or {}).get("worker_runtime") or {}
        pin = execution.get("code_commit") or runtime.get("code_commit")
        if job.status in {"queued", "running", "cancelling"} and not pin:
            return {"removed": [], "skipped": "An unfinished unpinned job may use any source checkout."}
        if pin:
            protected.add(pin)
        if runtime.get("repo_root"):
            paths_in_use.add(str(Path(runtime["repo_root"]).resolve()))
    # Completed jobs inside retention also protect their version.
    for job in session.scalars(select(Job).where(Job.status == "done")).all():
        when = job.finished_at or job.updated_at or job.created_at
        if not when or when >= cutoff:
            protected.add(((job.params_json or {}).get("execution") or {}).get("code_commit"))
    removed = []
    with preparation_lock(root):
        entries = git(repo, "worktree", "list", "--porcelain").split("\n\n")
        for entry in entries:
            lines = entry.splitlines()
            fields = dict(line.split(" ", 1) for line in lines if " " in line)
            path = Path(fields.get("worktree", str(repo))).resolve()
            commit = fields.get("HEAD")
            if path == repo or root not in path.parents or commit in protected or str(path) in paths_in_use:
                continue
            # Only directories managed by this launcher, never unrelated worktrees.
            if not (path.parent == root / "releases" and path.name == commit or
                    path.parent == root and path.name.startswith(str(commit) + "-")):
                continue
            usage = root / "usage" / str(commit)
            if not path.exists() or datetime.fromtimestamp(max(path.stat().st_mtime,
                    usage.stat().st_mtime if usage.exists() else 0), timezone.utc) >= cutoff:
                continue
            if git(path, "status", "--porcelain", "--untracked-files=all", "--ignored"):
                continue  # Preserve any local modification, including untracked data.
            if apply:
                unprotect_release(path)
                if "locked" in entry:
                    git(repo, "worktree", "unlock", str(path))
                try:
                    git(repo, "worktree", "remove", str(path))
                    protection = root / "protection" / (str(commit) + ".json")
                    if path.parent == root / "releases" and protection.exists():
                        protection.unlink()
                except Exception:
                    if path.exists():
                        protect_release(path)
                        git(repo, "worktree", "lock", str(path))
                    raise
            removed.append(str(path))
    return {"removed" if apply else "eligible": removed}


@contextmanager
def job_workspace(settings, job, session):
    from uuid import UUID
    _, root = cache_root(settings.matlab_repo_root, getattr(settings, "matlab_job_checkout_root", ""))
    path = root / "work" / str(UUID(str(job.id)))
    path.mkdir(parents=True, exist_ok=True)
    # Unique attempt directory preserves earlier logs/results when a job retries.
    from uuid import uuid4
    attempt = path / str(uuid4())
    attempt.mkdir()
    result = dict(job.result_json or {})
    runtime = dict(result.get("worker_runtime") or {})
    runtime.update({"work_dir": str(attempt), "stdout_log": str(attempt / "matlab_stdout.log"),
                    "stderr_log": str(attempt / "matlab_stderr.log")})
    result["worker_runtime"] = runtime
    job.result_json = result
    session.commit()
    try:
        yield str(attempt)
    finally:
        if os.name != "nt":
            short_temp = _short_matlab_temp_path(attempt)
            if short_temp.is_symlink() and short_temp.resolve() == (attempt / "tmp").resolve():
                short_temp.unlink()


def _short_matlab_temp_path(work_dir):
    return Path("/tmp/dd-matlab") / Path(work_dir).name


def job_environment(work_dir):
    temp = Path(work_dir) / "tmp"
    temp.mkdir(exist_ok=True)
    temp_path = temp
    if os.name != "nt":
        # R2024b can exit silently before -batch starts when TMPDIR contains
        # the full job/attempt UUID path. The short alias keeps files in the
        # persistent attempt directory without shortening its real location.
        temp_path = _short_matlab_temp_path(work_dir)
        temp_path.parent.mkdir(mode=0o700, exist_ok=True)
        short_root = temp_path.parent.stat()
        if short_root.st_uid != os.getuid() or short_root.st_mode & 0o077:
            raise RuntimeError("MATLAB temporary alias directory must be private to the worker user")
        temp_path.symlink_to(temp.resolve(), target_is_directory=True)
    return {**os.environ, "TMPDIR": str(temp_path), "TMP": str(temp_path), "TEMP": str(temp_path),
            "PYTHONDONTWRITEBYTECODE": "1"}


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
    try:
        cleanup_cache(session, settings, apply=True)
        session.commit()
    except Exception:
        session.rollback()
        import logging
        logging.getLogger(__name__).exception("MATLAB cache cleanup skipped; job preparation retained")
    return repo_root, commit


if __name__ == "__main__":
    import argparse
    import json
    from uuid import uuid4
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--commit")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--checkout-root", default="")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default="unstable")
    args = parser.parse_args()
    if args.cleanup:
        from api.db import SessionLocal
        from types import SimpleNamespace
        with SessionLocal() as session:
            print(json.dumps(cleanup_cache(session, SimpleNamespace(matlab_repo_root=args.repo_root,
                matlab_job_checkout_root=args.checkout_root), apply=args.apply)))
            session.commit()
        raise SystemExit(0)
    if not args.commit:
        parser.error("--commit is required unless --cleanup is selected")
    path, commit = prepare_checkout(args.repo_root, job_id=uuid4(), code_commit=args.commit,
                                   checkout_root=args.checkout_root, remote=args.remote, branch=args.branch)
    print(json.dumps({"repo_root": path, "code_commit": commit}))
