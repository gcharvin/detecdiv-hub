from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

LOGGER = logging.getLogger("detecdiv-hub")

RESTIC_CMD = os.environ.get("RESTIC_CMD", "restic")


@dataclass
class ResticSnapshot:
    snapshot_id: str
    hostname: str
    tags: list[str]
    paths: list[str]
    time: str
    summary: dict = field(default_factory=dict)


@dataclass
class ResticFileEntry:
    path: str
    kind: str  # "file" or "dir"
    size: int
    mtime: str


class ResticError(RuntimeError):
    pass


def _run_restic(args: list[str], *, repo: str, passphrase: str, timeout: int = 3600) -> str:
    env = {**os.environ, "RESTIC_REPOSITORY": repo, "RESTIC_PASSWORD": passphrase}
    cmd = [RESTIC_CMD] + args
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise ResticError(f"restic timed out after {timeout}s: {' '.join(cmd)}") from exc
    except FileNotFoundError as exc:
        raise ResticError(f"restic not found at '{RESTIC_CMD}'. Install restic on detecdiv-server.") from exc
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise ResticError(f"restic {args[0]} failed (rc={result.returncode}): {stderr}")
    return result.stdout


def init_repo(repo: str, passphrase: str) -> str:
    return _run_restic(["init"], repo=repo, passphrase=passphrase, timeout=60)


def repo_is_initialized(repo: str, passphrase: str) -> bool:
    try:
        _run_restic(["snapshots", "--json", "--last"], repo=repo, passphrase=passphrase, timeout=30)
        return True
    except ResticError:
        return False


def backup(
    *,
    repo: str,
    passphrase: str,
    source_path: str,
    tags: list[str],
    include_patterns: list[str] | None = None,
    timeout: int = 7200,
) -> dict:
    """Run a backup and return the summary dict from restic JSON output."""
    tag_args = []
    for t in tags:
        tag_args += ["--tag", t]
    include_args = []
    for p in (include_patterns or []):
        include_args += ["--include", p]
    stdout = _run_restic(
        ["backup", source_path, "--json"] + tag_args + include_args,
        repo=repo,
        passphrase=passphrase,
        timeout=timeout,
    )
    # restic outputs one JSON object per line; the last meaningful line is the summary
    summary: dict = {}
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("message_type") == "summary":
            summary = obj
    return summary


def snapshots(
    *,
    repo: str,
    passphrase: str,
    tags: list[str] | None = None,
) -> list[ResticSnapshot]:
    args = ["snapshots", "--json"]
    if tags:
        for t in tags:
            args += ["--tag", t]
    stdout = _run_restic(args, repo=repo, passphrase=passphrase, timeout=60)
    raw_list = json.loads(stdout or "[]")
    result = []
    for item in (raw_list or []):
        if not isinstance(item, dict):
            continue
        result.append(ResticSnapshot(
            snapshot_id=item.get("short_id") or item.get("id", "")[:8],
            hostname=item.get("hostname", ""),
            tags=item.get("tags") or [],
            paths=item.get("paths") or [],
            time=item.get("time", ""),
            summary=item,
        ))
    return result


def list_files(
    *,
    repo: str,
    passphrase: str,
    snapshot_id: str,
) -> list[ResticFileEntry]:
    stdout = _run_restic(
        ["ls", snapshot_id, "--json"],
        repo=repo,
        passphrase=passphrase,
        timeout=120,
    )
    entries: list[ResticFileEntry] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        # restic ls --json emits one JSON object per file/dir
        struct_type = obj.get("struct_type", "")
        if struct_type == "snapshot":
            continue
        entries.append(ResticFileEntry(
            path=obj.get("path", ""),
            kind="dir" if obj.get("type") == "dir" else "file",
            size=int(obj.get("size") or 0),
            mtime=obj.get("mtime", ""),
        ))
    return entries


def restore(
    *,
    repo: str,
    passphrase: str,
    snapshot_id: str,
    target_dir: str,
    include_paths: list[str] | None = None,
    timeout: int = 7200,
) -> str:
    """Restore snapshot (or specific paths) into target_dir.

    restic restores preserving the full absolute path under target_dir.
    E.g. if the original file was /data/projects/Foo/bar.mat and target_dir is /tmp/restore,
    the result lands at /tmp/restore/data/projects/Foo/bar.mat.
    Pass target_dir="/" to restore in-place (careful!).
    """
    args = ["restore", snapshot_id, "--target", target_dir]
    for p in (include_paths or []):
        args += ["--include", p]
    return _run_restic(args, repo=repo, passphrase=passphrase, timeout=timeout)


def check_repo(*, repo: str, passphrase: str) -> str:
    return _run_restic(["check"], repo=repo, passphrase=passphrase, timeout=300)


def forget_and_prune(
    *,
    repo: str,
    passphrase: str,
    keep_last: int = 10,
    tags: list[str] | None = None,
) -> str:
    args = ["forget", "--prune", f"--keep-last={keep_last}"]
    for t in (tags or []):
        args += ["--tag", t]
    return _run_restic(args, repo=repo, passphrase=passphrase, timeout=600)


def raw_dataset_tags(raw_dataset_id: str) -> list[str]:
    return ["type:raw_dataset", f"id:{raw_dataset_id}"]


def project_tags(project_id: str) -> list[str]:
    return ["type:project", f"id:{project_id}"]


def ensure_repo_initialized(repo: str, passphrase: str) -> None:
    """Initialize the restic repo if it doesn't exist yet."""
    if not passphrase:
        raise ResticError("Backup passphrase is not configured. Set it in Admin → Backup settings.")
    repo_path = Path(repo)
    if not repo_path.exists():
        repo_path.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Created backup repo directory: %s", repo)
    if not repo_is_initialized(repo, passphrase):
        LOGGER.info("Initializing restic repo at %s", repo)
        init_repo(repo, passphrase)
        LOGGER.info("Restic repo initialized at %s", repo)
