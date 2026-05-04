from __future__ import annotations

import hashlib
import os
import subprocess
from functools import lru_cache
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _should_include_fingerprint_path(path: Path) -> bool:
    if any(part == "__pycache__" for part in path.parts):
        return False
    return path.suffix not in {".pyc", ".pyo"}


def _iter_fingerprint_paths(root: Path) -> list[Path]:
    candidates = [
        root / "api",
        root / "worker",
        root / "db" / "schema.sql",
        root / "pyproject.toml",
    ]
    paths: list[Path] = []
    for candidate in candidates:
        if candidate.is_dir():
            paths.extend(
                sorted(
                    path
                    for path in candidate.rglob("*")
                    if path.is_file() and _should_include_fingerprint_path(path)
                )
            )
        elif candidate.is_file():
            if _should_include_fingerprint_path(candidate):
                paths.append(candidate)
    return paths


def _compute_code_fingerprint(root: Path) -> str:
    digest = hashlib.sha1()
    for path in _iter_fingerprint_paths(root):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(relative)
        try:
            data = path.read_bytes()
        except OSError:
            continue
        digest.update(b"\0")
        digest.update(data)
        digest.update(b"\0")
    return digest.hexdigest()[:12]


def _git_head(root: Path) -> str | None:
    git_dir = root / ".git"
    if not git_dir.exists():
        return None
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    value = str(completed.stdout or "").strip()
    return value or None


@lru_cache(maxsize=1)
def get_runtime_version_info() -> dict[str, str]:
    configured = str(os.environ.get("DETECDIV_HUB_DEPLOYMENT_VERSION") or "").strip()
    if configured:
        return {
            "deployment_version": configured,
            "version_source": "env",
            "code_fingerprint": _compute_code_fingerprint(_repo_root()),
        }

    root = _repo_root()
    git_head = _git_head(root)
    if git_head:
        return {
            "deployment_version": git_head,
            "version_source": "git",
            "code_fingerprint": _compute_code_fingerprint(root),
        }

    fingerprint = _compute_code_fingerprint(root)
    return {
        "deployment_version": f"fp-{fingerprint}",
        "version_source": "fingerprint",
        "code_fingerprint": fingerprint,
    }
