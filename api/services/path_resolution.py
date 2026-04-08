from __future__ import annotations

import re
from pathlib import PurePosixPath, PureWindowsPath


WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")


def compose_storage_path(root_prefix: str, relative_path: str | None = None, leaf_name: str | None = None) -> str:
    """Compose a catalog storage path without converting between OS path styles."""
    root = str(root_prefix or "").strip()
    if not root:
        return ""

    parts = [part for part in (relative_path, leaf_name) if str(part or "").strip()]
    if not parts:
        return root

    path_cls = PureWindowsPath if is_windows_style_path(root) else PurePosixPath
    path = path_cls(root)
    for part in parts:
        path = path.joinpath(*split_relative_parts(str(part)))
    return str(path)


def is_windows_style_path(path: str) -> bool:
    return bool(WINDOWS_DRIVE_RE.match(path)) or "\\" in path


def split_relative_parts(path: str) -> list[str]:
    normalized = path.strip().replace("\\", "/").strip("/")
    if not normalized:
        return []
    return [part for part in normalized.split("/") if part and part != "."]
