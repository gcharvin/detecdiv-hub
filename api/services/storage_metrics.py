from __future__ import annotations

from pathlib import Path


def safe_file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def safe_dir_size(path: Path) -> int:
    total = 0
    try:
        for item in path.rglob("*"):
            if item.is_file():
                total += safe_file_size(item)
    except OSError:
        return total
    return total
