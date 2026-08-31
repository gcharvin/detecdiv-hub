from __future__ import annotations

import shutil
from math import ceil
from collections.abc import Callable, Iterable
from pathlib import Path


DiskUsageReader = Callable[[str], object]


def collect_disk_usage(
    paths: Iterable[str],
    *,
    warning_threshold_percent: int = 95,
    usage_reader: DiskUsageReader = shutil.disk_usage,
) -> list[dict]:
    """Return disk pressure information for mounted paths visible to the API."""
    threshold = min(100, max(1, int(warning_threshold_percent)))
    results: list[dict] = []
    seen_paths: set[str] = set()

    for raw_path in paths:
        path = str(raw_path or "").strip()
        if not path or path in seen_paths:
            continue
        seen_paths.add(path)

        try:
            usage = usage_reader(path)
            total_bytes = int(getattr(usage, "total"))
            used_bytes = int(getattr(usage, "used"))
            free_bytes = int(getattr(usage, "free"))
        except (FileNotFoundError, NotADirectoryError):
            # Optional mounts such as /data are absent in some dev/test environments.
            continue
        except OSError as exc:
            results.append(
                {
                    "path": path,
                    "label": disk_label(path),
                    "status": "error",
                    "message": str(exc),
                }
            )
            continue

        usable_bytes = used_bytes + free_bytes
        # Match GNU df: filesystem percentages are rounded up to the next integer.
        used_percent = ceil((used_bytes / usable_bytes) * 100) if usable_bytes > 0 else 0
        results.append(
            {
                "path": path,
                "label": disk_label(path),
                "total_bytes": total_bytes,
                "used_bytes": used_bytes,
                "free_bytes": free_bytes,
                "used_percent": used_percent,
                "status": "warning" if used_percent >= threshold else "ok",
            }
        )

    return results


def configured_disk_paths(value: str) -> list[str]:
    return [path.strip() for path in str(value or "").split(",") if path.strip()]


def disk_label(path: str) -> str:
    normalized = Path(path).as_posix().rstrip("/") or "/"
    if normalized == "/":
        return "VM system disk"
    if normalized == "/data":
        return "Shared data storage"
    return f"Storage {path}"
