"""Translate canonical catalog paths into paths visible to one worker host."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any


@dataclass(frozen=True)
class WorkerPathMapping:
    source: str
    target: str


def parse_worker_path_mappings(value: str) -> tuple[WorkerPathMapping, ...]:
    if not str(value or "").strip():
        return ()
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("DETECDIV_HUB_WORKER_PATH_MAPPINGS must be valid JSON") from exc
    if not isinstance(parsed, list):
        raise TypeError("DETECDIV_HUB_WORKER_PATH_MAPPINGS must be a JSON list")

    mappings: list[WorkerPathMapping] = []
    for item in parsed:
        if not isinstance(item, dict):
            raise TypeError("Each worker path mapping must have source and target strings")
        source = _trim_prefix(str(item.get("source") or "").strip())
        target = _trim_prefix(str(item.get("target") or "").strip())
        if not source or not target:
            raise ValueError("Each worker path mapping needs nonempty source and target")
        if source == "/":
            raise ValueError("Worker path mapping source must name a specific storage root")
        if not _is_absolute_path(source) or not _is_absolute_path(target):
            raise ValueError("Worker path mapping source and target must be absolute paths")
        mappings.append(WorkerPathMapping(source=source, target=target))
    return tuple(sorted(mappings, key=lambda mapping: len(mapping.source), reverse=True))


def map_worker_path(path_text: str, mappings: tuple[WorkerPathMapping, ...]) -> str:
    candidate = str(path_text or "")
    normalized_candidate = candidate.replace("\\", "/")
    for mapping in mappings:
        normalized_source = mapping.source.replace("\\", "/").rstrip("/")
        match_candidate = (
            normalized_candidate.casefold()
            if _is_windows_path(mapping.source)
            else normalized_candidate
        )
        match_source = (
            normalized_source.casefold()
            if _is_windows_path(mapping.source)
            else normalized_source
        )
        if match_candidate == match_source:
            suffix = ""
        elif match_candidate.startswith(f"{match_source}/"):
            suffix = normalized_candidate[len(normalized_source) + 1 :]
        else:
            continue
        parts = [part for part in suffix.split("/") if part and part != "."]
        if ".." in parts:
            raise ValueError(f"Worker path mapping rejects parent traversal: {path_text}")
        path_type = PureWindowsPath if _is_windows_path(mapping.target) else PurePosixPath
        return str(path_type(mapping.target).joinpath(*parts))
    return candidate


def map_payload_path_fields(
    value: Any, mappings: tuple[WorkerPathMapping, ...], *, key: str = ""
) -> Any:
    if not mappings:
        return value
    if isinstance(value, dict):
        return {
            item_key: map_payload_path_fields(item_value, mappings, key=str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [map_payload_path_fields(item, mappings, key=key) for item in value]
    if isinstance(value, str) and _is_path_field(key):
        return map_worker_path(value, mappings)
    return value


def _is_windows_path(path_text: str) -> bool:
    return bool(re.match(r"^[A-Za-z]:[/\\]", path_text)) or path_text.startswith(("\\\\", "//"))


def _is_absolute_path(path_text: str) -> bool:
    return path_text.startswith("/") or _is_windows_path(path_text)


def _is_path_field(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in ("path", "file", "directory", "folder", "uri"))


def _trim_prefix(path_text: str) -> str:
    if path_text == "/":
        return path_text
    if re.fullmatch(r"[A-Za-z]:[/\\]+", path_text):
        return path_text[:2] + "\\"
    return path_text.rstrip("/\\")
