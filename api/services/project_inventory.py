from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class ProjectInventory:
    classifier_count: int = 0
    processor_count: int = 0
    pipeline_run_count: int = 0
    run_json_count: int = 0
    h5_count: int = 0
    h5_bytes: int = 0
    latest_run_status: str | None = None
    latest_run_at: datetime | None = None
    pipeline_runs: list[dict] | None = None
    classifier_runs: list[dict] | None = None
    processor_runs: list[dict] | None = None
    top_level_entries: list[str] | None = None

    def metadata_json(self) -> dict:
        return {
            "classifier_count": self.classifier_count,
            "processor_count": self.processor_count,
            "pipeline_run_count": self.pipeline_run_count,
            "run_json_count": self.run_json_count,
            "h5_count": self.h5_count,
            "h5_bytes": self.h5_bytes,
            "latest_run_status": self.latest_run_status,
            "latest_run_at": self.latest_run_at.isoformat() if self.latest_run_at else None,
            "pipeline_runs": serialize_json_value(self.pipeline_runs or []),
            "classifier_runs": serialize_json_value(self.classifier_runs or []),
            "processor_runs": serialize_json_value(self.processor_runs or []),
            "top_level_entries": self.top_level_entries or [],
        }


def inspect_project_directory(project_dir: Path) -> ProjectInventory:
    project_dir = Path(project_dir)
    try:
        top_level_entries = sorted(entry.name for entry in project_dir.iterdir()) if project_dir.is_dir() else []
    except OSError:
        top_level_entries = []
    inventory = ProjectInventory(
        classifier_count=count_child_dirs(project_dir / "classification"),
        processor_count=count_child_dirs(project_dir / "processor"),
        top_level_entries=top_level_entries,
    )

    h5_count = 0
    h5_bytes = 0
    for h5_path in project_dir.rglob("*.h5"):
        if not h5_path.is_file():
            continue
        h5_count += 1
        try:
            h5_bytes += h5_path.stat().st_size
        except OSError:
            continue
    inventory.h5_count = h5_count
    inventory.h5_bytes = h5_bytes

    pipeline_runs = collect_run_summaries(project_dir / "pipeline")
    classifier_runs = collect_classifier_or_processor_runs(project_dir / "classification")
    processor_runs = collect_classifier_or_processor_runs(project_dir / "processor")

    inventory.pipeline_runs = pipeline_runs
    inventory.classifier_runs = classifier_runs
    inventory.processor_runs = processor_runs
    inventory.pipeline_run_count = len(pipeline_runs)
    inventory.run_json_count = len(pipeline_runs) + len(classifier_runs) + len(processor_runs)

    latest = latest_run_record(pipeline_runs + classifier_runs + processor_runs)
    if latest is not None:
        inventory.latest_run_status = latest.get("status")
        inventory.latest_run_at = latest.get("timestamp")

    return inventory


def count_child_dirs(path: Path) -> int:
    if not path.is_dir():
        return 0
    try:
        return sum(1 for child in path.iterdir() if child.is_dir())
    except OSError:
        return 0


def collect_run_summaries(pipeline_dir: Path) -> list[dict]:
    if not pipeline_dir.is_dir():
        return []

    summaries: list[dict] = []
    for run_dir in sorted(pipeline_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        run_json_path = run_dir / "run.json"
        if not run_json_path.is_file():
            continue
        summary = parse_run_json(run_json_path, kind="pipeline")
        if summary is not None:
            summaries.append(summary)
    return summaries


def collect_classifier_or_processor_runs(base_dir: Path) -> list[dict]:
    if not base_dir.is_dir():
        return []

    summaries: list[dict] = []
    for run_json_path in sorted(base_dir.rglob("runs/*/run.json")):
        if not run_json_path.is_file():
            continue
        kind = "classification" if "classification" in run_json_path.parts else "processor"
        summary = parse_run_json(run_json_path, kind=kind)
        if summary is not None:
            summaries.append(summary)
    return summaries


def parse_run_json(run_json_path: Path, *, kind: str) -> dict | None:
    try:
        payload = json.loads(run_json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    timestamp = parse_run_timestamp(payload)
    return {
        "kind": kind,
        "run_json_path": str(run_json_path),
        "run_dir": str(run_json_path.parent),
        "status": run_status(payload, kind=kind),
        "timestamp": timestamp,
        "pipeline_id": nested_value(payload, ["pipelineRef", "id"]) or payload.get("strid"),
        "pipeline_path": nested_value(payload, ["pipelineRef", "path"]) or payload.get("path"),
        "tag": payload.get("tag"),
        "fun": payload.get("fun"),
        "summary": nested_value(payload, ["outputs", "report", "summary"]) or {},
    }


def parse_run_timestamp(payload: dict) -> datetime | None:
    for key in ("updatedAt", "finishedAt", "createdAt", "timestamp"):
        value = payload.get(key)
        parsed = parse_datetime_guess(value)
        if parsed is not None:
            return parsed
    return None


def parse_datetime_guess(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None

    formats = (
        "%d-%b-%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    )
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def run_status(payload: dict, *, kind: str) -> str:
    explicit = payload.get("status")
    if explicit:
        return str(explicit)

    if kind == "pipeline":
        summary = nested_value(payload, ["outputs", "report", "summary"]) or {}
        if summary:
            failed = int(summary.get("failedNodes", 0) or 0)
            done = int(summary.get("doneNodes", 0) or 0)
            total = int(summary.get("totalNodes", 0) or 0)
            if failed > 0:
                return "failed"
            if total > 0 and done >= total:
                return "done"
            if done > 0:
                return "partial"
        return "unknown"

    if payload.get("error") or payload.get("exception"):
        return "failed"
    return "done"


def latest_run_record(records: list[dict]) -> dict | None:
    dated = [record for record in records if record.get("timestamp") is not None]
    if not dated:
        return None
    return max(dated, key=lambda item: item["timestamp"])


def nested_value(payload: dict, path: list[str]):
    current = payload
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def serialize_json_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [serialize_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize_json_value(item) for key, item in value.items()}
    return value
