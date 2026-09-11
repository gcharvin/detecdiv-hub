from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import case
from sqlalchemy.orm import Session

from api.models import Job, SystemSetting
from api.services.raw_preview_settings import ensure_system_settings_table


JOB_PRIORITY_SETTING_KEY = "job_priority_settings"
MIN_JOB_PRIORITY = 0
MAX_JOB_PRIORITY = 10_000

# Lower numbers are claimed first. This registry is shared by the worker, API,
# and admin UI so scheduling classes have one source of truth.
JOB_PRIORITY_DEFINITIONS: tuple[tuple[str, str, int], ...] = (
    ("pipeline_run", "Pipeline run", 10),
    ("project_deletion", "Project deletion", 20),
    ("raw_dataset_deletion", "Raw dataset deletion", 20),
    ("raw_dataset_position_deletion", "Raw position deletion", 20),
    ("project_indexing", "Project indexing", 30),
    ("restore_raw_dataset", "Raw archive restore", 30),
    ("misc_storage_inventory", "Storage inventory", 30),
    ("restore_raw_dataset_from_backup", "Raw backup restore", 50),
    ("restore_project_from_backup", "Project backup restore", 50),
    ("prepare_user_home_storage", "Prepare user storage", 80),
    ("micromanager_post_ingest", "Micro-Manager post-ingest", 90),
    ("raw_preview_video", "Raw preview video", 100),
    ("external_eln_sync", "External ELN sync", 100),
    ("generic", "Generic / unknown job", 100),
    ("archive_raw_dataset", "Raw dataset archive", 150),
    ("init_backup_repo", "Initialize backup repository", 200),
    ("backup_raw_dataset", "Raw dataset backup", 200),
    ("backup_project", "Project backup", 200),
    ("list_snapshot_dir", "List backup snapshot", 200),
)


@dataclass(frozen=True)
class JobPriorityRuntimeConfig:
    priorities: dict[str, int]

    def priority_for(self, job_kind: str | None, *, requested_priority: int) -> int:
        normalized_kind = str(job_kind or "generic").strip() or "generic"
        return int(self.priorities.get(normalized_kind, requested_priority))


def default_job_priorities() -> dict[str, int]:
    return {key: default for key, _label, default in JOB_PRIORITY_DEFINITIONS}


def resolve_job_priority_runtime_config(session: Session) -> JobPriorityRuntimeConfig:
    ensure_system_settings_table(session)
    priorities = default_job_priorities()
    entry = session.get(SystemSetting, JOB_PRIORITY_SETTING_KEY)
    if entry is not None:
        payload = dict(entry.value_json or {})
        stored = payload.get("priorities")
        if isinstance(stored, dict):
            for key in priorities:
                if key in stored:
                    priorities[key] = normalize_job_priority(stored[key], default=priorities[key])
    return JobPriorityRuntimeConfig(priorities=priorities)


def update_job_priority_runtime_config(
    session: Session,
    *,
    updates: dict[str, int],
) -> JobPriorityRuntimeConfig:
    current = resolve_job_priority_runtime_config(session)
    priorities = dict(current.priorities)
    unknown = sorted(set(updates) - set(priorities))
    if unknown:
        raise ValueError(f"Unknown job priority type(s): {', '.join(unknown)}")
    for key, value in updates.items():
        priorities[key] = normalize_job_priority(value, default=priorities[key], strict=True)

    entry = session.get(SystemSetting, JOB_PRIORITY_SETTING_KEY)
    payload = {"priorities": priorities}
    if entry is None:
        session.add(SystemSetting(key=JOB_PRIORITY_SETTING_KEY, value_json=payload))
    else:
        entry.value_json = payload
    session.flush()
    return JobPriorityRuntimeConfig(priorities=priorities)


def effective_job_priority_expression(config: JobPriorityRuntimeConfig):
    job_kind = Job.params_json["job_kind"].as_string()
    rules = [
        (job_kind == key, priority)
        for key, priority in config.priorities.items()
        if key != "generic"
    ]
    rules.append((job_kind.is_(None), config.priorities["generic"]))
    return case(*rules, else_=Job.priority)


def job_priority_settings_items(config: JobPriorityRuntimeConfig) -> list[dict]:
    return [
        {
            "job_kind": key,
            "label": label,
            "priority": config.priorities[key],
            "default_priority": default,
        }
        for key, label, default in JOB_PRIORITY_DEFINITIONS
    ]


def normalize_job_priority(value: object, *, default: int, strict: bool = False) -> int:
    try:
        priority = int(value)
    except (TypeError, ValueError):
        if strict:
            raise ValueError(f"Priority must be an integer between {MIN_JOB_PRIORITY} and {MAX_JOB_PRIORITY}")
        return default
    if priority < MIN_JOB_PRIORITY or priority > MAX_JOB_PRIORITY:
        if strict:
            raise ValueError(f"Priority must be between {MIN_JOB_PRIORITY} and {MAX_JOB_PRIORITY}")
        return default
    return priority
