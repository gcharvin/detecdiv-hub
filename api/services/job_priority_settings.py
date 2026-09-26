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
    ("legacy_matlab", "Legacy MATLAB", 10),
    ("project_deletion", "Project deletion", 20),
    ("raw_dataset_deletion", "Raw dataset deletion", 20),
    ("raw_dataset_position_deletion", "Raw position deletion", 20),
    ("project_indexing", "Project indexing", 30),
    ("storage_optimization", "TIFF storage optimization", 200),
    ("restore_raw_dataset", "Raw archive restore", 30),
    ("misc_storage_inventory", "Storage inventory", 30),
    ("restore_raw_dataset_from_backup", "Raw backup restore", 50),
    ("restore_project_from_backup", "Project backup restore", 50),
    ("prepare_user_home_storage", "Prepare user storage", 80),
    ("micromanager_ingest", "Micro-Manager ingestion", 90),
    ("micromanager_post_ingest", "Micro-Manager post-ingest", 90),
    ("raw_preview_video", "Raw preview video", 100),
    ("external_eln_sync", "External ELN sync", 100),
    ("labguru_yeast_strain_sync", "Labguru yeast sync", 100),
    ("assistant_service_control", "Qwen service control", 100),
    ("generic", "Generic / unknown job", 100),
    ("archive_raw_dataset", "Raw dataset archive", 150),
    ("init_backup_repo", "Initialize backup repository", 200),
    ("backup_raw_dataset", "Raw dataset backup", 200),
    ("backup_project", "Project backup", 200),
    ("list_snapshot_dir", "List backup snapshot", 200),
)

# These internal steps are presented as one scheduling class in the admin UI:
# users should be able to prioritize a dataset's scan and its follow-up chunks
# together without exposing implementation-level jobs as separate settings.
JOB_PRIORITY_KIND_ALIASES: dict[str, tuple[str, ...]] = {
    "storage_optimization": (
        "storage_optimization",
        "storage_optimization_scan",
        "storage_optimization_chunk",
    ),
}

JOB_RESOURCE_SETTING_KEY = "job_resource_settings"
MAX_JOB_CPU_CORES = 36
MAX_DISK_IO_UNITS = 36
MAX_GPU_VRAM_MB = 262_144

# Disk units are relative admission weights, not a throughput promise in MB/s.
# A capacity of 8 is a cautious starting point until storage measurements exist.
JOB_RESOURCE_DEFAULTS: dict[str, tuple[int, bool, int, int]] = {
    "pipeline_run": (8, True, 0, 3),
    "legacy_matlab": (8, False, 0, 3),
    "project_deletion": (1, False, 0, 2),
    "raw_dataset_deletion": (1, False, 0, 2),
    "raw_dataset_position_deletion": (1, False, 0, 1),
    "project_indexing": (1, False, 0, 2),
    "storage_optimization": (1, False, 0, 2),
    "restore_raw_dataset": (1, False, 0, 3),
    "misc_storage_inventory": (1, False, 0, 2),
    "restore_raw_dataset_from_backup": (2, False, 0, 3),
    "restore_project_from_backup": (2, False, 0, 3),
    "prepare_user_home_storage": (1, False, 0, 1),
    "micromanager_ingest": (1, False, 0, 2),
    "micromanager_post_ingest": (1, False, 0, 2),
    "raw_preview_video": (2, False, 0, 3),
    "external_eln_sync": (1, False, 0, 0),
    "labguru_yeast_strain_sync": (1, False, 0, 0),
    "assistant_service_control": (1, False, 0, 0),
    "generic": (1, False, 0, 1),
    "archive_raw_dataset": (1, False, 0, 3),
    "init_backup_repo": (1, False, 0, 1),
    "backup_raw_dataset": (2, False, 0, 3),
    "backup_project": (2, False, 0, 3),
    "list_snapshot_dir": (1, False, 0, 1),
}

JOB_RESOURCE_KIND_ALIASES: dict[str, tuple[str, ...]] = {
    "storage_optimization": (
        "storage_optimization",
        "storage_optimization_scan",
        "storage_optimization_chunk",
    ),
}


@dataclass(frozen=True)
class JobResourceProfile:
    cpu_cores: int
    gpu_enabled: bool
    gpu_vram_mb: int
    disk_io_units: int


@dataclass(frozen=True)
class JobResourceRuntimeConfig:
    profiles: dict[str, JobResourceProfile]
    cpu_capacity_cores: int
    gpu_vram_capacity_mb: int
    disk_io_capacity_units: int


@dataclass(frozen=True)
class JobPriorityRuntimeConfig:
    priorities: dict[str, int]

    def priority_for(self, job_kind: str | None, *, requested_priority: int) -> int:
        normalized_kind = str(job_kind or "generic").strip() or "generic"
        for setting_key, aliases in JOB_PRIORITY_KIND_ALIASES.items():
            if normalized_kind in aliases:
                normalized_kind = setting_key
                break
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


def default_job_resource_profiles() -> dict[str, JobResourceProfile]:
    return {
        key: JobResourceProfile(
            cpu_cores=values[0],
            gpu_enabled=values[1],
            gpu_vram_mb=values[2],
            disk_io_units=values[3],
        )
        for key, values in JOB_RESOURCE_DEFAULTS.items()
    }


def resolve_job_resource_runtime_config(session: Session) -> JobResourceRuntimeConfig:
    ensure_system_settings_table(session)
    profiles = default_job_resource_profiles()
    cpu_capacity_cores = MAX_JOB_CPU_CORES
    gpu_vram_capacity_mb = 0
    disk_io_capacity_units = 8

    entry = session.get(SystemSetting, JOB_RESOURCE_SETTING_KEY)
    if entry is not None:
        payload = dict(entry.value_json or {})
        stored_profiles = payload.get("profiles")
        if isinstance(stored_profiles, dict):
            for key, stored in stored_profiles.items():
                if key not in profiles or not isinstance(stored, dict):
                    continue
                default = profiles[key]
                profiles[key] = JobResourceProfile(
                    cpu_cores=normalize_bounded_int(stored.get("cpu_cores"), default=default.cpu_cores, minimum=1, maximum=MAX_JOB_CPU_CORES),
                    gpu_enabled=normalize_bool(stored.get("gpu_enabled"), default=default.gpu_enabled),
                    gpu_vram_mb=normalize_bounded_int(stored.get("gpu_vram_mb"), default=default.gpu_vram_mb, minimum=0, maximum=MAX_GPU_VRAM_MB),
                    disk_io_units=normalize_bounded_int(stored.get("disk_io_units"), default=default.disk_io_units, minimum=0, maximum=MAX_DISK_IO_UNITS),
                )
        capacities = payload.get("capacities")
        if isinstance(capacities, dict):
            cpu_capacity_cores = normalize_bounded_int(capacities.get("cpu_cores"), default=cpu_capacity_cores, minimum=1, maximum=MAX_JOB_CPU_CORES)
            gpu_vram_capacity_mb = normalize_bounded_int(capacities.get("gpu_vram_mb"), default=gpu_vram_capacity_mb, minimum=0, maximum=MAX_GPU_VRAM_MB)
            disk_io_capacity_units = normalize_bounded_int(capacities.get("disk_io_units"), default=disk_io_capacity_units, minimum=1, maximum=MAX_DISK_IO_UNITS)

    return JobResourceRuntimeConfig(
        profiles=profiles,
        cpu_capacity_cores=cpu_capacity_cores,
        gpu_vram_capacity_mb=gpu_vram_capacity_mb,
        disk_io_capacity_units=disk_io_capacity_units,
    )


def update_job_resource_runtime_config(
    session: Session,
    *,
    profile_updates: dict[str, dict],
    capacity_updates: dict | None = None,
) -> JobResourceRuntimeConfig:
    current = resolve_job_resource_runtime_config(session)
    profiles = dict(current.profiles)
    unknown = sorted(set(profile_updates) - set(profiles))
    if unknown:
        raise ValueError(f"Unknown job resource type(s): {', '.join(unknown)}")

    for key, raw in profile_updates.items():
        if not isinstance(raw, dict):
            raise ValueError(f"Resource profile for {key} must be an object")
        previous = profiles[key]
        profiles[key] = JobResourceProfile(
            cpu_cores=normalize_bounded_int(raw.get("cpu_cores"), default=previous.cpu_cores, minimum=1, maximum=MAX_JOB_CPU_CORES, strict=True, label=f"{key} CPU cores"),
            gpu_enabled=normalize_bool(raw.get("gpu_enabled"), default=previous.gpu_enabled, strict=True, label=f"{key} GPU enabled"),
            gpu_vram_mb=normalize_bounded_int(raw.get("gpu_vram_mb"), default=previous.gpu_vram_mb, minimum=0, maximum=MAX_GPU_VRAM_MB, strict=True, label=f"{key} GPU VRAM MB"),
            disk_io_units=normalize_bounded_int(raw.get("disk_io_units"), default=previous.disk_io_units, minimum=0, maximum=MAX_DISK_IO_UNITS, strict=True, label=f"{key} disk I/O units"),
        )

    capacities = {
        "cpu_cores": current.cpu_capacity_cores,
        "gpu_vram_mb": current.gpu_vram_capacity_mb,
        "disk_io_units": current.disk_io_capacity_units,
    }
    for key, raw in (capacity_updates or {}).items():
        if key not in capacities:
            raise ValueError(f"Unknown resource capacity: {key}")
        if key == "cpu_cores":
            capacities[key] = normalize_bounded_int(raw, default=capacities[key], minimum=1, maximum=MAX_JOB_CPU_CORES, strict=True, label="CPU capacity")
        elif key == "gpu_vram_mb":
            capacities[key] = normalize_bounded_int(raw, default=capacities[key], minimum=0, maximum=MAX_GPU_VRAM_MB, strict=True, label="GPU VRAM capacity")
        else:
            capacities[key] = normalize_bounded_int(raw, default=capacities[key], minimum=1, maximum=MAX_DISK_IO_UNITS, strict=True, label="Disk I/O capacity")

    entry = session.get(SystemSetting, JOB_RESOURCE_SETTING_KEY)
    payload = {
        "profiles": {
            key: {
                "cpu_cores": value.cpu_cores,
                "gpu_enabled": value.gpu_enabled,
                "gpu_vram_mb": value.gpu_vram_mb,
                "disk_io_units": value.disk_io_units,
            }
            for key, value in profiles.items()
        },
        "capacities": capacities,
    }
    if entry is None:
        session.add(SystemSetting(key=JOB_RESOURCE_SETTING_KEY, value_json=payload))
    else:
        entry.value_json = payload
    session.flush()
    return JobResourceRuntimeConfig(
        profiles=profiles,
        cpu_capacity_cores=capacities["cpu_cores"],
        gpu_vram_capacity_mb=capacities["gpu_vram_mb"],
        disk_io_capacity_units=capacities["disk_io_units"],
    )


def job_resource_profile_for_kind(config: JobResourceRuntimeConfig, job_kind: str | None) -> JobResourceProfile:
    normalized_kind = str(job_kind or "generic").strip() or "generic"
    for setting_key, aliases in JOB_RESOURCE_KIND_ALIASES.items():
        if normalized_kind in aliases:
            normalized_kind = setting_key
            break
    return config.profiles.get(normalized_kind, config.profiles["generic"])


def effective_job_priority_expression(config: JobPriorityRuntimeConfig):
    job_kind = Job.params_json["job_kind"].as_string()
    rules = []
    for key, priority in config.priorities.items():
        if key == "generic":
            continue
        aliases = JOB_PRIORITY_KIND_ALIASES.get(key, (key,))
        rules.append((job_kind.in_(aliases), priority))
    rules.append((job_kind.is_(None), config.priorities["generic"]))
    return case(*rules, else_=Job.priority)


def job_priority_settings_items(
    config: JobPriorityRuntimeConfig,
    resource_config: JobResourceRuntimeConfig | None = None,
) -> list[dict]:
    resource_config = resource_config or JobResourceRuntimeConfig(
        profiles=default_job_resource_profiles(),
        cpu_capacity_cores=MAX_JOB_CPU_CORES,
        gpu_vram_capacity_mb=0,
        disk_io_capacity_units=8,
    )
    return [
        {
            "job_kind": key,
            "label": label,
            "priority": config.priorities[key],
            "default_priority": default,
            "cpu_cores": resource_config.profiles[key].cpu_cores,
            "default_cpu_cores": JOB_RESOURCE_DEFAULTS[key][0],
            "gpu_enabled": resource_config.profiles[key].gpu_enabled,
            "default_gpu_enabled": JOB_RESOURCE_DEFAULTS[key][1],
            "gpu_vram_mb": resource_config.profiles[key].gpu_vram_mb,
            "default_gpu_vram_mb": JOB_RESOURCE_DEFAULTS[key][2],
            "disk_io_units": resource_config.profiles[key].disk_io_units,
            "default_disk_io_units": JOB_RESOURCE_DEFAULTS[key][3],
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


def normalize_bounded_int(
    value: object,
    *,
    default: int,
    minimum: int,
    maximum: int,
    strict: bool = False,
    label: str = "Value",
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        if strict:
            raise ValueError(f"{label} must be an integer between {minimum} and {maximum}")
        return default
    if parsed < minimum or parsed > maximum:
        if strict:
            raise ValueError(f"{label} must be between {minimum} and {maximum}")
        return default
    return parsed


def normalize_bool(value: object, *, default: bool, strict: bool = False, label: str = "Value") -> bool:
    if isinstance(value, bool):
        return value
    if strict:
        raise ValueError(f"{label} must be true or false")
    return default
