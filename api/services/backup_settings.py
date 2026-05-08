from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from api.config import Settings, get_settings
from api.models import SystemSetting
from api.services.raw_preview_settings import ensure_system_settings_table


BACKUP_SETTING_KEY = "backup_settings"


@dataclass(frozen=True)
class BackupRuntimeConfig:
    backup_repo: str
    backup_passphrase: str
    backup_enabled: bool
    backup_interval_minutes: int
    backup_run_as_user_key: str
    backup_include_raw_datasets: bool
    backup_include_projects: bool

    def to_json(self) -> dict:
        return {
            "backup_repo": self.backup_repo,
            "backup_passphrase": self.backup_passphrase,
            "backup_enabled": self.backup_enabled,
            "backup_interval_minutes": self.backup_interval_minutes,
            "backup_run_as_user_key": self.backup_run_as_user_key,
            "backup_include_raw_datasets": self.backup_include_raw_datasets,
            "backup_include_projects": self.backup_include_projects,
        }

    def to_json_safe(self) -> dict:
        """Same as to_json but without the passphrase."""
        d = self.to_json()
        d["backup_passphrase"] = "***" if self.backup_passphrase else ""
        return d


def resolve_backup_runtime_config(session: Session, *, settings: Settings | None = None) -> BackupRuntimeConfig:
    ensure_system_settings_table(session)
    settings = settings or get_settings()
    base = BackupRuntimeConfig(
        backup_repo=settings.backup_repo,
        backup_passphrase=settings.backup_passphrase,
        backup_enabled=settings.backup_enabled,
        backup_interval_minutes=max(1, settings.backup_interval_minutes),
        backup_run_as_user_key=settings.backup_run_as_user_key,
        backup_include_raw_datasets=settings.backup_include_raw_datasets,
        backup_include_projects=settings.backup_include_projects,
    )
    entry = session.get(SystemSetting, BACKUP_SETTING_KEY)
    if entry is None:
        return base
    payload = dict(entry.value_json or {})
    return BackupRuntimeConfig(
        backup_repo=_clean_text(payload.get("backup_repo")) or base.backup_repo,
        backup_passphrase=_clean_text(payload.get("backup_passphrase")) or base.backup_passphrase,
        backup_enabled=_bool_or_default(payload.get("backup_enabled"), base.backup_enabled),
        backup_interval_minutes=_positive_int(payload.get("backup_interval_minutes"), base.backup_interval_minutes),
        backup_run_as_user_key=_clean_text(payload.get("backup_run_as_user_key")) or base.backup_run_as_user_key,
        backup_include_raw_datasets=_bool_or_default(payload.get("backup_include_raw_datasets"), base.backup_include_raw_datasets),
        backup_include_projects=_bool_or_default(payload.get("backup_include_projects"), base.backup_include_projects),
    )


def update_backup_runtime_config(session: Session, *, updates: dict) -> BackupRuntimeConfig:
    ensure_system_settings_table(session)
    current = resolve_backup_runtime_config(session)
    merged = current.to_json()
    allowed_keys = {
        "backup_repo", "backup_passphrase", "backup_enabled",
        "backup_interval_minutes", "backup_run_as_user_key",
        "backup_include_raw_datasets", "backup_include_projects",
    }
    for key in allowed_keys:
        if key in updates:
            merged[key] = updates[key]

    entry = session.get(SystemSetting, BACKUP_SETTING_KEY)
    if entry is None:
        entry = SystemSetting(key=BACKUP_SETTING_KEY, value_json=merged)
        session.add(entry)
    else:
        entry.value_json = merged
    session.flush()
    return resolve_backup_runtime_config(session)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _bool_or_default(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _positive_int(value: object, default: int) -> int:
    try:
        parsed = int(value)  # type: ignore[arg-type]
        return max(1, parsed)
    except (TypeError, ValueError):
        return default
