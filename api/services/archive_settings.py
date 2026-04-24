from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from api.config import Settings, get_settings
from api.models import SystemSetting
from api.services.raw_preview_settings import ensure_system_settings_table


RAW_ARCHIVE_SETTING_KEY = "raw_archive_settings"


@dataclass(frozen=True)
class RawArchiveRuntimeConfig:
    archive_root: str | None
    archive_compression: str
    delete_hot_source: bool

    def to_json(self) -> dict:
        return {
            "archive_root": self.archive_root,
            "archive_compression": self.archive_compression,
            "delete_hot_source": self.delete_hot_source,
        }


def resolve_raw_archive_runtime_config(session: Session, *, settings: Settings | None = None) -> RawArchiveRuntimeConfig:
    ensure_system_settings_table(session)
    settings = settings or get_settings()
    base = RawArchiveRuntimeConfig(
        archive_root=_clean_optional_text(settings.default_archive_root),
        archive_compression=_normalize_archive_compression(settings.default_archive_compression),
        delete_hot_source=bool(settings.archive_policy_delete_hot_source),
    )
    entry = session.get(SystemSetting, RAW_ARCHIVE_SETTING_KEY)
    if entry is None:
        return base
    payload = dict(entry.value_json or {})
    return RawArchiveRuntimeConfig(
        archive_root=_clean_optional_text(payload.get("archive_root")) or base.archive_root,
        archive_compression=_normalize_archive_compression(payload.get("archive_compression"), default=base.archive_compression),
        delete_hot_source=_bool_or_default(payload.get("delete_hot_source"), base.delete_hot_source),
    )


def update_raw_archive_runtime_config(session: Session, *, updates: dict) -> RawArchiveRuntimeConfig:
    ensure_system_settings_table(session)
    current = resolve_raw_archive_runtime_config(session)
    merged = current.to_json()
    for key in ("archive_root", "archive_compression", "delete_hot_source"):
        if key in updates:
            merged[key] = updates.get(key)
    merged["archive_root"] = _clean_optional_text(merged.get("archive_root"))
    merged["archive_compression"] = _normalize_archive_compression(merged.get("archive_compression"), default=current.archive_compression)
    merged["delete_hot_source"] = _bool_or_default(merged.get("delete_hot_source"), current.delete_hot_source)

    entry = session.get(SystemSetting, RAW_ARCHIVE_SETTING_KEY)
    if entry is None:
        entry = SystemSetting(key=RAW_ARCHIVE_SETTING_KEY, value_json=merged)
        session.add(entry)
    else:
        entry.value_json = merged
    session.flush()
    return resolve_raw_archive_runtime_config(session)


def _clean_optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_archive_compression(value: object, *, default: str = "zip") -> str:
    text = str(value or "").strip().lower()
    if text in {"zip", "tar.gz", "tgz"}:
        return "tar.gz" if text == "tgz" else text
    return default


def _bool_or_default(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)
