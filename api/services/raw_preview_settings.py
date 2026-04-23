from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

from api.config import Settings, get_settings
from api.models import SystemSetting


RAW_PREVIEW_SETTING_KEY = "raw_preview_quality"


@dataclass(frozen=True)
class RawPreviewRuntimeConfig:
    fps: int
    frame_mode: str
    max_frames: int
    max_dimension: int
    binning_factor: int
    crf: int
    preset: str
    include_existing: bool
    artifact_root: str | None
    ffmpeg_command: str | None

    def to_json(self) -> dict:
        return {
            "fps": self.fps,
            "frame_mode": self.frame_mode,
            "max_frames": self.max_frames,
            "max_dimension": self.max_dimension,
            "binning_factor": self.binning_factor,
            "crf": self.crf,
            "preset": self.preset,
            "include_existing": self.include_existing,
            "artifact_root": self.artifact_root,
            "ffmpeg_command": self.ffmpeg_command,
        }


def resolve_raw_preview_runtime_config(session: Session, *, settings: Settings | None = None) -> RawPreviewRuntimeConfig:
    ensure_system_settings_table(session)
    settings = settings or get_settings()
    base = RawPreviewRuntimeConfig(
        fps=max(1, settings.raw_preview_fps),
        frame_mode=_normalize_frame_mode(settings.raw_preview_frame_mode),
        max_frames=max(0, settings.raw_preview_max_frames),
        max_dimension=max(64, settings.raw_preview_max_dimension),
        binning_factor=max(1, int(settings.raw_preview_binning_factor)),
        crf=min(40, max(16, int(settings.raw_preview_crf))),
        preset=_clean_optional_text(settings.raw_preview_preset) or "medium",
        include_existing=bool(settings.raw_preview_include_existing),
        artifact_root=_clean_optional_text(settings.raw_preview_artifact_root),
        ffmpeg_command=_clean_optional_text(settings.raw_preview_ffmpeg_command),
    )
    entry = session.get(SystemSetting, RAW_PREVIEW_SETTING_KEY)
    if entry is None:
        return base
    payload = dict(entry.value_json or {})
    frame_mode = _normalize_frame_mode(payload.get("frame_mode"), default=base.frame_mode)
    return RawPreviewRuntimeConfig(
        fps=max(1, _int_or_default(payload.get("fps"), base.fps)),
        frame_mode=frame_mode,
        max_frames=max(0, _int_or_default(payload.get("max_frames"), base.max_frames)),
        max_dimension=max(64, _int_or_default(payload.get("max_dimension"), base.max_dimension)),
        binning_factor=max(1, _int_or_default(payload.get("binning_factor"), base.binning_factor)),
        crf=min(40, max(16, _int_or_default(payload.get("crf"), base.crf))),
        preset=_clean_optional_text(payload.get("preset")) or base.preset,
        include_existing=_bool_or_default(payload.get("include_existing"), base.include_existing),
        artifact_root=_clean_optional_text(payload.get("artifact_root")) or base.artifact_root,
        ffmpeg_command=_clean_optional_text(payload.get("ffmpeg_command")) or base.ffmpeg_command,
    )


def update_raw_preview_runtime_config(session: Session, *, updates: dict) -> RawPreviewRuntimeConfig:
    ensure_system_settings_table(session)
    current = resolve_raw_preview_runtime_config(session)
    merged = current.to_json()
    for key in ("fps", "frame_mode", "max_frames", "max_dimension", "binning_factor", "crf", "preset", "include_existing", "artifact_root", "ffmpeg_command"):
        if key in updates and updates.get(key) is not None:
            merged[key] = updates.get(key)
    merged["fps"] = max(1, _int_or_default(merged.get("fps"), current.fps))
    merged["frame_mode"] = _normalize_frame_mode(merged.get("frame_mode"), default=current.frame_mode)
    merged["max_frames"] = max(0, _int_or_default(merged.get("max_frames"), current.max_frames))
    merged["max_dimension"] = max(64, _int_or_default(merged.get("max_dimension"), current.max_dimension))
    merged["binning_factor"] = max(1, _int_or_default(merged.get("binning_factor"), current.binning_factor))
    merged["crf"] = min(40, max(16, _int_or_default(merged.get("crf"), current.crf)))
    merged["preset"] = _clean_optional_text(merged.get("preset")) or current.preset
    merged["include_existing"] = _bool_or_default(merged.get("include_existing"), current.include_existing)
    merged["artifact_root"] = _clean_optional_text(merged.get("artifact_root"))
    merged["ffmpeg_command"] = _clean_optional_text(merged.get("ffmpeg_command"))

    entry = session.get(SystemSetting, RAW_PREVIEW_SETTING_KEY)
    if entry is None:
        entry = SystemSetting(key=RAW_PREVIEW_SETTING_KEY, value_json=merged)
        session.add(entry)
    else:
        entry.value_json = merged
    session.flush()
    return resolve_raw_preview_runtime_config(session)


def _clean_optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int_or_default(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _bool_or_default(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _normalize_frame_mode(value: object, *, default: str = "full") -> str:
    text = str(value or "").strip().lower()
    if text in {"full", "limit"}:
        return text
    return default


def ensure_system_settings_table(session: Session) -> None:
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )
