from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "DetecDiv Hub"
    environment: str = "dev"
    database_url: str = Field(
        default="postgresql+psycopg://detecdiv:detecdiv@localhost:5432/detecdiv_hub"
    )
    api_host: str = "127.0.0.1"
    api_port: int = 8000
    matlab_command: str = "matlab"
    matlab_repo_root: str = ""
    worker_target_key: str = ""
    worker_instance: str = ""
    log_level: str = "INFO"
    worker_poll_interval_sec: float = 5.0
    default_user_key: str = "localdev"
    auto_provision_users: bool = True
    allow_legacy_user_key_auth: bool = True
    session_duration_hours: int = 168
    session_cookie_name: str = "detecdiv_hub_session"
    indexing_stale_after_minutes: int = 15
    default_publication_targets: str = "labguru,elabftw"
    default_archive_root: str = ""
    default_archive_compression: str = "zip"
    archive_policy_enabled: bool = False
    archive_policy_interval_minutes: int = 1440
    archive_policy_run_as_user_key: str = "localdev"
    archive_policy_older_than_days: int = 30
    archive_policy_min_total_bytes: int = 0
    archive_policy_limit: int = 25
    archive_policy_owner_key: str = ""
    archive_policy_search: str = ""
    archive_policy_lifecycle_tiers: str = "hot"
    archive_policy_archive_statuses: str = "none,restored,archive_failed,restore_failed"
    archive_policy_archive_uri: str = ""
    archive_policy_archive_compression: str = ""
    archive_policy_delete_hot_source: bool = False
    micromanager_ingest_enabled: bool = False
    micromanager_ingest_interval_minutes: int = 15
    micromanager_ingest_run_as_user_key: str = "micromanager-bot"
    micromanager_ingest_root: str = ""
    micromanager_ingest_storage_root_name: str = ""
    micromanager_ingest_host_scope: str = "server"
    micromanager_ingest_visibility: str = "private"
    micromanager_ingest_settle_seconds: int = 300
    micromanager_ingest_max_datasets: int = 25
    micromanager_ingest_grouping_window_hours: int = 12
    micromanager_post_ingest_pipeline_key: str = ""
    micromanager_post_ingest_requested_mode: str = "server"
    micromanager_post_ingest_priority: int = 90
    raw_preview_artifact_root: str = ""
    raw_preview_ffmpeg_command: str = ""
    raw_preview_fps: int = 6
    raw_preview_frame_mode: str = "full"
    raw_preview_max_frames: int = 0
    raw_preview_max_dimension: int = 768
    raw_preview_binning_factor: int = 4
    raw_preview_crf: int = 24
    raw_preview_preset: str = "medium"
    raw_preview_include_existing: bool = False

    model_config = SettingsConfigDict(
        env_prefix="DETECDIV_HUB_",
        env_file=".env",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
