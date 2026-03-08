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
    log_level: str = "INFO"
    worker_poll_interval_sec: float = 5.0
    default_user_key: str = "localdev"
    auto_provision_users: bool = True

    model_config = SettingsConfigDict(
        env_prefix="DETECDIV_HUB_",
        env_file=".env",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
