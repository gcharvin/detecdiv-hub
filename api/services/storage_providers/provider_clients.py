from __future__ import annotations

import os
import re
from urllib.parse import urlparse

from api.config import get_settings
from api.models import StorageProvider
from api.services.storage_providers.synology_dsm import SynologyDsmClient, SynologyDsmConfig, SynologyDsmError
from api.services.storage_providers.synology_ssh import SynologySshClient, SynologySshConfig


def _provider_environment_prefix(provider: StorageProvider) -> str:
    prefix = str((provider.config_json or {}).get("credentials_env_prefix") or "").strip()
    if not re.fullmatch(r"DETECDIV_HUB_[A-Z0-9_]+", prefix):
        raise SynologyDsmError(
            f"Provider {provider.provider_key} needs a DETECDIV_HUB_* credentials_env_prefix"
        )
    return prefix


def synology_dsm_client_for_provider(provider: StorageProvider) -> SynologyDsmClient:
    # Existing main-NAS settings remain backward compatible. A second NAS must
    # supply its own endpoint and credential references; never fall back to main.
    if provider.provider_key == "synology-main":
        return SynologyDsmClient()

    config = provider.config_json or {}
    prefix = _provider_environment_prefix(provider)
    settings = get_settings()
    return SynologyDsmClient(
        SynologyDsmConfig(
            base_url=str(config.get("dsm_base_url") or "").strip(),
            account=os.environ.get(f"{prefix}_DSM_ACCOUNT", ""),
            password=os.environ.get(f"{prefix}_DSM_PASSWORD", ""),
            session=settings.synology_dsm_session,
            verify_tls=bool(config.get("dsm_verify_tls", True)),
            timeout_sec=settings.synology_dsm_timeout_sec,
        )
    )


def synology_ssh_client_for_provider(provider: StorageProvider) -> SynologySshClient:
    if provider.provider_key == "synology-main":
        return SynologySshClient()

    config = provider.config_json or {}
    prefix = _provider_environment_prefix(provider)
    settings = get_settings()
    host = str(config.get("ssh_host") or urlparse(str(config.get("dsm_base_url") or "")).hostname or "")
    return SynologySshClient(
        SynologySshConfig(
            enabled=os.environ.get(f"{prefix}_SSH_ENABLED", "").lower() in {"1", "true", "yes"},
            host=host,
            port=int(config.get("ssh_port") or 22),
            username=os.environ.get(f"{prefix}_SSH_USERNAME", ""),
            password=os.environ.get(f"{prefix}_SSH_PASSWORD", ""),
            key_path=os.environ.get(f"{prefix}_SSH_KEY_PATH", ""),
            timeout_sec=settings.synology_ssh_timeout_sec,
            use_sudo=os.environ.get(f"{prefix}_SSH_USE_SUDO", "true").lower() not in {"0", "false", "no"},
            quota_share=str(config.get("quota_share") or "homes"),
        )
    )
