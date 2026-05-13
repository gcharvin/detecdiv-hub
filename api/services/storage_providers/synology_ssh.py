from __future__ import annotations

from dataclasses import dataclass
import shlex
from typing import Any
from urllib.parse import urlparse

from api.config import get_settings


class SynologySshError(RuntimeError):
    pass


@dataclass
class SynologySshConfig:
    enabled: bool
    host: str
    port: int = 22
    username: str = ""
    password: str = ""
    key_path: str = ""
    timeout_sec: float = 10.0
    synouser_command: str = "synouser"
    use_sudo: bool = True
    quota_command: str = "/usr/syno/sbin/synoquota"
    quota_volume_id: str = "1"

    @classmethod
    def from_settings(cls) -> "SynologySshConfig":
        settings = get_settings()
        host = settings.synology_ssh_host.strip()
        if not host and settings.synology_dsm_base_url:
            host = urlparse(settings.synology_dsm_base_url).hostname or ""
        return cls(
            enabled=settings.synology_ssh_enabled,
            host=host,
            port=settings.synology_ssh_port,
            username=settings.synology_ssh_username,
            password=settings.synology_ssh_password,
            key_path=settings.synology_ssh_key_path,
            timeout_sec=settings.synology_ssh_timeout_sec,
            synouser_command=settings.synology_ssh_synouser_command,
            use_sudo=settings.synology_ssh_use_sudo,
            quota_command=settings.synology_ssh_quota_command,
            quota_volume_id=settings.synology_ssh_quota_volume_id,
        )

    def validate_for_create_user(self) -> None:
        missing = []
        if not self.enabled:
            missing.append("DETECDIV_HUB_SYNOLOGY_SSH_ENABLED")
        if not self.host:
            missing.append("DETECDIV_HUB_SYNOLOGY_SSH_HOST")
        if not self.username:
            missing.append("DETECDIV_HUB_SYNOLOGY_SSH_USERNAME")
        if not self.password and not self.key_path:
            missing.append("DETECDIV_HUB_SYNOLOGY_SSH_PASSWORD or DETECDIV_HUB_SYNOLOGY_SSH_KEY_PATH")
        if self.use_sudo and not self.password:
            missing.append("DETECDIV_HUB_SYNOLOGY_SSH_PASSWORD for sudo")
        if missing:
            raise SynologySshError(f"Missing Synology SSH configuration: {', '.join(missing)}")


def build_synouser_add_command(
    *,
    synouser_command: str,
    user_name: str,
    initial_password: str,
    display_name: str | None = None,
    email: str | None = None,
) -> str:
    command = str(synouser_command or "synouser").strip() or "synouser"
    if any(char.isspace() for char in command):
        raise SynologySshError("Synology synouser command must be a single executable path")
    parts = [
        command,
        "--add",
        user_name,
        initial_password,
        display_name or user_name,
        "0",
        email or "",
        "0",
    ]
    return " ".join(shlex.quote(str(part)) for part in parts)


def build_synouser_delete_command(*, synouser_command: str, user_name: str) -> str:
    command = str(synouser_command or "synouser").strip() or "synouser"
    if any(char.isspace() for char in command):
        raise SynologySshError("Synology synouser command must be a single executable path")
    return " ".join(shlex.quote(str(part)) for part in [command, "--del", user_name])


def build_synoquota_set_command(
    *,
    quota_command: str,
    user_name: str,
    volume_id: str,
    quota_bytes: int,
) -> str:
    command = str(quota_command or "/usr/syno/sbin/synoquota").strip()
    if any(char.isspace() for char in command):
        raise SynologySshError("Synology quota command must be a single executable path")
    if quota_bytes <= 0:
        raise SynologySshError("Synology quota must be greater than zero")
    quota_gb = max(1, round(quota_bytes / (1024 * 1024 * 1024)))
    parts = [command, "--set", user_name, str(volume_id or "1"), f"{quota_gb}G"]
    return " ".join(shlex.quote(str(part)) for part in parts)


class SynologySshClient:
    def __init__(self, config: SynologySshConfig | None = None) -> None:
        self.config = config or SynologySshConfig.from_settings()

    def is_configured(self) -> bool:
        return bool(
            self.config.enabled
            and self.config.host
            and self.config.username
            and (self.config.password or self.config.key_path)
        )

    def create_user(
        self,
        *,
        user_name: str,
        initial_password: str,
        display_name: str | None = None,
        email: str | None = None,
    ) -> dict[str, Any]:
        if not str(user_name or "").strip():
            raise SynologySshError("Synology SSH user creation requires a user name")
        if not str(initial_password or "").strip():
            raise SynologySshError("Synology SSH user creation requires an initial password")
        self.config.validate_for_create_user()
        command = build_synouser_add_command(
            synouser_command=self.config.synouser_command,
            user_name=user_name,
            initial_password=initial_password,
            display_name=display_name,
            email=email,
        )
        return self._run(command)

    def delete_user(self, *, user_name: str) -> dict[str, Any]:
        if not str(user_name or "").strip():
            raise SynologySshError("Synology SSH user deletion requires a user name")
        self.config.validate_for_create_user()
        command = build_synouser_delete_command(
            synouser_command=self.config.synouser_command,
            user_name=user_name,
        )
        return self._run(command)

    def set_user_quota(self, *, user_name: str, quota_bytes: int) -> dict[str, Any]:
        if not str(user_name or "").strip():
            raise SynologySshError("Synology SSH quota update requires a user name")
        self.config.validate_for_create_user()
        command = build_synoquota_set_command(
            quota_command=self.config.quota_command,
            user_name=user_name,
            volume_id=self.config.quota_volume_id,
            quota_bytes=quota_bytes,
        )
        return self._run(command)

    def _run(self, command: str) -> dict[str, Any]:
        try:
            import paramiko
        except ImportError as exc:
            raise SynologySshError("Paramiko is required for Synology SSH provisioning") from exc

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs: dict[str, Any] = {
            "hostname": self.config.host,
            "port": self.config.port,
            "username": self.config.username,
            "timeout": self.config.timeout_sec,
            "banner_timeout": self.config.timeout_sec,
            "auth_timeout": self.config.timeout_sec,
        }
        if self.config.key_path:
            connect_kwargs["key_filename"] = self.config.key_path
        else:
            connect_kwargs["password"] = self.config.password
        try:
            client.connect(**connect_kwargs)
            remote_command = command
            if self.config.use_sudo:
                remote_command = f"sudo -S -p '' {command}"
            stdin, stdout, stderr = client.exec_command(remote_command, timeout=self.config.timeout_sec)
            if self.config.use_sudo:
                stdin.write(self.config.password + "\n")
                stdin.flush()
            try:
                stdin.close()
            except Exception:
                pass
            exit_status = stdout.channel.recv_exit_status()
            stdout_text = stdout.read().decode("utf-8", errors="replace").strip()
            stderr_text = stderr.read().decode("utf-8", errors="replace").strip()
        except Exception as exc:
            raise SynologySshError(f"Synology SSH command failed: {exc}") from exc
        finally:
            client.close()

        if exit_status != 0:
            detail = stderr_text or stdout_text or f"exit status {exit_status}"
            raise SynologySshError(f"Synology SSH command failed: {detail}")
        return {"exit_status": exit_status, "stdout": stdout_text, "stderr": stderr_text}
