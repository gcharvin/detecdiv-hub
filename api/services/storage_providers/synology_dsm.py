from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from api.config import get_settings


DEFAULT_DISCOVERY_QUERY = ",".join(
    [
        "SYNO.API.Auth",
        "SYNO.API.Info",
        "SYNO.Core.User",
        "SYNO.Core.Quota",
        "SYNO.Core.User.Home",
    ]
)


class SynologyDsmError(RuntimeError):
    def __init__(self, message: str, *, code: int | None = None, payload: dict | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.payload = payload or {}


@dataclass
class SynologyDsmConfig:
    base_url: str
    account: str
    password: str
    session: str = "DetecDivHub"
    verify_tls: bool = True
    timeout_sec: float = 10.0

    @classmethod
    def from_settings(cls) -> "SynologyDsmConfig":
        settings = get_settings()
        return cls(
            base_url=settings.synology_dsm_base_url,
            account=settings.synology_dsm_account,
            password=settings.synology_dsm_password,
            session=settings.synology_dsm_session,
            verify_tls=settings.synology_dsm_verify_tls,
            timeout_sec=settings.synology_dsm_timeout_sec,
        )

    def validate_for_network_call(self) -> None:
        missing = []
        if not self.base_url:
            missing.append("DETECDIV_HUB_SYNOLOGY_DSM_BASE_URL")
        if not self.account:
            missing.append("DETECDIV_HUB_SYNOLOGY_DSM_ACCOUNT")
        if not self.password:
            missing.append("DETECDIV_HUB_SYNOLOGY_DSM_PASSWORD")
        if missing:
            raise SynologyDsmError(f"Missing Synology DSM configuration: {', '.join(missing)}")


class SynologyDsmClient:
    def __init__(self, config: SynologyDsmConfig | None = None) -> None:
        self.config = config or SynologyDsmConfig.from_settings()
        self.sid: str | None = None
        self._api_info: dict[str, Any] | None = None

    def is_configured(self) -> bool:
        return bool(self.config.base_url and self.config.account and self.config.password)

    def discover(self, query: str = DEFAULT_DISCOVERY_QUERY) -> dict[str, Any]:
        payload = self._request(
            "entry.cgi",
            {
                "api": "SYNO.API.Info",
                "version": "1",
                "method": "query",
                "query": query,
            },
            include_sid=False,
            require_config=False,
        )
        info = payload.get("data") or {}
        if not isinstance(info, dict):
            raise SynologyDsmError("Unexpected DSM discovery response", payload=payload)
        self._api_info = info
        return info

    def login(self) -> str:
        self.config.validate_for_network_call()
        auth_info = self.api_info("SYNO.API.Auth")
        version = choose_auth_version(auth_info)
        path = str(auth_info.get("path") or "auth.cgi")
        payload = self._request(
            path,
            {
                "api": "SYNO.API.Auth",
                "version": str(version),
                "method": "login",
                "account": self.config.account,
                "passwd": self.config.password,
                "session": self.config.session,
                "format": "sid",
            },
            include_sid=False,
            require_config=True,
        )
        sid = ((payload.get("data") or {}).get("sid") or "").strip()
        if not sid:
            raise SynologyDsmError("DSM login did not return a session id", payload=payload)
        self.sid = sid
        return sid

    def logout(self) -> None:
        if not self.sid:
            return
        try:
            auth_info = self.api_info("SYNO.API.Auth")
            self._request(
                str(auth_info.get("path") or "auth.cgi"),
                {
                    "api": "SYNO.API.Auth",
                    "version": str(choose_auth_version(auth_info)),
                    "method": "logout",
                    "session": self.config.session,
                },
                include_sid=True,
                require_config=True,
            )
        finally:
            self.sid = None

    def login_check(self) -> dict[str, Any]:
        sid = self.login()
        try:
            return {
                "success": True,
                "session": self.config.session,
                "sid_received": bool(sid),
                "discovered_apis": sorted((self._api_info or {}).keys()),
            }
        finally:
            self.logout()

    def list_users(self) -> list[dict[str, Any]]:
        self.login()
        try:
            payload = self.call_discovered_api(api_name="SYNO.Core.User", method="list", login=True)
            users = ((payload.get("data") or {}).get("users") or [])
            if not isinstance(users, list):
                raise SynologyDsmError("Unexpected DSM user list response", payload=payload)
            return [user for user in users if isinstance(user, dict)]
        finally:
            self.logout()

    def get_user_home_settings(self) -> dict[str, Any]:
        self.login()
        try:
            payload = self.call_discovered_api(api_name="SYNO.Core.User.Home", method="get", login=True)
            data = payload.get("data") or {}
            if not isinstance(data, dict):
                raise SynologyDsmError("Unexpected DSM user home response", payload=payload)
            return data
        finally:
            self.logout()

    def get_user_quota(self, user_name: str) -> dict[str, Any]:
        if not str(user_name or "").strip():
            raise SynologyDsmError("DSM quota lookup requires a user name")
        self.login()
        try:
            payload = self.call_discovered_api(
                api_name="SYNO.Core.Quota",
                method="get",
                params={"name": user_name},
                login=True,
            )
            data = payload.get("data") or {}
            if not isinstance(data, dict):
                raise SynologyDsmError("Unexpected DSM quota response", payload=payload)
            return data
        finally:
            self.logout()

    def call_discovered_api(
        self,
        *,
        api_name: str,
        method: str,
        params: dict[str, Any] | None = None,
        version: int | None = None,
        login: bool = True,
    ) -> dict[str, Any]:
        api_info = self.api_info(api_name)
        selected_version = version or choose_max_version(api_info)
        path = str(api_info.get("path") or "entry.cgi")
        if login and not self.sid:
            self.login()
        request_params = {
            "api": api_name,
            "version": str(selected_version),
            "method": method,
            **(params or {}),
        }
        return self._request(path, request_params, include_sid=login, require_config=login)

    def api_info(self, api_name: str) -> dict[str, Any]:
        if self._api_info is None:
            self.discover(api_name)
        info = (self._api_info or {}).get(api_name)
        if info is None:
            info = self.discover(api_name).get(api_name)
        if not isinstance(info, dict):
            raise SynologyDsmError(f"DSM API is not available: {api_name}")
        return info

    def _request(
        self,
        path: str,
        params: dict[str, Any],
        *,
        include_sid: bool,
        require_config: bool,
    ) -> dict[str, Any]:
        if require_config:
            self.config.validate_for_network_call()
        if not self.config.base_url:
            raise SynologyDsmError("Synology DSM base URL is not configured")
        request_params = dict(params)
        if include_sid:
            if not self.sid:
                raise SynologyDsmError("DSM session id is missing")
            request_params["_sid"] = self.sid
        url = f"{self.config.base_url.rstrip('/')}/webapi/{path.lstrip('/')}"
        try:
            response = requests.get(
                url,
                params=request_params,
                timeout=self.config.timeout_sec,
                verify=self.config.verify_tls,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise SynologyDsmError(f"DSM request failed: {exc}") from exc
        except ValueError as exc:
            raise SynologyDsmError("DSM response was not valid JSON") from exc

        if not isinstance(payload, dict):
            raise SynologyDsmError("Unexpected DSM response payload")
        if not payload.get("success"):
            error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
            code = error.get("code") if isinstance(error, dict) else None
            raise SynologyDsmError(f"DSM API call failed: {params.get('api')}.{params.get('method')}", code=code, payload=payload)
        return payload


def choose_auth_version(auth_info: dict[str, Any]) -> int:
    try:
        max_version = int(auth_info.get("maxVersion") or auth_info.get("maxversion") or 6)
    except (TypeError, ValueError):
        max_version = 6
    if max_version >= 6:
        return 6
    return max(max_version, 1)


def choose_max_version(api_info: dict[str, Any]) -> int:
    try:
        return int(api_info.get("maxVersion") or api_info.get("maxversion") or 1)
    except (TypeError, ValueError):
        return 1


def summarize_discovered_capabilities(api_info: dict[str, Any]) -> dict[str, Any]:
    available = set(api_info.keys())
    return {
        "can_discover": "SYNO.API.Info" in available,
        "can_login": "SYNO.API.Auth" in available,
        "can_manage_users": "SYNO.Core.User" in available,
        "can_read_quota": "SYNO.Core.Quota" in available,
        "can_set_quota": "SYNO.Core.Quota" in available,
        "can_manage_homes": "SYNO.Core.User.Home" in available,
        "discovered_apis": sorted(available),
    }


def parse_user_quota_payload(payload: dict[str, Any]) -> dict[str, Any]:
    entries = payload.get("user_quota") or []
    if not isinstance(entries, list):
        entries = []
    parsed_entries: list[dict[str, Any]] = []
    quota_bytes = None
    used_bytes = None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        parsed_entries.append(entry)
        quota_bytes = quota_bytes if quota_bytes is not None else first_int_value(entry, ("quota_byte", "quota_bytes", "quota", "size"))
        used_bytes = used_bytes if used_bytes is not None else first_int_value(entry, ("used_byte", "used_bytes", "used"))
    return {
        "entries": parsed_entries,
        "entry_count": len(parsed_entries),
        "quota_bytes": quota_bytes,
        "used_bytes": used_bytes,
    }


def first_int_value(payload: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None
