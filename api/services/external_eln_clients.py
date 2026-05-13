from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import urljoin

import requests

from api.config import get_settings


SUPPORTED_EXTERNAL_ELN_SYSTEMS = {"labguru", "elabftw"}


@dataclass(slots=True)
class ExternalElnExperiment:
    external_id: str
    title: str
    external_url: str | None = None
    owner_name: str | None = None
    started_at: datetime | None = None
    updated_external_at: datetime | None = None
    payload_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExternalElnUser:
    external_id: str
    display_name: str
    email: str | None = None
    payload_json: dict[str, Any] = field(default_factory=dict)


class ExternalElnClient(Protocol):
    system_key: str

    def list_experiments(self) -> list[ExternalElnExperiment]:
        ...

    def get_experiment(self, external_id: str) -> ExternalElnExperiment:
        ...

    def list_users_or_observed_members(self) -> list[ExternalElnUser]:
        ...

    def build_experiment_url(self, external_id: str) -> str:
        ...


class LabguruClient:
    system_key = "labguru"

    def __init__(self, *, base_url: str, token: str, timeout_seconds: float = 30.0):
        self.base_url = normalize_base_url(base_url)
        self.token = token
        self.timeout_seconds = timeout_seconds
        self._observed_users: dict[str, ExternalElnUser] = {}

    def list_experiments(self) -> list[ExternalElnExperiment]:
        experiments: list[ExternalElnExperiment] = []
        for item in self._list_paginated("/api/v2/experiments", fallback_endpoint="/api/v1/experiments.json"):
            experiment = labguru_experiment_from_payload(item, base_url=self.base_url)
            experiments.append(experiment)
            for user in labguru_observed_users_from_payload(item):
                self._observed_users[user.external_id] = user
        return experiments

    def get_experiment(self, external_id: str) -> ExternalElnExperiment:
        payload = self._request_json(f"/api/v2/experiments/{external_id}", fallback_endpoint=f"/api/v1/experiments/{external_id}.json")
        experiment = labguru_experiment_from_payload(payload, base_url=self.base_url)
        for user in labguru_observed_users_from_payload(payload):
            self._observed_users[user.external_id] = user
        return experiment

    def list_users_or_observed_members(self) -> list[ExternalElnUser]:
        return sorted(self._observed_users.values(), key=lambda user: user.display_name.lower())

    def build_experiment_url(self, external_id: str) -> str:
        return urljoin(self.base_url, f"/knowledge/experiments/{external_id}")

    def _list_paginated(self, endpoint: str, *, fallback_endpoint: str | None = None) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self._request_json(endpoint, fallback_endpoint=fallback_endpoint, page=page, page_size=200)
            page_items = extract_list_payload(payload)
            if not page_items:
                break
            items.extend(page_items)
            if len(page_items) < 200:
                break
            page += 1
        return items

    def _request_json(self, endpoint: str, *, fallback_endpoint: str | None = None, **params: Any) -> Any:
        try:
            return self._request_json_once(endpoint, **params)
        except requests.HTTPError:
            if not fallback_endpoint:
                raise
            return self._request_json_once(fallback_endpoint, **params)

    def _request_json_once(self, endpoint: str, **params: Any) -> Any:
        data = dict(params)
        data["token"] = self.token
        response = requests.get(
            urljoin(self.base_url, endpoint),
            json=data,
            params=data,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()


class ElabftwClient:
    system_key = "elabftw"

    def __init__(self, *, base_url: str, token: str):
        self.base_url = normalize_base_url(base_url)
        self.token = token

    def list_experiments(self) -> list[ExternalElnExperiment]:
        raise NotImplementedError("eLabFTW connector is planned but not implemented in V1.")

    def get_experiment(self, external_id: str) -> ExternalElnExperiment:
        raise NotImplementedError("eLabFTW connector is planned but not implemented in V1.")

    def list_users_or_observed_members(self) -> list[ExternalElnUser]:
        raise NotImplementedError("eLabFTW connector is planned but not implemented in V1.")

    def build_experiment_url(self, external_id: str) -> str:
        return urljoin(self.base_url, f"/experiments.php?mode=view&id={external_id}")


def build_external_eln_client(system_key: str) -> ExternalElnClient:
    settings = get_settings()
    normalized = normalize_system_key(system_key)
    if normalized == "labguru":
        if not settings.labguru_enabled or not settings.labguru_token.strip():
            raise ValueError("Labguru connector is not enabled or token is missing.")
        return LabguruClient(base_url=settings.labguru_base_url, token=settings.labguru_token)
    if normalized == "elabftw":
        if not settings.elabftw_enabled or not settings.elabftw_token.strip():
            raise ValueError("eLabFTW connector is not enabled or token is missing.")
        return ElabftwClient(base_url=settings.elabftw_base_url, token=settings.elabftw_token)
    raise ValueError(f"Unsupported external ELN system: {system_key}")


def normalize_system_key(system_key: str) -> str:
    normalized = str(system_key or "").strip().lower()
    if normalized not in SUPPORTED_EXTERNAL_ELN_SYSTEMS:
        raise ValueError(f"Unsupported external ELN system: {system_key}")
    return normalized


def normalize_base_url(base_url: str) -> str:
    text = str(base_url or "").strip()
    if not text:
        raise ValueError("External ELN base URL is required.")
    return text.rstrip("/") + "/"


def extract_list_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "experiments", "items", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def labguru_experiment_from_payload(payload: dict[str, Any], *, base_url: str) -> ExternalElnExperiment:
    external_id = str(payload.get("id") or payload.get("external_id") or "").strip()
    if not external_id:
        raise ValueError("Labguru experiment payload is missing id.")
    title = str(payload.get("title") or payload.get("name") or f"Labguru experiment {external_id}").strip()
    return ExternalElnExperiment(
        external_id=external_id,
        title=title,
        external_url=normalize_external_url(
            str(payload.get("url") or "").strip() or f"/knowledge/experiments/{external_id}",
            base_url=base_url,
        ),
        owner_name=labguru_owner_name(payload),
        started_at=parse_datetime(payload.get("start_date") or payload.get("started_at")),
        updated_external_at=parse_datetime(payload.get("updated_at")),
        payload_json=payload,
    )


def normalize_external_url(value: str, *, base_url: str) -> str:
    text = str(value or "").strip()
    if not text:
        return normalize_base_url(base_url)
    return urljoin(normalize_base_url(base_url), text)


def labguru_observed_users_from_payload(payload: dict[str, Any]) -> list[ExternalElnUser]:
    users: dict[str, ExternalElnUser] = {}
    for key in ("owner", "member", "created_by", "user"):
        value = payload.get(key)
        if isinstance(value, dict):
            user = external_user_from_labguru_dict(value)
            if user is not None:
                users[user.external_id] = user
        elif isinstance(value, str) and value.strip():
            external_id = f"name:{normalize_name(value)}"
            users[external_id] = ExternalElnUser(
                external_id=external_id,
                display_name=value.strip(),
                payload_json={"source_field": key, "value": value},
            )
    return list(users.values())


def external_user_from_labguru_dict(payload: dict[str, Any]) -> ExternalElnUser | None:
    display_name = str(payload.get("name") or payload.get("display_name") or payload.get("full_name") or "").strip()
    external_id = str(payload.get("id") or payload.get("uuid") or "").strip()
    if not display_name and not external_id:
        return None
    if not display_name:
        display_name = external_id
    if not external_id:
        external_id = f"name:{normalize_name(display_name)}"
    return ExternalElnUser(
        external_id=external_id,
        display_name=display_name,
        email=str(payload.get("email") or "").strip() or None,
        payload_json=payload,
    )


def labguru_owner_name(payload: dict[str, Any]) -> str | None:
    for key in ("owner", "member"):
        value = payload.get(key)
        if isinstance(value, dict):
            name = str(value.get("name") or value.get("display_name") or "").strip()
            if name:
                return name
    for key in ("created_by", "user"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            name = str(value.get("name") or value.get("display_name") or "").strip()
            if name:
                return name
    return None


def parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    for suffix in ("Z", " UTC"):
        if text.endswith(suffix):
            text = text[: -len(suffix)] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def normalize_name(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())
