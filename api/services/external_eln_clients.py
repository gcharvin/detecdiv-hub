from __future__ import annotations

import html
import re
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


@dataclass(slots=True)
class ExternalElnContainer:
    external_id: str
    name: str
    payload_json: dict[str, Any] = field(default_factory=dict)


class ExternalElnClient(Protocol):
    system_key: str

    def list_experiments(self) -> list[ExternalElnExperiment]:
        ...

    def get_experiment(self, external_id: str) -> ExternalElnExperiment:
        ...

    def create_experiment(
        self,
        *,
        title: str,
        description: str | None = None,
        procedure: str | None = None,
        conditions: str | None = None,
        project_id: str | None = None,
        folder_id: str | None = None,
    ) -> ExternalElnExperiment:
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
        self._hydrate_experiment_text_elements(payload)
        experiment = labguru_experiment_from_payload(payload, base_url=self.base_url)
        for user in labguru_observed_users_from_payload(payload):
            self._observed_users[user.external_id] = user
        return experiment

    def create_experiment(
        self,
        *,
        title: str,
        description: str | None = None,
        procedure: str | None = None,
        conditions: str | None = None,
        project_id: str | None = None,
        folder_id: str | None = None,
    ) -> ExternalElnExperiment:
        clean_title = str(title or "").strip()
        if not clean_title:
            raise ValueError("Labguru experiment title is required.")
        experiment_payload: dict[str, Any] = {
            "title": clean_title,
            "name": clean_title,
        }
        if project_id:
            experiment_payload["project_id"] = str(project_id)
        if folder_id:
            experiment_payload["folder_id"] = str(folder_id)
        if description:
            experiment_payload["description"] = str(description)
        procedure_sections = []
        if procedure:
            procedure_sections.append(("Procedure", procedure))
        if conditions:
            procedure_sections.append(("Conditions", conditions))
        if procedure_sections:
            experiment_payload["experiment_procedures_attributes"] = [
                {
                    "name": f"DetecDiv acquisition {section_name.lower()}",
                    "description": str(section_text),
                    "elements_attributes": [
                        {
                            "element_type": "text",
                            "data": html.escape(str(section_text)).replace("\n", "<br>"),
                        }
                    ],
                }
                for section_name, section_text in procedure_sections
            ]
        payload = self._post_json(
            "/api/v2/experiments",
            fallback_endpoint="/api/v1/experiments.json",
            json_payload={"experiment": experiment_payload},
        )
        if isinstance(payload, dict):
            created_payload = payload.get("experiment") if isinstance(payload.get("experiment"), dict) else payload
            experiment = labguru_experiment_from_payload(created_payload, base_url=self.base_url)
            if description or procedure or conditions or project_id or folder_id:
                experiment.payload_json.setdefault(
                    "detecdiv_widget_request",
                    {
                        "description": description,
                        "procedure": procedure,
                        "conditions": conditions,
                        "project_id": project_id,
                        "folder_id": folder_id,
                    },
                )
            return experiment
        raise ValueError("Labguru create experiment response was not a JSON object.")

    def list_projects(self) -> list[ExternalElnContainer]:
        return [
            labguru_container_from_payload(item)
            for item in self._list_paginated("/api/v2/projects", fallback_endpoint="/api/v1/projects.json")
        ]

    def list_folders(self, *, project_id: str | None = None) -> list[ExternalElnContainer]:
        params = {"project_id": project_id} if project_id else {}
        try:
            payloads = self._list_paginated_with_params(
                "/api/v2/folders",
                fallback_endpoint="/api/v1/folders.json",
                **params,
            )
        except requests.HTTPError:
            if not project_id:
                raise
            payloads = self._list_paginated_with_params(f"/api/v2/projects/{project_id}/folders")
        return [labguru_container_from_payload(item) for item in payloads]

    def create_project(self, *, name: str) -> ExternalElnContainer:
        payload = self._post_json(
            "/api/v2/projects",
            fallback_endpoint="/api/v1/projects.json",
            json_payload={"project": {"name": name, "title": name}},
        )
        created = payload.get("project") if isinstance(payload, dict) and isinstance(payload.get("project"), dict) else payload
        if not isinstance(created, dict):
            raise ValueError("Labguru create project response was not a JSON object.")
        return labguru_container_from_payload(created)

    def create_folder(self, *, name: str, project_id: str | None = None) -> ExternalElnContainer:
        folder_payload: dict[str, Any] = {"name": name, "title": name}
        if project_id:
            folder_payload["project_id"] = project_id
        payload = self._post_json(
            "/api/v2/folders",
            fallback_endpoint="/api/v1/folders.json",
            json_payload={"folder": folder_payload},
        )
        created = payload.get("folder") if isinstance(payload, dict) and isinstance(payload.get("folder"), dict) else payload
        if not isinstance(created, dict):
            raise ValueError("Labguru create folder response was not a JSON object.")
        return labguru_container_from_payload(created)

    def ensure_default_project_folder(self, *, project_name: str, folder_name: str) -> dict[str, ExternalElnContainer]:
        projects = self.list_projects()
        project = next((item for item in projects if item.name.casefold() == project_name.casefold()), None)
        if project is None:
            project = self.create_project(name=project_name)
        folders = self.list_folders(project_id=project.external_id)
        folder = next((item for item in folders if item.name.casefold() == folder_name.casefold()), None)
        if folder is None:
            folder = self.create_folder(name=folder_name, project_id=project.external_id)
        return {"project": project, "folder": folder}

    def list_users_or_observed_members(self) -> list[ExternalElnUser]:
        return sorted(self._observed_users.values(), key=lambda user: user.display_name.lower())

    def build_experiment_url(self, external_id: str) -> str:
        return urljoin(self.base_url, f"/knowledge/experiments/{external_id}")

    def _list_paginated(self, endpoint: str, *, fallback_endpoint: str | None = None) -> list[dict[str, Any]]:
        return self._list_paginated_with_params(endpoint, fallback_endpoint=fallback_endpoint)

    def _list_paginated_with_params(
        self,
        endpoint: str,
        *,
        fallback_endpoint: str | None = None,
        **params: Any,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self._request_json(
                endpoint,
                fallback_endpoint=fallback_endpoint,
                page=page,
                page_size=200,
                **params,
            )
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

    def _post_json(
        self,
        endpoint: str,
        *,
        fallback_endpoint: str | None = None,
        json_payload: dict[str, Any] | None = None,
    ) -> Any:
        try:
            return self._post_json_once(endpoint, json_payload=json_payload)
        except requests.HTTPError:
            if not fallback_endpoint:
                raise
            return self._post_json_once(fallback_endpoint, json_payload=json_payload)

    def _post_json_once(self, endpoint: str, *, json_payload: dict[str, Any] | None = None) -> Any:
        params = {"token": self.token}
        payload = dict(json_payload or {})
        response = requests.post(
            urljoin(self.base_url, endpoint),
            json=payload,
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def _hydrate_experiment_text_elements(self, payload: dict[str, Any]) -> None:
        for procedure in labguru_experiment_procedures(payload):
            elements = procedure.get("elements")
            if not isinstance(elements, list):
                continue
            for element in elements:
                if not isinstance(element, dict) or element.get("element_type") != "text":
                    continue
                element_id = str(element.get("id") or "").strip()
                if not element_id or element.get("data"):
                    continue
                try:
                    element_payload = self._request_json(
                        f"/api/v2/elements/{element_id}",
                        fallback_endpoint=f"/api/v1/elements/{element_id}.json",
                    )
                except requests.HTTPError:
                    continue
                if not isinstance(element_payload, dict):
                    continue
                for key in ("data", "description", "field_name", "updated_at", "created_at"):
                    if key in element_payload:
                        element[key] = element_payload.get(key)
                element["element_payload"] = element_payload


class ElabftwClient:
    system_key = "elabftw"

    def __init__(self, *, base_url: str, token: str):
        self.base_url = normalize_base_url(base_url)
        self.token = token

    def list_experiments(self) -> list[ExternalElnExperiment]:
        raise NotImplementedError("eLabFTW connector is planned but not implemented in V1.")

    def get_experiment(self, external_id: str) -> ExternalElnExperiment:
        raise NotImplementedError("eLabFTW connector is planned but not implemented in V1.")

    def create_experiment(
        self,
        *,
        title: str,
        description: str | None = None,
        procedure: str | None = None,
        conditions: str | None = None,
        project_id: str | None = None,
        folder_id: str | None = None,
    ) -> ExternalElnExperiment:
        _ = (title, description, procedure, conditions, project_id, folder_id)
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


def labguru_container_from_payload(payload: dict[str, Any]) -> ExternalElnContainer:
    external_id = str(payload.get("id") or payload.get("external_id") or "").strip()
    if not external_id:
        raise ValueError("Labguru container payload is missing id.")
    name = str(payload.get("name") or payload.get("title") or payload.get("display_name") or external_id).strip()
    return ExternalElnContainer(external_id=external_id, name=name, payload_json=payload)


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


def labguru_experiment_procedures(payload: dict[str, Any]) -> list[dict[str, Any]]:
    procedures: list[dict[str, Any]] = []
    raw_procedures = payload.get("experiment_procedures")
    if not isinstance(raw_procedures, list):
        return procedures
    for item in raw_procedures:
        if not isinstance(item, dict):
            continue
        procedure = item.get("experiment_procedure", item)
        if isinstance(procedure, dict):
            procedures.append(procedure)
    return procedures


def extract_labguru_text_sections(payload: dict[str, Any]) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    for procedure in labguru_experiment_procedures(payload):
        section_key = normalize_labguru_section_key(procedure)
        elements = procedure.get("elements")
        if not isinstance(elements, list):
            continue
        for element in elements:
            if not isinstance(element, dict):
                continue
            text = html_to_text(element.get("data") or element.get("description") or "")
            if text:
                sections.setdefault(section_key, []).append(text)
    return {key: "\n\n".join(values) for key, values in sections.items() if values}


def normalize_labguru_section_key(procedure: dict[str, Any]) -> str:
    raw = str(procedure.get("section_type") or procedure.get("name") or "section").strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return normalized or "section"


def html_to_text(value: object) -> str:
    text = str(value or "")
    if not text.strip():
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    lines = [" ".join(line.split()) for line in text.splitlines()]
    return "\n".join(line for line in lines if line).strip()


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
