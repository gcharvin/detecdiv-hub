from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class HubBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class HealthResponse(HubBaseModel):
    status: str = "ok"
    service: str = "detecdiv-hub"


class ProjectSummary(HubBaseModel):
    id: UUID
    project_key: str | None = None
    project_name: str
    status: str
    health_status: str
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class StorageRootSummary(HubBaseModel):
    id: int
    name: str
    root_type: str
    host_scope: str
    path_prefix: str


class ProjectLocationSummary(HubBaseModel):
    id: int
    relative_path: str
    project_file_name: str | None = None
    access_mode: str
    is_preferred: bool
    storage_root: StorageRootSummary


class ProjectDetail(ProjectSummary):
    locations: list[ProjectLocationSummary] = Field(default_factory=list)


class JobCreateRequest(HubBaseModel):
    project_id: UUID | None = None
    pipeline_id: UUID | None = None
    execution_target_id: UUID | None = None
    requested_mode: str = "auto"
    priority: int = 100
    requested_by: str | None = None
    requested_from_host: str | None = None
    params_json: dict[str, Any] = Field(default_factory=dict)


class JobSummary(HubBaseModel):
    id: UUID
    project_id: UUID | None = None
    pipeline_id: UUID | None = None
    execution_target_id: UUID | None = None
    requested_mode: str
    resolved_mode: str | None = None
    status: str
    priority: int
    requested_by: str | None = None
    requested_from_host: str | None = None
    params_json: dict[str, Any] = Field(default_factory=dict)
    result_json: dict[str, Any] = Field(default_factory=dict)
    error_text: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime | None = None


class IndexRequest(HubBaseModel):
    source_kind: str = "project_root"
    source_path: str
    requested_by: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
