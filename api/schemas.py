from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str = "detecdiv-hub"


class ProjectSummary(BaseModel):
    id: UUID
    project_key: str | None = None
    project_name: str
    status: str
    health_status: str
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class JobCreateRequest(BaseModel):
    project_id: UUID | None = None
    pipeline_id: UUID | None = None
    execution_target_id: UUID | None = None
    requested_mode: str = "auto"
    priority: int = 100
    requested_by: str | None = None
    requested_from_host: str | None = None
    params_json: dict[str, Any] = Field(default_factory=dict)


class JobSummary(BaseModel):
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


class IndexRequest(BaseModel):
    source_kind: str = "project_root"
    source_path: str
    requested_by: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)

