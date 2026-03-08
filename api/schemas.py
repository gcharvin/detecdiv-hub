from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class HubBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class HealthResponse(HubBaseModel):
    status: str = "ok"
    service: str = "detecdiv-hub"


class UserSummary(HubBaseModel):
    id: UUID
    user_key: str
    display_name: str
    email: str | None = None
    role: str
    is_active: bool


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


class ProjectSummary(HubBaseModel):
    id: UUID
    project_key: str | None = None
    project_name: str
    status: str
    health_status: str
    visibility: str
    project_mat_bytes: int = 0
    project_dir_bytes: int = 0
    estimated_raw_bytes: int = 0
    total_bytes: int = 0
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    owner: UserSummary | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ProjectDetail(ProjectSummary):
    locations: list[ProjectLocationSummary] = Field(default_factory=list)


class ProjectAclSummary(HubBaseModel):
    id: int
    access_level: str
    user: UserSummary
    created_at: datetime | None = None


class ProjectAclCreate(HubBaseModel):
    user_key: str
    access_level: str = "viewer"


class ProjectDeletionRequest(HubBaseModel):
    delete_project_files: bool = False
    delete_linked_raw_data: bool = False
    confirm: bool = False


class ProjectDeletionPreview(HubBaseModel):
    project_id: UUID
    project_name: str
    delete_project_files: bool
    delete_linked_raw_data: bool
    reclaimable_bytes: int = 0
    preview_json: dict[str, Any] = Field(default_factory=dict)


class ProjectDeletionResult(HubBaseModel):
    event_id: UUID
    project_id: UUID
    status: str
    reclaimable_bytes: int = 0
    result_json: dict[str, Any] = Field(default_factory=dict)


class ProjectNoteSummary(HubBaseModel):
    id: int
    note_text: str
    is_pinned: bool
    author: UserSummary | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ProjectNoteCreate(HubBaseModel):
    note_text: str
    is_pinned: bool = False


class ProjectGroupSummary(HubBaseModel):
    id: UUID
    group_key: str
    display_name: str
    description: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    owner: UserSummary | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ProjectGroupDetail(ProjectGroupSummary):
    project_count: int = 0
    projects: list[ProjectSummary] = Field(default_factory=list)


class ProjectGroupCreate(HubBaseModel):
    group_key: str
    display_name: str
    description: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


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
    storage_root_name: str | None = None
    host_scope: str = "server"
    root_type: str = "project_root"
    owner_user_key: str | None = None
    visibility: str = "private"
    clear_existing_for_root: bool = False
    requested_by: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class IndexResponse(HubBaseModel):
    status: str
    source_kind: str
    source_path: str
    storage_root_name: str
    owner_user_key: str
    visibility: str
    scanned_projects: int
    indexed_projects: int
    deleted_projects: int = 0
    message: str
