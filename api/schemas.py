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


class UserCreate(HubBaseModel):
    user_key: str
    display_name: str
    email: str | None = None
    role: str = "user"
    is_active: bool = True
    password: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class UserBulkUpsertItem(HubBaseModel):
    user_key: str
    display_name: str | None = None
    email: str | None = None
    role: str = "user"
    is_active: bool = True
    password: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class UserBulkUpsertRequest(HubBaseModel):
    users: list[UserBulkUpsertItem] = Field(default_factory=list)


class UserBulkUpsertResponse(HubBaseModel):
    created_count: int
    updated_count: int
    users: list[UserSummary] = Field(default_factory=list)


class UserUpdate(HubBaseModel):
    display_name: str | None = None
    email: str | None = None
    role: str | None = None
    is_active: bool | None = None
    password: str | None = None
    metadata_json: dict[str, Any] | None = None


class AuthLoginRequest(HubBaseModel):
    user_key: str
    password: str
    client_label: str | None = None


class AuthLoginResponse(HubBaseModel):
    user: UserSummary
    session_token: str
    expires_at: datetime


class AuthSessionResponse(HubBaseModel):
    authenticated: bool
    auth_mode: str
    user: UserSummary | None = None
    expires_at: datetime | None = None


class UserSessionSummary(HubBaseModel):
    id: UUID
    status: str
    client_label: str | None = None
    last_seen_at: datetime | None = None
    expires_at: datetime
    created_at: datetime | None = None
    user: UserSummary | None = None


class StorageRootSummary(HubBaseModel):
    id: int
    name: str
    root_type: str
    host_scope: str
    path_prefix: str


class StorageRootBrowseEntry(HubBaseModel):
    name: str
    relative_path: str
    absolute_path: str


class StorageRootBrowseResponse(HubBaseModel):
    storage_root: StorageRootSummary
    current_relative_path: str
    current_absolute_path: str
    parent_relative_path: str | None = None
    directories: list[StorageRootBrowseEntry] = Field(default_factory=list)


class RawDatasetSummary(HubBaseModel):
    id: UUID
    external_key: str | None = None
    microscope_name: str | None = None
    acquisition_label: str
    data_format: str = "unknown"
    visibility: str
    status: str
    completeness_status: str
    lifecycle_tier: str
    archive_status: str
    archive_uri: str | None = None
    archive_compression: str | None = None
    reclaimable_bytes: int = 0
    last_accessed_at: datetime | None = None
    total_bytes: int = 0
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    owner: UserSummary | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ProjectLocationSummary(HubBaseModel):
    id: int
    relative_path: str
    project_file_name: str | None = None
    project_mat_path: str | None = None
    project_dir_path: str | None = None
    access_mode: str
    is_preferred: bool
    storage_root: StorageRootSummary


class ArtifactSummary(HubBaseModel):
    id: UUID
    job_id: UUID
    artifact_kind: str
    uri: str
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


class RawDatasetPositionSummary(HubBaseModel):
    id: UUID
    raw_dataset_id: UUID
    position_key: str
    display_name: str
    position_index: int | None = None
    status: str
    preview_status: str
    preview_artifact: ArtifactSummary | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ProjectSummary(HubBaseModel):
    id: UUID
    experiment_project_id: UUID | None = None
    project_key: str | None = None
    project_name: str
    status: str
    health_status: str
    visibility: str
    fov_count: int = 0
    roi_count: int = 0
    classifier_count: int = 0
    processor_count: int = 0
    pipeline_run_count: int = 0
    available_raw_count: int = 0
    missing_raw_count: int = 0
    run_json_count: int = 0
    h5_count: int = 0
    h5_bytes: int = 0
    latest_run_status: str | None = None
    latest_run_at: datetime | None = None
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
    raw_datasets: list[RawDatasetSummary] = Field(default_factory=list)


class ProjectUpdate(HubBaseModel):
    owner_user_key: str | None = None
    visibility: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


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


class ExperimentProjectSummary(HubBaseModel):
    id: UUID
    experiment_key: str | None = None
    title: str
    visibility: str
    status: str
    summary: str | None = None
    total_raw_bytes: int = 0
    total_derived_bytes: int = 0
    raw_dataset_count: int = 0
    analysis_project_count: int = 0
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    owner: UserSummary | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    last_indexed_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class PublicationRecordSummary(HubBaseModel):
    id: UUID
    system_key: str
    status: str
    external_id: str | None = None
    external_url: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)
    error_text: str | None = None
    last_attempt_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class StorageLifecycleEventSummary(HubBaseModel):
    id: UUID
    event_kind: str
    from_tier: str | None = None
    to_tier: str | None = None
    archive_status: str | None = None
    reclaimable_bytes: int = 0
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    requested_by: UserSummary | None = None
    created_at: datetime | None = None


class ExperimentProjectDetail(ExperimentProjectSummary):
    raw_datasets: list[RawDatasetSummary] = Field(default_factory=list)
    analysis_projects: list[ProjectSummary] = Field(default_factory=list)
    publication_records: list[PublicationRecordSummary] = Field(default_factory=list)


class RawDatasetLocationSummary(HubBaseModel):
    id: int
    relative_path: str
    absolute_path: str | None = None
    access_mode: str
    is_preferred: bool
    storage_root: StorageRootSummary


class RawDatasetDetail(RawDatasetSummary):
    locations: list[RawDatasetLocationSummary] = Field(default_factory=list)
    experiment_ids: list[UUID] = Field(default_factory=list)
    analysis_project_ids: list[UUID] = Field(default_factory=list)
    analysis_projects: list[ProjectSummary] = Field(default_factory=list)
    positions: list[RawDatasetPositionSummary] = Field(default_factory=list)
    lifecycle_events: list[StorageLifecycleEventSummary] = Field(default_factory=list)


class RawDatasetUpdate(HubBaseModel):
    data_format: str | None = None
    owner_user_key: str | None = None
    visibility: str | None = None
    lifecycle_tier: str | None = None
    archive_status: str | None = None
    archive_uri: str | None = None
    archive_compression: str | None = None
    metadata_json: dict[str, Any] | None = None


class RawDatasetArchiveRequest(HubBaseModel):
    archive_uri: str | None = None
    archive_compression: str | None = None
    mark_archived: bool = False


class RawDatasetArchivePreview(HubBaseModel):
    raw_dataset_id: UUID
    acquisition_label: str
    current_tier: str
    target_tier: str
    reclaimable_bytes: int = 0
    preview_json: dict[str, Any] = Field(default_factory=dict)


class RawDatasetArchivePolicyRequest(HubBaseModel):
    older_than_days: int = 30
    min_total_bytes: int = 0
    limit: int = 25
    owner_key: str | None = None
    search: str | None = None
    lifecycle_tiers: list[str] = Field(default_factory=lambda: ["hot"])
    archive_statuses: list[str] = Field(
        default_factory=lambda: ["none", "restored", "archive_failed", "restore_failed"]
    )
    archive_uri: str | None = None
    archive_compression: str | None = None
    mark_archived: bool = False


class RawDatasetArchivePolicyCandidate(HubBaseModel):
    raw_dataset_id: UUID
    acquisition_label: str
    owner: UserSummary | None = None
    lifecycle_tier: str
    archive_status: str
    total_bytes: int = 0
    reclaimable_bytes: int = 0
    last_activity_at: datetime | None = None
    suggested_archive_uri: str | None = None
    suggested_archive_compression: str | None = None


class RawDatasetArchivePolicyPreview(HubBaseModel):
    generated_at: datetime
    older_than_days: int
    min_total_bytes: int = 0
    candidate_count: int = 0
    total_candidate_bytes: int = 0
    total_reclaimable_bytes: int = 0
    skipped_conflicts: int = 0
    candidates: list[RawDatasetArchivePolicyCandidate] = Field(default_factory=list)


class RawDatasetArchivePolicyQueueResult(HubBaseModel):
    generated_at: datetime
    queued_count: int = 0
    skipped_count: int = 0
    queued_job_ids: list[UUID] = Field(default_factory=list)
    raw_dataset_ids: list[UUID] = Field(default_factory=list)
    message: str


class ArchivePolicyAutomaticConfig(HubBaseModel):
    enabled: bool
    interval_minutes: int
    run_as_user_key: str
    older_than_days: int
    min_total_bytes: int = 0
    limit: int = 0
    owner_key: str | None = None
    search: str | None = None
    lifecycle_tiers: list[str] = Field(default_factory=list)
    archive_statuses: list[str] = Field(default_factory=list)
    archive_uri: str | None = None
    archive_compression: str | None = None
    delete_hot_source: bool = False


class ArchivePolicyRunSummary(HubBaseModel):
    id: UUID
    trigger_mode: str
    status: str
    report_only: bool = False
    candidate_count: int = 0
    queued_count: int = 0
    skipped_count: int = 0
    total_reclaimable_bytes: int = 0
    error_text: str | None = None
    config_json: dict[str, Any] = Field(default_factory=dict)
    result_json: dict[str, Any] = Field(default_factory=dict)
    triggered_by: UserSummary | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class ArchivePolicyAutomaticStatus(HubBaseModel):
    config: ArchivePolicyAutomaticConfig
    last_run: ArchivePolicyRunSummary | None = None
    recent_runs: list[ArchivePolicyRunSummary] = Field(default_factory=list)


class ArchivePolicyAutomaticRunRequest(HubBaseModel):
    report_only: bool = True


class MicroManagerIngestAutomaticConfig(HubBaseModel):
    enabled: bool
    interval_minutes: int
    run_as_user_key: str
    landing_root: str | None = None
    storage_root_name: str | None = None
    host_scope: str
    visibility: str
    settle_seconds: int
    max_datasets: int
    grouping_window_hours: int
    post_ingest_pipeline_key: str | None = None
    post_ingest_requested_mode: str | None = None
    post_ingest_priority: int | None = None


class MicroManagerIngestRunSummary(HubBaseModel):
    id: UUID
    trigger_mode: str
    status: str
    report_only: bool = False
    candidate_count: int = 0
    ingested_count: int = 0
    experiment_count: int = 0
    skipped_count: int = 0
    error_text: str | None = None
    config_json: dict[str, Any] = Field(default_factory=dict)
    result_json: dict[str, Any] = Field(default_factory=dict)
    triggered_by: UserSummary | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class MicroManagerIngestAutomaticStatus(HubBaseModel):
    config: MicroManagerIngestAutomaticConfig
    last_run: MicroManagerIngestRunSummary | None = None
    recent_runs: list[MicroManagerIngestRunSummary] = Field(default_factory=list)


class MicroManagerIngestRunRequest(HubBaseModel):
    report_only: bool = True


class ExperimentProjectCreate(HubBaseModel):
    experiment_key: str | None = None
    title: str
    visibility: str = "private"
    status: str = "indexed"
    summary: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime | None = None
    ended_at: datetime | None = None
    last_indexed_at: datetime | None = None


class ExperimentProjectUpdate(HubBaseModel):
    owner_user_key: str | None = None
    title: str | None = None
    visibility: str | None = None
    status: str | None = None
    summary: str | None = None
    metadata_json: dict[str, Any] | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    last_indexed_at: datetime | None = None


class StorageMigrationItemSummary(HubBaseModel):
    id: int
    item_type: str
    legacy_path: str
    legacy_key: str | None = None
    display_name: str
    status: str
    action: str
    proposed_experiment_key: str | None = None
    proposed_project_key: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class StorageMigrationPlanSummary(HubBaseModel):
    id: UUID
    batch_name: str
    source_kind: str
    source_path: str
    storage_root_name: str | None = None
    host_scope: str
    root_type: str
    strategy: str
    status: str
    owner: UserSummary | None = None
    summary_json: dict[str, Any] = Field(default_factory=dict)
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class StorageMigrationPlanDetail(StorageMigrationPlanSummary):
    items: list[StorageMigrationItemSummary] = Field(default_factory=list)


class StorageMigrationPlanCreate(HubBaseModel):
    batch_name: str
    source_kind: str
    source_path: str
    storage_root_name: str | None = None
    host_scope: str = "server"
    root_type: str = "legacy_root"
    strategy: str = "discover_only"
    max_items: int = 200
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class StorageMigrationItemUpdate(HubBaseModel):
    status: str | None = None
    action: str | None = None
    proposed_experiment_key: str | None = None
    proposed_project_key: str | None = None
    metadata_json: dict[str, Any] | None = None


class StorageMigrationAttachExistingRequest(HubBaseModel):
    experiment_key: str


class StorageMigrationExecuteResponse(HubBaseModel):
    plan_id: UUID
    processed_items: int
    experiment_ids: list[UUID] = Field(default_factory=list)
    message: str


class JobCreateRequest(HubBaseModel):
    project_id: UUID | None = None
    raw_dataset_id: UUID | None = None
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
    raw_dataset_id: UUID | None = None
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
    heartbeat_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime | None = None


class RawPreviewVideoQueueRequest(HubBaseModel):
    position_id: UUID | None = None
    position_key: str | None = None
    force: bool = False
    requested_mode: str = "auto"
    priority: int = 100
    params_json: dict[str, Any] = Field(default_factory=dict)


class RawPreviewVideoQueueResult(HubBaseModel):
    raw_dataset_id: UUID
    position_id: UUID | None = None
    position_key: str | None = None
    job: JobSummary
    message: str


class RawPreviewQualityConfig(HubBaseModel):
    fps: int
    frame_mode: str
    max_frames: int
    max_dimension: int
    binning_factor: int
    crf: int
    preset: str
    include_existing: bool
    artifact_root: str | None = None
    ffmpeg_command: str | None = None


class RawPreviewQualityUpdate(HubBaseModel):
    fps: int | None = None
    frame_mode: str | None = None
    max_frames: int | None = None
    max_dimension: int | None = None
    binning_factor: int | None = None
    crf: int | None = None
    preset: str | None = None
    include_existing: bool | None = None
    artifact_root: str | None = None
    ffmpeg_command: str | None = None


class RawPreviewQualitySummary(HubBaseModel):
    sample_count: int = 0
    avg_width: float | None = None
    avg_height: float | None = None
    avg_fps: float | None = None
    avg_bitrate_kbps: float | None = None


class RawPreviewQualitySample(HubBaseModel):
    artifact_id: UUID
    created_at: datetime | None = None
    raw_dataset_id: UUID | None = None
    acquisition_label: str | None = None
    position_key: str | None = None
    width: int | None = None
    height: int | None = None
    fps: int | None = None
    frame_count: int | None = None
    duration_seconds: float | None = None
    file_size_bytes: int | None = None
    bitrate_kbps: float | None = None


class RawPreviewQualityStatus(HubBaseModel):
    config: RawPreviewQualityConfig
    summary: RawPreviewQualitySummary
    recent_samples: list[RawPreviewQualitySample] = Field(default_factory=list)


class ProjectRawPreviewQueueRequest(HubBaseModel):
    force: bool = False
    requested_mode: str = "auto"
    priority: int = 100
    params_json: dict[str, Any] = Field(default_factory=dict)


class ProjectRawPreviewQueueResult(HubBaseModel):
    project_id: UUID
    queued_count: int = 0
    skipped_count: int = 0
    raw_dataset_ids: list[UUID] = Field(default_factory=list)
    queued_job_ids: list[UUID] = Field(default_factory=list)
    message: str


class PipelineRunCreateRequest(HubBaseModel):
    project_id: UUID
    pipeline_id: UUID | None = None
    execution_target_id: UUID | None = None
    requested_mode: str = "auto"
    priority: int = 100
    requested_by: str | None = None
    requested_from_host: str | None = None
    project_ref: dict[str, Any] = Field(default_factory=dict)
    pipeline_ref: dict[str, Any] = Field(default_factory=dict)
    run_request: dict[str, Any] = Field(default_factory=dict)
    execution: dict[str, Any] = Field(default_factory=dict)


class PipelineRunSummary(JobSummary):
    pass


class PipelineRunUpdateRequest(HubBaseModel):
    project_id: UUID | None = None
    pipeline_id: UUID | None = None
    execution_target_id: UUID | None = None
    requested_mode: str | None = None
    priority: int | None = None
    requested_by: str | None = None
    requested_from_host: str | None = None
    project_ref: dict[str, Any] | None = None
    pipeline_ref: dict[str, Any] | None = None
    run_request: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None


class ExecutionTargetSummary(HubBaseModel):
    id: UUID
    target_key: str | None = None
    display_name: str
    target_kind: str
    host_name: str | None = None
    supports_gpu: bool
    supports_matlab: bool
    supports_python: bool
    status: str
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class ExecutionTargetCreate(HubBaseModel):
    target_key: str | None = None
    display_name: str
    target_kind: str
    host_name: str | None = None
    supports_gpu: bool = False
    supports_matlab: bool = False
    supports_python: bool = True
    status: str = "online"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class ExecutionTargetUpdate(HubBaseModel):
    display_name: str | None = None
    target_kind: str | None = None
    host_name: str | None = None
    supports_gpu: bool | None = None
    supports_matlab: bool | None = None
    supports_python: bool | None = None
    status: str | None = None
    metadata_json: dict[str, Any] | None = None


class PipelineSummary(HubBaseModel):
    id: UUID
    pipeline_key: str | None = None
    display_name: str
    version: str
    runtime_kind: str
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ObservedPipelineSummary(HubBaseModel):
    identity: str
    display_name: str
    pipeline_key: str | None = None
    runtime_kind: str = "matlab"
    source: str
    project_count: int = 0
    run_count: int = 0
    latest_run_status: str | None = None
    latest_run_at: datetime | None = None
    project_names: list[str] = Field(default_factory=list)
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class PipelineCreate(HubBaseModel):
    pipeline_key: str | None = None
    display_name: str
    version: str = "1.0"
    runtime_kind: str = "matlab"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class PipelineUpdate(HubBaseModel):
    display_name: str | None = None
    version: str | None = None
    runtime_kind: str | None = None
    metadata_json: dict[str, Any] | None = None


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


class IndexJobSummary(HubBaseModel):
    id: UUID
    source_kind: str
    source_path: str
    storage_root_name: str | None = None
    host_scope: str
    root_type: str
    visibility: str
    clear_existing_for_root: bool = False
    status: str
    phase: str = "queued"
    total_projects: int = 0
    scanned_projects: int = 0
    indexed_projects: int = 0
    failed_projects: int = 0
    deleted_projects: int = 0
    mat_files_seen: int = 0
    current_project_path: str | None = None
    message: str | None = None
    error_text: str | None = None
    owner: UserSummary | None = None
    requested_by: UserSummary | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    heartbeat_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime | None = None


class IndexJobDetail(IndexJobSummary):
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    result_json: dict[str, Any] = Field(default_factory=dict)


class IndexJobLaunchResponse(HubBaseModel):
    status: str
    launch_mode: str
    job: IndexJobSummary
    message: str


class IndexResponse(HubBaseModel):
    status: str
    source_kind: str
    source_path: str
    storage_root_name: str
    owner_user_key: str
    visibility: str
    scanned_projects: int
    indexed_projects: int
    failed_projects: int = 0
    deleted_projects: int = 0
    total_projects: int = 0
    indexed_pipelines: int = 0
    failed_pipelines: int = 0
    stale_cleanup_skipped: bool = False
    message: str


class DashboardHealthBucket(HubBaseModel):
    health_status: str
    project_count: int
    total_bytes: int = 0


class DashboardSummary(HubBaseModel):
    user: UserSummary
    total_projects: int = 0
    owned_projects: int = 0
    shared_projects: int = 0
    public_projects: int = 0
    total_bytes: int = 0
    owned_bytes: int = 0
    note_count: int = 0
    group_count: int = 0
    deleted_projects: int = 0
    health: list[DashboardHealthBucket] = Field(default_factory=list)
