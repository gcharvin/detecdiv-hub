from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BIGINT, Boolean, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from api.db import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_key: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str | None] = mapped_column(String)
    role: Mapped[str] = mapped_column(String, nullable=False, default="user")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    password_hash: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    owned_projects: Mapped[list["Project"]] = relationship(back_populates="owner")
    owned_raw_datasets: Mapped[list["RawDataset"]] = relationship(back_populates="owner")
    project_acl_entries: Mapped[list["ProjectAcl"]] = relationship(back_populates="user")
    project_groups: Mapped[list["ProjectGroup"]] = relationship(back_populates="owner")
    project_notes: Mapped[list["ProjectNote"]] = relationship(back_populates="author")
    requested_indexing_jobs: Mapped[list["IndexingJob"]] = relationship(
        back_populates="requested_by", foreign_keys="IndexingJob.requested_by_user_id"
    )
    owned_indexing_jobs: Mapped[list["IndexingJob"]] = relationship(
        back_populates="owner", foreign_keys="IndexingJob.owner_user_id"
    )
    sessions: Mapped[list["UserSession"]] = relationship(back_populates="user")


class StorageRoot(Base):
    __tablename__ = "storage_roots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    root_type: Mapped[str] = mapped_column(String, nullable=False)
    host_scope: Mapped[str] = mapped_column(String, nullable=False)
    path_prefix: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    project_locations: Mapped[list["ProjectLocation"]] = relationship(back_populates="storage_root")
    raw_dataset_locations: Mapped[list["RawDatasetLocation"]] = relationship(back_populates="storage_root")


class UserSession(Base):
    __tablename__ = "user_sessions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    token_hash: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String, nullable=False, default="active")
    client_label: Mapped[str | None] = mapped_column(String)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    user: Mapped[User] = relationship(back_populates="sessions")


class RawDataset(Base):
    __tablename__ = "raw_datasets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    external_key: Mapped[str | None] = mapped_column(String, unique=True)
    microscope_name: Mapped[str | None] = mapped_column(String)
    acquisition_label: Mapped[str] = mapped_column(String, nullable=False)
    visibility: Mapped[str] = mapped_column(String, nullable=False, default="private")
    status: Mapped[str] = mapped_column(String, nullable=False, default="discovered")
    completeness_status: Mapped[str] = mapped_column(String, nullable=False, default="unknown")
    total_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    last_size_scan_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    owner: Mapped[User | None] = relationship(back_populates="owned_raw_datasets")
    locations: Mapped[list["RawDatasetLocation"]] = relationship(back_populates="raw_dataset")
    project_links: Mapped[list["ProjectRawLink"]] = relationship(back_populates="raw_dataset")


class RawDatasetLocation(Base):
    __tablename__ = "raw_dataset_locations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    raw_dataset_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("raw_datasets.id", ondelete="CASCADE"), nullable=False
    )
    storage_root_id: Mapped[int] = mapped_column(
        ForeignKey("storage_roots.id", ondelete="RESTRICT"), nullable=False
    )
    relative_path: Mapped[str] = mapped_column(String, nullable=False)
    is_preferred: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    access_mode: Mapped[str] = mapped_column(String, nullable=False, default="read")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    raw_dataset: Mapped[RawDataset] = relationship(back_populates="locations")
    storage_root: Mapped[StorageRoot] = relationship(back_populates="raw_dataset_locations")


class Project(Base):
    __tablename__ = "detecdiv_projects"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    project_key: Mapped[str | None] = mapped_column(String, unique=True)
    project_name: Mapped[str] = mapped_column(String, nullable=False)
    visibility: Mapped[str] = mapped_column(String, nullable=False, default="private")
    status: Mapped[str] = mapped_column(String, nullable=False, default="indexed")
    health_status: Mapped[str] = mapped_column(String, nullable=False, default="ok")
    fov_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    roi_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    classifier_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processor_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pipeline_run_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_raw_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    missing_raw_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    run_json_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    h5_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    h5_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    latest_run_status: Mapped[str | None] = mapped_column(String)
    latest_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    project_mat_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    project_dir_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    estimated_raw_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    total_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    last_size_scan_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    owner: Mapped[User | None] = relationship(back_populates="owned_projects")
    jobs: Mapped[list["Job"]] = relationship(back_populates="project")
    locations: Mapped[list["ProjectLocation"]] = relationship(back_populates="project")
    raw_links: Mapped[list["ProjectRawLink"]] = relationship(back_populates="project")
    acl_entries: Mapped[list["ProjectAcl"]] = relationship(back_populates="project")
    notes: Mapped[list["ProjectNote"]] = relationship(back_populates="project")
    group_memberships: Mapped[list["ProjectGroupMember"]] = relationship(back_populates="project")
    deletion_events: Mapped[list["ProjectDeletionEvent"]] = relationship(back_populates="project")


class ProjectLocation(Base):
    __tablename__ = "project_locations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("detecdiv_projects.id", ondelete="CASCADE"), nullable=False
    )
    storage_root_id: Mapped[int] = mapped_column(
        ForeignKey("storage_roots.id", ondelete="RESTRICT"), nullable=False
    )
    relative_path: Mapped[str] = mapped_column(String, nullable=False)
    project_file_name: Mapped[str | None] = mapped_column(String)
    access_mode: Mapped[str] = mapped_column(String, nullable=False, default="readwrite")
    is_preferred: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    project: Mapped[Project] = relationship(back_populates="locations")
    storage_root: Mapped[StorageRoot] = relationship(back_populates="project_locations")


class ProjectRawLink(Base):
    __tablename__ = "project_raw_links"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("detecdiv_projects.id", ondelete="CASCADE"), nullable=False
    )
    raw_dataset_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("raw_datasets.id", ondelete="CASCADE"), nullable=False
    )
    link_type: Mapped[str] = mapped_column(String, nullable=False, default="source")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    project: Mapped[Project] = relationship(back_populates="raw_links")
    raw_dataset: Mapped[RawDataset] = relationship(back_populates="project_links")


class ProjectAcl(Base):
    __tablename__ = "project_acl"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("detecdiv_projects.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    access_level: Mapped[str] = mapped_column(String, nullable=False, default="viewer")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    project: Mapped[Project] = relationship(back_populates="acl_entries")
    user: Mapped[User] = relationship(back_populates="project_acl_entries")


class ProjectGroup(Base):
    __tablename__ = "project_groups"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    group_key: Mapped[str] = mapped_column(String, nullable=False)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    owner: Mapped[User] = relationship(back_populates="project_groups")
    members: Mapped[list["ProjectGroupMember"]] = relationship(back_populates="group")


class ProjectGroupMember(Base):
    __tablename__ = "project_group_members"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("project_groups.id", ondelete="CASCADE"), nullable=False
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("detecdiv_projects.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    group: Mapped[ProjectGroup] = relationship(back_populates="members")
    project: Mapped[Project] = relationship(back_populates="group_memberships")


class ProjectNote(Base):
    __tablename__ = "project_notes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("detecdiv_projects.id", ondelete="CASCADE"), nullable=False
    )
    author_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    note_text: Mapped[str] = mapped_column(Text, nullable=False)
    is_pinned: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    project: Mapped[Project] = relationship(back_populates="notes")
    author: Mapped[User | None] = relationship(back_populates="project_notes")


class ProjectDeletionEvent(Base):
    __tablename__ = "project_deletion_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("detecdiv_projects.id", ondelete="CASCADE"), nullable=False
    )
    requested_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    status: Mapped[str] = mapped_column(String, nullable=False, default="previewed")
    delete_project_files: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    delete_linked_raw_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reclaimable_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    preview_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    result_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    project: Mapped[Project] = relationship(back_populates="deletion_events")


class IndexingJob(Base):
    __tablename__ = "indexing_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    requested_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    owner_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    source_kind: Mapped[str] = mapped_column(String, nullable=False, default="project_root")
    source_path: Mapped[str] = mapped_column(Text, nullable=False)
    storage_root_name: Mapped[str | None] = mapped_column(String)
    host_scope: Mapped[str] = mapped_column(String, nullable=False, default="server")
    root_type: Mapped[str] = mapped_column(String, nullable=False, default="project_root")
    visibility: Mapped[str] = mapped_column(String, nullable=False, default="private")
    clear_existing_for_root: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="queued")
    total_projects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    scanned_projects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    indexed_projects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_projects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deleted_projects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    current_project_path: Mapped[str | None] = mapped_column(Text)
    message: Mapped[str | None] = mapped_column(Text)
    error_text: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    result_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    requested_by: Mapped[User | None] = relationship(
        back_populates="requested_indexing_jobs", foreign_keys=[requested_by_user_id]
    )
    owner: Mapped[User | None] = relationship(
        back_populates="owned_indexing_jobs", foreign_keys=[owner_user_id]
    )


class Pipeline(Base):
    __tablename__ = "pipelines"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_key: Mapped[str | None] = mapped_column(String, unique=True)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[str] = mapped_column(String, nullable=False, default="1.0")
    runtime_kind: Mapped[str] = mapped_column(String, nullable=False, default="matlab")
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ExecutionTarget(Base):
    __tablename__ = "execution_targets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    target_key: Mapped[str | None] = mapped_column(String, unique=True)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    target_kind: Mapped[str] = mapped_column(String, nullable=False)
    host_name: Mapped[str | None] = mapped_column(String)
    supports_gpu: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    supports_matlab: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    supports_python: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    status: Mapped[str] = mapped_column(String, nullable=False, default="online")
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("detecdiv_projects.id", ondelete="SET NULL")
    )
    raw_dataset_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("raw_datasets.id", ondelete="SET NULL")
    )
    pipeline_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="SET NULL")
    )
    execution_target_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("execution_targets.id", ondelete="SET NULL")
    )
    requested_mode: Mapped[str] = mapped_column(String, nullable=False, default="auto")
    resolved_mode: Mapped[str | None] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, nullable=False, default="queued")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    requested_by: Mapped[str | None] = mapped_column(String)
    requested_from_host: Mapped[str | None] = mapped_column(String)
    params_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    result_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    project: Mapped[Project | None] = relationship(back_populates="jobs")


class Artifact(Base):
    __tablename__ = "artifacts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    artifact_kind: Mapped[str] = mapped_column(String, nullable=False)
    uri: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
