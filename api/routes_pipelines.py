import json
from datetime import datetime
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.db import get_db
from api.models import Pipeline, Project, User
from api.schemas import ObservedPipelineSummary, PipelineCreate, PipelineSummary, PipelineUpdate
from api.services.users import get_current_user, project_access_filter


router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.get("", response_model=list[PipelineSummary])
def list_pipelines(
    search: str | None = None,
    runtime_kind: str | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[Pipeline]:
    _ = current_user
    stmt = select(Pipeline).order_by(Pipeline.display_name.asc(), Pipeline.version.asc())
    if search:
        pattern = f"%{search.strip()}%"
        stmt = stmt.where(
            (Pipeline.display_name.ilike(pattern))
            | (Pipeline.pipeline_key.ilike(pattern))
            | (Pipeline.version.ilike(pattern))
        )
    if runtime_kind:
        stmt = stmt.where(Pipeline.runtime_kind == runtime_kind)
    return list(db.scalars(stmt))


@router.post("", response_model=PipelineSummary, status_code=status.HTTP_201_CREATED)
def create_pipeline(
    payload: PipelineCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Pipeline:
    if current_user.role not in {"admin", "service"}:
        # For now, allow normal users too. This guard documents the future tightening point.
        pass
    normalized_metadata = normalize_pipeline_metadata(payload.metadata_json)
    pipeline = Pipeline(
        pipeline_key=payload.pipeline_key,
        display_name=payload.display_name,
        version=payload.version,
        runtime_kind=payload.runtime_kind,
        metadata_json=normalized_metadata,
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    return pipeline


@router.get("/observed", response_model=list[ObservedPipelineSummary])
def list_observed_pipelines(
    search: str | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[ObservedPipelineSummary]:
    stmt = (
        select(Project)
        .options(joinedload(Project.owner))
        .where(Project.status != "deleted")
        .where(project_access_filter(current_user))
        .order_by(Project.project_name.asc())
    )
    projects = list(db.scalars(stmt).unique())

    aggregated: dict[str, dict] = {}
    for project in projects:
        inventory = (project.metadata_json or {}).get("inventory") or {}
        runs = inventory.get("pipeline_runs") or []
        if not isinstance(runs, list):
            continue
        for run in runs:
            if not isinstance(run, dict):
                continue
            pipeline_key = local_text(run.get("pipeline_id")) or local_text(run.get("tag"))
            pipeline_path = local_text(run.get("pipeline_path"))
            display_name = local_pipeline_display_name(run)
            identity = pipeline_key or pipeline_path or display_name
            if not identity:
                continue
            bucket = aggregated.get(identity)
            if bucket is None:
                bucket = {
                    "identity": identity,
                    "display_name": display_name,
                    "pipeline_key": pipeline_key,
                    "runtime_kind": "matlab",
                    "source": "project_observed",
                    "project_ids": set(),
                    "project_names": [],
                    "run_count": 0,
                    "latest_run_status": None,
                    "latest_run_at": None,
                    "metadata_json": {"pipeline_path": pipeline_path, "samples": []},
                }
                aggregated[identity] = bucket

            bucket["run_count"] += 1
            if project.id not in bucket["project_ids"]:
                bucket["project_ids"].add(project.id)
                if len(bucket["project_names"]) < 8:
                    bucket["project_names"].append(project.project_name)

            if len(bucket["metadata_json"]["samples"]) < 5:
                bucket["metadata_json"]["samples"].append(
                    {
                        "project_name": project.project_name,
                        "pipeline_path": pipeline_path,
                        "status": run.get("status"),
                        "timestamp": run.get("timestamp"),
                    }
                )

            run_time = local_parse_datetime(run.get("timestamp"))
            if run_time is not None:
                current_latest = bucket["latest_run_at"]
                if current_latest is None or run_time > current_latest:
                    bucket["latest_run_at"] = run_time
                    bucket["latest_run_status"] = local_text(run.get("status"))

    observed = []
    pattern = (search or "").strip().lower()
    for bucket in aggregated.values():
        project_count = len(bucket.pop("project_ids"))
        summary = ObservedPipelineSummary.model_validate(
            {
                **bucket,
                "project_count": project_count,
            }
        )
        if pattern:
            haystack = " ".join(
                [
                    summary.display_name or "",
                    summary.pipeline_key or "",
                    *summary.project_names,
                    str(summary.metadata_json.get("pipeline_path") or ""),
                ]
            ).lower()
            if pattern not in haystack:
                continue
        observed.append(summary)

    observed.sort(key=lambda item: ((item.display_name or "").lower(), (item.pipeline_key or "").lower()))
    return observed


@router.post("/import-observed", response_model=list[PipelineSummary], status_code=status.HTTP_201_CREATED)
def import_observed_pipelines(
    search: str | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[Pipeline]:
    if current_user.role not in {"admin", "service"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Pipeline admin required")

    observed = list_observed_pipelines(search=search, db=db, current_user=current_user)
    imported: list[Pipeline] = []
    for item in observed:
        stmt = select(Pipeline).where(
            Pipeline.pipeline_key == item.pipeline_key if item.pipeline_key else Pipeline.display_name == item.display_name
        )
        pipeline = db.scalars(stmt).first()
        if pipeline is None:
            pipeline = Pipeline(
                pipeline_key=item.pipeline_key,
                display_name=item.display_name,
                version="observed",
                runtime_kind=item.runtime_kind,
                metadata_json={"source": item.source, "observed": item.metadata_json},
            )
            db.add(pipeline)
        else:
            merged = dict(pipeline.metadata_json or {})
            merged["observed"] = item.metadata_json
            pipeline.metadata_json = merged
        imported.append(pipeline)

    db.commit()
    for pipeline in imported:
        db.refresh(pipeline)
    return imported


@router.get("/{pipeline_id}", response_model=PipelineSummary)
def get_pipeline(
    pipeline_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Pipeline:
    _ = current_user
    pipeline = db.get(Pipeline, pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    return pipeline


@router.patch("/{pipeline_id}", response_model=PipelineSummary)
def update_pipeline(
    pipeline_id: UUID,
    payload: PipelineUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Pipeline:
    if current_user.role not in {"admin", "service"}:
        # Same future tightening point as create_pipeline.
        pass
    pipeline = db.get(Pipeline, pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    if payload.display_name is not None:
        pipeline.display_name = payload.display_name
    if payload.version is not None:
        pipeline.version = payload.version
    if payload.runtime_kind is not None:
        pipeline.runtime_kind = payload.runtime_kind
    if payload.metadata_json is not None:
        merged_metadata = dict(pipeline.metadata_json or {})
        merged_metadata.update(payload.metadata_json)
        pipeline.metadata_json = normalize_pipeline_metadata(merged_metadata)

    db.commit()
    db.refresh(pipeline)
    return pipeline


@router.delete("/{pipeline_id}")
def delete_pipeline(
    pipeline_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    if current_user.role not in {"admin", "service"}:
        # Same future tightening point as create/update.
        pass
    pipeline = db.get(Pipeline, pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    db.delete(pipeline)
    db.commit()
    return {"status": "deleted", "pipeline_id": str(pipeline_id)}


def local_text(value) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def local_pipeline_display_name(run: dict) -> str:
    for key in ("tag", "pipeline_id", "fun"):
        value = local_text(run.get(key))
        if value:
            return value
    path = local_text(run.get("pipeline_path"))
    if path:
        parts = path.replace("\\", "/").split("/")
        return parts[-1]
    return "Unnamed observed pipeline"


def local_parse_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        lambda: datetime.fromisoformat(text.replace("Z", "+00:00")),
        lambda: datetime.strptime(text, "%d-%b-%Y %H:%M:%S"),
        lambda: datetime.strptime(text, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            return parser()
        except ValueError:
            continue
    return None


def normalize_pipeline_metadata(metadata_json: dict | None) -> dict:
    metadata = dict(metadata_json or {})

    # Keep path separators stable for Linux workers even if manifests were exported on Windows.
    for key in ("pipeline_json_path", "pipeline_bundle_uri", "export_manifest_uri", "pipeline_path"):
        value = local_text(metadata.get(key))
        if value:
            metadata[key] = value.replace("\\", "/")

    export_manifest_uri = local_text(metadata.get("export_manifest_uri"))
    pipeline_json_path = local_text(metadata.get("pipeline_json_path"))
    if export_manifest_uri and not pipeline_json_path:
        manifest_path = Path(export_manifest_uri)
        if manifest_path.is_file():
            try:
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload = {}
            bundle_rel = local_text((payload.get("pipeline") or {}).get("bundlePipelinePath"))
            if bundle_rel:
                bundle_parts = [part for part in bundle_rel.replace("\\", "/").split("/") if part and part != "."]
                candidate = manifest_path.parent.joinpath(*bundle_parts)
                if candidate.is_file():
                    metadata["pipeline_json_path"] = str(candidate)

    return metadata
