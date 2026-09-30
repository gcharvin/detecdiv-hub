"""Worker-side eligibility for pipeline runs whose target is still automatic."""

from __future__ import annotations

import json
import os
from pathlib import Path

from sqlalchemy.orm import Session

from api.models import ExecutionTarget, Job
from api.services.pipeline_raw_ingest import existing_linked_raw_dataset, pipeline_run_requests_raw_ingest
from worker.path_mappings import map_worker_path, parse_worker_path_mappings
from worker.pipeline_run_executor import resolve_project_mat_path


def can_claim_automatic_pipeline(
    session: Session, *, job: Job, target: ExecutionTarget | None, path_mappings: str
) -> tuple[bool, str]:
    """Keep the queued job movable until a capable worker actually claims it."""
    if target is None or not target.supports_matlab:
        return False, "target lacks MATLAB"
    if (target.metadata_json or {}).get("matlab_code_isolation_ready") is not True:
        return False, "target has no isolated DetecDiv releases"
    if os.name != "nt":
        return True, ""

    mappings = parse_worker_path_mappings(path_mappings)
    if job.project_id is not None:
        project_ref = dict((job.params_json or {}).get("project_ref") or {})
        run_paths = dict(((job.params_json or {}).get("run_request") or {}).get("paths") or {})
        project_paths = [
            str(run_paths.get("server_project_path") or run_paths.get("project_path") or "").strip(),
            str(project_ref.get("project_mat_path") or "").strip(),
        ]
        if not any(project_paths):
            try:
                project_paths.append(resolve_project_mat_path(session, project_id=job.project_id))
            except ValueError as exc:
                return False, str(exc)
        mapped_projects = [Path(map_worker_path(path, mappings)) for path in project_paths if path]
        # DetecDiv accepts the JSON sibling of a legacy catalog MAT path.
        project_variants: list[Path] = []
        for path in mapped_projects:
            if path.suffix.lower() == ".mat":
                project_variants.append(path.with_suffix(".json"))
            project_variants.append(path)
        if not any(path.is_file() for path in project_variants):
            return False, f"project path unavailable: {', '.join(map(str, project_variants))}"

    if pipeline_run_requests_raw_ingest(job.params_json):
        if existing_linked_raw_dataset(session, job=job) is None:
            return False, "raw input is not indexed and linked to this project"
        raw_paths = dict(((job.params_json or {}).get("run_request") or {}).get("paths") or {})
        raw_path = str(raw_paths.get("server_raw_data_path") or raw_paths.get("raw_data_path") or "")
        mapped_raw = map_worker_path(raw_path, mappings)
        if not Path(mapped_raw).is_dir():
            return False, f"raw path unavailable: {mapped_raw}"

    pipeline_ref = dict((job.params_json or {}).get("pipeline_ref") or {})
    pipeline_path = str(pipeline_ref.get("pipeline_json_path") or "").strip()
    if pipeline_path:
        worker_pipeline = Path(map_worker_path(pipeline_path, mappings))
        if not worker_pipeline.is_file():
            return False, f"pipeline definition unavailable: {worker_pipeline}"
        try:
            pipeline = json.loads(worker_pipeline.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            return False, f"pipeline definition unreadable: {exc}"
        unsupported_packages = {
            str(package).strip().lower()
            for package in (target.metadata_json or {}).get("unsupported_classifier_packages", [])
        }
        for node in pipeline.get("nodes") or []:
            if not isinstance(node, dict) or str(node.get("type") or "").lower() != "classifier":
                continue
            if node.get("enabled") is False:
                continue
            params = node.get("params") or {}
            if not isinstance(params, dict):
                continue
            package = str(params.get("pkg") or node.get("pkg") or "").strip().lower()
            if package in unsupported_packages:
                return False, f"classifier package unsupported on this target: {package}"
            module_path = str(params.get("modulePath") or "").strip()
            if module_path:
                worker_module = Path(map_worker_path(module_path, mappings))
                if not worker_module.is_dir():
                    return False, f"classifier module unavailable for {node.get('id')}: {worker_module}"
    return True, ""
