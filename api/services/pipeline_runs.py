from __future__ import annotations

from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.models import ExecutionTarget, Job, Pipeline, Project, User
from api.schemas import (
    PipelineRunCreateRequest,
    PipelineRunPreflightIssue,
    PipelineRunPreflightResult,
)
from api.services.project_locks import (
    ProjectLockConflict,
    active_project_locks,
    create_server_job_lock,
)
from api.services.users import ensure_project_readable, user_can_edit_project


REQUESTED_MODES = {"auto", "server", "local"}
PIPELINE_REF_PATH_KEYS = ("pipeline_bundle_uri", "pipeline_json_path", "export_manifest_uri")


def normalized_pipeline_run_params(
    payload: PipelineRunCreateRequest,
    *,
    current_user: User,
    submitted_via: str,
) -> dict[str, Any]:
    project_ref = dict(payload.project_ref or {})
    pipeline_ref = dict(payload.pipeline_ref or {})
    run_request = dict(payload.run_request or {})
    execution = dict(payload.execution or {})
    client_context = dict(payload.client_context or {})

    if payload.project_id is not None:
        project_ref.setdefault("project_id", str(payload.project_id))
    if payload.pipeline_id is not None:
        pipeline_ref.setdefault("pipeline_id", str(payload.pipeline_id))
    if payload.execution_target_id is not None:
        execution.setdefault("execution_target_id", str(payload.execution_target_id))

    execution.setdefault("requested_mode", payload.requested_mode)
    execution.setdefault("allow_gui", False)
    execution.setdefault("interactive", False)
    execution.setdefault("save_project", payload.project_id is not None)

    client_context.setdefault("submitted_via", submitted_via)
    client_context.setdefault("submitted_by_user_key", current_user.user_key)
    client_context.setdefault("schema", "pipeline_run_contract_v1")

    return {
        "job_kind": "pipeline_run",
        "project_ref": project_ref,
        "pipeline_ref": pipeline_ref,
        "run_request": run_request,
        "execution": execution,
        "client_context": client_context,
    }


def preflight_pipeline_run_request(
    session: Session,
    *,
    payload: PipelineRunCreateRequest,
    current_user: User,
    submitted_via: str = "hub_web",
) -> PipelineRunPreflightResult:
    issues: list[PipelineRunPreflightIssue] = []
    pipeline = resolve_pipeline_for_request(session, payload=payload)
    target = (
        session.get(ExecutionTarget, payload.execution_target_id)
        if payload.execution_target_id is not None
        else None
    )
    normalized = normalized_pipeline_run_params(
        payload,
        current_user=current_user,
        submitted_via=submitted_via,
    )
    classifier_scoped = payload.project_id is None and is_classifier_scoped_pipeline_run(normalized)
    project = (
        ensure_project_readable(session.get(Project, payload.project_id), current_user)
        if payload.project_id is not None
        else None
    )

    if payload.project_id is None and not classifier_scoped:
        issues.append(
            issue(
                "error",
                "project_required",
                "Projectless pipeline runs are only allowed for classifier-scoped runs.",
            )
        )

    if project is not None and project.status == "deleted":
        issues.append(
            issue("error", "project_deleted", "Deleted projects cannot receive pipeline runs.")
        )
    if project is not None and not user_can_edit_project(project, current_user):
        issues.append(
            issue(
                "error",
                "project_edit_required",
                "Project edit access is required to submit a run.",
            )
        )

    project_ref = dict(normalized.get("project_ref") or {})
    if project is not None and str(project_ref.get("project_id") or "") != str(project.id):
        issues.append(
            issue(
                "error",
                "project_ref_mismatch",
                "project_ref.project_id must match the submitted project_id.",
                {
                    "project_id": str(project.id),
                    "project_ref_project_id": project_ref.get("project_id"),
                },
            )
        )

    pipeline_ref = dict(normalized.get("pipeline_ref") or {})
    if pipeline is not None:
        pipeline_ref.setdefault("pipeline_id", str(pipeline.id))
        if pipeline.pipeline_key:
            pipeline_ref.setdefault("pipeline_key", pipeline.pipeline_key)
        normalized["pipeline_ref"] = pipeline_ref

    if payload.pipeline_id is not None and pipeline is None:
        issues.append(
            issue("error", "pipeline_not_found", "Requested registry pipeline does not exist.")
        )
    if payload.pipeline_id is not None:
        pipeline_ref_id = str(pipeline_ref.get("pipeline_id") or "").strip()
        if pipeline_ref_id and pipeline_ref_id != str(payload.pipeline_id):
            issues.append(
                issue(
                    "error",
                    "pipeline_ref_mismatch",
                    "pipeline_ref.pipeline_id must match the submitted pipeline_id.",
                    {
                        "pipeline_id": str(payload.pipeline_id),
                        "pipeline_ref_pipeline_id": pipeline_ref_id,
                    },
                )
            )
    has_portable_pipeline_source = any(
        str(pipeline_ref.get(key) or "").strip() for key in PIPELINE_REF_PATH_KEYS
    )
    if (
        payload.pipeline_id is None
        and pipeline_ref.get("pipeline_key")
        and pipeline is None
        and not has_portable_pipeline_source
    ):
        issues.append(
            issue(
                "error",
                "pipeline_key_not_found",
                "pipeline_ref.pipeline_key does not match a registry pipeline.",
                {"pipeline_key": pipeline_ref.get("pipeline_key")},
            )
        )
    if payload.pipeline_id is None and not pipeline_ref_has_source(normalized):
        issues.append(
            issue(
                "error",
                "pipeline_source_required",
                "A prepared run must reference a registry pipeline or a portable pipeline source.",
            )
        )

    requested_mode = str(payload.requested_mode or "").strip()
    if requested_mode not in REQUESTED_MODES:
        issues.append(
            issue(
                "error",
                "invalid_requested_mode",
                "requested_mode must be one of auto, server, or local.",
                {"requested_mode": requested_mode},
            )
        )

    execution = dict(normalized.get("execution") or {})
    if bool(execution.get("allow_gui")):
        issues.append(
            issue(
                "error",
                "gui_not_allowed",
                "Hub-submitted runs must not allow GUI execution.",
            )
        )
    if bool(execution.get("interactive")):
        issues.append(
            issue(
                "error",
                "interactive_not_allowed",
                "Hub-submitted runs must be non-interactive.",
            )
        )
    if requested_mode == "local" and payload.execution_target_id is None:
        issues.append(
            issue(
                "warning",
                "local_mode_without_target",
                "Local mode is intended for client-side execution; "
                "hub workers normally use auto or server.",
            )
        )

    if payload.execution_target_id is not None and target is None:
        issues.append(
            issue(
                "error",
                "execution_target_not_found",
                "Requested execution target does not exist.",
            )
        )
    elif target is not None:
        if target.status != "online":
            issues.append(
                issue(
                    "warning",
                    "execution_target_not_online",
                    "Requested execution target is not currently online.",
                    {"target_status": target.status},
                )
            )
        runtime_kind = str(pipeline.runtime_kind if pipeline is not None else "").lower()
        needs_matlab = runtime_kind in {"matlab", "hybrid"} or pipeline is None
        if needs_matlab and not target.supports_matlab:
            issues.append(
                issue(
                    "error",
                    "execution_target_lacks_matlab",
                    "Requested execution target does not support MATLAB runs.",
                    {"target_id": str(target.id)},
                )
            )

    if project is not None and not project.locations and not str(project_ref.get("project_mat_path") or "").strip():
        issues.append(
            issue(
                "error",
                "project_location_required",
                "Project has no registered server-visible location and no direct "
                "project_mat_path hint.",
            )
        )

    run_request = dict(normalized.get("run_request") or {})
    selected_nodes = run_request.get("selected_nodes")
    if selected_nodes is not None and not isinstance(selected_nodes, list):
        issues.append(
            issue(
                "error",
                "selected_nodes_must_be_list",
                "run_request.selected_nodes must be a list.",
            )
        )
    node_params = run_request.get("node_params")
    if node_params is not None and not isinstance(node_params, list):
        issues.append(
            issue(
                "error",
                "node_params_must_be_list",
                "run_request.node_params must be a list.",
            )
        )

    locks = active_project_locks(session, project_id=project.id) if project is not None else []
    if locks:
        issues.append(
            issue(
                "error",
                "project_locked",
                "Project already has an active write lock.",
                {"locks": [lock_summary(lock) for lock in locks]},
            )
        )

    can_submit = not any(item.severity == "error" for item in issues)
    return PipelineRunPreflightResult(
        can_submit=can_submit,
        project_id=project.id if project is not None else None,
        pipeline_id=pipeline.id if pipeline is not None else payload.pipeline_id,
        execution_target_id=payload.execution_target_id,
        normalized_payload=normalized,
        issues=issues,
    )


def create_pipeline_run_job(
    session: Session,
    *,
    payload: PipelineRunCreateRequest,
    current_user: User,
    submitted_via: str = "hub_web",
) -> Job:
    preflight = preflight_pipeline_run_request(
        session,
        payload=payload,
        current_user=current_user,
        submitted_via=submitted_via,
    )
    if not preflight.can_submit:
        issue_codes = {item.code for item in preflight.issues if item.severity == "error"}
        if "project_edit_required" in issue_codes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Project edit access required",
            )
        if "project_locked" in issue_codes:
            lock_issue = next(
                (item for item in preflight.issues if item.code == "project_locked"),
                None,
            )
            locks = list((lock_issue.details or {}).get("locks") or []) if lock_issue else []
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Project is locked",
                    "locks": locks,
                },
            )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "Prepared pipeline run failed preflight.",
                "issues": [item.model_dump(mode="json") for item in preflight.issues],
            },
        )

    project = session.get(Project, payload.project_id) if payload.project_id is not None else None
    if payload.project_id is not None and project is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    job = Job(
        project_id=payload.project_id,
        pipeline_id=preflight.pipeline_id,
        execution_target_id=payload.execution_target_id,
        requested_mode=payload.requested_mode,
        priority=payload.priority,
        requested_by=payload.requested_by or current_user.user_key,
        requested_from_host=payload.requested_from_host,
        params_json=preflight.normalized_payload,
        status="queued",
    )
    session.add(job)
    session.flush()
    if project is not None:
        try:
            execution = dict(preflight.normalized_payload.get("execution") or {})
            run_request = dict(preflight.normalized_payload.get("run_request") or {})
            create_server_job_lock(
                session,
                project_id=project.id,
                job=job,
                owner=current_user,
                holder_host=payload.requested_from_host,
                write_scope=str(execution.get("write_scope") or "project_update"),
                reason="pipeline_run",
                metadata_json={
                    "run_id": run_request.get("run_id"),
                    "requested_mode": payload.requested_mode,
                    "submitted_via": submitted_via,
                },
            )
        except ProjectLockConflict as exc:
            session.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Project is locked",
                    "locks": [lock_summary(lock) for lock in exc.locks],
                },
            ) from exc

    session.commit()
    session.refresh(job)
    return job


def pipeline_ref_has_source(preflight: dict[str, Any]) -> bool:
    pipeline_ref = dict(preflight.get("pipeline_ref") or {})
    if str(pipeline_ref.get("pipeline_id") or "").strip():
        return True
    if str(pipeline_ref.get("pipeline_key") or "").strip():
        return True
    return any(str(pipeline_ref.get(key) or "").strip() for key in PIPELINE_REF_PATH_KEYS)


def is_classifier_scoped_pipeline_run(preflight: dict[str, Any]) -> bool:
    project_ref = dict(preflight.get("project_ref") or {})
    run_request = dict(preflight.get("run_request") or {})
    paths = dict(run_request.get("paths") or {})
    scope = str(project_ref.get("scope") or project_ref.get("type") or "").strip().lower()
    input_source = str(run_request.get("input_source") or "").strip().lower()
    has_classifier_ref = bool(
        str(project_ref.get("classifier_path") or "").strip()
        or str(project_ref.get("local_classifier_path") or "").strip()
        or str(paths.get("server_classifier_path") or "").strip()
        or str(paths.get("classifier_path") or "").strip()
    )
    return (
        pipeline_ref_has_source(preflight)
        and has_classifier_ref
        and (scope in {"classifier", "classi"} or "classifier" in input_source)
    )


def resolve_pipeline_for_request(
    session: Session,
    *,
    payload: PipelineRunCreateRequest,
) -> Pipeline | None:
    if payload.pipeline_id is not None:
        return session.get(Pipeline, payload.pipeline_id)
    pipeline_key = str((payload.pipeline_ref or {}).get("pipeline_key") or "").strip()
    if not pipeline_key:
        return None
    return session.scalars(select(Pipeline).where(Pipeline.pipeline_key == pipeline_key)).first()


def issue(
    severity: str,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> PipelineRunPreflightIssue:
    return PipelineRunPreflightIssue(
        severity=severity,
        code=code,
        message=message,
        details=details or {},
    )


def lock_summary(lock) -> dict[str, Any]:
    return {
        "id": str(lock.id),
        "lock_kind": lock.lock_kind,
        "job_id": str(lock.job_id) if lock.job_id else None,
        "holder_key": lock.holder_key,
        "holder_host": lock.holder_host,
        "reason": lock.reason,
        "expires_at": lock.expires_at.isoformat() if lock.expires_at else None,
    }
