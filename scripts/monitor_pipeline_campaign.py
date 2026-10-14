"""Inspect and retry a user-authorized pipeline campaign through Hub services."""
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import re
from pathlib import Path


def retry_payload(source: dict, campaign: dict, *, classification_only: bool, cut: int) -> dict:
    params = copy.deepcopy(source)
    run = params["run_request"]
    run_id = f"{campaign['run_prefix']}_attempt_{campaign['retry_count'] + 1}"
    run.update(run_id=run_id, run_policy="restart", existing_data_policy="replace")
    run["gpu"] = {"mode": "force_gpu"}
    run["selection"] = copy.deepcopy(campaign["selection"])
    run["paths"]["run_path"] = campaign["project_folder"] + "/pipeline/" + run_id
    run["paths"]["server_run_path"] = run["paths"]["run_path"]
    # A new job must never inherit the previous attempt's stop/result paths.
    run["control"] = {k: v for k, v in run.get("control", {}).items()
                      if k not in {"cancel_token_path", "cancel_token_file", "result_path"}}
    for node in run.get("node_params", []):
        if node["id"] == "extract_cropped_images_legacy":
            node["params"]["cut"] = cut
        if node["id"] == "xtra_full_channels_legacy":
            node["params"]["executionEnvironment"] = "gpu"
    if classification_only:
        run["selected_nodes"] = ["xtra_full_channels_legacy"]
    else:
        run["selected_nodes"] = list(campaign["full_nodes"])
    execution = {k: v for k, v in params.get("execution", {}).items()
                 if k not in {"cancel_token_file", "result_json_path", "console_log_path",
                              "progress_json_path", "worker_path_mappings"}}
    execution.update(allow_gui=False, interactive=False, save_project=True, write_scope="project_update")
    if campaign.get("retry_code_commit"):
        commit = campaign["retry_code_commit"]
        if not re.fullmatch(r"[0-9a-f]{40}", commit):
            raise ValueError("Retry code must be a prepared full commit SHA")
        execution["code_commit"] = commit
    params["execution"] = execution
    return params


def ensure_retry_allowed(status: str, result_status: str, campaign: dict) -> None:
    if status in {"queued", "running", "cancelling"}:
        raise ValueError("An attempt is still active; do not create a duplicate")
    if status == "cancelled" or result_status in {"cancelled", "canceled"}:
        raise ValueError("Human cancellation requires a new instruction, not an automatic retry")
    if campaign["retry_count"] >= campaign["max_retries"]:
        raise ValueError("Retry limit reached; report the diagnosis")
    if status != "failed" and campaign.get("verification_status") != "failed":
        raise ValueError("No diagnosed technical failure to retry")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=Path)
    parser.add_argument("--retry", action="store_true")
    parser.add_argument("--classification-only", action="store_true")
    parser.add_argument("--cut", type=int, choices=[88, 44, 22], default=88)
    args = parser.parse_args()
    state = json.loads(args.campaign.read_text(encoding="utf-8"))
    helper_path = Path(__file__).with_name("deploy_detecdiv_release.py")
    spec = importlib.util.spec_from_file_location("release_ops", helper_path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    config = json.loads(helper.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    code = f"""
import json
from uuid import UUID
from api.db import SessionLocal
from api.models import Job
with SessionLocal() as s:
 j=s.get(Job,UUID({state['current_job_id']!r}));r=j.result_json or {{}}
 print(json.dumps({{'id':str(j.id),'status':j.status,'result_status':r.get('status'),
  'error':j.error_text,'heartbeat':str(j.heartbeat_at),'finished_at':str(j.finished_at),
  'runtime':r.get('worker_runtime'),'progress':r.get('progress'),
  'cancel_requested_by':r.get('cancel_requested_by'),'params':j.params_json}}))
"""
    current = helper.api_code(config, code)
    if not args.retry:
        current.pop("params", None)
        print(json.dumps(current, ensure_ascii=True, indent=2))
        return
    ensure_retry_allowed(current["status"], current.get("result_status") or "", state)
    if args.classification_only and not state.get("extraction_verified"):
        raise ValueError("Validate the extracted frames before skipping extraction")
    params = retry_payload(current["params"], state,
                           classification_only=args.classification_only, cut=args.cut)
    run_id = params["run_request"]["run_id"]
    code = f"""
import json
from uuid import UUID
from sqlalchemy import select,event
from api.db import SessionLocal
from api.models import Job,User
from api.schemas import PipelineRunCreateRequest
from api.services.pipeline_runs import create_pipeline_run_job
p=json.loads({json.dumps(params)!r})
with SessionLocal() as s:
 source=s.get(Job,UUID({current['id']!r}))
 existing=s.scalars(select(Job).where(Job.project_id==source.project_id,
   Job.params_json['run_request']['run_id'].astext=={run_id!r})).first()
 if existing:
  job=existing
 else:
  active=s.scalars(select(Job).where(Job.project_id==source.project_id,
     Job.status.in_(['queued','running','cancelling']))).first()
  if active:raise RuntimeError('Project already has an active job: '+str(active.id))
  actor=s.scalars(select(User).where(User.user_key=={state['operator']!r})).one()
  req=PipelineRunCreateRequest(project_id=source.project_id,pipeline_id=source.pipeline_id,
   execution_target_id=source.execution_target_id,requested_mode='server',
   requested_by={state['requested_by']!r},requested_from_host='Codex monitored recovery',
   project_ref=p['project_ref'],pipeline_ref=p['pipeline_ref'],run_request=p['run_request'],
   execution=p['execution'],client_context={{'schema':'pipeline_run_contract_v1',
     'campaign_id':{state['campaign_id']!r},'retry_of':str(source.id)}})
  def resources(session,*_):
   for obj in session.new:
    if isinstance(obj,Job) and (obj.params_json or {{}}).get('run_request',{{}}).get('run_id')=={run_id!r}:
     obj.params_json={{**obj.params_json,'resources':{{'memory_mb':24576}}}}
  event.listen(s,'before_flush',resources)
  job=create_pipeline_run_job(s,payload=req,current_user=actor,submitted_via='hub_web')
 print(json.dumps({{'job_id':str(job.id),'status':job.status,'run_id':{run_id!r}}}))
"""
    submitted = helper.api_code(config, code)
    state.setdefault("attempts", []).append({"previous_job_id":state["current_job_id"],
                                            **submitted, "cut":args.cut,
                                            "classification_only":args.classification_only})
    state["current_job_id"] = submitted["job_id"]
    state["retry_count"] += 1
    state["verification_status"] = "pending"
    temporary = args.campaign.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=True, indent=2)+"\n", encoding="utf-8")
    temporary.replace(args.campaign)
    print(json.dumps(submitted, indent=2))


if __name__ == "__main__":
    main()
