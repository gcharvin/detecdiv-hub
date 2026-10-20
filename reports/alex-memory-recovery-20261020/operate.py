"""Audited project RAM correction and one authorized resume of Alex's run."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("release_ops", ROOT / "scripts/deploy_detecdiv_release.py")
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)
CONFIG = json.loads(ops.DEFAULT_CONFIG.read_text(encoding="utf-8"))

COMMON = """
import copy,json
from datetime import datetime,timezone
from uuid import UUID
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,Project,ProjectLock,User
PROJECT=UUID('a6d98a53-2290-4c60-a3ec-eadcff03d434')
SOURCE=UUID('4bd92189-f80c-4a28-8ad1-00d6fe863d3e')
CAMPAIGN='alex_memory_cuda_recovery_20261020'
"""

SNAPSHOT = """
with SessionLocal() as s:
 project=s.get(Project,PROJECT)
 jobs=list(s.scalars(select(Job).where(Job.project_id==PROJECT).order_by(Job.created_at.desc()).limit(6)))
 active=list(s.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))))
 watched=[s.get(Job,UUID(value)) for value in [
  'e28d8d5e-3601-4c8d-8d69-ae67fe4c8974',
  '137eec41-3bfc-4c7a-bbbf-03d5a948a329',
  'cd39f046-677f-4794-b934-e89f08cb5aaa']]
 locks=list(s.scalars(select(ProjectLock).where(ProjectLock.project_id==PROJECT,ProjectLock.status=='active')))
 def summary(j):
  return {'id':str(j.id),'status':j.status,'requested_by':j.requested_by,'started_at':j.started_at,
   'heartbeat_at':j.heartbeat_at,'finished_at':j.finished_at,'params':j.params_json,
   'result':j.result_json,'error':j.error_text}
 print(json.dumps({'at':datetime.now(timezone.utc),'project_metadata':project.metadata_json,
  'recent_jobs':[summary(j) for j in jobs],
  'watched_jobs':[summary(j) for j in watched if j is not None],
  'other_active_jobs':[summary(j) for j in active if j.project_id!=PROJECT],
  'locks':[{'id':str(l.id),'kind':l.lock_kind,'holder':l.holder_key,'host':l.holder_host,
    'expires_at':l.expires_at,'metadata':l.metadata_json} for l in locks]},default=str))
"""

RESTART = """
from sqlalchemy import event
from api.schemas import PipelineRunCreateRequest
from api.services.pipeline_runs import create_pipeline_run_job,project_pipeline_run_resources
from api.services.project_locks import release_project_lock
with SessionLocal() as s:
 project=s.get(Project,PROJECT,with_for_update=True)
 source=s.get(Job,SOURCE)
 if source.status!='failed':raise RuntimeError('Source job is no longer failed')
 actor=s.scalars(select(User).where(User.user_key=='gilles')).one()
 if actor.role not in ('admin','service') or not actor.is_active:raise RuntimeError('Active administrator required')
 existing=s.scalars(select(Job).where(Job.project_id==PROJECT,
  Job.params_json['client_context']['campaign_id'].astext==CAMPAIGN)).first()
 if existing:
  print(json.dumps({'job_id':str(existing.id),'status':existing.status,'existing':True}))
 else:
  active=s.scalars(select(Job).where(Job.project_id==PROJECT,Job.status.in_(['queued','running','cancelling']))).first()
  if active:raise RuntimeError('Project already has an active job: '+str(active.id))
  now=datetime.now(timezone.utc)
  previous=copy.deepcopy(project.metadata_json or {})
  resources=project_pipeline_run_resources({'memory_mb':49152})
  project.metadata_json={**previous,'pipeline_run_resources':resources,
   'pipeline_run_resources_audit':{'at':now.isoformat(),'actor':actor.user_key,
    'reason':'User-requested durable additional margin: 48 GiB for future submissions; current urgent recovery has a verified 30 GiB exception.',
    'source_job_id':str(SOURCE),'previous':previous.get('pipeline_run_resources')}}
  released=[]
  for lock in s.scalars(select(ProjectLock).where(ProjectLock.project_id==PROJECT,ProjectLock.status=='active').with_for_update()):
   if lock.lock_kind!='client_edit_lease' or lock.holder_key!='alexander' or lock.holder_host!='GC-DELL01-308':
    raise RuntimeError('Unexpected project lock; refusing transfer')
   lock.metadata_json={**(lock.metadata_json or {}),'administrative_release':{
    'at':now.isoformat(),'actor':actor.user_key,'campaign_id':CAMPAIGN,
    'reason':'User explicitly requested immediate restart and confirmed Alexander is away and cannot close MATLAB.'}}
   release_project_lock(s,lock=lock)
   released.append(str(lock.id))
  s.flush()
  p=copy.deepcopy(source.params_json)
  execution=p['execution']
  for key in ['cancel_token_file','result_json_path','console_log_path','progress_json_path','worker_path_mappings']:
   execution.pop(key,None)
  control=p['run_request'].get('control',{})
  for key in ['cancel_token_path','cancel_token_file','result_path']:control.pop(key,None)
  context={**p.get('client_context',{}),'campaign_id':CAMPAIGN,'retry_of':str(SOURCE),
   'initiated_by':actor.user_key,'released_client_lease_ids':released,
   'resource_exception_reason':'Immediate recovery within free capacity: 30 GiB verified to yield 20.51 GiB usable, above the 17.77 GiB minimum; future default is 48 GiB.'}
  req=PipelineRunCreateRequest(project_id=PROJECT,pipeline_id=source.pipeline_id,
   execution_target_id=source.execution_target_id,requested_mode='server',priority=source.priority,
   requested_by='alexander',requested_from_host='Codex authorized memory recovery',
   project_ref=p['project_ref'],pipeline_ref=p['pipeline_ref'],run_request=p['run_request'],
   execution=execution,client_context=context)
  def current_recovery_resources(session,*_):
   for obj in session.new:
    if isinstance(obj,Job) and obj.project_id==PROJECT:
     obj.params_json={**obj.params_json,'resources':{'memory_mb':30720},
      'client_context':{**obj.params_json['client_context'],'resource_defaults_source':'operator_verified_recovery'}}
  event.listen(s,'before_flush',current_recovery_resources)
  job=create_pipeline_run_job(s,payload=req,current_user=actor,submitted_via='hub_web')
  print(json.dumps({'job_id':str(job.id),'status':job.status,'resources':job.params_json.get('resources'),
   'released_leases':released,'run_id':job.params_json['run_request']['run_id']},default=str))
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["snapshot", "restart"])
    parser.add_argument("--name", default=None)
    args = parser.parse_args()
    result = ops.api_code(CONFIG, COMMON + (SNAPSHOT if args.action == "snapshot" else RESTART))
    name = args.name or args.action
    (OUT / f"{name}.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if args.action == "restart":
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps({"saved":str(OUT / f"{name}.json"),
            "recent":[{"id":j["id"],"status":j["status"]} for j in result["recent_jobs"]],
            "other_active":len(result["other_active_jobs"])}))


if __name__ == "__main__":
    main()
