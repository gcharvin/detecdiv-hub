import json
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import ExecutionTarget,Job,WorkerInstance,User
from api.routes_pipeline_runs import update_pipeline_run
from api.schemas import PipelineRunUpdateRequest

with SessionLocal() as s:
    s.execute(text("SET LOCAL lock_timeout='5s'"))
    t=s.scalar(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56').with_for_update())
    j=s.scalar(select(Job).where(Job.id=='43637ab4-4044-443d-b8f9-3372935f8356').with_for_update())
    assert t.metadata_json.get('drain_new_jobs') and t.supports_gpu
    assert j.execution_target_id==t.id and j.status=='queued' and j.started_at is None
    workers=list(s.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id==t.id)))
    assert len(workers)==1 and workers[0].current_job_id is None and workers[0].code_fingerprint=='957aa209126d'
    now=datetime.now(timezone.utc)
    assert (now-workers[0].last_seen_at).total_seconds()<30
    run_id=j.params_json['run_request']['run_id']
    others=list(s.scalars(select(Job).where(Job.id!=j.id,Job.status.in_(['queued','running','cancelling']),Job.params_json['run_request']['run_id'].astext==run_id)))
    assert not others,'Duplicate job found; do not dispatch'
    m=dict(t.metadata_json)
    assert m['windows_gpu_activation']['id']=='windows-gpu-20261016'
    m.pop('drain_new_jobs')
    m['windows_gpu_activation']={**m['windows_gpu_activation'],'state':'active','validated_at':now.isoformat(),'worker_fingerprint':'957aa209126d','job_id':str(j.id)}
    t.metadata_json=m
    u=s.scalar(select(User).where(User.user_key=='gilles'))
    audit={**j.params_json['client_context']['windows_gpu_activation'],'state':'dispatched','dispatched_at':now.isoformat(),'same_job_id_preserved':True,'validated_account':'GMGM\\Charvin-Admin','validated_session_id':1,'worker_fingerprint':'957aa209126d'}
    update_pipeline_run(j.id,PipelineRunUpdateRequest(client_context={'windows_gpu_activation':audit}),db=s,current_user=u)
    print(json.dumps({'released':True,'job_id':str(j.id),'run_id':run_id,'target':t.target_key,'dispatched_at':now.isoformat(),'duplicate_jobs':len(others)}))
