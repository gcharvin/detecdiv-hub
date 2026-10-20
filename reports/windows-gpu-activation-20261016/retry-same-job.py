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
    assert t.metadata_json.get('drain_new_jobs') and j.status=='failed' and j.execution_target_id==t.id
    assert not list(s.scalars(select(WorkerInstance).where(WorkerInstance.current_job_id==j.id)))
    duplicates=list(s.scalars(select(Job).where(Job.id!=j.id,Job.params_json['run_request']['run_id'].astext==j.params_json['run_request']['run_id'])))
    assert not duplicates
    previous={'status':j.status,'started_at':str(j.started_at),'finished_at':str(j.finished_at),'error':j.error_text,'result':j.result_json,'training_started':False}
    u=s.scalar(select(User).where(User.user_key=='gilles'))
    update_pipeline_run(j.id,PipelineRunUpdateRequest(execution={'code_commit':'594217dd37bfa9b2b529f7f2204253d9901340bf'}),db=s,current_user=u)
    # Reacquire admission and the job after the route's commit. The target is
    # still drained and the job is still terminal, so it cannot run in between.
    t=s.scalar(select(ExecutionTarget).where(ExecutionTarget.id==t.id).with_for_update())
    j=s.scalar(select(Job).where(Job.id==j.id).with_for_update())
    assert t.metadata_json.get('drain_new_jobs') and j.status=='failed'
    now=datetime.now(timezone.utc)
    p=dict(j.params_json); p.pop('_hub_resource_allocation',None)
    ctx=dict(p.get('client_context') or {})
    ctx['windows_gpu_activation']={**ctx['windows_gpu_activation'],'state':'dispatched_after_path_fix','retry_dispatched_at':now.isoformat(),'full_pipeline_dryrun':'passed','code_commit':'594217dd37bfa9b2b529f7f2204253d9901340bf'}
    p['client_context']=ctx; j.params_json=p
    j.result_json={'attempt_history':[previous],'retry_reason':'Fix authoritative worker mapping priority; first attempt failed before training','retry_authorized_by':'gilles'}
    j.status='queued';j.started_at=None;j.finished_at=None;j.heartbeat_at=None;j.error_text=None;j.updated_at=now
    m=dict(t.metadata_json);m.pop('drain_new_jobs')
    m['windows_gpu_activation']={**m['windows_gpu_activation'],'state':'active','code_commit':'594217dd37bfa9b2b529f7f2204253d9901340bf','retry_at':now.isoformat()}
    t.metadata_json=m
    s.commit()
    print(json.dumps({'requeued_same_job':str(j.id),'duplicate_jobs':len(duplicates),'previous_attempt_retained':True,'at':now.isoformat()}))
