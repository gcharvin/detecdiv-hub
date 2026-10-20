import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,WorkerInstance,ExecutionTarget
from datetime import datetime,timezone
with SessionLocal() as s:
    j=s.get(Job,'43637ab4-4044-443d-b8f9-3372935f8356')
    t=s.get(ExecutionTarget,j.execution_target_id)
    ws=list(s.scalars(select(WorkerInstance).where(WorkerInstance.current_job_id==j.id)))
    duplicates=list(s.scalars(select(Job).where(Job.id!=j.id,Job.params_json['run_request']['run_id'].astext==j.params_json['run_request']['run_id'])))
    print(json.dumps({'at':datetime.now(timezone.utc).isoformat(),'id':str(j.id),'status':j.status,'target':t.target_key,'started_at':str(j.started_at),'heartbeat_at':str(j.heartbeat_at),'error':j.error_text,'allocation':j.params_json.get('_hub_resource_allocation'),'result':j.result_json,'workers':[{'host':w.worker_host,'instance':w.worker_instance,'pid':w.process_id,'seen':str(w.last_seen_at),'fingerprint':w.code_fingerprint} for w in ws],'other_jobs_same_run':[{'id':str(x.id),'status':x.status} for x in duplicates]},default=str))
