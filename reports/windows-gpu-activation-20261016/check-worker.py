import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget,Job,WorkerInstance
with SessionLocal() as s:
 t=s.scalar(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56'))
 j=s.get(Job,'43637ab4-4044-443d-b8f9-3372935f8356')
 print(json.dumps({'job':j.status,'target':str(j.execution_target_id),'drain':t.metadata_json.get('drain_new_jobs'),'workers':[{'pid':w.process_id,'fingerprint':w.code_fingerprint,'seen':str(w.last_seen_at),'job':str(w.current_job_id)} for w in s.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id==t.id))]}))
