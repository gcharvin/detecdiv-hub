import json
import ops
job_file=ops.OUT/'smoke-ui-job.json'
if not job_file.exists():job_file=ops.OUT/'smoke-job.json'
job=json.loads(job_file.read_text())
result=ops.api(f'''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,WorkerInstance,ProjectLock,Artifact
with SessionLocal() as s:
 j=s.get(Job,{job['job_id']!r})
 w=s.scalars(select(WorkerInstance).where(WorkerInstance.current_job_id==j.id)).first()
 locks=list(s.scalars(select(ProjectLock).where(ProjectLock.job_id==j.id)))
 print(json.dumps({{'id':str(j.id),'status':j.status,'error':j.error_text,
  'requested':j.force_stop_requested_at,'stopped':j.force_stopped_at,'runtime':(j.result_json or {{}}).get('worker_runtime'),
  'worker':{{'id':w.worker_instance,'pid':w.process_id,'fingerprint':w.code_fingerprint}} if w else None,
  'locks':[{{'id':str(l.id),'status':l.status}} for l in locks]}},default=str))
''')
ops.save('smoke-latest.json',result)
print(json.dumps(result,indent=2))
trees=ops.remote('linux','''
import json,psutil
from pathlib import Path
folder=Path('.deploy-staging/job-force-stop-20261020/smoke')
files={p.name:int(p.read_text()) for p in folder.glob('*.pid')}
print(json.dumps({'pids':files,'processes':[{'pid':pid,'alive':psutil.pid_exists(pid)} for pid in files.values()]}))
''')
ops.save('smoke-processes-latest.json',trees)
print(trees)
