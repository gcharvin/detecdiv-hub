import json
import ops

first=json.loads((ops.OUT/'smoke-job.json').read_text())
second=json.loads((ops.OUT/'smoke-ui-job.json').read_text())
result=ops.api(f'''
import json
from datetime import datetime,timezone
from pathlib import Path
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,Project,ProjectLock,Artifact,WorkerInstance,UserSession
ids={[first['job_id'],second['job_id']]!r}
results=[]
with SessionLocal.begin() as s:
 for identity in ids:
  j=s.get(Job,identity)
  assert j.status=='cancelled' and j.force_stopped_at is not None
  locks=list(s.scalars(select(ProjectLock).where(ProjectLock.job_id==j.id)))
  assert all(l.status=='released' for l in locks)
  artifacts=list(s.scalars(select(Artifact).where(Artifact.job_id==j.id)))
  assert {{a.artifact_kind for a in artifacts}}>={{'stdout_log','stderr_log'}}
  results.append({{'id':str(j.id),'status':j.status,'requested':j.force_stop_requested_at,'stopped':j.force_stopped_at,
   'latency_seconds':(j.force_stopped_at-j.force_stop_requested_at).total_seconds(),
   'requested_by':j.force_stop_requested_by,'locks_released':True,'log_artifacts':[a.uri for a in artifacts]}})
 for identity in {[first['auth_session_id'],second['auth_session_id']]!r}:
  record=s.get(UserSession,identity);assert record.client_label.startswith('job-force-stop-20261020');record.status='revoked'
 project=s.get(Project,{first['project_id']!r});project.project_name='OPS - Force-stop test 2026-10-20'
 print(json.dumps({{'jobs':results,'ops_sessions_revoked':True,'ui_confirmation_accepted_by':'user'}},default=str))
p=Path('/tmp/{ops.ROLLOUT}-smoke.json')
if p.exists():p.unlink()
''')
ops.save('smoke-final.json',result)
print(json.dumps(result,indent=2))
v=ops.remote('linux','''
import json,psutil
from pathlib import Path
from datetime import datetime,timezone
folder=Path('.deploy-staging/job-force-stop-20261020/smoke')
pids=[int(p.read_text()) for p in folder.glob('*.pid')]
assert len(pids)==3
assert all(not psutil.pid_exists(pid) for pid in pids)
workers={}
for pid in (382618,382619):
 p=psutil.Process(pid)
 assert not p.children(recursive=True)
 workers[str(pid)]={'alive':p.is_running(),'created':p.create_time(),'children':0}
print(json.dumps({'synthetic_processes_exited':pids,'workers_survived':workers,'checked_at':datetime.now(timezone.utc).isoformat()}))
''')
ops.save('smoke-final-processes.json',v)
print(v)
