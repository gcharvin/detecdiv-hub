import json
from common import ops, OUT

demo=json.loads((OUT/'demo-job.json').read_text())
v=ops.api(f'''
import json
from datetime import datetime,timezone
from sqlalchemy import select
from api.db import SessionLocal
from api.models import User,Project,Job,WorkerInstance,ProjectLock,ExecutionTarget
from api.services.users import user_can_edit_project
with SessionLocal() as s:
 j=s.get(Job,{demo['job_id']!r});p=s.get(Project,j.project_id)
 alex=s.scalars(select(User).where(User.user_key=='alexander')).one()
 assert p.owner_user_id==alex.id and user_can_edit_project(p,alex) and j.requested_by=='alexander'
 assert j.status in ('queued','running'),j.status
 runtime=(j.result_json or {{}}).get('worker_runtime') or {{}}
 w=s.scalars(select(WorkerInstance).where(WorkerInstance.current_job_id==j.id)).one_or_none()
 locks=list(s.scalars(select(ProjectLock).where(ProjectLock.job_id==j.id)))
 worker1=s.scalars(select(WorkerInstance).join(ExecutionTarget).where(ExecutionTarget.target_key=='detecdiv-server',WorkerInstance.worker_instance=='@1')).one()
 assert worker1.code_fingerprint=='3327f21659f7' and worker1.process_id!=227710
 old=s.get(Job,'c0fc44c4-4380-4c48-8f9d-8ad2b3286e6e');assert old.status=='cancelled' and old.force_stopped_at
 oldlocks=list(s.scalars(select(ProjectLock).where(ProjectLock.job_id==old.id)));assert all(l.status=='released' for l in oldlocks)
 print(json.dumps({{'job_id':str(j.id),'status':j.status,'owner':alex.user_key,'alex_can_edit':True,
  'force_stop_supported':runtime.get('force_stop_supported') is True,'runtime':runtime,
  'started_at':j.started_at,'worker':{{'instance':w.worker_instance,'pid':w.process_id,'fingerprint':w.code_fingerprint}} if w else None,
  'project_locks':[l.status for l in locks],'old_job_cancelled':True,
  'updated_worker_1':{{'pid':worker1.process_id,'fingerprint':worker1.code_fingerprint}},
  'checked_at':datetime.now(timezone.utc).isoformat()}},default=str))
''')
ops.save('demo-verification.json',v)
print(json.dumps(v,indent=2))
fixture=ops.remote('linux','''
import json,psutil
from pathlib import Path
p=Path('.deploy-staging/alex-force-stop-demo-20261020/fixture')
ready=json.loads((p/'ready.json').read_text()) if (p/'ready.json').exists() else None
children={f.name:int(f.read_text()) for f in p.glob('python-*.pid')}
print(json.dumps({'ready':ready,'child_pids':children,'children_alive':all(psutil.pid_exists(v) for v in children.values()) if children else False}))
''')
ops.save('demo-fixture-verification.json',fixture)
print(fixture)
