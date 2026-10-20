import json
import ops

result=ops.api('''
import json,time,requests
from pathlib import Path
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,ProjectLock,WorkerInstance
state=json.loads(Path('/tmp/job-force-stop-20261020-smoke.json').read_text())
r=requests.post('http://127.0.0.1:8000/pipeline-runs/'+state['job_id']+'/force-stop',headers={'Authorization':'Bearer '+state['token']},json={},timeout=10)
assert r.status_code==200,(r.status_code,r.text[:1500])
for i in range(30):
 with SessionLocal() as s:
  j=s.get(Job,state['job_id'])
  if j.status=='cancelled':
   locks=list(s.scalars(select(ProjectLock).where(ProjectLock.job_id==j.id)))
   assert all(l.status!='active' for l in locks)
   assert j.force_stopped_at is not None
   assert j.force_stop_requested_by=='gilles'
   print(json.dumps({'id':str(j.id),'status':j.status,'requested':j.force_stop_requested_at,'stopped':j.force_stopped_at,'finished':j.finished_at,'locks_released':True},default=str))
   break
  assert j.status=='cancelling',j.status
 time.sleep(.5)
else:raise RuntimeError('Stop not confirmed within 15s')
''')
ops.save('smoke-api-stop.json',result)
print(result)
