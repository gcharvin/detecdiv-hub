"""Read-only inspection of the pre-upgrade cancellation; sends no signals."""
import json
import ops

job_id = 'c0fc44c4-4380-4c48-8f9d-8ad2b3286e6e'
result = ops.api(f'''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,Project,WorkerInstance,ProjectLock
with SessionLocal() as s:
 j=s.get(Job,{job_id!r});assert j is not None
 p=s.get(Project,j.project_id)
 w=s.scalars(select(WorkerInstance).where(WorkerInstance.current_job_id==j.id)).one_or_none()
 locks=list(s.scalars(select(ProjectLock).where(ProjectLock.job_id==j.id)))
 print(json.dumps({{'id':str(j.id),'project_id':str(j.project_id),'project_name':p.project_name if p else None,
  'requested_by':j.requested_by,'status':j.status,'started_at':j.started_at,'heartbeat_at':j.heartbeat_at,
  'runtime':(j.result_json or {{}}).get('worker_runtime'),
  'progress':(j.result_json or {{}}).get('progress'),
  'allocation':(j.params_json or {{}}).get('_hub_resource_allocation'),
  'worker':{{'instance':w.worker_instance,'pid':w.process_id,'fingerprint':w.code_fingerprint}} if w else None,
  'locks':[{{'status':l.status,'scope':l.lock_scope,'heartbeat':l.heartbeat_at}} for l in locks]}},default=str))
''')
ops.save('alex-current-job.json',result)
print(json.dumps({k:v for k,v in result.items() if k!='progress'},indent=2))
if result['worker']:
    tree = ops.remote('linux',f'''
import json,psutil,subprocess
from pathlib import Path
p=psutil.Process({result['worker']['pid']!r})
children=p.children(recursive=True)
unit='detecdiv-worker'+{result['worker']['instance']!r}+'.service'
cg=subprocess.check_output(['systemctl','show',unit,'-p','ControlGroup','--value'],text=True).strip()
rows=[]
for proc in [p,*children]:
 try:
  rows.append({{'pid':proc.pid,'parent':proc.ppid(),'name':proc.name(),'created':proc.create_time(),
               'status':proc.status(),'command':proc.cmdline(),
               'cgroup':Path('/proc',str(proc.pid),'cgroup').read_text()}})
 except psutil.NoSuchProcess:pass
print(json.dumps({{'unit':unit,'cgroup':cg,'processes':rows}}))
''')
    ops.save('alex-current-process-tree.json',tree)
    print(json.dumps(tree,indent=2))
