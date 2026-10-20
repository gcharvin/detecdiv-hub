"""Authorized targeted stop of Alex's legacy execution, then idle-only reload."""
from datetime import datetime, timezone
import json
import time
from common import ops, OLD

JOB = "c0fc44c4-4380-4c48-8f9d-8ad2b3286e6e"
KEY = "alex_legacy_stop_20261020"
before = ops.save("before.json", ops.snapshot())
inspection = json.loads((OLD / "alex-current-job.json").read_text())
tree = json.loads((OLD / "alex-current-process-tree.json").read_text())
worker_pid = inspection["worker"]["pid"]
worker_created = next(p["created"] for p in tree["processes"] if p["pid"] == worker_pid)
reservation = ops.api(f'''
import json
from datetime import datetime,timezone
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,ExecutionTarget,WorkerInstance,User
with SessionLocal.begin() as s:
 t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='detecdiv-server').with_for_update()).one()
 j=s.get(Job,{JOB!r},with_for_update=True)
 assert j.requested_by=='alexander' and j.status=='cancelling'
 w=s.scalars(select(WorkerInstance).where(WorkerInstance.current_job_id==j.id)).one()
 assert w.worker_instance=='@1' and w.process_id=={worker_pid}
 assert (datetime.now(timezone.utc)-w.last_seen_at).total_seconds()<30
 m=dict(t.metadata_json or {{}});assert {KEY!r} not in m
 owner={{'previous_present':'drain_new_jobs' in m,'previous':m.get('drain_new_jobs')}}
 assert not m.get('drain_new_jobs'),'Existing admission drain; inspect before continuing'
 m[{KEY!r}]=owner;m['drain_new_jobs']=True;t.metadata_json=m
 actor=s.scalars(select(User).where(User.user_key=='gilles',User.is_active.is_(True))).one()
 assert actor.role=='admin'
 assert j.force_stop_requested_at is None
 j.force_stop_requested_at=datetime.now(timezone.utc);j.force_stop_requested_by=actor.user_key
 j.force_stop_requested_by_user_id=actor.id
 print(json.dumps({{'reserved':True,'worker_pid':w.process_id,'requested_at':j.force_stop_requested_at}},default=str))
''')
ops.save("reservation.json", reservation)
print("Legacy force-stop request recorded", flush=True)
try:
    stopped = ops.remote("linux", f'''
import json,time,signal,subprocess
from datetime import datetime,timezone
from pathlib import Path
import psutil
worker=psutil.Process({worker_pid});assert worker.create_time()=={worker_created!r}
unit='detecdiv-worker@1.service'
assert int(subprocess.check_output(['systemctl','show',unit,'-p','MainPID','--value'],text=True))==worker.pid
assert worker.cwd()==str(Path.cwd())
cg=subprocess.check_output(['systemctl','show',unit,'-p','ControlGroup','--value'],text=True).strip()
assert cg.endswith('/detecdiv-worker@1.service')
cgfile=Path('/sys/fs/cgroup'+cg)/'cgroup.procs'
def members():return {{int(x) for x in cgfile.read_text().split()}}
children=worker.children(recursive=True)
assert children
assert members()=={{worker.pid,*[p.pid for p in children]}}
main=[p for p in worker.children() if p.name()=='MATLAB' and {JOB!r} in ' '.join(p.cmdline())]
assert len(main)==1,'MATLAB command does not identify the authorized job'
assert {inspection['runtime']['work_dir']!r} in ' '.join(main[0].cmdline())
frozen={{}};kill_started=False
try:
 deadline=time.monotonic()+8
 while True:
  pending=False
  for identity in members()-{{worker.pid}}:
   p=psutil.Process(identity)
   assert Path('/proc',str(p.pid),'cgroup').read_text().strip()=='0::'+cg
   frozen[p.pid]=p
   if p.status() not in (psutil.STATUS_STOPPED,psutil.STATUS_ZOMBIE):
    p.send_signal(signal.SIGSTOP);pending=True
  if not pending:break
  assert time.monotonic()<deadline,'Processes did not freeze; no kill issued'
  time.sleep(.02)
 assert worker.create_time()=={worker_created!r}
 assert worker.pid not in frozen
 kill_started=True
 # Main MATLAB is frozen; terminate its helpers/pool before terminating it.
 for p in [p for p in frozen.values() if p.pid!=main[0].pid]+main:
  try:p.kill()
  except psutil.NoSuchProcess:pass
 deadline=time.monotonic()+25
 while members()-{{worker.pid}}:
  assert time.monotonic()<deadline,'Some job processes still present; keep reservations'
  time.sleep(.1)
 assert worker.is_running() and worker.create_time()=={worker_created!r}
 print(json.dumps({{'worker_survived':worker.pid,'terminated_pids':list(frozen),
  'confirmed_at':datetime.now(timezone.utc).isoformat(),'cgroup_empty_except_worker':True}}))
finally:
 if not kill_started:
  for p in frozen.values():
   try:p.send_signal(signal.SIGCONT)
   except psutil.NoSuchProcess:pass
''', timeout=50)
    ops.save("process-stop.json", stopped)
    print("Job process tree stopped; worker preserved:", stopped, flush=True)
    cancelled = ops.api(f'''
import json,time
from datetime import datetime
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,ProjectLock,WorkerInstance
for i in range(40):
 with SessionLocal.begin() as s:
  j=s.get(Job,{JOB!r},with_for_update=True)
  if j.status=='cancelled':
   j.force_stopped_at=datetime.fromisoformat({stopped['confirmed_at']!r})
   result=dict(j.result_json or {{}})
   result['legacy_force_stop']={{'method':'validated_service_cgroup_children_only','worker_pid':{worker_pid},'terminated_pids':{stopped['terminated_pids']!r},'authorized_by':'gilles','worker_preserved':True}}
   j.result_json=result
   locks=list(s.scalars(select(ProjectLock).where(ProjectLock.job_id==j.id)))
   assert all(l.status=='released' for l in locks)
   w=s.scalars(select(WorkerInstance).where(WorkerInstance.worker_instance=='@1',WorkerInstance.execution_target_id==j.execution_target_id)).one()
   assert w.current_job_id is None and w.process_id=={worker_pid}
   print(json.dumps({{'status':j.status,'finished_at':j.finished_at,'locks_released':True,'confirmed_at':j.force_stopped_at}},default=str));break
  assert j.status=='cancelling',j.status
 time.sleep(.5)
else:raise RuntimeError('Old worker did not finalize cancellation')
''')
    ops.save("cancelled.json", cancelled)
    print("Cancellation finalized and locks released", flush=True)
    reloaded = ops.remote("linux", f'''
import importlib.util,json,sys
from pathlib import Path
root=Path.cwd();helper=root/'.deploy-staging/job-force-stop-20261020/activate_when_idle.py'
sys.argv=[str(helper),'linux']
spec=importlib.util.spec_from_file_location('idle_activation',helper);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
from api.services.runtime_version import get_runtime_version_info
assert get_runtime_version_info()['code_fingerprint']==module.EXPECTED
with module.SessionLocal() as s:
 w=s.scalars(module.select(module.WorkerInstance).where(module.WorkerInstance.worker_instance=='@1')).one()
 assert w.process_id=={worker_pid} and w.current_job_id is None
result=module.linux_reload({{'instance':'@1','pid':{worker_pid}}})
assert 'reloaded_pid' in result,result
print(json.dumps({{'activation':result,'expected_fingerprint':module.EXPECTED}}))
''')
    ops.save("idle-activation.json", reloaded)
    for i in range(15):
        latest=ops.snapshot()
        worker=next(w for w in latest["workers"] if w["target_key"]=="detecdiv-server" and w["worker_instance"]=="@1")
        if worker["process_id"]!=worker_pid and worker["code_fingerprint"]==reloaded["expected_fingerprint"]:
            ops.save("updated-worker.json",worker)
            print("Fresh worker verified:",worker,flush=True)
            break
        time.sleep(1)
    else:
        raise RuntimeError("Worker activation heartbeat not confirmed")
finally:
    restored=ops.api(f'''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget
with SessionLocal.begin() as s:
 t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='detecdiv-server').with_for_update()).one()
 m=dict(t.metadata_json or {{}});owner=m.pop({KEY!r})
 if owner['previous_present']:m['drain_new_jobs']=owner['previous']
 else:m.pop('drain_new_jobs',None)
 t.metadata_json=m
 print(json.dumps({{'admission_restored':True}}))
''')
    ops.save("admission-restored.json",restored)
