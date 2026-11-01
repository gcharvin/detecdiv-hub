"""Activate the verified per-user Windows home mapping behind the admission lock."""
import ast
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('existing_ops', ROOT/'reports/job-force-stop-deployment-20261020/ops.py')
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)
remote = ops.remote
api = ops.api
ROLLOUT = 'homes2-windows-20261101'
MARKER = 'homes2_windows_mapping_activation'

def save(name, value):
    (OUT/name).write_text(json.dumps(value, indent=2, default=str)+'\n', encoding='utf-8')
    return value

def preflight():
    code = r'''
import json,hashlib,subprocess
from pathlib import Path
root=Path.cwd(); env=root/'.env'
raw=env.read_bytes(); text=raw.decode('utf-8-sig')
lines=[line.split('=',1)[1].strip() for line in text.splitlines() if line.startswith('DETECDIV_HUB_WORKER_PATH_MAPPINGS=')]
assert len(lines)==1, 'Expected exactly one mapping setting'
value=lines[0]
if value[:1] in ('"',"'") and value[-1:]==value[:1]:value=value[1:-1]
old=json.loads(value)
new={'source':'/homes2/maliavko/DetecdivHub','target':'//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub'}
assert not any(m['source'].startswith('/homes2') for m in old), 'Existing home mapping requires review'
receipt_path=Path('C:/ProgramData/DetecDivHub/homes2-connection-private/027011a7058f45aba2040edc49234e00.json')
receipt=json.loads(receipt_path.read_text(encoding='utf-8-sig'))
assert receipt['success'] and receipt['write_read_probe_passed'] and receipt['credentials_saved_for_worker']
assert receipt['windows_identity']=='GMGM\\Charvin-Admin' and receipt['session_id']==1
print(json.dumps({'env_sha256':hashlib.sha256(raw).hexdigest(),'existing_mappings':old,'new_mapping':new,'user_connection_receipt':receipt}))
'''
    result = remote('windows', code, timeout=30)
    save('mapping-preflight.json', result)
    print(json.dumps(result))

def drain():
    code = r'''
import json
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import ExecutionTarget,Job,WorkerInstance
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 target=db.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56').with_for_update()).one()
 meta=dict(target.metadata_json or {})
 assert not meta.get('drain_new_jobs') and MARKER not in meta, 'Existing maintenance must be preserved'
 jobs=list(db.scalars(select(Job).where(Job.execution_target_id==target.id,Job.status.in_(['running','cancelling']))))
 assert not jobs, 'Windows jobs are active; do not stop worker'
 workers=list(db.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id==target.id)))
 assert len(workers)==1 and workers[0].current_job_id is None
 w=workers[0]; now=datetime.now(timezone.utc)
 assert (now-w.last_seen_at).total_seconds()<30, 'Worker heartbeat is stale'
 before={'process_id':w.process_id,'instance':w.worker_instance,'fingerprint':w.code_fingerprint,'last_seen':w.last_seen_at,'active_jobs':len(jobs),'previous_drain_present':'drain_new_jobs' in meta,'previous_drain_value':meta.get('drain_new_jobs')}
 meta[MARKER]={'id':ROLLOUT,'previous_present':'drain_new_jobs' in meta,'previous_value':meta.get('drain_new_jobs')}
 meta['drain_new_jobs']=True
 target.metadata_json=meta
 print(json.dumps(before,default=str))
'''.replace('MARKER', repr(MARKER)).replace('ROLLOUT', repr(ROLLOUT))
    result = api(code)
    save('mapping-admission-before.json', result)
    print(json.dumps(result))

def activate():
    before = json.loads((OUT/'mapping-preflight.json').read_text())
    admission = json.loads((OUT/'mapping-admission-before.json').read_text())
    # Reuse the previously validated managed-tree guard. It rejects all child
    # processes other than the Python launch wrapper before any stop command.
    tree = ast.parse((ROOT/'reports/hub-reconciled-deployment-20261020/activate_when_idle.py').read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name=='windows_reload')
    assignment = next(n for n in function.body if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name) and n.targets[0].id=='ps')
    ps = assignment.value.func.value.value.replace('WORKERPID', str(admission['process_id']))
    ps = ps.replace('$repo/.deploy-staging/hub-reconciled-20261020/windows-task-stop-requested',
                    'C:/ProgramData/DetecDivHub/homes2-connection-private/mapping-worker-stop-requested')
    code = r'''
import json,hashlib,os,re,subprocess
from pathlib import Path
from worker.path_mappings import parse_worker_path_mappings,map_worker_path
root=Path.cwd(); env=root/'.env'
original=env.read_bytes()
assert hashlib.sha256(original).hexdigest()==EXPECTED_SHA, 'Environment changed since preflight'
text=original.decode('utf-8-sig')
line=re.search(r'^DETECDIV_HUB_WORKER_PATH_MAPPINGS=(.*)$',text,re.M)
assert line is not None
old_value=line.group(1).strip()
if old_value[:1] in ('"',"\x27") and old_value[-1:]==old_value[:1]:old_value=old_value[1:-1]
old=json.loads(old_value)
assert old==EXPECTED_MAPPINGS
updated=[NEW_MAPPING,*old]
new_value=json.dumps(updated,separators=(',',':'))
parsed=parse_worker_path_mappings(new_value)
assert map_worker_path('/homes2/maliavko/DetecdivHub/raw/example',parsed).replace(chr(92),'/')=='//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub/raw/example'
assert map_worker_path('/homes2/another-user/DetecdivHub/raw',parsed)=='/homes2/another-user/DetecdivHub/raw'
for old_mapping in old:
 sample=old_mapping['source'].rstrip('/\\')+'/unchanged'
 assert map_worker_path(sample,parsed)==map_worker_path(sample,parse_worker_path_mappings(json.dumps(old)))
replacement='DETECDIV_HUB_WORKER_PATH_MAPPINGS='+new_value
modified=text[:line.start()]+replacement+text[line.end():]
new_bytes=(b'\xef\xbb\xbf' if original.startswith(b'\xef\xbb\xbf') else b'')+modified.encode('utf-8')
backup=Path('C:/ProgramData/DetecDivHub/homes2-connection-private/worker.env.before-homes2-20261101')
assert not backup.exists(), 'Backup already exists; inspect the previous activation'
backup.write_bytes(original)
marker=Path('C:/ProgramData/DetecDivHub/homes2-connection-private/mapping-worker-stop-requested')
assert not marker.exists()
stopped=False; written=False; success=False
def ps(command,timeout=35):
 p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',command],capture_output=True,text=True,timeout=timeout)
 if p.returncode:raise RuntimeError(p.stdout[-1000:]+p.stderr[-1500:])
 return p.stdout.strip()
try:
 ps(STOP_SCRIPT)
 stopped=True
 assert marker.exists()
 assert env.read_bytes()==original, 'Environment changed before stop'
 env.write_bytes(new_bytes)  # Preserve the protected existing file ACL.
 written=True
 assert env.read_bytes()==new_bytes
 ps("& '"+str(root/'scripts/run_worker.ps1')+"' -EnvFile '"+str(env)+"' -Check",timeout=40)
 success=True
finally:
 if written and not success:
  env.write_bytes(original)
 if marker.exists():
  ps("Start-ScheduledTask -TaskName 'DetecDiv Hub Worker'",timeout=15)
  marker.unlink()
print(json.dumps({'success':success,'previous_worker_pid':EXPECTED_PID,'environment_sha256_before':hashlib.sha256(original).hexdigest(),'environment_sha256_after':hashlib.sha256(env.read_bytes()).hexdigest(),'new_mapping':NEW_MAPPING,'mapping_count':len(updated),'other_mappings_preserved':True,'backup':str(backup),'worker_restarted':stopped,'worker_task_reconfigured':False,'database_tunnel_untouched':True}))
'''
    code = code.replace('EXPECTED_SHA', repr(before['env_sha256'])).replace('EXPECTED_MAPPINGS', repr(before['existing_mappings'])).replace('NEW_MAPPING', repr(before['new_mapping'])).replace('EXPECTED_PID', repr(admission['process_id'])).replace('STOP_SCRIPT', repr(ps))
    result = remote('windows', code, timeout=100)
    save('mapping-activation.json', result)
    print(json.dumps(result))

def status():
    code = r'''
import json
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import WorkerInstance,ExecutionTarget,Job
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 t=db.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56')).one()
 ws=[{'pid':w.process_id,'fingerprint':w.code_fingerprint,'last_seen':w.last_seen_at,'health':w.health,'current_job':str(w.current_job_id)} for w in db.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id==t.id))]
 jobs=[{'id':str(j.id),'status':j.status} for j in db.scalars(select(Job).where(Job.execution_target_id==t.id,Job.status.in_(['running','cancelling'])))]
 print(json.dumps({'workers':ws,'active_jobs':jobs,'drain':(t.metadata_json or {}).get('drain_new_jobs'),'maintenance_marker':(t.metadata_json or {}).get(MARKER)},default=str))
'''.replace('MARKER', repr(MARKER))
    result = api(code)
    save('mapping-live-status.json', result)
    print(json.dumps(result))

def restore():
    before = json.loads((OUT/'mapping-admission-before.json').read_text())
    code = r'''
import json
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import ExecutionTarget,WorkerInstance
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 target=db.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56').with_for_update()).one()
 meta=dict(target.metadata_json or {}); owner=meta.get(MARKER)
 assert owner and owner['id']==ROLLOUT
 workers=list(db.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id==target.id)))
 assert len(workers)==1
 w=workers[0]
 assert w.process_id!=OLD_PID and w.code_fingerprint==EXPECTED_FINGERPRINT
 assert (datetime.now(timezone.utc)-w.last_seen_at).total_seconds()<30 and w.current_job_id is None
 if owner['previous_present']:meta['drain_new_jobs']=owner['previous_value']
 else:meta.pop('drain_new_jobs',None)
 meta.pop(MARKER)
 target.metadata_json=meta
 print(json.dumps({'admission_restored':True,'pid':w.process_id,'fingerprint':w.code_fingerprint,'last_seen':w.last_seen_at},default=str))
'''.replace('MARKER', repr(MARKER)).replace('ROLLOUT', repr(ROLLOUT)).replace('OLD_PID', repr(before['process_id'])).replace('EXPECTED_FINGERPRINT', repr(before['fingerprint']))
    result = api(code)
    save('mapping-admission-restored.json', result)
    print(json.dumps(result))

if __name__=='__main__':
    {'preflight':preflight,'drain':drain,'activate':activate,'status':status,'restore':restore}[sys.argv[1]]()
