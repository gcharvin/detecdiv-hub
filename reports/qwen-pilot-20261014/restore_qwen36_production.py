"""Restore the original running assistant through the worker arbitration path."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root.parent.parent / 'scripts'))
from evaluate_qwen_runs import gpu_guard, remote, write_json, now

args = SimpleNamespace(ssh='C:/Windows/System32/OpenSSH/ssh.exe', api_host='webserver-labo',
                       compute_host='detecdiv-server', coexistence_owner=None)
guard = gpu_guard(args)
if guard['gpu_jobs']:
    result = {'deferred_for_science': True, 'guard': guard}
else:
    source = '''
import json,subprocess
from sqlalchemy import select,text
from api.config import Settings
from api.db import SessionLocal
from api.models import ExecutionTarget,Job
from worker.gpu_arbitration import resume_qwen_if_gpu_is_idle,job_requires_gpu
settings=Settings()
assert settings.assistant_gpu_arbitration_enabled
with SessionLocal() as session:
 session.execute(text("SET LOCAL lock_timeout='1000ms'"))
 session.execute(text("SET LOCAL statement_timeout='10000ms'"))
 session.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='detecdiv-server').with_for_update()).one()
 jobs=session.scalars(select(Job).where(Job.status.in_(('queued','running','cancelling')))).all()
 if any(job_requires_gpu(session,job=j) for j in jobs):
  print(json.dumps({'deferred_for_science':True}));raise SystemExit()
 # Existing normal arbitration checks the unchanged desired state before start.
 resume_qwen_if_gpu_is_idle(session,settings=settings)
 print(json.dumps({'normal_arbitration_restore_requested':True,'service':subprocess.run(['systemctl','is-active','detecdiv-qwen.service'],capture_output=True,text=True).stdout.strip()}))
'''
    result = remote(args, args.compute_host, f'''
import pathlib,json,subprocess,hashlib
launcher=pathlib.Path('/home/charvin-admin/repos/detecdiv-hub-webvm/ops/scripts/run-qwen-server.sh')
assert hashlib.sha256(launcher.read_bytes()).hexdigest()=='0e681f73e64b5bc571b96b513d47789cef25e366b9059b08e0144e98cb33824c'
state=json.loads(pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014/watchdog-state.json').read_text());assert state['status']=='stopped'
status=subprocess.run(['systemctl','is-active','detecdiv-qwen.service'],capture_output=True,text=True).stdout.strip()
if status=='active':print(json.dumps({{'already_active_original_service':True}}));raise SystemExit()
assert status=='inactive'
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid,process_name,used_gpu_memory','--format=csv,noheader'],text=True).strip()
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  command=(p/'cmdline').read_bytes().split(b'\\0')
  if not any(a.endswith(b'worker/run_worker.py') for a in command):continue
  env={{a.split(b'=',1)[0].decode():a.split(b'=',1)[1].decode() for a in (p/'environ').read_bytes().split(b'\\0') if b'=' in a}}
  assert env.get('DETECDIV_HUB_ASSISTANT_GPU_ARBITRATION_ENABLED')=='true'
  process=subprocess.run([command[0].decode(),'-c',{source!r}],cwd=str((p/'cwd').resolve()),env=env,capture_output=True,text=True,timeout=30)
  if process.returncode:raise RuntimeError('Normal arbitration restore failed: '+process.stderr[-1200:])
  print(process.stdout);break
 except (FileNotFoundError,ProcessLookupError):continue
else:raise RuntimeError('No live worker runtime found for normal arbitration restore')
''')
write_json(root / 'qwen36-production-restore.json', {'checked_at': now(), **result})
print(json.dumps(result))
