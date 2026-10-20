"""Private read-only end-of-campaign verification, no service mutations."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root.parent.parent / 'scripts'))
from evaluate_qwen_runs import remote, write_json, now

args = SimpleNamespace(ssh='C:/Windows/System32/OpenSSH/ssh.exe')
api = remote(args, 'webserver-labo', '''
import json,os,pathlib,urllib.request
from sqlalchemy import text
from api.db import SessionLocal
from api.services.assistant_control import get_assistant_desired_state
root=pathlib.Path('/tmp/qwen-pilot-coexistence-20261014')
state=json.loads((root/'state.json').read_text())
p=pathlib.Path('/proc')/str(state['pid'])
proc_state=(p/'stat').read_text().rsplit(')',1)[1].split()[0] if p.exists() else 'gone'
with SessionLocal() as s:
 s.execute(text('SET TRANSACTION READ ONLY'))
 jobs=[dict(r) for r in s.execute(text("SELECT j.id::text,j.status,j.finished_at,t.target_key,j.params_json->'_hub_resource_allocation' allocation FROM jobs j LEFT JOIN execution_targets t ON t.id=j.execution_target_id WHERE j.status IN ('queued','running','cancelling') ORDER BY j.created_at")).mappings()]
 workers=[dict(r) for r in s.execute(text("SELECT worker_instance,worker_host,process_id,health,current_job_id::text,last_seen_at FROM worker_instances WHERE last_seen_at > now()-interval '2 minutes' ORDER BY worker_host,worker_instance")).mappings()]
 completed=[dict(r) for r in s.execute(text("SELECT id::text,status,finished_at FROM jobs WHERE id IN ('5cbf3dca-a43b-45c1-9135-cfc9d1966403','e2a1b570-be3b-42f9-a911-ee4231d707a5')")).mappings()]
 desired=get_assistant_desired_state(s)
health=json.load(urllib.request.urlopen('http://127.0.0.1:8000/health',timeout=5))
print(json.dumps({'jobs':jobs,'workers':workers,'initial_science_jobs':completed,'desired':desired,'health':health,'arbitration':os.environ.get('DETECDIV_HUB_ASSISTANT_GPU_ARBITRATION_ENABLED'),'admission':state,'admission_process_state':proc_state},default=str))
''', api=True)
compute = remote(args, 'detecdiv-server', '''
import json,pathlib,subprocess,hashlib,stat,urllib.request
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014')
state=json.loads((root/'watchdog-state.json').read_text())
manifest=json.loads((root/'manifest.json').read_text())
p=pathlib.Path('/proc')/str(state['pid'])
proc_state=(p/'stat').read_text().rsplit(')',1)[1].split()[0] if p.exists() else 'gone'
def run(cmd):
 p=subprocess.run(cmd,capture_output=True,text=True);return {'exit':p.returncode,'output':p.stdout.strip()}
workers=[]
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  cmd=(p/'cmdline').read_bytes().split(b'\\0')
  if not any(a.endswith(b'worker/run_worker.py') for a in cmd):continue
  env=(p/'environ').read_bytes().split(b'\\0')
  flag=[a.split(b'=',1)[1].decode() for a in env if a.startswith(b'DETECDIV_HUB_ASSISTANT_GPU_ARBITRATION_ENABLED=')]
  workers.append({'pid':int(p.name),'arbitration':flag,'cgroup':(p/'cgroup').read_text().strip()})
 except (FileNotFoundError,ProcessLookupError):pass
models=json.load(urllib.request.urlopen('http://127.0.0.1:8001/v1/models',timeout=10))
metrics=urllib.request.urlopen('http://127.0.0.1:8001/metrics',timeout=10).read().decode()
requests=[l for l in metrics.splitlines() if not l.startswith('#') and ('num_requests_running{' in l or 'num_requests_waiting{' in l)]
try:
 urllib.request.urlopen('http://127.0.0.1:8002/v1/models',timeout=2)
 trial_endpoint='unexpectedly_available'
except Exception:trial_endpoint='unavailable'
gpu_processes=[]
for line in run(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'])['output'].splitlines():
 p=pathlib.Path('/proc')/line.strip()
 gpu_processes.append({'pid':int(line.strip()),'command':(p/'cmdline').read_bytes().replace(bytes([0]),b' ').decode(),'cgroup':(p/'cgroup').read_text().strip()})
launcher=pathlib.Path('/home/charvin-admin/repos/detecdiv-hub-webvm/ops/scripts/run-qwen-server.sh')
print(json.dumps({'models':models,'request_metrics':requests,'trial_endpoint':trial_endpoint,'gpu_processes':gpu_processes,'watchdog':{k:v for k,v in state.items() if k!='samples'},'manifest_owner':manifest['owner'],'watchdog_process_state':proc_state,'workers':workers,'worker_services':run(['systemctl','list-units','detecdiv-worker@*.service','--all','--no-pager','--no-legend']),'qwen':run(['systemctl','is-active','detecdiv-qwen.service']),'gpu':run(['nvidia-smi','--query-gpu=memory.total,memory.used,utilization.gpu','--format=csv,noheader']),'compute_processes':run(['nvidia-smi','--query-compute-apps=pid,process_name,used_gpu_memory','--format=csv,noheader']),'launcher_sha256':hashlib.sha256(launcher.read_bytes()).hexdigest(),'launcher_mode':stat.S_IMODE(launcher.stat().st_mode)}))
''')
record = {'checked_at': now(), 'api': api, 'compute': compute}
write_json(root / 'qwen36-final-verification.json', record)
assert api['admission']['status'] == 'released'
assert api['admission_process_state'] in ('gone', 'Z')
assert api['arbitration'] == 'true'
assert compute['watchdog']['status'] == 'stopped'
assert compute['watchdog']['owner'] == compute['manifest_owner'] == api['admission']['owner']
assert compute['watchdog_process_state'] in ('gone', 'Z')
assert compute['launcher_sha256'] == '0e681f73e64b5bc571b96b513d47789cef25e366b9059b08e0144e98cb33824c'
assert compute['launcher_mode'] == 493
assert compute['qwen']['output'] == 'active'
assert [m['id'] for m in compute['models']['data']] == ['Qwen/Qwen3-14B-AWQ']
assert compute['trial_endpoint'] == 'unavailable'
assert compute['request_metrics'] and all(float(l.rsplit(' ',1)[1])==0 for l in compute['request_metrics'])
assert compute['gpu_processes'] and all('detecdiv-qwen.service' in p['cgroup'] for p in compute['gpu_processes'])
assert len(compute['workers']) == 6
assert all(w['arbitration'] == ['true'] for w in compute['workers'])
print(json.dumps(record, ensure_ascii=True))
