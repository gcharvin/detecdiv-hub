"""Private, read-only live monitor; never alters scientific jobs or services."""
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent.parent / 'scripts'))
from evaluate_qwen_runs import gpu_guard, remote, write_json, now

args = SimpleNamespace(ssh='C:/Windows/System32/OpenSSH/ssh.exe',
                       api_host='webserver-labo', compute_host='detecdiv-server',
                       coexistence_owner=None)
previous = json.loads((ROOT / 'monitor-latest.json').read_text(encoding='utf-8'))
guard = gpu_guard(args)
api = remote(args, args.api_host, """
import json,pathlib,os
from sqlalchemy import text
from api.db import SessionLocal
with SessionLocal() as s:
 s.execute(text('SET TRANSACTION READ ONLY'))
 rows=s.execute(text("SELECT j.id::text,j.status,j.heartbeat_at,t.target_key,j.params_json->'_hub_resource_allocation' allocation FROM jobs j LEFT JOIN execution_targets t ON t.id=j.execution_target_id WHERE j.status IN ('queued','running','cancelling') ORDER BY j.created_at")).mappings().all()
 p=pathlib.Path('/tmp/qwen-pilot-coexistence-20261014/state.json')
 print(json.dumps({'jobs':[dict(r) for r in rows],'arbitration':os.environ.get('DETECDIV_HUB_ASSISTANT_GPU_ARBITRATION_ENABLED'),'admission':json.loads(p.read_text()) if p.exists() else None},default=str))
""", api=True)
compute = remote(args, args.compute_host, """
import json,subprocess,pathlib,hashlib,os
out={}
def run(cmd):
 p=subprocess.run(cmd,capture_output=True,text=True);return {'exit':p.returncode,'output':p.stdout.strip()}
out['gpu']=run(['nvidia-smi','--query-gpu=name,memory.total,memory.used,utilization.gpu','--format=csv,noheader'])
out['processes']=run(['nvidia-smi','--query-compute-apps=pid,process_name,used_gpu_memory','--format=csv,noheader'])
out['process_owners']=[]
for line in out['processes']['output'].splitlines():
 pid=line.split(',')[0].strip();p=pathlib.Path('/proc')/pid
 entry={'pid':pid,'cgroup':(p/'cgroup').read_text().strip() if p.exists() else 'gone'}
 if p.exists():
  stat=(p/'stat').read_text().rsplit(')',1)[1].split()
  entry.update(state=stat[0],cpu_seconds=(int(stat[11])+int(stat[12]))/os.sysconf('SC_CLK_TCK'),start_ticks=int(stat[19]))
  try:entry['io']={k:int(v) for k,v in (l.split(':',1) for l in (p/'io').read_text().splitlines())}
  except PermissionError:entry['io']='unreadable'
 out['process_owners'].append(entry)
out['service']=run(['systemctl','is-active','detecdiv-qwen.service'])
p=pathlib.Path('/home/charvin-admin/repos/detecdiv-hub-webvm/ops/scripts/run-qwen-server.sh')
out['launcher_sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
p=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014/state.json')
if p.exists():out['watchdog']={k:v for k,v in json.loads(p.read_text()).items() if k!='samples'}
print(json.dumps(out))
""")
assert hashlib.sha256((ROOT / 'corpus.json').read_bytes()).hexdigest() == 'e3fc6d927b24cbb18bf2b936d7405e4bd7b42eadee0be76c2dbc6ea216e276ce'
old = {p['pid']: p for p in previous['compute']['process_owners']}
deltas = []
for p in compute['process_owners']:
    o = old.get(p['pid'], {})
    if o.get('start_ticks') == p.get('start_ticks') and 'cpu_seconds' in o:
        delta = {'pid': p['pid'], 'cpu_seconds': round(p['cpu_seconds'] - o['cpu_seconds'], 2)}
        if isinstance(p.get('io'), dict) and isinstance(o.get('io'), dict):
            delta.update({k: p['io'][k] - o['io'][k] for k in ('read_bytes', 'write_bytes')})
        deltas.append(delta)
record = {'checked_at': now(), 'guard': guard, 'api': api, 'compute': compute,
          'activity_since': previous['checked_at'], 'activity_deltas': deltas,
          'action': 'wait' if not guard['available'] else 'ready_for_main'}
write_json(ROOT / 'monitor-latest.json', record)
print(json.dumps({'checked_at': record['checked_at'], 'guard': guard,
                  'jobs': [{'id': j['id'], 'status': j['status'], 'heartbeat_at': j['heartbeat_at'],
                            'target': j['target_key'], 'gpu_vram_mb': (j['allocation'] or {}).get('gpu_vram_mb')}
                           for j in api['jobs']],
                  'arbitration': api['arbitration'], 'gpu': compute['gpu'], 'service': compute['service'],
                  'launcher_sha256': compute['launcher_sha256'],
                  'activity_since': record['activity_since'], 'deltas': deltas}, ensure_ascii=True))
