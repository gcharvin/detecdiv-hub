"""Read-only deployment audit transports; never print credentials or payloads."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
COMMIT = "01204c96829b0b676384e7f182ae54074b56aa8d"
spec = importlib.util.spec_from_file_location(
    "previous_transport", ROOT / "reports/job-force-stop-deployment-20261020/ops.py"
)
transport = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport)
remote = transport.remote


def api(code):
    command = [*transport.transport.ssh_args(transport.TARGETS['api']),
               'docker exec -i detecdiv-hub-api python -']
    result = subprocess.run(command, input=code, capture_output=True,
                            encoding='utf-8', errors='replace', timeout=90)
    if result.returncode:
        raise RuntimeError(result.stdout[-1500:] + result.stderr[-1500:])
    return json.loads(result.stdout.strip().splitlines()[-1])


def save(name, value):
    path = OUT / name
    if path.exists():
        raise FileExistsError(path)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return value


def desired_files():
    paths = subprocess.check_output(
        ["git", "ls-tree", "-r", "--name-only", COMMIT, "api", "worker",
         "db/schema.sql", "db/migrations/20261020_job_force_stop.sql", "pyproject.toml",
         "constraints.txt", "scripts/register_windows_worker_task.ps1"],
        cwd=ROOT, text=True,
    ).splitlines()
    return {p: subprocess.check_output(["git", "show", f"{COMMIT}:{p}"], cwd=ROOT)
            for p in paths}


AUDIT = '''
import hashlib,json,subprocess,os
from pathlib import Path
root=Path.cwd()
files={}
for name in EXPECTED:
 p=root/name
 if p.is_file():
  b=p.read_bytes()
  files[name]={'sha256':hashlib.sha256(b).hexdigest(),
   'lf_sha256':hashlib.sha256(b.replace(b'\\r\\n',b'\\n')).hexdigest(),'bytes':len(b)}
 else: files[name]=None
extra=[]
for folder in ['api','worker']:
 for p in (root/folder).rglob('*'):
  if p.is_file() and '__pycache__' not in p.parts and p.suffix not in ['.pyc','.pyo'] and p.relative_to(root).as_posix() not in EXPECTED:
   extra.append(p.relative_to(root).as_posix())
git=None
if (root/'.git').exists():
 git={'head':subprocess.check_output(['git','-c','safe.directory='+str(root),'rev-parse','HEAD'],cwd=root,text=True).strip(),
      'status':subprocess.check_output(['git','-c','safe.directory='+str(root),'status','--short'],cwd=root,text=True).splitlines()}
from api.services.runtime_version import get_runtime_version_info
trees=[]
try:
 import psutil
 for p in psutil.process_iter(['pid','name','cmdline','create_time']):
  try:
   command=' '.join(p.info['cmdline'] or [])
   if 'python' in (p.info['name'] or '').lower() and '-m worker.run_worker' in command:
    trees.append({'pid':p.pid,'created':p.info['create_time'],
     'children':[{'pid':c.pid,'name':c.name(),'created':c.create_time()} for c in p.children(recursive=True)]})
  except (psutil.NoSuchProcess,psutil.AccessDenied):pass
except ImportError:pass
print(json.dumps({'root':str(root),'files':files,'extra_runtime_files':extra,'git':git,
 'source_runtime':get_runtime_version_info(),'worker_trees':trees},default=str))
'''


def audit_host(label):
    code = AUDIT.replace("EXPECTED", repr(list(desired_files())))
    return remote(label, code, timeout=90)


def snapshot():
    return api('''
import json,hashlib,requests
from sqlalchemy import text
from api.db import SessionLocal
from api.services.matlab_code_versions import default_commit
with SessionLocal() as s:
 jobs=[dict(r) for r in s.execute(text("select id,status,started_at,heartbeat_at,execution_target_id,params_json from jobs where status in ('running','cancelling','queued') order by created_at")).mappings()]
 for j in jobs:
  p=j.pop('params_json') or {}
  j['params_sha256']=hashlib.sha256(json.dumps(p,sort_keys=True,default=str).encode()).hexdigest()
  j['allocation']=p.get('_hub_resource_allocation');j['job_kind']=p.get('job_kind')
 workers=[dict(r) for r in s.execute(text('select w.worker_instance,w.process_id,w.current_job_id,w.last_seen_at,w.code_fingerprint,t.target_key from worker_instances w join execution_targets t on t.id=w.execution_target_id order by t.target_key,w.worker_instance')).mappings()]
 indexing=[dict(r) for r in s.execute(text("select id,status,phase,heartbeat_at from indexing_jobs where status in ('queued','running')")).mappings()]
 targets=[dict(r) for r in s.execute(text('select target_key,metadata_json from execution_targets')).mappings()]
 columns=list(s.execute(text("select column_name from information_schema.columns where table_name='jobs' and column_name like 'force_%' order by column_name")).scalars())
 health=requests.get('http://127.0.0.1:8000/health',timeout=10);health.raise_for_status()
 print(json.dumps({'jobs':jobs,'workers':workers,'indexing':indexing,'targets':targets,
  'force_stop_columns':columns,'health':health.json(),'matlab_default_commit':default_commit(s)},default=str))
''')


def summarize(label, value):
    desired = desired_files()
    differs = [name for name, data in desired.items()
               if not value['files'].get(name) or value['files'][name]['lf_sha256'] !=
               hashlib.sha256(data.replace(b'\r\n', b'\n')).hexdigest()]
    return {'host': label, 'source_runtime': value['source_runtime'],
            'different_or_missing': differs, 'extra_runtime_files': value['extra_runtime_files'],
            'worker_trees': value['worker_trees']}


if __name__ == '__main__':
    from concurrent.futures import ThreadPoolExecutor
    if not (OUT / 'before.json').exists():
        save('before.json', snapshot())
    with ThreadPoolExecutor(max_workers=3) as pool:
        values = list(pool.map(audit_host, ['api', 'linux', 'windows']))
    for label, value in zip(['api', 'linux', 'windows'], values):
        save(label + '-before.json', value)
        print(json.dumps(summarize(label, value)), flush=True)
