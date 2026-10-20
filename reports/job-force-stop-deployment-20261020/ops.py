"""Deployment transport. Never print credentials or session tokens."""
import base64
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "scripts"))
import deploy_detecdiv_release as transport

LINUX_ROOT = "/home/charvin-admin/repos/detecdiv-hub-webvm"
WINDOWS_ROOT = "C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub"
ROLLOUT = "job-force-stop-20261020"
FILES = ["api/models.py", "api/schemas.py", "api/routes_pipeline_runs.py", "api/static/app.js",
         "db/schema.sql", "db/migrations/20261020_job_force_stop.sql",
         "worker/run_worker.py", "worker/pipeline_run_executor.py",
         "worker/executors/matlab_executor.py", "worker/executors/job_process_scope.py",
         "worker/executors/linux_job_supervisor.py"]
TARGETS = {"linux": {"ssh_host": "detecdiv-server"},
           "api": {"ssh_host": "webserver-labo"},
           "windows": {"ssh_host": "10.20.11.56", "ssh_user": "detecdiv-ops",
                       "ssh_key": str(Path.home() / ".ssh/id_ed25519_detecdiv_windows")}}

def remote(label, code, timeout=90):
    if label == "api":
        command = f"cd {LINUX_ROOT} && python3 -"
    elif label == "linux":
        command = f"cd {LINUX_ROOT} && .venv/bin/python -"
    else:
        command = f'cd /d "{WINDOWS_ROOT}" && ".venv\\Scripts\\python.exe" -'
    result = subprocess.run([*transport.ssh_args(TARGETS[label]), command], input=code,
                            capture_output=True, encoding="utf-8", errors="replace", timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stdout[-3500:] + result.stderr[-3500:])
    return json.loads(result.stdout.strip().splitlines()[-1])

def api(code):
    config = {"api": {**TARGETS["api"], "container": "detecdiv-hub-api"}}
    return transport.api_code(config, code)

def save(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return value

def snapshot():
    return api('''
import json,hashlib
from sqlalchemy import text
from api.db import SessionLocal
with SessionLocal() as s:
 jobs=[dict(r) for r in s.execute(text("select id,status,started_at,heartbeat_at,execution_target_id,params_json from jobs where status in ('running','cancelling','queued') order by created_at")).mappings()]
 for j in jobs:
  p=j.pop('params_json') or {}
  j['params_sha256']=hashlib.sha256(json.dumps(p,sort_keys=True,default=str).encode()).hexdigest()
  j['allocation']=p.get('_hub_resource_allocation')
  j['job_kind']=p.get('job_kind')
 workers=[dict(r) for r in s.execute(text('select w.worker_instance,w.process_id,w.current_job_id,w.last_seen_at,w.code_fingerprint,t.target_key from worker_instances w join execution_targets t on t.id=w.execution_target_id order by t.target_key,w.worker_instance')).mappings()]
 indexing=[dict(r) for r in s.execute(text("select id,status,phase,heartbeat_at from indexing_jobs where status in ('queued','running')")).mappings()]
 targets=[dict(r) for r in s.execute(text('select target_key,metadata_json from execution_targets')).mappings()]
 print(json.dumps({'jobs':jobs,'workers':workers,'indexing':indexing,'targets':targets},default=str))
''')
