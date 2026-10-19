import base64
import json
from pathlib import Path

import ops

snapshot_code = '''
import hashlib,json
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import ExecutionTarget,WorkerInstance,Job
with SessionLocal() as s:
 s.execute(text('SET TRANSACTION READ ONLY'))
 targets=list(s.scalars(select(ExecutionTarget)))
 keys={str(t.id):t.target_key for t in targets}
 jobs=[{'id':str(j.id),'status':j.status,'started_at':str(j.started_at),
 'heartbeat':str(j.heartbeat_at),'allocation':(j.params_json or {}).get('_hub_resource_allocation'),
 'params_hash':hashlib.sha256(json.dumps(j.params_json,sort_keys=True).encode()).hexdigest()}
 for j in s.scalars(select(Job).where(Job.status.in_(('running','cancelling'))))]
 workers=[{'target':keys[str(w.execution_target_id)],'instance':w.worker_instance,
 'pid':w.process_id,'job':str(w.current_job_id) if w.current_job_id else None,
 'fingerprint':w.code_fingerprint,'seen':str(w.last_seen_at),'health':w.health}
 for w in s.scalars(select(WorkerInstance))]
 print(json.dumps({'at':datetime.now(timezone.utc).isoformat(),'jobs':jobs,'workers':workers,
 'targets':{t.target_key:{k:(t.metadata_json or {}).get(k) for k in ['drain_new_jobs','roi_memory_rollout','worker_dynamic_resources','worker_manager_seen_at','worker_autoscale_plan']} for t in targets}}))
'''


if __name__ == '__main__':
    ops.save('before-activation.json', ops.api(snapshot_code))
    source = base64.b64encode((ops.OUT / 'activate_when_idle.py').read_bytes()).decode()
    for label in ops.TARGETS:
        result = ops.remote(label, f'''
import base64,json,subprocess,sys
from pathlib import Path
stage=Path.cwd()/'.deploy-staging/roi-auto-memory-20261019'
p=stage/'activate_when_idle.py'
p.write_bytes(base64.b64decode({source!r}))
compile(p.read_bytes(),str(p),'exec')
result=subprocess.run([sys.executable,str(p),{label!r}],capture_output=True,text=True,timeout=75)
assert result.returncode==0,result.stderr
print(result.stdout.strip().splitlines()[-1])
''', timeout=90)
        ops.save(label + '-first-activation.json', result)
        print(label, result)
