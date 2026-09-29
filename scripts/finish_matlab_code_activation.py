"""One-shot Linux rollout: reload old pollers only after they become idle.

Run on the administration workstation with its existing SSH aliases. No DB
credentials are copied out of the API container. Active jobs are never signalled.
"""
import argparse
import base64
import json
import subprocess
import time


def remote(ssh, host, code, *, api=False):
    encoded = base64.b64encode(code.encode()).decode()
    python = "docker exec detecdiv-hub-api python" if api else "python3"
    command = f'{python} -c "import base64; exec(base64.b64decode(\'{encoded}\'))"'
    result = subprocess.run([ssh, "-o", "BatchMode=yes", host, command],
                            text=True, capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError(f"Remote rollout command failed on {host} (exit {result.returncode}); inspect host logs.")
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ssh", required=True)
    parser.add_argument("--fingerprint", required=True)
    parser.add_argument("--old-poller", action="append", required=True, help="instance=original-pid")
    args = parser.parse_args()
    old = dict(item.split("=", 1) for item in args.old_poller)
    old = {name: int(pid) for name, pid in old.items()}
    assert all(name.startswith("@") and name[1:].isdigit() and pid > 1 for name, pid in old.items())
    while True:
        reserve = remote(args.ssh, "webserver-labo", f'''
import json
from datetime import datetime, timezone
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget, WorkerInstance
with SessionLocal() as s:
    t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='detecdiv-server').with_for_update()).one()
    workers=s.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id==t.id)).all()
    required={{'@'+str(i) for i in range(1,7)}}
    recent=[w for w in workers if w.worker_instance in required and w.last_seen_at and (datetime.now(timezone.utc)-w.last_seen_at).total_seconds()<60]
    meta=dict(t.metadata_json or {{}})
    ready=len(recent)==6 and all(w.code_fingerprint=={args.fingerprint!r} for w in recent)
    if ready:
        meta['matlab_code_isolation_ready']=True
        t.metadata_json=meta
        s.commit()
        result={{'ready':True}}
    else:
        idle=[w.worker_instance for w in recent if w.worker_instance in {old!r} and w.current_job_id is None and w.code_fingerprint!={args.fingerprint!r}]
        if idle and not meta.get('drain_new_jobs',False):
            previous={{'present':'drain_new_jobs' in meta,'value':meta.get('drain_new_jobs')}}
            meta['drain_new_jobs']=True
            t.metadata_json=meta
            s.commit()
            result={{'ready':False,'idle':idle,'previous':previous}}
        else: result={{'ready':False,'idle':[]}}
print(json.dumps(result))
''', api=True)
        if reserve["ready"]:
            print("Linux target ready for versioned MATLAB jobs", flush=True)
            return
        if reserve["idle"]:
            try:
                selected = {name: old[name] for name in reserve["idle"]}
                outcome = remote(args.ssh, "detecdiv-server", f'''
import json,os,signal,subprocess
reloaded=[]
for name,expected in {selected!r}.items():
    unit='detecdiv-worker'+name+'.service'
    pid=int(subprocess.check_output(['systemctl','show',unit,'-p','MainPID','--value'],text=True))
    if pid!=expected: continue
    assert os.stat('/proc/'+str(pid)).st_uid==os.getuid()
    assert b'worker/run_worker.py' in open('/proc/'+str(pid)+'/cmdline','rb').read()
    assert subprocess.check_output(['systemctl','show',unit,'-p','Restart','--value'],text=True).strip()=='always'
    os.kill(pid,signal.SIGTERM)
    reloaded.append(name)
print(json.dumps(reloaded))
''')
                print("Idle pollers reloaded: " + ", ".join(outcome), flush=True)
            finally:
                previous = reserve["previous"]
                remote(args.ssh, "webserver-labo", f'''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget
with SessionLocal() as s:
    t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='detecdiv-server').with_for_update()).one()
    meta=dict(t.metadata_json or {{}})
    previous={previous!r}
    if previous['present']: meta['drain_new_jobs']=previous['value']
    else: meta.pop('drain_new_jobs',None)
    t.metadata_json=meta
    s.commit()
print(json.dumps({{'admission_restored':True}}))
''', api=True)
        time.sleep(10)


if __name__ == "__main__":
    main()
