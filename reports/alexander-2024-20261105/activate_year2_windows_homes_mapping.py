"""Replace Alexander's Windows mapping and restart only the idle worker."""

import ast
import json
import time
from pathlib import Path

from year2_final_host_operations import HERE, PRIVATE, remote, write_receipt
from activate_windows_mapping import api

CHANGE = "alexander_year2_windows_homes_20261112"
TARGET = "windows-10-20-11-56"
SOURCE = "/homes2/maliavko/DetecdivHub"
OLD = "//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub"
NEW = "//10.20.8.250/homes/maliavko/DetecdivHub"

DB = r"""
import json
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import ExecutionTarget,Job,WorkerInstance
with SessionLocal.begin() as db:
    db.execute(text("SET LOCAL lock_timeout='10s'"))
    query=select(ExecutionTarget).where(ExecutionTarget.target_key==target_key)
    target=db.scalars(query.with_for_update() if mode!='status' else query).one()
    metadata=dict(target.metadata_json or {})
    own=metadata.get(marker)
    workers=list(db.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id==target.id)))
    assert len(workers)==1 and workers[0].worker_instance==target_key+'-main'
    worker=workers[0]
    active=db.scalar(select(Job.id).where(Job.execution_target_id==target.id,
        Job.status.in_(('running','cancelling'))).limit(1)) is not None
    idle=not active and worker.current_job_id is None
    age=(datetime.now(timezone.utc)-worker.last_seen_at).total_seconds()
    if mode=='drain':
        assert own is None and not metadata.get('drain_new_jobs')
        assert idle and 0<=age<30, 'Windows worker busy or stale'
        metadata[marker]={'old_pid':worker.process_id,'fingerprint':worker.code_fingerprint,
                          'prior_exists':'drain_new_jobs' in metadata,'prior':metadata.get('drain_new_jobs')}
        metadata['drain_new_jobs']=True
        target.metadata_json=metadata
    elif mode=='restore':
        assert own is not None and metadata.get('drain_new_jobs') is True
        assert idle and 0<=age<30 and worker.code_fingerprint==own['fingerprint']
        if own['prior_exists']:metadata['drain_new_jobs']=own['prior']
        else:metadata.pop('drain_new_jobs')
        metadata.pop(marker)
        target.metadata_json=metadata
    result={'pid':worker.process_id,'idle':idle,'age_seconds':age,'fingerprint':worker.code_fingerprint,
            'own_drain':marker in metadata,'drained':bool(metadata.get('drain_new_jobs'))}
print(json.dumps(result))
"""

ENV = r"""
import hashlib,json,re
from pathlib import Path
from worker.path_mappings import parse_worker_path_mappings,map_worker_path
env=Path.cwd()/'.env'
original=env.read_bytes()
name=b'DETECDIV_HUB_WORKER_PATH_MAPPINGS='
matches=list(re.finditer(rb'^'+name+rb'([^\r\n]*)',original,re.M))
assert len(matches)==1
line=matches[0]
value=line[1].strip()
if value[:1] in (b'"',b"'") and value[-1:]==value[:1]:value=value[1:-1]
mappings=json.loads(value)
expected={'source':source,'target':old}
assert sum(item.get('source')==source for item in mappings)==1 and mappings.count(expected)==1
assert line[1].count(old.encode())==1
updated=line[1].replace(old.encode(),new.encode())
new_bytes=original[:line.start(1)]+updated+original[line.end(1):]
before=parse_worker_path_mappings(value.decode())
after=parse_worker_path_mappings(value.replace(old.encode(),new.encode()).decode())
assert map_worker_path(source,after).replace(chr(92),'/')==new
assert all(map_worker_path(item.source+'/example',before)==map_worker_path(item.source+'/example',after)
           for item in before if item.source!=source)
digest=lambda data:hashlib.sha256(data).hexdigest()
"""

ACTIVATE = r"""
import subprocess
assert digest(original)==expected_hash
backup=Path('C:/ProgramData/DetecDivHub/homes2-connection-private/worker.env.before-alexander-homes-20261112')
marker=Path('C:/ProgramData/DetecDivHub/homes2-connection-private/alexander-homes-worker-stop-20261112')
assert not marker.exists()
def ps(script,timeout=35):
    subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',script],
                   check=True,capture_output=True,timeout=timeout)
with backup.open('xb') as stream:stream.write(original)
assert backup.read_bytes()==original
written=False
try:
    ps(stop_script,45)
    assert marker.exists() and env.read_bytes()==original
    written=True
    env.write_bytes(new_bytes)
    assert env.read_bytes()==new_bytes
    ps("& './scripts/run_worker.ps1' -EnvFile '.env' -Check",40)
    ps("Start-ScheduledTask -TaskName 'DetecDiv Hub Worker'",15)
except Exception:
    if written:env.write_bytes(original)
    if marker.exists():ps("Start-ScheduledTask -TaskName 'DetecDiv Hub Worker'",15)
    raise
finally:
    if marker.exists():marker.unlink()
print(json.dumps({'mapping_before':old,'mapping_after':new,'environment_before_sha256':digest(original),
    'environment_after_sha256':digest(new_bytes),'other_mappings_preserved':True,'worker_restart_requested':True}))
"""


def database(mode):
    return api(f"mode={mode!r}\ntarget_key={TARGET!r}\nmarker={CHANGE!r}\n" + DB)


def stop_script(pid):
    source_file = HERE.parent / "hub-reconciled-deployment-20261020/activate_when_idle.py"
    tree = ast.parse(source_file.read_bytes())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "windows_reload")
    assignment = next(node for node in function.body if isinstance(node, ast.Assign)
                      and isinstance(node.targets[0], ast.Name) and node.targets[0].id == "ps")
    return assignment.value.func.value.value.replace("WORKERPID", str(pid)).replace(
        "$repo/.deploy-staging/hub-reconciled-20261020/windows-task-stop-requested",
        "C:/ProgramData/DetecDivHub/homes2-connection-private/alexander-homes-worker-stop-20261112",
    )


def main():
    assert not (PRIVATE / "year2-windows-mapping-activation.json").exists()
    receipt = json.loads((PRIVATE / "year2-worker-token-mapping-observation.json").read_bytes())
    assert receipt["alternate_readable"] == receipt["projects"] == 17
    assert all(row["matches_logon"] for row in receipt["worker_logon_checks"])
    prefix = f"source={SOURCE!r}\nold={OLD!r}\nnew={NEW!r}\n"
    plan = remote("windows", prefix + ENV + "\nprint(json.dumps({'hash':digest(original)}))", timeout=30)
    admission = database("drain")
    write_receipt("year2-windows-mapping-admission.json", admission)
    try:
        fresh = database("status")
        assert fresh["own_drain"] and fresh["idle"] and fresh["age_seconds"] < 30 and fresh["pid"] == admission["pid"]
        inputs = f"expected_hash={plan['hash']!r}\nstop_script={stop_script(fresh['pid'])!r}\n"
        activation = remote("windows", prefix + inputs + ENV + ACTIVATE, timeout=115)
        write_receipt("year2-windows-mapping-activation.json", activation)
        for _ in range(16):
            state = database("status")
            if state["pid"] != admission["pid"] and state["age_seconds"] < 20:
                break
            time.sleep(2)
        assert state["pid"] != admission["pid"] and state["age_seconds"] < 20
        assert state["idle"] and state["fingerprint"] == admission["fingerprint"]
        write_receipt("year2-windows-mapping-worker-restarted.json", state)
    finally:
        restored = database("restore")
        write_receipt("year2-windows-mapping-admission-restored.json", restored)
    print(json.dumps({**activation,"old_pid":admission["pid"],"new_pid":state["pid"],"admission_restored":not restored["drained"]}))


if __name__ == "__main__":
    main()
