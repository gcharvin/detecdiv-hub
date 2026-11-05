"""One-shot exact cache retirement after live home-serving gates succeed."""
import base64
from datetime import datetime,timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

WORK=Path(__file__).resolve().parent
STATE={'phase':'preflight','files_unlinked':0,'bytes_unlinked':0}


def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=WORK/'state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/'state.json')


def main():
    os.umask(0o077);assert not (WORK/'state.json').exists(),'Inspect prior or partial retirement; never replay'
    gate=json.loads((WORK/'gates.json').read_text())
    assert gate['success'] and gate['standing_authorization'] and gate['cache_only']
    assert gate['home_pairs_fully_verified']==gate['actual_home_handlers_fully_verified']==1926
    assert gate['bytes_fully_verified']==28981723684 and gate['api_home_bind_readonly']
    assert 0<=(datetime.now(timezone.utc)-datetime.fromisoformat(gate['checked_at'])).total_seconds()<120
    assert time.time()<gate['lease_deadline_unix']
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==gate['controller_sha256']
    script=(WORK/'cache_manifest_native.py').read_bytes();assert hashlib.sha256(script).hexdigest()==gate['native_script_sha256']
    manifest=(WORK.parent/'source-before.jsonl').read_bytes();assert hashlib.sha256(manifest).hexdigest()==gate['manifest_sha256']
    rows=[json.loads(line) for line in manifest.splitlines()];assert len(rows)==1926 and sum(r['bytes'] for r in rows)==28981723684
    assert {r['year'] for r in rows}=={'2023_1','2023_2','2023_3'}
    with (WORK.parent/'retirement.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        request={'mode':'unlink','verified_home_serving':True,'manifest_sha256':gate['manifest_sha256'],'lease_deadline_unix':gate['lease_deadline_unix']}
        data=WORK/'native-input.jsonl'
        with data.open('xb') as f:f.write((json.dumps(request)+'\n').encode());f.write(manifest)
        loader='import base64;exec(compile(base64.b64decode('+repr(base64.b64encode(script).decode())+'),"cache_manifest_native.py","exec"))'
        cmd=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-i','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519','Gilles@10.20.11.250',shlex.join(['nice','-n','19','python3','-u','-c',loader])]
        STATE.update(phase='retiring_exact_verified_caches',batch_id=gate['batch_id'],manifest_sha256=gate['manifest_sha256']);save()
        complete=None;last=time.monotonic()
        with data.open('rb') as source,(WORK/'unlink-receipt.jsonl').open('xb') as receipt,(WORK/'native-errors.log').open('xb') as errors:
            p=subprocess.Popen(cmd,stdin=source,stdout=subprocess.PIPE,stderr=errors);STATE['ssh_pid']=p.pid;save()
            for line in p.stdout:
                row=json.loads(line);receipt.write(line)
                if row['event']=='unlinked':STATE['files_unlinked']+=1;STATE['bytes_unlinked']+=row['bytes']
                elif row['event']=='complete':complete=row
                elif row['event']=='preflight_passed':STATE['native_preflight']=row
                else:raise ValueError('Unexpected native cache retirement event')
                if time.monotonic()-last>2:receipt.flush();os.fsync(receipt.fileno());save();last=time.monotonic()
            assert p.wait()==0,'Native retirement stopped; preserve and inspect partial receipts'
        assert complete and complete['files_unlinked']==1926 and complete['logical_bytes_unlinked']==28981723684
        assert STATE['files_unlinked']==1926 and STATE['bytes_unlinked']==28981723684
        STATE.update(phase='cache_manifest_retired_post_checks_pending',result=complete,receipt_sha256=hashlib.sha256((WORK/'unlink-receipt.jsonl').read_bytes()).hexdigest(),finished_at=datetime.now(timezone.utc).isoformat());save()


if __name__=='__main__':
    try:main()
    except Exception as error:
        STATE.update(phase='stopped_requires_receipt_review',error=type(error).__name__+': '+str(error));save();raise
