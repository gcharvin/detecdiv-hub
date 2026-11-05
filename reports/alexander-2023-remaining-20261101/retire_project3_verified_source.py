"""Execute only the fully gated, exact 2023_3 manifest retirement once."""
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

WORK=Path(__file__).resolve().parent
MANIFEST_SHA='4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3'
STATE={'phase':'preflight','source_deleted':False,'files_unlinked':0,'bytes_unlinked':0}


def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    tmp=WORK/'state.tmp';tmp.write_text(json.dumps(STATE,indent=2)+'\n');tmp.replace(WORK/'state.json')


def main():
    os.umask(0o077)
    assert not (WORK/'state.json').exists(),'Inspect any prior retirement; do not replay'
    gate=json.loads((WORK/'gates.json').read_text())
    assert gate['success'] and gate['verified_cutover'] and gate['standing_source_retirement_authorization']
    assert gate['original_projects_preserved'] and gate['linux_windows_final_paths_verified']
    assert gate['actual_api_handlers_fully_verified']==798 and gate['raw_full_sha1_and_source_stability_passed']
    assert gate['source_manifest_sha256']==MANIFEST_SHA and len(gate['leases'])==11
    assert 0<=(datetime.now(timezone.utc)-datetime.fromisoformat(gate['checked_at'])).total_seconds()<120
    assert time.time()<gate['lease_deadline_unix']
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==gate['controller_sha256']
    script=(WORK/'retire_project3_manifest_native.py').read_bytes()
    assert hashlib.sha256(script).hexdigest()==gate['native_script_sha256']
    target=WORK.parent/'project3-raw-destination-check-after-timestamp-repair-20261105'
    assert (target/'controller.exit').read_text().strip()=='0'
    target_state=json.loads((target/'state.json').read_text())
    assert target_state['phase']=='native_destination_metadata_verified' and target_state['success']
    assert hashlib.sha256((target/'receipt.json').read_bytes()).hexdigest()==gate['destination_receipt_sha256']
    assert (datetime.now(timezone.utc)-datetime.fromisoformat(target_state['finished_at'])).total_seconds()<3600
    raw=WORK.parent/'2023_3';state=json.loads((raw/'state.json').read_text())
    assert state['full_sha1_passed'] and state['source_stability_verified']
    manifest=(raw/'source-before.jsonl').read_bytes();assert hashlib.sha256(manifest).hexdigest()==MANIFEST_SHA
    assert hashlib.sha256((raw/'source-after.jsonl').read_bytes()).hexdigest()==MANIFEST_SHA
    rows=[r for line in manifest.splitlines() if (r:=json.loads(line))['kind']=='file']
    assert len(rows)==708590 and sum(r['bytes'] for r in rows)==5967100516555
    with (WORK.parent/'project3-source-retirement.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        loader='import base64;exec(compile(base64.b64decode('+repr(base64.b64encode(script).decode())+'),"retire_project3_manifest_native.py","exec"))'
        cmd=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-i','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519','Gilles@10.20.11.250',shlex.join(['nice','-n','19','python3','-u','-c',loader])]
        request={'mode':'unlink','source_root':'/volume1/DATA/Alexander/data/2023_3','verified_cutover':True,'lease_deadline_unix':gate['lease_deadline_unix']}
        data=WORK/'native-input.jsonl'
        with data.open('xb') as out:out.write((json.dumps(request)+'\n').encode());out.write(manifest)
        STATE.update(phase='retiring_exact_verified_source_manifest',batch_id=gate['batch_id'],source_manifest_sha256=MANIFEST_SHA)
        save();complete=None;last=time.monotonic()
        with data.open('rb') as source,(WORK/'unlink-receipt.jsonl').open('xb') as receipt,(WORK/'native-errors.log').open('xb') as errors:
            child=subprocess.Popen(cmd,stdin=source,stdout=subprocess.PIPE,stderr=errors)
            STATE['ssh_pid']=child.pid;save()
            for line in child.stdout:
                row=json.loads(line);receipt.write(line)
                if row['event']=='unlinked':
                    STATE['files_unlinked']+=1;STATE['bytes_unlinked']+=row['bytes'];STATE['source_deleted']=True
                elif row['event']=='complete':complete=row
                elif row['event']=='preflight_passed':STATE['native_preflight']=row
                else:raise ValueError('Unexpected native retirement event')
                if time.monotonic()-last>5:receipt.flush();os.fsync(receipt.fileno());save();last=time.monotonic()
            assert child.wait()==0,'Native retirement stopped; inspect partial receipts, never replay blindly'
        assert complete and complete['files_unlinked']==708590 and complete['logical_bytes_unlinked']==5967100516555
        assert STATE['files_unlinked']==708590 and STATE['bytes_unlinked']==5967100516555
        STATE.update(phase='source_manifest_retired_catalog_finalization_pending',result=complete,receipt_sha256=hashlib.sha256((WORK/'unlink-receipt.jsonl').read_bytes()).hexdigest(),finished_at=datetime.now(timezone.utc).isoformat())
        save()


if __name__=='__main__':
    try:main()
    except Exception as error:
        STATE.update(phase='retirement_stopped_requires_review',error=type(error).__name__+': '+str(error));save();raise
