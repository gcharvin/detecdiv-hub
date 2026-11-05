"""Durable, read-only destination metadata check before source retirement."""
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess

WORK=Path(__file__).resolve().parent
STATE={'phase':'preflight','source_deleted':False}


def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=WORK/'state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/'state.json')


def main():
    os.umask(0o077)
    assert not (WORK/'state.json').exists()
    request=json.loads((WORK/'request.json').read_text())
    script=(WORK/'verify_project3_raw_destination_native.py').read_bytes()
    assert hashlib.sha256(script).hexdigest()==request['native_script_sha256']
    manifest=(WORK.parent/'2023_3/source-before.jsonl').read_bytes()
    assert hashlib.sha256(manifest).hexdigest()=='4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3'
    state=json.loads((WORK.parent/'2023_3/state.json').read_text());assert state['full_sha1_passed'] and state['source_stability_verified']
    loader='import base64;exec(compile(base64.b64decode('+repr(base64.b64encode(script).decode())+'),"verify_project3_raw_destination_native.py","exec"))'
    audit=request.get('audit_timestamps',False)
    args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['nice','-n','19','python3','-c',loader]+(['--audit-timestamps'] if audit else []))]
    STATE['phase']='checking_native_destination';save()
    receipt=WORK/('timestamp-audit.jsonl' if audit else 'receipt.json')
    with receipt.open('xb') as out,(WORK/'errors.log').open('xb') as err:
        p=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=out,stderr=err)
        STATE['ssh_pid']=p.pid;save();p.communicate(manifest,timeout=7200)
    assert p.returncode==0,'Destination check failed; inspect native errors'
    events=[json.loads(line) for line in receipt.read_text().splitlines()];result=events[-1]
    assert (result['audit_completed'] if audit else result['success']) and result['files']==708590 and result['bytes']==5967100516555
    if audit:assert len(events)-1==result['timestamp_outliers']
    STATE.update(phase='native_destination_timestamps_audited' if audit else 'native_destination_metadata_verified',success=True,result=result,receipt_sha256=hashlib.sha256(receipt.read_bytes()).hexdigest(),finished_at=datetime.now(timezone.utc).isoformat());save()


if __name__=='__main__':
    try:main()
    except Exception as error:
        STATE.update(phase='failed_safe_readonly',error=type(error).__name__+': '+str(error));save();raise
