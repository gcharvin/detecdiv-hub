"""Read-only post-retirement checks in the real Linux/Windows host session."""
import ctypes
from ctypes import wintypes
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess

WORK=Path(__file__).resolve().parent
HOST='windows' if os.name=='nt' else 'linux'
STATE={'phase':'preflight','host':HOST,'success':False,'project_saved':False}


def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=WORK/('post-'+HOST+'-state.tmp');p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/('post-'+HOST+'-state.json'))


def main():
    request=json.loads((WORK/'project3-host-request.json').read_text())
    assert request['source_manifest_retired'] and len(request['projects'])==11
    assert hashlib.sha256((WORK/'verify_project3_after_retirement.m').read_bytes()).hexdigest()==request['post_script_sha256']
    assert not (WORK/('post-'+HOST+'-state.json')).exists()
    if HOST=='windows':
        identity=subprocess.check_output(['whoami'],text=True).strip();session=wintypes.DWORD()
        assert ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(),ctypes.byref(session))
        assert identity.casefold()=='gmgm\\charvin-admin' and session.value==1
        STATE.update(windows_identity=identity,session_id=session.value)
        release='C:/Users/Charvin-Admin/Documents/GitHub/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7';matlab='C:/Program Files/MATLAB/R2025b/bin/matlab.exe'
    else:
        os.umask(0o077);release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7';matlab='/usr/local/bin/matlab'
    STATE['phase']='reading_after_source_retirement';save()
    expression="maxNumCompThreads(1);addpath('"+WORK.as_posix()+"');verify_project3_after_retirement('"+WORK.as_posix()+"','"+release+"','"+HOST+"');"
    args=[matlab,'-singleCompThread','-batch',expression] if HOST=='windows' else ['nice','-n','19','ionice','-c','3',matlab,'-batch',expression]
    options={'creationflags':subprocess.CREATE_NO_WINDOW|subprocess.BELOW_NORMAL_PRIORITY_CLASS} if HOST=='windows' else {}
    with (WORK/('post-'+HOST+'-matlab.log')).open('xb') as log:p=subprocess.run(args,stdout=log,stderr=subprocess.STDOUT,timeout=3600,**options)
    assert p.returncode==0
    report=json.loads((WORK/('post-'+HOST+'-verification.json')).read_text(encoding='utf-8-sig'))
    assert report['success'] and not report['project_saved'] and len(report['projects'])==11
    assert sum(x['raw_reads_verified'] for x in report['projects'])==123
    assert sum(x['roi_image_reads_verified'] for x in report['projects'])==11
    assert report['source_retirement_receipt_sha256']==request['source_retirement_receipt_sha256']
    STATE.update(phase='post_retirement_reads_verified',success=True,finished_at=datetime.now(timezone.utc).isoformat());save()


if __name__=='__main__':
    try:main()
    except Exception as error:
        STATE.update(phase='failed_readonly_requires_review',error=type(error).__name__+': '+str(error));save();raise
