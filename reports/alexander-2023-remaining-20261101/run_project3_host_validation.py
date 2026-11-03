"""Read-only Linux/Windows host checks for the unpublished, relinked copies."""
import ctypes
from ctypes import wintypes
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time

WORK=Path(__file__).resolve().parent
WINDOWS=os.name=='nt'
HOST='windows' if WINDOWS else 'linux'
STATE={'phase':'preflight','host':HOST,'success':False,'project_saved':False,'source_deleted':False}


def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    tmp=WORK/('project3-host-'+HOST+'-state.tmp');tmp.write_text(json.dumps(STATE,indent=2)+'\n',encoding='utf-8')
    tmp.replace(WORK/('project3-host-'+HOST+'-state.json'))


def main():
    request=json.loads((WORK/'project3-host-request.json').read_text(encoding='utf-8'))
    assert len(request['projects'])==11
    assert hashlib.sha256((WORK/'verify_project3_hosts.m').read_bytes()).hexdigest()==request['script_sha256']
    assert not (WORK/('project3-host-'+HOST+'-verification.json')).exists(),'Do not duplicate host verification'
    if WINDOWS:
        identity=subprocess.check_output(['whoami'],text=True).strip();session=wintypes.DWORD()
        assert ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(),ctypes.byref(session))
        assert identity.casefold()=='gmgm\\charvin-admin' and session.value==1
        STATE.update(windows_identity=identity,session_id=session.value)
        root=Path('//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub')
        release='C:/Users/Charvin-Admin/Documents/GitHub/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
        matlab='C:/Program Files/MATLAB/R2025b/bin/matlab.exe'
    else:
        os.umask(0o077);root=Path('/homes2/maliavko/DetecdivHub')
        release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
        matlab='/usr/local/bin/matlab'
    assert Path(release).is_dir()
    until=time.monotonic()+3600
    while True:
        missing=[]
        for row in request['raw_sample_files']:
            path=root/'raw/.incoming-alexander-2023-20261008/2023_3'/row['path']
            try:
                s=path.lstat();present=stat.S_ISREG(s.st_mode) and s.st_size==row['bytes']
            except FileNotFoundError:present=False
            if not present:
                missing.append(row['path'])
                if len(missing)>=10:break
        if not missing:break
        STATE.update(phase='waiting_for_sample_files',missing_sample_examples=missing);save()
        assert time.monotonic()<until,'Raw sample paths are not ready; source preserved, no project was saved'
        time.sleep(60)
    STATE.pop('missing_sample_examples',None)
    STATE.update(phase='validating_project_reads',raw_sample_files_ready=len(request['raw_sample_files']));save()
    expression="maxNumCompThreads(1);addpath('"+WORK.as_posix()+"');verify_project3_hosts('"+WORK.as_posix()+"','"+release+"','"+HOST+"');"
    args=([matlab,'-singleCompThread','-batch',expression] if WINDOWS else ['nice','-n','19','ionice','-c','3',matlab,'-batch',expression])
    options={'creationflags':subprocess.CREATE_NO_WINDOW|subprocess.BELOW_NORMAL_PRIORITY_CLASS} if WINDOWS else {}
    with (WORK/('project3-host-'+HOST+'-matlab.log')).open('xb') as log:
        p=subprocess.run(args,cwd=WORK,stdout=log,stderr=subprocess.STDOUT,timeout=7200,**options)
    STATE['matlab_exit_code']=p.returncode;assert p.returncode==0,'Read-only MATLAB host verification failed'
    report=json.loads((WORK/('project3-host-'+HOST+'-verification.json')).read_text(encoding='utf-8-sig'))
    assert report['success'] and not report['project_saved'] and len(report['projects'])==11
    assert sum(p['fovs'] for p in report['projects'])==123 and sum(p['rois'] for p in report['projects'])==53623
    assert sum(p['raw_sample_reads_verified'] for p in report['projects'])==1728
    STATE.update(phase='host_reads_verified_in_staging',success=True,finished_at=datetime.now(timezone.utc).isoformat());save()


if __name__=='__main__':
    try:main()
    except Exception as error:
        STATE.update(phase='failed_safe_readonly',error=type(error).__name__+': '+str(error));save();raise
