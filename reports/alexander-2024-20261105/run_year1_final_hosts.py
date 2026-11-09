"""Run readonly final-path tests under the actual Linux or limited Windows identity."""
import ctypes
from ctypes import wintypes
from datetime import datetime,timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

WORK=Path(__file__).resolve().parent
WINDOWS=os.name=='nt'
HOST='windows' if WINDOWS else 'linux'
STATE={'phase':'preflight','host':HOST,'success':False,'project_saved':False,'source_deleted':False}
STARTED=False

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(4*1024*1024),b''):h.update(block)
    return h.hexdigest()

def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=WORK/('host-'+HOST+'-state.tmp');p.write_text(json.dumps(STATE,indent=2)+'\n',encoding='utf-8');p.replace(WORK/('host-'+HOST+'-state.json'))

def main():
    global STARTED
    assert not (WORK/('host-'+HOST+'-state.json')).exists() and not (WORK/('host-'+HOST+'-verification.json')).exists(),'No replay'
    STARTED=True;save()
    c=json.loads((WORK/'controller-request.json').read_text(encoding='utf-8'))
    assert digest(Path(__file__))==c['runner_sha256'] and digest(WORK/'verify_year1_final_hosts.m')==c['script_sha256']
    assert digest(WORK/'host-request.json')==c['request_sha256']
    assert time.time()<c['own_lease_deadline_unix'] and c['filesystem_publication_verified'] is True
    q=json.loads((WORK/'host-request.json').read_text(encoding='utf-8'))
    assert q['read_only'] and q['project_namespace']=='final' and q['expected_projects']==len(q['projects'])==16
    expected={key:q[key] for key in ['expected_projects','expected_fovs','expected_rois','expected_raw_samples']}
    if WINDOWS:
        identity=subprocess.check_output(['whoami'],text=True).strip();session=wintypes.DWORD()
        assert ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(),ctypes.byref(session))
        assert identity.casefold()=='gmgm\\charvin-admin' and session.value==1
        STATE.update(windows_identity=identity,session_id=session.value)
        root=Path('//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub/projects')
        release=Path('C:/Users/Charvin-Admin/Documents/GitHub/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7')
        matlab='C:/Program Files/MATLAB/R2025b/bin/matlab.exe'
    else:
        os.umask(0o077);root=Path('/homes2/maliavko/DetecdivHub/projects')
        release=Path('/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7');matlab='/usr/local/bin/matlab'
    assert subprocess.check_output(['git','-C',str(release),'rev-parse','HEAD'],text=True).strip()=='d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
    assert subprocess.run(['git','-C',str(release),'diff','--quiet'],capture_output=True).returncode==0
    STATE.update(engine_sha='d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7',engine_release=release.as_posix())
    assert all((root/p['name']/(p['name']+'.mat')).is_file() for p in q['projects'])
    del q;gc.collect();STATE.update(phase='validating_final_project_reads',expected=expected);save()
    expression="maxNumCompThreads(1);addpath('"+WORK.as_posix()+"');verify_year1_final_hosts('"+WORK.as_posix()+"','"+release.as_posix()+"','"+HOST+"');"
    args=[matlab,'-singleCompThread','-batch',expression] if WINDOWS else ['nice','-n','19','ionice','-c','3',matlab,'-batch',expression]
    options={'creationflags':subprocess.CREATE_NO_WINDOW|subprocess.BELOW_NORMAL_PRIORITY_CLASS} if WINDOWS else {}
    env=dict(os.environ);env['CUDA_VISIBLE_DEVICES']=''
    with (WORK/('host-'+HOST+'-matlab.log')).open('xb') as stream:
        result=subprocess.run(args,cwd=WORK,stdout=stream,stderr=subprocess.STDOUT,timeout=14400,env=env,**options)
    STATE['matlab_exit_code']=result.returncode;assert result.returncode==0 and time.time()<c['own_lease_deadline_unix']
    report=json.loads((WORK/('host-'+HOST+'-verification.json')).read_text(encoding='utf-8-sig'))
    assert report['success'] and report['project_namespace']=='final' and not report['project_saved'] and not report['source_deleted']
    assert len(report['projects'])==expected['expected_projects'] and sum(p['fovs'] for p in report['projects'])==expected['expected_fovs']
    assert sum(p['rois'] for p in report['projects'])==expected['expected_rois'] and sum(p['raw_sample_reads'] for p in report['projects'])==expected['expected_raw_samples']
    STATE.update(phase='final_available_data_reads_and_baselines_verified',success=True,finished_at=datetime.now(timezone.utc).isoformat(),report_sha256=digest(WORK/('host-'+HOST+'-verification.json')));save()

if __name__=='__main__':
    try:main()
    except Exception as error:
        if STARTED:STATE.update(phase='failed_safe_readonly',error=type(error).__name__+': '+str(error));save()
        raise
