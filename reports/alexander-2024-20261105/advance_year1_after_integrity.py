"""One bounded local controller: integrity -> publication -> two hosts -> project preference.

No raw/source retirement is implemented here. All originals stay untouched.
"""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import traceback

HERE=Path(__file__).resolve().parent
PRIVATE=HERE/'runtime'
import year1_publication_operations as ops

STATE={'phase':'preflight','source_deleted':False,'catalog_cutover_complete':False,'own_leases_finalized':False}

def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat();p=PRIVATE/'year1-advance-state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(PRIVATE/'year1-advance-state.json')

def pins(plan):
    for name,expected in plan['source_pins'].items():assert hashlib.sha256((HERE/name).read_bytes()).hexdigest()==expected,'Controller source changed; inspect current phase'

def integrity():
    return ops.remote('linux',r'''
import json
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year1-prepublication-audit-20261109')
s=json.loads((w/'state.json').read_text());p=w/'controller.exit'
print(json.dumps({'exit':p.read_text().strip() if p.exists() else None,'phase':s['phase'],'verified_projects':len(s['projects'])}))
''',timeout=30)

def owned_tasks_terminal():
    r=ops.remote('linux',r'''
import json
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
r={}
for name in ['year1-final-publication-20261109','year1-final-hosts-20261109']:
 w=root/name;r[name]=(not w.exists()) or (w/'controller.exit').exists()
print(json.dumps({'terminal':all(r.values())}))
''',timeout=30)
    if not r['terminal']:return False
    if (HERE/'year1-windows-cleanup.json').exists():return True
    if (HERE/'year1-windows-stage.json').exists():
        r=ops.remote('windows',r'''
import json,subprocess,base64
script="$t=Get-ScheduledTask -TaskName 'DetecDiv Alexander 2024 Year1 Final Hosts 20261109' -ErrorAction SilentlyContinue;@{running=($null -ne $t -and $t.State -eq 'Running')}|ConvertTo-Json -Compress"
p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(script.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=30);assert p.returncode==0
print(p.stdout.strip())
''',timeout=45)
        return not r['running']
    return True

def main():
    plan=json.loads((PRIVATE/'year1-advance-plan.json').read_text());assert plan['source_deletion_allowed'] is False
    lock=os.open(PRIVATE/'year1-advance-started.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(lock,'w') as stream:json.dump({'pid':os.getpid(),'started_at':datetime.now(timezone.utc).isoformat()},stream)
    STATE['pid']=os.getpid();save()
    try:
        STATE['phase']='waiting_existing_integrity_audit';save()
        while True:
            pins(plan);r=integrity();STATE['integrity']=r;save()
            if r['exit'] is not None:assert r['exit']=='0','Existing integrity audit failed';break
            assert time.time()<plan['wait_deadline_unix'],'Integrity wait deadline reached; no replay'
            time.sleep(60)
        pins(plan);STATE['phase']='collecting_terminal_integrity';save();ops.collect()
        pins(plan);STATE['phase']='acquiring_fresh_own_publication_leases';save();a=ops.admit();STATE['batch_id']=a['batch_id'];save()
        pins(plan);STATE['phase']='publishing_final_folders';save();ops.publish()
        while True:
            pins(plan);r=ops.publication_status()
            if r.get('controller.exit') is not None:assert r['controller.exit']=='0' and r['result.json']['success'];break
            assert time.time()<min(datetime.fromisoformat(x['expires_at']).timestamp() for x in a['leases']),'Own publication leases expired'
            time.sleep(60)
        pins(plan);STATE['phase']='starting_final_linux_windows_readers';save();ops.start_hosts()
        STATE['phase']='waiting_final_linux_windows_readers';save();host_start_monotonic=time.monotonic()
        while True:
            pins(plan);r=ops.hosts_status();ls=r['linux'];ws=r['windows']
            if ls.get('controller.exit') not in [None,'']:assert ls['controller.exit']=='0','Linux host validator failed'
            if ws.get('host-windows-state.json',{}).get('phase')=='failed_safe_readonly':raise AssertionError('Windows host validator failed')
            if ws['task']['state']!='Running' and not ws.get('host-windows-state.json',{}).get('success') and time.monotonic()-host_start_monotonic>180:raise AssertionError('Windows diagnostic ended before success')
            if ls.get('host-linux-state.json',{}).get('success') and ws.get('host-windows-state.json',{}).get('success') and ls.get('controller.exit')=='0' and ws['task']['state']!='Running':break
            assert time.time()<min(datetime.fromisoformat(x['expires_at']).timestamp() for x in a['leases']),'Own final host leases expired'
            time.sleep(60)
        pins(plan);ops.verify_hosts();STATE['phase']='transactional_project_catalog_cutover';save();ops.cutover();STATE['catalog_cutover_complete']=True;save()
        STATE['phase']='finalizing_own_leases';save();assert owned_tasks_terminal();ops.finalize('project_catalog_cutover_complete');STATE['own_leases_finalized']=True;save()
        try:
            from year1_windows_host_operations import operate
            operate('cleanup');STATE['windows_diagnostic_cleanup_complete']=True
        except Exception as error:STATE['windows_diagnostic_cleanup_error_type']=type(error).__name__
        STATE.update(phase='sixteen_projects_preferred_on_homes2_raw_retirement_gates_pending',success=True,finished_at=datetime.now(timezone.utc).isoformat());save()
    except Exception as error:
        STATE.update(phase='stopped_safe_requires_review',success=False,error_type=type(error).__name__,error_frames=[{'file':f.filename,'line':f.lineno,'function':f.name} for f in traceback.extract_tb(error.__traceback__)]);save()
        if (PRIVATE/'year1-final-admission.json').exists() and not (HERE/'year1-publication-lease-finalization.json').exists():
            try:
                if owned_tasks_terminal():ops.finalize('stopped_safe_requires_review');STATE['own_leases_finalized']=True;save()
            except Exception as final_error:STATE['lease_finalization_error_type']=type(final_error).__name__;save()
        raise SystemExit(1)

if __name__=='__main__':main()
