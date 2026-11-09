"""New bounded readonly name search for Lox archive candidates, not pixel identity."""
import base64
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

WORK=Path(__file__).resolve().parent
STATE={'phase':'preflight','read_only':True,'source_deleted':False,'project_saved':False,'cleanup_gate_passed':False,'roots':[]}

NATIVE=r'''
import json,os,re,stat,sys,time
from pathlib import Path
v=json.load(sys.stdin);os.nice(19);root=Path(v['root'])
assert str(root) in ['/volume1/DATA/Alexander','/volume1/Sauvegarde/Alexander'] and root.resolve()==root and root.is_dir()
deadline=time.monotonic()+7200;stack=[root];directories=entries=errors=candidates=0
pattern=re.compile(r'(290424|29042024|20240429|lox.?p.?15)',re.I)
while stack:
 assert time.monotonic()<deadline,'Bounded readonly search deadline reached'
 folder=stack.pop();directories+=1
 try:
  with os.scandir(folder) as scan:
   for item in scan:
    entries+=1
    if item.name.startswith(('.','@','#')) or item.is_symlink():continue
    p=Path(item.path);directory=item.is_dir(follow_symlinks=False)
    if directory:stack.append(p)
    main=item.name.lower().endswith('.mat') and item.name.lower().startswith(('anof_290424','290424_lox'))
    if (directory and pattern.search(item.name)) or main:
     s=item.stat(follow_symlinks=False);candidates+=1
     print(json.dumps({'kind':'name_candidate','path':str(p),'directory':directory,'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino,'uid':s.st_uid,'mode':s.st_mode,'scientific_identity_verified':False}),flush=True)
 except OSError as error:
  errors+=1;print(json.dumps({'kind':'scan_error','path':str(folder),'error_type':type(error).__name__,'errno':error.errno}),flush=True)
 if directories%5000==0:print(json.dumps({'kind':'progress','directories':directories,'entries':entries,'candidates':candidates,'errors':errors}),flush=True)
print(json.dumps({'kind':'complete','root':str(root),'directories':directories,'entries':entries,'candidates':candidates,'errors':errors,'hidden_metadata_and_symlinks_skipped':True,'scientific_identity_verified':False,'cleanup_gate_passed':False}),flush=True)
'''

def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=WORK/'state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/'state.json')

def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    os.umask(0o077);assert not (WORK/'state.json').exists(),'No replay'
    request=json.loads((WORK/'request.json').read_text());assert request['read_only'] and not request['source_deletion_allowed'] and not request['matlab_loading_allowed']
    assert digest(Path(__file__))==request['runner_sha256'];save()
    roles=[('primary','Gilles@10.20.11.250','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519','/volume1/DATA/Alexander'),
           ('secondary','charvin-admin@10.20.8.250','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','/volume1/Sauvegarde/Alexander')]
    for label,host,key,root in roles:
        loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
        args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-i',key,host,shlex.join(['python3','-u','-c',loader])]
        path=WORK/(label+'-receipt.jsonl');last=None;STATE.update(phase='readonly_archive_name_search',current_root=label);save()
        with path.open('xb') as output,(WORK/(label+'-errors.log')).open('xb') as errors:
            child=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errors);child.stdin.write(json.dumps({'root':root}).encode());child.stdin.close();STATE['child_pid']=child.pid;save()
            for line in child.stdout:
                output.write(line);output.flush();last=json.loads(line)
                if last['kind'] in ['progress','complete']:STATE['root_progress']=last;save()
            code=child.wait();os.fsync(output.fileno());assert code==0 and last['kind']=='complete','Readonly search failed or incomplete; preserve partials'
        STATE['roots'].append({**last,'receipt_sha256':digest(path)});save()
    STATE.update(phase='readonly_archive_name_search_completed_not_identity_or_cleanup_approval',success=True,finished_at=datetime.now(timezone.utc).isoformat());save()
    (WORK/'result.json').write_text(json.dumps(STATE,indent=2)+'\n')

if __name__=='__main__':
    try:main()
    except Exception as error:
        STATE.update(phase='failed_safe_readonly_search_partial',success=False,error_type=type(error).__name__);save();raise
