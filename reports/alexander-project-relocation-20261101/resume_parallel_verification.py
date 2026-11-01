"""Resume the copied project's exhaustive verification using spare NAS cores.

Retain the already completed SHA256 prefix; compare both copies of every
remaining file with SHA1, matching the raw migration's content-check policy.
"""
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
NATIVE=r'''
import json,sys,os,stat,pwd,hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
files=json.load(sys.stdin)
source=Path('/volume1/Sauvegarde/Alexander/analysis')
dest=Path('/volume1/homes/maliavko/DetecdivHub/projects/.incoming-anof_26102023_yam76s_100ms-20261101')
uid=pwd.getpwnam('maliavko').pw_uid
def digest(path):
 h=hashlib.sha1()
 with path.open('rb') as f:
  for data in iter(lambda:f.read(4*1024*1024),b''):h.update(data)
 return h.hexdigest()
def verify(row):
 rel=Path(row['path']);assert not rel.is_absolute() and '..' not in rel.parts
 assert rel.parts[0] in ('anof_26102023_yam76s_100ms','anof_26102023_yam76s_100ms.mat')
 src=source/rel;dst=dest/rel;s=src.lstat();d=dst.lstat()
 assert stat.S_ISREG(s.st_mode) and stat.S_ISREG(d.st_mode) and d.st_uid==uid
 assert s.st_size==d.st_size==row['bytes'] and s.st_mtime_ns==row['mtime_ns']
 assert (s.st_dev,s.st_ino)!=(d.st_dev,d.st_ino)
 a=digest(src);b=digest(dst);assert a==b,'Content mismatch: '+row['path']
 after=src.stat();assert (after.st_size,after.st_mtime_ns)==(s.st_size,s.st_mtime_ns)
 return {'path':row['path'],'bytes':s.st_size,'sha1':a,'source_and_destination_match':True,'owner':'maliavko'}
with ThreadPoolExecutor(max_workers=min(3,max(1,(os.cpu_count() or 2)-1))) as pool:
 for row in pool.map(verify,files):print(json.dumps(row,sort_keys=True),flush=True)
'''

def main():
    os.umask(0o077)
    state=json.loads((WORK/'copy-state.json').read_text())
    assert state['phase']=='verify' and state['cloned']['files']==11450
    assert not Path('/proc',str(state['pid'])).exists(), 'Original verifier controller is still alive'
    files=[json.loads(line) for line in (WORK/'source-project-inventory.jsonl').open() if json.loads(line)['kind']=='file']
    previous=[json.loads(line) for line in (WORK/'verify-receipt.jsonl').open()]
    for index,row in enumerate(previous):
        assert row['path']==files[index]['path'] and row['bytes']==files[index]['bytes'] and row['source_and_destination_match'] and row['sha256']
    remaining=files[len(previous):]
    request=WORK/'verify-parallel-request.json';request.write_text(json.dumps(remaining))
    state.update(phase='verify_parallel_remaining',pid=os.getpid(),sha256_prefix_files=len(previous),files=len(previous),bytes=sum(r['bytes'] for r in previous),verification_algorithms=['SHA256','SHA1'])
    def save():
        state['heartbeat_at']=datetime.now(timezone.utc).isoformat()
        temporary=WORK/'copy-state.tmp';temporary.write_text(json.dumps(state,indent=2)+'\n');temporary.replace(WORK/'copy-state.json')
    save()
    launcher='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
    command=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['nice','-n','19','python3','-u','-c',launcher])]
    count=len(previous);size=sum(r['bytes'] for r in previous);last=time.monotonic()
    try:
        with request.open('rb') as input_file,(WORK/'verify-resumed-receipt.jsonl').open('x') as receipt,(WORK/'verify-parallel.errors.log').open('xb') as errors:
            for row in previous:receipt.write(json.dumps(row,sort_keys=True)+'\n')
            p=subprocess.Popen(command,stdin=input_file,stdout=subprocess.PIPE,stderr=errors)
            state['native_ssh_pid']=p.pid;save()
            try:
                for line in p.stdout:
                    row=json.loads(line);receipt.write(line.decode());count+=1;size+=row['bytes']
                    if time.monotonic()-last>10:state.update(files=count,bytes=size);save();last=time.monotonic()
                assert p.wait()==0,'Native parallel content verification failed; inspect the error log'
            finally:
                if p.poll() is None:p.terminate();p.wait()
        assert count==11450 and size==152861817319
        state.update(phase='verified_copy_ready_for_path_rewrite',files=count,bytes=size,
            full_content_verification={'files':count,'bytes':size,'algorithms':['SHA256','SHA1'],'receipt_sha256':hashlib.sha256((WORK/'verify-resumed-receipt.jsonl').read_bytes()).hexdigest()},finished_at=datetime.now(timezone.utc).isoformat());save()
    except Exception as exc:state.update(phase='failed_safe',error=type(exc).__name__+': '+str(exc));save();raise

if __name__=='__main__':main()
