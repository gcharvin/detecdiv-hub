"""Copy the explicit project as independent Btrfs clones, then hash every file.

Empty destination files are created through the maliavko SMB mount so ownership
and quota attribution are correct. Native reflinks preserve independent inodes.
No source, catalog entry or MAT payload is modified by this phase.
"""
from datetime import datetime,timezone
import base64
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

WORK=Path(__file__).resolve().parent
STEM='anof_26102023_yam76s_100ms'
STAGE=Path('/homes2/maliavko/DetecdivHub/projects/.incoming-'+STEM+'-20261101')
NATIVE_STAGE='/volume1/homes/maliavko/DetecdivHub/projects/.incoming-'+STEM+'-20261101'
SOURCE='/volume1/Sauvegarde/Alexander/analysis'
state={'started_at':datetime.now(timezone.utc).isoformat(),'phase':'preflight','source_preserved':True,
       'source_deleted':False,'database_changed':False,'pid':os.getpid()}
def save():
    state['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    temporary=WORK/'copy-state.tmp';temporary.write_text(json.dumps(state,indent=2)+'\n');temporary.replace(WORK/'copy-state.json')

NATIVE=r'''
import json,sys,os,stat,pwd,fcntl,hashlib,time
from pathlib import Path
payload=json.load(sys.stdin);mode=payload['mode']
source=Path('/volume1/Sauvegarde/Alexander/analysis')
dest=Path(NATIVE_STAGE)
expected_uid=pwd.getpwnam('maliavko').pw_uid
def digest(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for data in iter(lambda:f.read(4*1024*1024),b''):h.update(data)
 return h.hexdigest()
for row in payload['files']:
 rel=Path(row['path']);assert not rel.is_absolute() and '..' not in rel.parts
 assert rel.parts[0] in ['anof_26102023_yam76s_100ms','anof_26102023_yam76s_100ms.mat']
 src=source/rel;dst=dest/rel
 before=src.lstat();d=dst.lstat()
 assert stat.S_ISREG(before.st_mode) and before.st_size==row['bytes'] and before.st_mtime_ns==row['mtime_ns']
 assert stat.S_ISREG(d.st_mode) and d.st_uid==expected_uid and d.st_nlink==1
 assert (before.st_dev,before.st_ino)!=(d.st_dev,d.st_ino)
 if mode=='clone':
  assert d.st_size==0
  with src.open('rb') as s,dst.open('r+b') as target:fcntl.ioctl(target.fileno(),0x40049409,s.fileno())
  assert dst.stat().st_size==before.st_size
  result={'path':row['path'],'bytes':before.st_size,'mtime_ns':before.st_mtime_ns,'source_inode':before.st_ino,'destination_inode':d.st_ino,'owner':'maliavko','independent_reflink':True}
 else:
  assert mode=='verify' and d.st_size==before.st_size
  a=digest(src);b=digest(dst);assert a==b,'Content mismatch: '+row['path']
  after=src.stat();assert (after.st_size,after.st_mtime_ns)==(before.st_size,before.st_mtime_ns)
  result={'path':row['path'],'bytes':before.st_size,'sha256':a,'source_and_destination_match':True,'owner':'maliavko'}
 print(json.dumps(result,sort_keys=True),flush=True)
'''.replace('NATIVE_STAGE',repr(NATIVE_STAGE))

def native(mode,files):
    launcher='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
    command=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3',
             '-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519',
             'charvin-admin@10.20.8.250',shlex.join(['nice','-n','19','python3','-u','-c',launcher])]
    state.update(phase=mode,files=0,bytes=0);save()
    payload=WORK/(mode+'-request.json');payload.write_text(json.dumps({'mode':mode,'files':files}))
    count=size=0;last=time.monotonic()
    with payload.open('rb') as input_file,(WORK/(mode+'-receipt.jsonl')).open('x') as receipt,(WORK/(mode+'.errors.log')).open('xb') as errors:
        p=subprocess.Popen(command,stdin=input_file,stdout=subprocess.PIPE,stderr=errors)
        state['native_ssh_pid']=p.pid;save()
        try:
            for line in p.stdout:
                row=json.loads(line);receipt.write(line.decode());count+=1;size+=row['bytes']
                if mode=='clone':
                    path=STAGE/row['path'];os.utime(path,ns=(row['mtime_ns'],row['mtime_ns']))
                if time.monotonic()-last>10:
                    state.update(files=count,bytes=size);save();last=time.monotonic()
            assert p.wait()==0,mode+' failed; inspect the error log'
        finally:
            if p.poll() is None:p.terminate();p.wait()
    assert count==len(files)
    state.update(files=count,bytes=size);save()
    return {'files':count,'bytes':size,'receipt_sha256':hashlib.sha256((WORK/(mode+'-receipt.jsonl')).read_bytes()).hexdigest()}

def main():
    os.umask(0o077)
    rows=[json.loads(line) for line in (WORK/'source-project-inventory.jsonl').open()]
    files=[r for r in rows if r['kind']=='file'];dirs=[r for r in rows if r['kind']=='directory']
    assert len(files)==11450 and sum(r['bytes'] for r in files)==152861817319
    mount=json.loads(subprocess.check_output(['findmnt','-J','-T',str(STAGE),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
    assert mount['target']=='/homes2/maliavko' and mount['source']=='//10.20.8.250/home' and 'username=maliavko' in mount['options'].split(',')
    assert STAGE.is_dir() and not STAGE.is_symlink() and not list(STAGE.iterdir())
    free=os.statvfs(STAGE);assert free.f_bavail*free.f_frsize>sum(r['bytes'] for r in files)+1_000_000_000_000
    state.update(phase='create_owned_placeholders',expected_files=len(files),expected_bytes=sum(r['bytes'] for r in files));save()
    for row in dirs:(STAGE/row['path']).mkdir(mode=0o700,parents=True,exist_ok=False)
    for index,row in enumerate(files):
        path=STAGE/row['path'];assert path.parent.is_dir()
        with path.open('xb'):pass
        if index%500==0:state['placeholders']=index+1;save()
    state['cloned']=native('clone',files)
    state['full_content_verification']=native('verify',files)
    state.update(phase='verified_copy_ready_for_path_rewrite',finished_at=datetime.now(timezone.utc).isoformat());save()

if __name__=='__main__':
    try:main()
    except Exception as exc:state.update(phase='failed_safe',error=type(exc).__name__+': '+str(exc));save();raise
