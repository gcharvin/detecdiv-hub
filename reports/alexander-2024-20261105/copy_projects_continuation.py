"""Bounded continuation of only the final three standard 2024 project copies.

Only unpublished copies change. No MAT save, raw/source removal or DB write.
Partial stages are never overwritten; any failure needs inspected recovery.
"""
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

WORK=Path(__file__).resolve().parent
RECEIPTS=WORK.parent/'project-copies'
EXPECTED=('movie_12224_w303MBsnabI_test','movie_250124_diploids','qp_22032024_DHY210_nts1ng')
MOUNT=Path('/homes2/maliavko/DetecdivHub/projects')
NATIVE=r'''
import os,sys,json,stat,re,hashlib,fcntl,shutil,subprocess
from pathlib import Path
import pwd as posix_accounts
v=json.load(sys.stdin);stem=v['name'];mode=v['mode']
assert re.fullmatch(r'[A-Za-z0-9_]+',stem)
assert mode in {'inventory','clone','verify'}
source=Path('/volume1/Sauvegarde/Alexander/analysis')
dest=Path('/volume1/homes/maliavko/DetecdivHub/projects')/('.incoming-'+stem+'-20261105')
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','.rsync-partial'}
if shutil.which('ionice'):subprocess.run(['ionice','-c','3','-p',str(os.getpid())],capture_output=True)
assert source.resolve()==source and source.is_dir()
def emit(row):print(json.dumps(row,sort_keys=True),flush=True)
def regular(p,root):
 assert root.resolve()==root and p.parent.resolve()==p.parent and root in p.parents
 s=p.lstat();assert stat.S_ISREG(s.st_mode)
 return s
def row(p):
 s=regular(p,source)
 return {'kind':'file','path':str(p.relative_to(source)),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,
  'inode':s.st_ino,'uid':s.st_uid,'gid':s.st_gid,'mode':s.st_mode,'nlink':s.st_nlink}
def fail(e):raise e
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for block in iter(lambda:f.read(4*1024*1024),b''):h.update(block)
 return h.hexdigest()
if mode=='inventory':
 mat=source/(stem+'.mat');directory=source/stem
 assert mat.is_file() and directory.is_dir() and directory.resolve()==directory
 assert not (source/(stem+'.json')).exists(),'JSON sibling needs layout review'
 emit(row(mat))
 for parent,dirs,files in os.walk(directory,followlinks=False,onerror=fail):
  dirs[:]=sorted(d for d in dirs if d not in ignored)
  for d in dirs:assert not (Path(parent)/d).is_symlink()
  emit({'kind':'directory','path':str(Path(parent).relative_to(source))})
  for name in sorted(files):
   if name not in ignored:emit(row(Path(parent)/name))
else:
 assert dest.is_dir() and dest.resolve()==dest
 uid=posix_accounts.getpwnam('maliavko').pw_uid
 expected={r['path'] for r in v['files']};seen=set()
 for parent,dirs,files in os.walk(dest,followlinks=False,onerror=fail):
  dirs[:]=sorted(d for d in dirs if d not in ignored)
  for d in dirs:assert not (Path(parent)/d).is_symlink()
  for name in files:
   if name not in ignored:seen.add(str((Path(parent)/name).relative_to(dest)))
 assert seen==expected,'Unexpected stage payload'
 for r in v['files']:
  rel=Path(r['path']);assert not rel.is_absolute() and '..' not in rel.parts and rel.parts[0] in {stem,stem+'.mat'}
  src=source/rel;dst=dest/rel;s=regular(src,source);d=regular(dst,dest)
  assert row(src)==r,'Source metadata changed'
  assert d.st_uid==uid and d.st_nlink==1 and (d.st_dev,d.st_ino)!=(s.st_dev,s.st_ino)
  if mode=='clone':
   assert d.st_size==0,'Partial clone must not be overwritten'
   with src.open('rb') as a,dst.open('r+b') as b:fcntl.ioctl(b.fileno(),0x40049409,a.fileno())
   assert dst.stat().st_size==s.st_size
   emit({'path':r['path'],'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'independent_owned_clone':True})
  else:
   assert d.st_size==s.st_size
   a=digest(src);b=digest(dst);assert a==b,'Source/copy hash mismatch'
   assert row(src)==r,'Source changed during hash'
   emit({'path':r['path'],'bytes':s.st_size,'sha256':a,'source_destination_hash_equal':True,'owner':'maliavko'})
'''

state={'phase':'preflight','projects':[],'source_deleted':False,'project_paths_changed':False,'catalog_changed':False}


def save():
    state['heartbeat_at']=datetime.now(timezone.utc).isoformat();state['runner_pid']=os.getpid()
    p=WORK/'state.tmp'
    with p.open('w') as f:json.dump(state,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    p.replace(WORK/'state.json')


def native(project,directory,mode,files=None,name=None):
    output=directory/(name or mode+'-receipt.jsonl')
    assert not output.exists()
    value={'name':project['name'],'mode':mode}
    if files is not None:value['files']=files
    request=directory/(output.stem+'-request.json');assert not request.exists();request.write_text(json.dumps(value)+'\n')
    loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
    args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3',
          '-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',
          shlex.join(['nice','-n','19','python3','-u','-c',loader])]
    rows=[];count=size=0;h=hashlib.sha256();last=time.monotonic()
    with request.open('rb') as inp,output.open('xb') as receipt,output.with_suffix('.errors.log').open('xb') as errors:
        process=subprocess.Popen(args,stdin=inp,stdout=subprocess.PIPE,stderr=errors)
        state['child_pid']=process.pid;save()
        try:
            for line in process.stdout:
                r=json.loads(line);receipt.write(line);h.update(line)
                if mode=='inventory':rows.append(r)
                if r.get('kind')=='file' or mode!='inventory':count+=1;size+=r['bytes']
                if mode=='clone':os.utime(MOUNT/('.incoming-'+project['name']+'-20261105')/r['path'],ns=(r['mtime_ns'],r['mtime_ns']))
                if time.monotonic()-last>15:
                    state.update(phase=mode,current_project=project['name'],phase_files=count,phase_bytes=size);receipt.flush();save();last=time.monotonic()
            assert process.wait()==0,'Native '+mode+' failed; inspect durable error receipt'
            receipt.flush();os.fsync(receipt.fileno())
        finally:
            if process.poll() is None:process.terminate();process.wait()
    if files is not None:assert count==len(files)
    return {'files':count,'bytes':size,'sha256':h.hexdigest()},rows


def main():
    os.umask(0o077)
    request=json.loads((WORK/'request.json').read_text())
    assert request['runner_sha256']==hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    assert not request['source_deletion_allowed'] and not request['catalog_cutover_allowed']
    assert tuple(p['name'] for p in request['projects'])==EXPECTED
    assert (RECEIPTS/'controller.exit').read_text().strip()=='1'
    previous=json.loads((RECEIPTS/'state.json').read_text());assert previous['phase']=='failed_safe' and previous['current_project']=='anof_290424_loxP15' and len(previous['projects'])==28
    assert (RECEIPTS/'anof_290424_loxP15/source-before.jsonl').stat().st_size==0
    original_lock=(RECEIPTS/'copy.lock').open('a');fcntl.flock(original_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    lock=(WORK/'copy.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    assert not (WORK/'state.json').exists(),'One-shot work already started'
    mount=json.loads(subprocess.check_output(['findmnt','-J','-T',str(MOUNT),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
    assert mount['target']=='/homes2/maliavko' and mount['source']=='//10.20.8.250/home'
    assert 'username=maliavko' in mount['options'].split(',') and 'rw' in mount['options'].split(',')
    for p in request['projects']:
        assert re.fullmatch(r'[A-Za-z0-9_]+',p['name']) and p['owner']=='alexander'
        assert p['source_mat']=='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis/'+p['name']+'.mat'
        assert not (MOUNT/p['name']).exists() and not (MOUNT/('.incoming-'+p['name']+'-20261105')).exists()
    save()
    for p in request['projects']:
        stem=p['name'];directory=RECEIPTS/stem;directory.mkdir(mode=0o700)
        state.update(phase='inventory',current_project=stem);save()
        before,rows=native(p,directory,'inventory',name='source-before.jsonl')
        files=[r for r in rows if r['kind']=='file'];dirs=[r for r in rows if r['kind']=='directory']
        assert files and any(r['path']==stem+'.mat' for r in files)
        free=os.statvfs(MOUNT)
        assert free.f_bavail*free.f_frsize>before['bytes']+13_034_331_124_398,'Reserve both raw years plus 2 TB'
        stage=MOUNT/('.incoming-'+stem+'-20261105');stage.mkdir(mode=0o700)
        state.update(phase='create_owned_placeholders',expected_files=len(files),expected_bytes=before['bytes']);save()
        for r in dirs:(stage/r['path']).mkdir(mode=0o700,parents=True,exist_ok=False)
        for i,r in enumerate(files):
            with (stage/r['path']).open('xb'):pass
            if i%1000==0:state['placeholders']=i+1;save()
        clone,_=native(p,directory,'clone',files)
        verify,_=native(p,directory,'verify',files)
        after,_=native(p,directory,'inventory',name='source-after.jsonl')
        assert before==after,'Original project changed'
        result={'project_id':p['project_id'],'name':stem,'phase':'copy_verified_identity_relink_and_hosts_pending',
                'source_mat':p['source_mat'],'stage':str(stage),'files':before['files'],'bytes':before['bytes'],
                'source_inventory':before,'clone_receipt':clone,'verification_receipt':verify,'original_preserved':True}
        (directory/'verified-copy.json').write_text(json.dumps(result,indent=2)+'\n')
        state['projects'].append(result);save()
    state.update(phase='final_three_standard_copies_verified_missing_project29_isolated',finished_at=datetime.now(timezone.utc).isoformat());save()


if __name__=='__main__':
    try:main()
    except Exception as error:
        state.update(phase='failed_safe',error=type(error).__name__+': '+str(error));save();raise
