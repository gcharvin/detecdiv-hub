"""Bounded owned copies for the two explicitly reviewed nonstandard layouts.

No scientific file save, source removal, publication or catalog mutation.
"""
import base64
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

HERE=Path(__file__).resolve().parent
SCOPES={
 'qp_250724_dbud4stest':{'owner':'alexander','id':'eb49f629-6546-4fbe-bdb7-71884e7fa69f','source':'/mnt/detecdiv-secondary-sauvegarde/Alexander/nobackup/qp','native':'/volume1/Sauvegarde/Alexander/nobackup/qp','host':'secondary'},
 '281124_Alex':{'owner':'gilles','id':'ba44b140-ca07-4bdc-8e17-e41428dcdf51','source':'/data/Gilles/analysis/projects','native':'/volume1/DATA/Gilles/analysis/projects','host':'primary'}}
NATIVE=r'''
import json,os,stat,hashlib
from pathlib import Path
import pwd as posix_accounts
v=json.load(__import__('sys').stdin);name=v['name'];role=v['role']
scopes={'qp_250724_dbud4stest':'/volume1/Sauvegarde/Alexander/nobackup/qp','281124_Alex':'/volume1/DATA/Gilles/analysis/projects'}
assert name in scopes and role in {'source','destination'}
root=Path(scopes[name]) if role=='source' else Path('/volume1/homes/maliavko/DetecdivHub/projects')/('.incoming-'+name+'-20261106')
assert root.is_dir() and root.resolve()==root
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','.rsync-partial'}
uid=posix_accounts.getpwnam('maliavko').pw_uid if role=='destination' else None
def emit(r):print(json.dumps(r,sort_keys=True),flush=True)
def file(p):
 assert p.parent.resolve()==p.parent and root in p.parents
 s=p.lstat();assert stat.S_ISREG(s.st_mode)
 if role=='destination':assert s.st_uid==uid and s.st_nlink==1
 h=hashlib.sha256()
 with p.open('rb') as f:
  for block in iter(lambda:f.read(4*1024*1024),b''):h.update(block)
 a=p.lstat();assert (s.st_ino,s.st_size,s.st_mtime_ns)==(a.st_ino,a.st_size,a.st_mtime_ns)
 emit({'kind':'file','path':str(p.relative_to(root)),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino,'uid':s.st_uid,'gid':s.st_gid,'mode':s.st_mode,'nlink':s.st_nlink,'sha256':h.hexdigest()})
assert not (root/(name+'.json')).exists(),'JSON sibling appeared; review layout'
file(root/(name+'.mat'));directory=root/name
assert directory.is_dir() and directory.resolve()==directory
def fail(e):raise e
for parent,dirs,files in os.walk(directory,followlinks=False,onerror=fail):
 dirs[:]=sorted(d for d in dirs if d not in ignored)
 for d in dirs:assert not (Path(parent)/d).is_symlink()
 emit({'kind':'directory','path':str(Path(parent).relative_to(root))})
 for f in sorted(files):
  if f not in ignored:file(Path(parent)/f)
if role=='destination':
 assert {p.name for p in root.iterdir() if p.name not in ignored}=={name,name+'.mat'}
'''


def atomic(path,value):
    p=path.with_suffix('.tmp');p.write_text(json.dumps(value,indent=2)+'\n');p.replace(path)


def native(name,role,output,progress):
    item=SCOPES[name];host=item['host'] if role=='source' else 'secondary'
    account,address,key=('Gilles','10.20.11.250','detecdiv_main_nas_ed25519') if host=='primary' else ('charvin-admin','10.20.8.250','detecdiv_backup_ed25519')
    loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
    args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3',
          '-i','/home/charvin-admin/.ssh/'+key,account+'@'+address,shlex.join(['nice','-n','19','python3','-u','-c',loader])]
    assert not output.exists();rows=[];digest=hashlib.sha256()
    request=output.with_suffix('.request.json');assert not request.exists();request.write_text(json.dumps({'name':name,'role':role})+'\n')
    with request.open('rb') as inp,output.open('xb') as receipt,output.with_suffix('.errors.log').open('xb') as errors:
        p=subprocess.Popen(args,stdin=inp,stdout=subprocess.PIPE,stderr=errors)
        progress(p.pid)
        try:
            for line in p.stdout:
                r=json.loads(line);rows.append(r);receipt.write(line);digest.update(line);progress(p.pid)
            assert p.wait()==0,'Native receipt failed; preserve state and inspect errors'
            receipt.flush();os.fsync(receipt.fileno())
        finally:
            if p.poll() is None:p.terminate();p.wait()
    return rows,digest.hexdigest()


def run():
    import fcntl,time
    os.umask(0o077)
    request=json.loads((HERE/'request.json').read_text())
    assert request['runner_sha256']==hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    assert request['source_deletion_allowed'] is False and request['catalog_cutover_allowed'] is False
    lock=(HERE/'copy.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    assert not (HERE/'state.json').exists(),'Do not replay partial work'
    state={'phase':'preflight','projects':[],'source_deleted':False,'catalog_changed':False,'paths_changed':False}
    def save():
        state['heartbeat_at']=datetime.now(timezone.utc).isoformat();state['runner_pid']=os.getpid();atomic(HERE/'state.json',state)
    def progress(pid):state['child_pid']=pid;save()
    try:
        save();mount=Path('/homes2/maliavko/DetecdivHub/projects')
        m=json.loads(subprocess.check_output(['findmnt','-J','-T',str(mount),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
        assert m['target']=='/homes2/maliavko' and m['source']=='//10.20.8.250/home' and 'username=maliavko' in m['options'].split(',')
        free=os.statvfs(mount)
        assert free.f_bavail*free.f_frsize>sum(p['bytes'] for p in request['layouts'].values())+2_000_000_000_000
        for name,item in SCOPES.items():
            stage=mount/('.incoming-'+name+'-20261106');assert not stage.exists() and not (mount/name).exists()
            source=Path(item['source']);assert source.resolve()==source
            w=HERE/name;w.mkdir(mode=0o700);state.update(phase='hash_source_before',current_project=name);save()
            before,prior=native(name,'source',w/'source-before.jsonl',progress)
            files=[r for r in before if r['kind']=='file'];dirs=[r for r in before if r['kind']=='directory']
            measured=request['layouts'][name]
            assert len(files)==measured['files'] and sum(r['bytes'] for r in files)==measured['bytes']
            assert next(r['sha256'] for r in files if r['path']==name+'.mat')==measured['main_sha256']
            stage.mkdir(mode=0o700)
            for r in dirs:(stage/r['path']).mkdir(parents=True,mode=0o700)
            manifest=w/'copy-files.nul';manifest.write_bytes(b''.join(r['path'].encode()+b'\0' for r in files))
            args=['nice','-n','19','ionice','-c','3','rsync','--recursive','--relative','--times','--omit-dir-times','--no-owner','--no-group','--no-perms',
                  '--partial-dir=.rsync-partial','--bwlimit=32768','--from0','--files-from='+str(manifest),'--',str(source)+'/',str(stage)+'/']
            state['phase']='copying';save()
            with (w/'copy.log').open('xb') as log:
                p=subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT)
                while p.poll() is None:progress(p.pid);time.sleep(10)
                assert p.returncode==0,'Copy failed; never overwrite partial copy'
            state['phase']='hash_owned_destination';save()
            target,target_digest=native(name,'destination',w/'destination.jsonl',progress)
            desired={(r['kind'],r['path']):r for r in before};seen={(r['kind'],r['path']):r for r in target};assert desired.keys()==seen.keys()
            for key,r in desired.items():
                if r['kind']=='file':assert (r['bytes'],r['sha256'])==(seen[key]['bytes'],seen[key]['sha256'])
            state['phase']='hash_source_after';save();after,post=native(name,'source',w/'source-after.jsonl',progress)
            assert before==after and prior==post,'Original changed; keep all data for review'
            result={'project_id':item['id'],'name':name,'catalog_owner':item['owner'],'stage':str(stage),'files':len(files),'bytes':sum(r['bytes'] for r in files),
                    'source_inventory_sha256':prior,'destination_receipt_sha256':target_digest,'hashes_equal':True,'original_unchanged':True,'phase':'copy_verified_other_gates_pending'}
            atomic(w/'verified-copy.json',result);state['projects'].append(result);save()
        state.update(phase='both_exception_copies_verified_remaining_gates_pending',finished_at=datetime.now(timezone.utc).isoformat());save()
    except Exception as e:state.update(phase='failed_safe',error=type(e).__name__+': '+str(e));save();raise


def launch():
    sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
    from activate_windows_mapping import api,remote
    layouts=json.loads((HERE/'exception-project-layouts.json').read_text())['projects']
    assert len(layouts)==2 and all(p['mat_exists'] and p['directory_exists'] and not p['json_sibling_exists'] and not p['stage_exists'] and not p['final_exists'] for p in layouts)
    ids=[p['project_id'] for p in layouts]
    code=r'''
import json,uuid
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Job,ProjectLock,IndexingJob
ids={uuid.UUID(i) for i in IDS}
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'));now=datetime.now(timezone.utc)
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  assert j.project_id not in ids and not any(str(i) in json.dumps(j.params_json or {}) for i in ids),'Affected job active'
 assert not [l for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(ids),ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 print(json.dumps({'checked_at':now.isoformat(),'affected_jobs':0,'affected_locks':0,'read_only':True}))
'''
    admission=api('IDS='+repr(ids)+'\n'+code);script=Path(__file__).read_bytes()
    request={'layouts':{p['name']:p for p in layouts},'admission':admission,'runner_sha256':hashlib.sha256(script).hexdigest(),'source_deletion_allowed':False,'catalog_cutover_allowed':False}
    code=r'''
import base64,json,hashlib,os,shlex,subprocess
from pathlib import Path
os.umask(0o077)
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/exception-copies-20261106');assert not w.exists()
task='alex2024-exception-copies-20261106'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==REQUEST['runner_sha256']
w.mkdir(mode=0o700);(w/'exception_copies.py').write_bytes(script);(w/'request.json').write_text(json.dumps(REQUEST)+'\n')
args=['/home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python','-u',str(w/'exception_copies.py'),'run']
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'runner_sha256':REQUEST['runner_sha256'],'projects':2,'originals_preserved':True,'catalog_changed':False,'paths_changed':False}))
'''
    result=remote('linux','REQUEST='+repr(request)+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code,timeout=60)
    with (HERE/'exception-copy-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    if sys.argv[1:] == ['run']:run()
    elif sys.argv[1:] == ['launch']:launch()
    else:raise SystemExit('Choose launch or run; never replay partial work.')
