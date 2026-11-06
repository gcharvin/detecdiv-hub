"""Guarded first 2024 relink batch: 23 audited metadata MATs, no publication/deletion."""
import ast
from datetime import datetime, timezone
import fcntl
import base64
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess

WORK=Path(__file__).resolve().parent
STATE={'phase':'preflight','source_deleted':False,'catalog_changed':False,'copies_may_have_been_relinked':False}
STARTED=False

NATIVE_CHECK=r'''
import json,sys,stat,hashlib,pwd as posix_accounts
from pathlib import Path
v=json.load(sys.stdin);uid=posix_accounts.getpwnam('maliavko').pw_uid
original=Path('/volume1/Sauvegarde/Alexander/analysis')
homes=Path('/volume1/homes/maliavko/DetecdivHub')
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
 return h.hexdigest()
out=[]
for row in v['files']:
 rel=Path(row['relative_path']);assert not rel.is_absolute() and '..' not in rel.parts
 stem=row['stem'];assert rel.parts[0] in {stem,stem+'.mat'}
 paths=[original/rel,homes/'projects'/('.incoming-'+stem+'-20261105')/rel]
 if v['with_backups']:paths.append(homes/'.migration-backups/alexander-2024-20261105/relink-batch1-20261106'/rel)
 ids=[]
 for i,p in enumerate(paths):
  for parent in [p.parent,*p.parents]:
   if parent in [original,homes]:break
   assert not parent.is_symlink()
  s=p.lstat();assert stat.S_ISREG(s.st_mode) and s.st_size==row['bytes']
  if i:assert s.st_uid==uid and s.st_nlink==1
  ids.append((s.st_dev,s.st_ino));assert digest(p)==row['sha256']
 assert len(set(ids))==len(ids)
 out.append({'relative_path':row['relative_path'],'sha256':row['sha256'],'original_and_copy_match':True,'backup_matches':v['with_backups'],'native_owner':'maliavko'})
print(json.dumps({'success':True,'files':out}))
'''


def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    f=WORK/'state.tmp';f.write_text(json.dumps(STATE,indent=2)+'\n');f.replace(WORK/'state.json')


def ssh_native(code,payload,output):
    loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(code.encode()).decode())+'))'
    args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3',
          '-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',
          shlex.join(['nice','-n','19','python3','-u','-c',loader])]
    assert not output.exists()
    with output.open('xb') as stream,(Path(str(output)+'.errors.log')).open('xb') as errors:
        r=subprocess.run(args,input=json.dumps(payload).encode(),stdout=stream,stderr=errors,timeout=1800)
    assert r.returncode==0,'Native gate failed; inspect private error receipt'


def main():
    global STARTED
    os.umask(0o077)
    with (WORK/'relink.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        assert not (WORK/'state.json').exists(),'Do not replay a one-shot relink'
        STARTED=True
        r=json.loads((WORK/'controller-request.json').read_text())
        assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==r['runner_sha256']
        assert hashlib.sha256((WORK/'relink_2024_batch1.m').read_bytes()).hexdigest()==r['matlab_sha256']
        assert not r['source_deletion_allowed'] and not r['catalog_cutover_allowed']
        assert len(r['files'])==23 and len(r['projects'])==len(r['lease_ids'])==12
        deadline=r['lease_deadline_unix'];assert datetime.now(timezone.utc).timestamp()<deadline
        mount=json.loads(subprocess.check_output(['findmnt','-J','-T','/homes2/maliavko/DetecdivHub/projects','-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
        assert mount['source']=='//10.20.8.250/home' and mount['target']=='/homes2/maliavko'
        assert 'username=maliavko' in mount['options'].split(',') and 'rw' in mount['options'].split(',')
        free=os.statvfs('/homes2/maliavko');assert free.f_bavail*free.f_frsize>8_000_000_000_000
        save()
        parent=WORK.parent;copy=parent/'project-copies';source_runner=copy/'copy_projects.py'
        pinned=json.loads((copy/'request.json').read_text())['runner_sha256']
        assert hashlib.sha256(source_runner.read_bytes()).hexdigest()==pinned
        tree=ast.parse(source_runner.read_text())
        native_inventory=ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='NATIVE' for t in n.targets)))
        for p in r['projects']:
            STATE.update(phase='recheck_original_inventory',project=p['name']);save()
            f=WORK/('source-fresh-'+p['name']+'.jsonl')
            ssh_native(native_inventory,{'name':p['name'],'mode':'inventory'},f)
            assert f.read_bytes()==(copy/p['name']/'source-before.jsonl').read_bytes(),'Original project changed since its verified copy'
        STATE.update(phase='native_input_hash_gate');save()
        ssh_native(NATIVE_CHECK,{'files':r['files'],'with_backups':False},WORK/'native-inputs.json')
        backup=Path('/homes2/maliavko/DetecdivHub/.migration-backups/alexander-2024-20261105/relink-batch1-20261106')
        assert not backup.exists();backup.mkdir(parents=True,mode=0o700)
        for i,row in enumerate(r['files']):
            assert datetime.now(timezone.utc).timestamp()<deadline
            target=Path(row['target']);dst=Path(row['backup']);assert dst==backup/row['relative_path']
            dst.parent.mkdir(parents=True,exist_ok=True,mode=0o700);assert not dst.exists()
            with target.open('rb') as a,dst.open('xb') as b:shutil.copyfileobj(a,b,4*1024*1024)
            STATE.update(phase='backup_before_relink',backups=i+1);save()
        ssh_native(NATIVE_CHECK,{'files':r['files'],'with_backups':True},WORK/'native-input-and-backup-gate.json')
        assert datetime.now(timezone.utc).timestamp()<deadline
        (WORK/'request.json').write_text(json.dumps(r,indent=2)+'\n')
        STATE.update(phase='relinking_unpublished_copies',copies_may_have_been_relinked=True);save()
        release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
        expression="maxNumCompThreads(1);addpath('"+str(WORK)+"');relink_2024_batch1('"+str(WORK)+"','"+release+"');"
        with (WORK/'matlab.log').open('xb') as out:
            process=subprocess.Popen(['nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression],stdout=out,stderr=subprocess.STDOUT,env={**os.environ,'CUDA_VISIBLE_DEVICES':''})
            STATE['matlab_pid']=process.pid;save();code=process.wait()
        (WORK/'matlab.exit').write_text(str(code)+'\n');assert code==0,'MATLAB relink failed safely; inspect preserved copies/backups'
        result=json.loads((WORK/'result.json').read_text());assert result['success'] and len(result['files'])==23
        assert all(x['scientific_values_types_and_annotations_preserved'] and x['save_reopen_verified'] for x in result['files'])
        STATE.update(phase='copied_paths_relinked_science_verified_raw_and_host_gates_pending',finished_at=datetime.now(timezone.utc).isoformat());save()


if __name__=='__main__':
    try:main()
    except Exception as error:
        if STARTED:
            STATE.update(phase='failed_safe_unpublished_copies',error=type(error).__name__+': '+str(error));save()
        raise
