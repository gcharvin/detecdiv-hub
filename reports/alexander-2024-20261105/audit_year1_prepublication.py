"""Readonly fresh source inventories and full after-hash verification of 16 year1 copies."""
import ast
import base64
from datetime import datetime,timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

WORK=Path(__file__).resolve().parent
ROOT=WORK.parent
STATE={'phase':'preflight','read_only':True,'projects':[],'source_deleted':False,'published':False,'catalog_changed':False}
STARTED=False

TARGET=r'''
import json,sys,stat,hashlib,os,pwd,re
from pathlib import Path
v=json.load(sys.stdin);stem=v['name'];assert re.fullmatch(r'[A-Za-z0-9_]+',stem)
parent=Path('/volume1/homes/maliavko/DetecdivHub/projects');root=parent/('.incoming-'+stem+'-20261105')
assert root.resolve()==root and root.is_dir() and not (parent/stem).exists()
uid=pwd.getpwnam('maliavko').pw_uid;assert root.lstat().st_uid==uid
expected={x['path']:x for x in v['files']};assert len(expected)==len(v['files'])
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','.rsync-partial'}
seen=set();dirs_seen=set()
def fail(error):raise error
for directory,dirs,files in os.walk(root,followlinks=False,onerror=fail):
 if Path(directory)!=root:dirs_seen.add(str(Path(directory).relative_to(root)))
 dirs[:]=sorted(d for d in dirs if d not in ignored)
 for d in dirs:assert not (Path(directory)/d).is_symlink()
 for name in sorted(files):
  if name in ignored:continue
  p=Path(directory)/name;relative=str(p.relative_to(root));assert relative in expected and p.parent.resolve()==p.parent
  row=expected[relative];before=p.lstat();assert stat.S_ISREG(before.st_mode) and before.st_uid==uid and before.st_nlink==1
  if not row['metadata_relinked']:assert before.st_size==row['source_bytes']
  h=hashlib.sha256()
  with p.open('rb') as stream:
   for b in iter(lambda:stream.read(4*1024*1024),b''):h.update(b)
  assert h.hexdigest()==row['expected_sha256'],'Current target hash mismatch: '+relative
  after=p.lstat();assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
  seen.add(relative);print(json.dumps({'kind':'verified_file','path':relative,'bytes':before.st_size,'mtime_ns':before.st_mtime_ns,'inode':before.st_ino,'uid':before.st_uid,'sha256':h.hexdigest()},sort_keys=True),flush=True)
assert seen==set(expected) and dirs_seen==set(v['directories'])
print(json.dumps({'kind':'complete','files':len(seen),'directories':len(dirs_seen),'owner':'maliavko','published':False},sort_keys=True),flush=True)
'''

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for b in iter(lambda:stream.read(4*1024*1024),b''):h.update(b)
    return h.hexdigest()

def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=WORK/'state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/'state.json')

def native(code,value,path):
    assert not path.exists()
    loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(code.encode()).decode())+'))'
    command=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3',
      '-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['nice','-n','19','python3','-u','-c',loader])]
    last=time.monotonic();count=size=0;final=None
    with path.open('xb') as output,Path(str(path)+'.errors.log').open('xb') as errors:
        child=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errors)
        child.stdin.write(json.dumps(value).encode());child.stdin.close();STATE['child_pid']=child.pid;save()
        for line in child.stdout:
            output.write(line);row=json.loads(line);final=row
            if row.get('kind') in ['file','verified_file']:count+=1;size+=row['bytes']
            if time.monotonic()-last>15:
                output.flush();STATE.update(phase_files=count,phase_bytes=size);save();last=time.monotonic()
        rc=child.wait();output.flush();os.fsync(output.fileno());assert rc==0,'Native readonly gate failed; preserve receipts'
    return {'files':count,'bytes':size,'sha256':digest(path),'last_row':final}

def main():
    global STARTED
    os.umask(0o077)
    with (WORK/'audit.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);assert not (WORK/'state.json').exists(),'No replay'
        STARTED=True
        q=json.loads((WORK/'request.json').read_text());assert digest(Path(__file__))==q['runner_sha256']
        assert q['read_only'] and len(q['projects'])==16 and not q['publication_allowed'] and not q['source_deletion_allowed']
        original=ROOT/'project-copies/copy_projects.py'
        assert digest(original)==json.loads((ROOT/'project-copies/request.json').read_text())['runner_sha256']
        tree=ast.parse(original.read_text());source_code=ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='NATIVE' for t in n.targets)))
        save()
        for index,p in enumerate(q['projects']):
            stem=p['name'];STATE.update(project_index=index+1,project=stem,phase='source_inventory_before',phase_files=0,phase_bytes=0);save()
            folder=WORK/stem;folder.mkdir(mode=0o700)
            old=ROOT/'project-copies'/stem/'source-before.jsonl';assert digest(old)==p['source_inventory_sha256']
            before=native(source_code,{'name':stem,'mode':'inventory'},folder/'source-fresh-before.jsonl')
            assert before['sha256']==p['source_inventory_sha256'],'Original inventory changed; review required'
            STATE.update(phase='target_full_after_hash_verification');save()
            target=native(TARGET,p,folder/'target-full-hashes.jsonl')
            assert target['last_row']['kind']=='complete' and target['files']==len(p['files'])
            STATE.update(phase='source_inventory_after');save()
            after=native(source_code,{'name':stem,'mode':'inventory'},folder/'source-fresh-after.jsonl')
            assert after['sha256']==before['sha256'],'Original changed during readonly review'
            result={'project_id':p['project_id'],'name':stem,'source_before_after_unchanged':True,'target_full_after_hashes_verified':True,
              'target_native_owner':'maliavko','target_files':target['files'],'target_bytes':target['bytes'],'source_manifest_sha256':before['sha256'],
              'target_receipt_sha256':target['sha256'],'published':False}
            (folder/'result.json').write_text(json.dumps(result,indent=2)+'\n');STATE['projects'].append(result);save()
        result={'success':True,'projects':STATE['projects'],'read_only':True,'source_deleted':False,'published':False,'catalog_changed':False,
          'fresh_publication_admission_and_host_catalog_api_gates_still_required':True}
        (WORK/'result.json').write_text(json.dumps(result,indent=2)+'\n');STATE.update(phase='readonly_prepublication_integrity_verified',finished_at=datetime.now(timezone.utc).isoformat());save()

if __name__=='__main__':
    try:main()
    except Exception as error:
        if STARTED:STATE.update(phase='failed_safe_readonly',error=type(error).__name__+': '+str(error));save()
        raise
