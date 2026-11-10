"""Readonly fresh integrity of remaining incoming copies, including two special layouts."""
import ast
import base64
from datetime import datetime,timezone
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
ROOT=WORK.parent
STATE={'phase':'preflight','read_only':True,'projects':[],'source_deleted':False,'published':False,'catalog_changed':False}
STARTED=False

SOURCE_STANDARD=r'''
import json,os,stat,sys
from pathlib import Path
v=json.load(sys.stdin);stem=v['name'];root=Path('/volume1/Sauvegarde/Alexander/analysis')
assert root.resolve()==root and (root/stem).resolve()==root/stem and (root/stem).is_dir()
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','.rsync-partial'}
def emit(row):print(json.dumps(row,sort_keys=True),flush=True)
def file(p):
 assert p.parent.resolve()==p.parent and root in p.parents;s=p.lstat();assert stat.S_ISREG(s.st_mode)
 emit({'kind':'file','path':str(p.relative_to(root)),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino,'uid':s.st_uid,'gid':s.st_gid,'mode':s.st_mode,'nlink':s.st_nlink})
assert not (root/(stem+'.json')).exists();file(root/(stem+'.mat'))
def fail(error):raise error
for directory,dirs,files in os.walk(root/stem,followlinks=False,onerror=fail):
 dirs[:]=sorted(d for d in dirs if d not in ignored)
 for name in dirs:assert not (Path(directory)/name).is_symlink()
 emit({'kind':'directory','path':str(Path(directory).relative_to(root))})
 for name in sorted(files):
  if name not in ignored:file(Path(directory)/name)
'''

TARGET=r'''
import json,sys,stat,hashlib,os,pwd,re
from pathlib import Path
v=json.load(sys.stdin);stem=v['name'];assert re.fullmatch(r'[A-Za-z0-9_]+',stem)
assert v['stage_suffix'] in ['20261105','20261106']
parent=Path('/volume1/homes/maliavko/DetecdivHub/projects');root=parent/('.incoming-'+stem+'-'+v['stage_suffix'])
assert root.resolve()==root and root.is_dir() and not os.path.lexists(parent/stem)
uid=pwd.getpwnam('maliavko').pw_uid;assert root.lstat().st_uid==uid
expected={x['path']:x for x in v['files']};assert len(expected)==len(v['files'])
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','.rsync-partial'};seen=set();dirs_seen=set()
def fail(error):raise error
for directory,dirs,files in os.walk(root,followlinks=False,onerror=fail):
 if Path(directory)!=root:dirs_seen.add(str(Path(directory).relative_to(root)))
 dirs[:]=sorted(d for d in dirs if d not in ignored)
 for name in dirs:assert not (Path(directory)/name).is_symlink()
 for name in sorted(files):
  if name in ignored:continue
  p=Path(directory)/name;relative=str(p.relative_to(root));assert relative in expected and p.parent.resolve()==p.parent
  row=expected[relative];before=p.lstat();assert stat.S_ISREG(before.st_mode) and before.st_uid==uid and before.st_nlink==1
  if not row['metadata_relinked']:assert before.st_size==row['source_bytes']
  h=hashlib.sha256()
  with p.open('rb') as stream:
   for block in iter(lambda:stream.read(4*1024*1024),b''):h.update(block)
  assert h.hexdigest()==row['expected_sha256'],'Target content differs from verified copy/relink evidence: '+relative
  after=p.lstat();assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
  seen.add(relative);print(json.dumps({'kind':'verified_file','path':relative,'bytes':before.st_size,'mtime_ns':before.st_mtime_ns,'inode':before.st_ino,'uid':before.st_uid,'sha256':h.hexdigest()},sort_keys=True),flush=True)
assert seen==set(expected) and dirs_seen==set(v['directories'])
print(json.dumps({'kind':'complete','files':len(seen),'directories':len(dirs_seen),'owner':'maliavko','published':False},sort_keys=True),flush=True)
'''

def digest(p):
    h=hashlib.sha256()
    with p.open('rb') as stream:
        for block in iter(lambda:stream.read(4*1024*1024),b''):h.update(block)
    return h.hexdigest()

def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat();p=WORK/'state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/'state.json')

def native(code,value,path,role):
    assert role in ['primary','secondary'] and not path.exists()
    host,key=('Gilles@10.20.11.250','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519') if role=='primary' else ('charvin-admin@10.20.8.250','/home/charvin-admin/.ssh/detecdiv_backup_ed25519')
    loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(code.encode()).decode())+'))'
    args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-i',key,host,shlex.join(['nice','-n','19','python3','-u','-c',loader])]
    count=size=0;last=None;updated=time.monotonic()
    with path.open('xb') as out,Path(str(path)+'.errors.log').open('xb') as errors:
        child=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errors);child.stdin.write(json.dumps(value).encode());child.stdin.close();STATE['child_pid']=child.pid;save()
        for line in child.stdout:
            out.write(line);last=json.loads(line)
            if last['kind'] in ['file','verified_file']:count+=1;size+=last['bytes']
            if time.monotonic()-updated>15:out.flush();STATE.update(phase_files=count,phase_bytes=size);save();updated=time.monotonic()
        code=child.wait();out.flush();os.fsync(out.fileno());assert code==0,'Native readonly guard failed; preserve partial receipts'
    return {'sha256':digest(path),'files':count,'bytes':size,'last_row':last}

def main():
    global STARTED
    os.umask(0o077)
    with (WORK/'audit.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);assert not (WORK/'state.json').exists(),'No replay'
        STARTED=True;q=json.loads((WORK/'request.json').read_text());assert digest(Path(__file__))==q['runner_sha256']
        assert q['read_only'] and not q['publication_allowed'] and not q['source_deletion_allowed']
        assert len(q['projects'])==q['expected_projects'] and 1<=len(q['projects'])<=17
        assert len({p['project_id'] for p in q['projects']})==len(q['projects'])
        original=ROOT/'exception-copies-20261106/exception_copies.py';assert digest(original)==json.loads((original.parent/'request.json').read_text())['runner_sha256']
        tree=ast.parse(original.read_text());special=ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='NATIVE' for t in n.targets)))
        STATE.update(expected_projects=len(q['projects']),deferred_projects=q['admission']['deferred_projects'],raw_year2_checksum_and_publication_gates_still_required=True);save()
        for index,p in enumerate(q['projects']):
            stem=p['name'];assert re.fullmatch(r'[A-Za-z0-9_]+',stem)
            special_layout=stem in ['qp_250724_dbud4stest','281124_Alex']
            expected_relative=('exception-copies-20261106/' if special_layout else 'project-copies/')+stem+'/source-before.jsonl'
            assert p['source_receipt_relative']==expected_relative
            folder=WORK/stem;folder.mkdir(mode=0o700);STATE.update(project_index=index+1,project=stem,phase='source_inventory_before',phase_files=0,phase_bytes=0);save()
            source_file=ROOT/p['source_receipt_relative'];assert digest(source_file)==p['source_inventory_sha256']
            assert (p['stage_suffix']=='20261106')==special_layout
            role='primary' if stem=='281124_Alex' else 'secondary';assert p['source_role']==role
            code=special if special_layout else SOURCE_STANDARD;value={'name':stem,'role':'source'} if special_layout else {'name':stem}
            before=native(code,value,folder/'source-fresh-before.jsonl',role);assert before['sha256']==p['source_inventory_sha256'],'Original source inventory/content changed; review required'
            STATE['phase']='target_full_after_hash_verification';save();target=native(TARGET,p,folder/'target-full-hashes.jsonl','secondary')
            assert target['last_row']['kind']=='complete' and target['files']==len(p['files'])
            STATE['phase']='source_inventory_after';save();after=native(code,value,folder/'source-fresh-after.jsonl',role);assert after['sha256']==before['sha256'],'Original changed during readonly verification'
            result={'project_id':p['project_id'],'name':stem,'source_before_after_unchanged':True,'target_full_after_hashes_verified':True,'target_native_owner':'maliavko','target_files':target['files'],'target_bytes':target['bytes'],'source_manifest_sha256':before['sha256'],'target_receipt_sha256':target['sha256'],'stage_suffix':p['stage_suffix'],'published':False}
            (folder/'result.json').write_text(json.dumps(result,indent=2)+'\n');STATE['projects'].append(result);save()
        result={'success':True,'projects':STATE['projects'],'deferred_projects':q['admission']['deferred_projects'],'read_only':True,'source_deleted':False,'published':False,'catalog_changed':False,'raw_year2_full_sha1_stability_and_final_host_catalog_api_gates_still_required':True}
        (WORK/'result.json').write_text(json.dumps(result,indent=2)+'\n');STATE.update(phase='readonly_incoming_integrity_verified_raw_year2_gates_pending',success=True,finished_at=datetime.now(timezone.utc).isoformat());save()

if __name__=='__main__':
    try:main()
    except Exception as error:
        if STARTED:STATE.update(phase='failed_safe_readonly',success=False,error_type=type(error).__name__,error=str(error));save()
        raise
