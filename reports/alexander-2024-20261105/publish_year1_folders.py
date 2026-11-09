"""Publish only the 16 verified year1 project folders; never change originals/catalog."""
import base64
import ctypes
from datetime import datetime,timezone
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import time

WORK=Path(__file__).resolve().parent
ROOT=WORK.parent
STATE={'phase':'preflight','published_projects':[],'source_deleted':False,'catalog_changed':False}
STARTED=False

NATIVE=r'''
import json,sys,os,stat,pwd,hashlib,time
from pathlib import Path
v=json.load(sys.stdin);assert v['namespace'] in ['incoming','final'] and len(v['projects'])==16
source=Path('/volume1/Sauvegarde/Alexander/analysis');parent=Path('/volume1/homes/maliavko/DetecdivHub/projects');uid=pwd.getpwnam('maliavko').pw_uid
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','.rsync-partial'}
def deadline():assert time.time()<v['own_lease_deadline_unix']
def source_hash(stem):
 h=hashlib.sha256()
 def add(row):h.update((json.dumps(row,sort_keys=True)+'\n').encode())
 def payload(p):
  assert p.parent.resolve()==p.parent and source in p.parents;s=p.lstat();assert stat.S_ISREG(s.st_mode)
  return {'kind':'file','path':str(p.relative_to(source)),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino,'uid':s.st_uid,'gid':s.st_gid,'mode':s.st_mode,'nlink':s.st_nlink}
 directory=source/stem;assert source.resolve()==source and directory.resolve()==directory and directory.is_dir()
 assert not (source/(stem+'.json')).exists();add(payload(source/(stem+'.mat')))
 def fail(error):raise error
 for directory,dirs,files in os.walk(directory,followlinks=False,onerror=fail):
  deadline();dirs[:]=sorted(d for d in dirs if d not in ignored)
  for name in dirs:assert not (Path(directory)/name).is_symlink()
  add({'kind':'directory','path':str(Path(directory).relative_to(source))})
  for name in sorted(files):
   if name not in ignored:add(payload(Path(directory)/name))
 return h.hexdigest()
out=[]
for p in v['projects']:
 deadline();stem=p['name'];assert source_hash(stem)==p['source_inventory_sha256'],'Original project changed'
 root=parent/('.incoming-'+stem+'-20261105' if v['namespace']=='incoming' else stem)
 assert root.resolve()==root and root.is_dir() and root.lstat().st_uid==uid
 other=parent/(stem if v['namespace']=='incoming' else '.incoming-'+stem+'-20261105');assert not os.path.lexists(other)
 expected={f['path']:f for f in p['target_rows']};seen=set();dirs_seen=set();total=main=metadata=0
 def fail(error):raise error
 for directory,dirs,files in os.walk(root,followlinks=False,onerror=fail):
  deadline()
  if Path(directory)!=root:dirs_seen.add(str(Path(directory).relative_to(root)))
  dirs[:]=sorted(d for d in dirs if d not in ignored)
  for name in dirs:assert not (Path(directory)/name).is_symlink()
  for name in files:
   if name in ignored:continue
   file=Path(directory)/name;rel=str(file.relative_to(root));assert rel in expected and file.parent.resolve()==file.parent
   row=expected[rel];s=file.lstat();assert stat.S_ISREG(s.st_mode) and s.st_uid==uid and s.st_nlink==1
   assert (s.st_ino,s.st_size,s.st_mtime_ns)==(row['inode'],row['bytes'],row['mtime_ns']),'Target metadata changed after full hash'
   if rel in p['relinked_paths']:
    h=hashlib.sha256()
    with file.open('rb') as stream:
     for block in iter(lambda:stream.read(4*1024*1024),b''):deadline();h.update(block)
    assert h.hexdigest()==row['sha256'];after=file.lstat();assert (s.st_ino,s.st_size,s.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns);metadata+=1
   seen.add(rel);total+=s.st_size
   if rel==stem+'.mat':main=s.st_size
 assert seen==set(expected) and dirs_seen==set(p['directories']) and main>0
 out.append({'project_id':p['project_id'],'name':stem,'files':len(seen),'total_bytes':total,'project_mat_bytes':main,'project_dir_bytes':total-main,'metadata_hashes_rechecked':metadata,'native_owner':'maliavko','source_inventory_unchanged':True})
print(json.dumps({'success':True,'namespace':v['namespace'],'projects':out}))
'''

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for b in iter(lambda:stream.read(4*1024*1024),b''):h.update(b)
    return h.hexdigest()

def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat();p=WORK/'state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/'state.json')

def rename_directory_noreplace(parent_fd,source_name,destination_name):
    match=re.fullmatch(r'\.incoming-([A-Za-z0-9_]+)-20261105',source_name)
    if not match or destination_name!=match.group(1):raise ValueError('Unexpected folder scope')
    before=os.stat(source_name,dir_fd=parent_fd,follow_symlinks=False)
    if not stat.S_ISDIR(before.st_mode):raise ValueError('Source is not a no-follow directory')
    libc=ctypes.CDLL(None,use_errno=True);call=libc.renameat2
    call.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint];call.restype=ctypes.c_int
    if call(parent_fd,source_name.encode(),parent_fd,destination_name.encode(),1)!=0:
        error=ctypes.get_errno();raise OSError(error,os.strerror(error))
    after=os.stat(destination_name,dir_fd=parent_fd,follow_symlinks=False)
    if not stat.S_ISDIR(after.st_mode) or (before.st_dev,before.st_ino)!=(after.st_dev,after.st_ino):raise ValueError('Renamed object changed')
    return {'source_inode':before.st_ino,'destination_inode':after.st_ino,'no_overwrite':True,'directory_no_follow':True}

def native(value,name):
    output=WORK/name;assert not output.exists()
    loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
    args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['nice','-n','19','python3','-u','-c',loader])]
    with output.open('xb') as stream,Path(str(output)+'.errors.log').open('xb') as errors:
        result=subprocess.run(args,input=json.dumps(value).encode(),stdout=stream,stderr=errors,timeout=7200)
    assert result.returncode==0,'Native publication guard failed; inspect preserved receipts'
    report=json.loads(output.read_text());assert report['success'] and len(report['projects'])==16;return report

def main():
    global STARTED
    os.umask(0o077)
    with (WORK/'publication.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);assert not (WORK/'state.json').exists(),'No replay of partial publication'
        STARTED=True;save();q=json.loads((WORK/'request.json').read_text());a=q['admission']
        assert digest(Path(__file__))==q['runner_sha256'] and len(q['projects'])==16
        assert len({p['name'] for p in q['projects']})==16 and all(re.fullmatch(r'[A-Za-z0-9_]+',p['name']) for p in q['projects'])
        assert {p['project_id'] for p in q['projects']}=={p['project_id'] for p in a['leases']}
        assert not q['source_deletion_allowed'] and not q['catalog_cutover_allowed']
        assert time.time()<q['own_lease_deadline_unix'] and (datetime.now(timezone.utc)-datetime.fromisoformat(a['checked_at'])).total_seconds()<120
        state=json.loads((ROOT/'state.json').read_text());assert state['years']['2024_1']['full_sha1_passed'] and state['years']['2024_1']['source_stability_verified']
        integrity=ROOT/'year1-prepublication-audit-20261109'
        assert (integrity/'controller.exit').read_text().strip()=='0' and digest(integrity/'result.json')==q['integrity_result_sha256']
        payload={'projects':q['projects'],'own_lease_deadline_unix':q['own_lease_deadline_unix'],'namespace':'incoming'}
        STATE['phase']='fresh_native_source_and_target_guards';save();native(payload,'native-before.json')
        root=Path('/homes2/maliavko/DetecdivHub/projects');assert root.resolve()==root
        mount=json.loads(subprocess.check_output(['findmnt','-J','-T',str(root),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
        assert mount['target']=='/homes2/maliavko' and mount['source']=='//10.20.8.250/home' and 'username=maliavko' in mount['options'].split(',') and 'rw' in mount['options'].split(',')
        fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            STATE['phase']='publishing_verified_folders';save()
            with (WORK/'renames.jsonl').open('x') as receipt:
                for p in q['projects']:
                    assert time.time()<q['own_lease_deadline_unix'];stem=p['name'];row=rename_directory_noreplace(fd,'.incoming-'+stem+'-20261105',stem)
                    row.update(project_id=p['project_id'],name=stem,utc=datetime.now(timezone.utc).isoformat());receipt.write(json.dumps(row)+'\n');receipt.flush();os.fsync(receipt.fileno());STATE['published_projects'].append(row);save()
        finally:os.close(fd)
        payload['namespace']='final';STATE['phase']='verifying_native_final_folders';save();report=native(payload,'native-after.json')
        result={'success':True,'projects':report['projects'],'filesystem_published':True,'source_deleted':False,'catalog_changed':False,'host_and_catalog_gates_pending':True,'renames_sha256':digest(WORK/'renames.jsonl')}
        (WORK/'result.json').write_text(json.dumps(result,indent=2)+'\n');STATE.update(phase='filesystem_published_final_host_gates_pending',success=True,finished_at=datetime.now(timezone.utc).isoformat());save()

if __name__=='__main__':
    try:main()
    except Exception as error:
        if STARTED:STATE.update(phase='failed_safe_partial_publication_requires_review',error=type(error).__name__+': '+str(error));save()
        raise
