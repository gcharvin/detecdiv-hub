"""Preserve the available MAT of the explicitly diagnosed missing-folder project.

No missing derived folder or sidecar is invented. No publication or deletion.
"""
import json
import hashlib
import os
from pathlib import Path
import shlex
import subprocess

WORK=Path(__file__).resolve().parent
NAME='anof_290424_loxP15'
STAGE=Path('/homes2/maliavko/DetecdivHub/projects/.incoming-anof_290424_loxP15-20261106')
EXPECTED='c1a32a8cca1847fe72937423501a1ae8ddbb0c9d1b03460d33f081b5b6558898'
NATIVE=r'''
import json,sys,stat,hashlib,fcntl
from pathlib import Path
import pwd as posix_accounts
v=json.load(sys.stdin);mode=v['mode'];assert mode in ['before','clone_verify']
name='anof_290424_loxP15';parent=Path('/volume1/Sauvegarde/Alexander/analysis');src=parent/(name+'.mat')
stage=Path('/volume1/homes/maliavko/DetecdivHub/projects/.incoming-anof_290424_loxP15-20261106')
assert parent.resolve()==parent and src.parent.resolve()==src.parent
assert not (parent/name).exists() and not (parent/(name+'.json')).exists(),'Available source layout changed; review it'
s=src.lstat();assert stat.S_ISREG(s.st_mode)
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
 return h.hexdigest()
before={'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino,'uid':s.st_uid,'gid':s.st_gid,'mode':s.st_mode,'nlink':s.st_nlink,'sha256':digest(src)}
assert before['sha256']==v['expected_sha256']
if mode=='clone_verify':
 assert before==v['before']
 assert stage.is_dir() and stage.resolve()==stage and {p.name for p in stage.iterdir()}=={name+'.mat'}
 dst=stage/(name+'.mat');d=dst.lstat();assert stat.S_ISREG(d.st_mode) and d.st_size==0 and d.st_nlink==1
 uid=posix_accounts.getpwnam('maliavko').pw_uid;assert d.st_uid==uid and (d.st_dev,d.st_ino)!=(s.st_dev,s.st_ino)
 with src.open('rb') as a,dst.open('r+b') as b:fcntl.ioctl(b.fileno(),0x40049409,a.fileno())
 assert dst.stat().st_size==s.st_size and digest(dst)==before['sha256'] and digest(src)==before['sha256']
 after=src.lstat();assert (after.st_size,after.st_mtime_ns,after.st_ino,after.st_uid,after.st_gid,after.st_nlink)==(s.st_size,s.st_mtime_ns,s.st_ino,s.st_uid,s.st_gid,s.st_nlink)
 print(json.dumps({'success':True,'files':1,'bytes':s.st_size,'sha256':before['sha256'],'target_owner':'maliavko','independent_inode':True,'original_unchanged':True,'associated_folder_absent_and_not_created':True,'source_deleted':False,'catalog_changed':False}))
else:print(json.dumps(before))
'''


def native(payload,leaf):
    out=WORK/(leaf+'.json');assert not out.exists()
    args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['nice','-n','19','python3','-u','-c',NATIVE])]
    with out.open('xb') as stream,(WORK/(leaf+'.errors.log')).open('xb') as errors:r=subprocess.run(args,input=json.dumps(payload).encode(),stdout=stream,stderr=errors)
    assert r.returncode==0,'Inspect durable partial copy receipt, do not replay'
    return json.loads(out.read_text())


def main():
    os.umask(0o077);request=json.loads((WORK/'request.json').read_text())
    assert request['runner_sha256']==hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    mount=json.loads(subprocess.check_output(['findmnt','-J','-T',str(STAGE.parent),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
    assert mount['source']=='//10.20.8.250/home' and mount['target']=='/homes2/maliavko' and 'username=maliavko' in mount['options'].split(',')
    assert not STAGE.exists() and not (STAGE.parent/NAME).exists()
    before=native({'mode':'before','expected_sha256':EXPECTED},'native-before')
    free=os.statvfs(STAGE.parent);assert free.f_bavail*free.f_frsize>before['bytes']+2_000_000_000_000
    STAGE.mkdir(mode=0o700)
    with (STAGE/(NAME+'.mat')).open('xb'):pass
    result=native({'mode':'clone_verify','expected_sha256':EXPECTED,'before':before},'native-verified')
    os.utime(STAGE/(NAME+'.mat'),ns=(before['mtime_ns'],before['mtime_ns']))
    result.update(project_id='de636b9e-684a-4ab1-bb4a-0bc64d435020',name=NAME,stage=str(STAGE),phase='available_main_copied_missing_derived_folder_review_pending',complete_project_or_identity_gate_passed=False)
    (WORK/'result.json').write_text(json.dumps(result,indent=2)+'\n')


if __name__=='__main__':main()
