"""Prepare independent, fully hashed project copies without publishing them.

Sources stay on Sauvegarde. Copied MAT paths are not rewritten here. Raw copy,
alias review, Linux/Windows validation and catalog publication remain separate.
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

WORK = Path(__file__).resolve().parent
MOUNT = Path('/homes2/maliavko/DetecdivHub/projects')
NATIVE = r'''
from pathlib import Path
import sys,json,os,stat,fcntl,hashlib,shutil,subprocess
import pwd as posix_accounts
if shutil.which('ionice'):subprocess.run(['ionice','-c','3','-p',str(os.getpid())],capture_output=True)
v=json.load(sys.stdin);stem=v['stem'];mode=v['mode']
import re
assert re.fullmatch(r'[A-Za-z0-9_]+',stem)
source=Path('/volume1/Sauvegarde/Alexander/analysis')
dest=Path('/volume1/homes/maliavko/DetecdivHub/projects')/('.incoming-'+stem+'-20261101')
ignored={'@eaDir','#recycle','#snapshot','.rsync-partial'}
def emit(row):print(json.dumps(row,sort_keys=True),flush=True)
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for data in iter(lambda:f.read(4*1024*1024),b''):h.update(data)
 return h.hexdigest()
def fail(error):raise error
if mode=='inventory':
 assert not (source/(stem+'.json')).exists(),'JSON sibling appeared; review actual source format'
 root=source/stem;mat=source/(stem+'.mat')
 assert root.is_dir() and not root.is_symlink() and mat.is_file() and not mat.is_symlink()
 def file_row(p):
  s=p.lstat();assert stat.S_ISREG(s.st_mode)
  return {'kind':'file','path':str(p.relative_to(source)),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns}
 emit(file_row(mat))
 for parent,dirs,files in os.walk(root,followlinks=False,onerror=fail):
  dirs[:]=sorted(n for n in dirs if n not in ignored)
  for n in dirs:assert not (Path(parent)/n).is_symlink()
  emit({'kind':'directory','path':str(Path(parent).relative_to(source))})
  for n in sorted(files):
   if n not in ignored:emit(file_row(Path(parent)/n))
else:
 # Account-name lookup only; no authentication material is read here.
 uid=posix_accounts.getpwnam('maliavko').pw_uid
 assert mode in {'clone','verify','preflight_empty'} and dest.is_dir() and not dest.is_symlink()
 if mode=='preflight_empty':
  expected={r['path'] for r in v['files']};seen=set()
  for parent,dirs,files in os.walk(dest,followlinks=False,onerror=fail):
   dirs[:]=[d for d in dirs if d not in ignored]
   for d in dirs:assert not (Path(parent)/d).is_symlink()
   for n in files:
    if n not in ignored:seen.add(str((Path(parent)/n).relative_to(dest)))
  assert seen==expected,'Unexpected stage content'
 for row in v['files']:
  rel=Path(row['path']);assert not rel.is_absolute() and '..' not in rel.parts
  assert rel.parts[0] in {stem,stem+'.mat'}
  src=source/rel;dst=dest/rel;s=src.lstat();d=dst.lstat()
  assert stat.S_ISREG(s.st_mode) and s.st_size==row['bytes'] and s.st_mtime_ns==row['mtime_ns']
  assert stat.S_ISREG(d.st_mode) and d.st_uid==uid and d.st_nlink==1
  assert (s.st_dev,s.st_ino)!=(d.st_dev,d.st_ino)
  if mode=='preflight_empty':
   assert d.st_size==0,'A partial clone requires manual review'
   result={'path':row['path'],'bytes':0,'empty_owned_placeholder_verified':True}
  elif mode=='clone':
   assert d.st_size==0
   with src.open('rb') as a,dst.open('r+b') as b:fcntl.ioctl(b.fileno(),0x40049409,a.fileno())
   assert dst.stat().st_size==s.st_size
   result={'path':row['path'],'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'independent_reflink':True,'owner':'maliavko'}
  else:
   assert d.st_size==s.st_size
   a=digest(src);b=digest(dst);assert a==b,'Copied content differs'
   after=src.stat();assert (after.st_size,after.st_mtime_ns)==(s.st_size,s.st_mtime_ns)
   result={'path':row['path'],'bytes':s.st_size,'sha256':a,'source_and_destination_match':True,'owner':'maliavko'}
  emit(result)
'''

state = {'phase': 'preflight', 'projects': [], 'source_deleted': False,
         'catalog_changed': False, 'project_paths_rewritten': False}
STARTED = False


def save():
    state['heartbeat_at'] = datetime.now(timezone.utc).isoformat()
    state['runner_pid'] = os.getpid()
    temporary = WORK/'state.tmp'
    temporary.write_text(json.dumps(state, indent=2)+'\n'); temporary.replace(WORK/'state.json')


def native(stem, directory, mode, files=None, output=None):
    loader = 'import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
    args = ['ssh', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'ServerAliveInterval=15',
            '-o', 'ServerAliveCountMax=3', '-i', '/home/charvin-admin/.ssh/detecdiv_backup_ed25519',
            'charvin-admin@10.20.8.250', shlex.join(['nice', '-n', '19', 'python3', '-u', '-c', loader])]
    data = {'stem': stem, 'mode': mode}
    if files is not None: data['files'] = files
    request = directory/(mode+'-'+('after' if output == 'source-after.jsonl' else 'before')+'-request.json')
    assert not request.exists(); request.write_text(json.dumps(data))
    output = directory/(output or (mode+'-receipt.jsonl'))
    assert not output.exists()
    count = size = 0; rows = []; last = time.monotonic(); h = hashlib.sha256()
    with request.open('rb') as stream, output.open('xb') as receipt, (directory/(output.name+'.errors.log')).open('xb') as errors:
        process = subprocess.Popen(args, stdin=stream, stdout=subprocess.PIPE, stderr=errors)
        for line in process.stdout:
            row = json.loads(line); receipt.write(line); h.update(line)
            if mode == 'inventory': rows.append(row)
            if row.get('kind') == 'file' or mode != 'inventory': count += 1; size += row['bytes']
            if mode == 'clone':
                p = MOUNT/('.incoming-'+stem+'-20261101')/row['path']
                os.utime(p, ns=(row['mtime_ns'], row['mtime_ns']))
            if time.monotonic()-last > 10:
                state.update(phase=mode, current_project=stem, phase_files=count, phase_bytes=size)
                receipt.flush(); save(); last = time.monotonic()
        assert process.wait() == 0, 'Native '+mode+' failed; inspect private error receipt'
    if files is not None: assert count == len(files)
    return {'files': count, 'bytes': size, 'receipt_sha256': h.hexdigest()}, rows


def main():
    global STARTED
    os.umask(0o077)
    with (WORK/'project-copies.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        request = json.loads((WORK/'request.json').read_text())
        assert request['runner_sha256'] == hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        assert request['project_count'] == len(request['projects']) == 11
        assert not request['source_deletion_allowed'] and not request['catalog_cutover_allowed']
        resume = False
        if (WORK/'state.json').exists():
            previous=json.loads((WORK/'state.json').read_text())
            assert request.get('resume_empty_first_stage') and previous['phase']=='failed_safe' and previous['projects']==[]
            assert previous['current_project']==request['projects'][0]['name']
            state.update(previous);state.pop('error_type',None);resume=True
        STARTED = True
        mount = json.loads(subprocess.check_output(['findmnt', '-J', '-T', str(MOUNT), '-o', 'SOURCE,TARGET,OPTIONS'], text=True))['filesystems'][0]
        assert mount['target'] == '/homes2/maliavko' and mount['source'] == '//10.20.8.250/home'
        assert 'username=maliavko' in mount['options'].split(',')
        save()
        for project in request['projects']:
            stem = project['name']; assert re.fullmatch(r'[A-Za-z0-9_]+', stem)
            final = MOUNT/stem; stage = MOUNT/('.incoming-'+stem+'-20261101')
            assert not final.exists()
            directory = WORK/stem
            state.update(phase='inventory', current_project=stem); save()
            if resume and stem==request['projects'][0]['name']:
                assert directory.is_dir() and stage.is_dir()
                raw=(directory/'source-before.jsonl').read_bytes()
                rows=[json.loads(line) for line in raw.splitlines()]
                before={'files':sum(r['kind']=='file' for r in rows),'bytes':sum(r.get('bytes',0) for r in rows),'receipt_sha256':hashlib.sha256(raw).hexdigest()}
            else:
                assert not stage.exists();directory.mkdir(mode=0o700)
                before, rows = native(stem, directory, 'inventory', output='source-before.jsonl')
            files = [r for r in rows if r['kind'] == 'file']; dirs = [r for r in rows if r['kind'] == 'directory']
            assert files and any(r['path'] == stem+'.mat' for r in files)
            free = os.statvfs(MOUNT); assert free.f_bavail*free.f_frsize > before['bytes']+8_000_000_000_000
            if resume and stem==request['projects'][0]['name']:
                _,_=native(stem,directory,'preflight_empty',files)
                assert (directory/'clone-receipt.jsonl').stat().st_size==0
                for name in ['clone-before-request.json','clone-receipt.jsonl','clone-receipt.jsonl.errors.log']:
                    p=directory/name;q=directory/('attempt1-'+name);assert not q.exists();p.rename(q)
            else:
                stage.mkdir(mode=0o700)
                state.update(phase='create_owned_placeholders', expected_files=len(files), expected_bytes=before['bytes']); save()
                for row in dirs: (stage/row['path']).mkdir(mode=0o700, parents=True, exist_ok=False)
                for index, row in enumerate(files):
                    with (stage/row['path']).open('xb'): pass
                    if index % 1000 == 0: state['placeholders'] = index+1; save()
            cloned, _ = native(stem, directory, 'clone', files)
            verified, _ = native(stem, directory, 'verify', files)
            after, _ = native(stem, directory, 'inventory', output='source-after.jsonl')
            assert before == after, 'Source changed across the copy'
            result = {'project_id': project['project_id'], 'name': stem, 'files': before['files'],
                      'bytes': before['bytes'], 'stage': str(stage), 'source_inventory': before,
                      'clone_receipt': cloned, 'verification_receipt': verified,
                      'phase': 'verified_copy_raw_and_path_gates_pending', 'source_preserved': True}
            (directory/'verified-copy.json').write_text(json.dumps(result, indent=2)+'\n')
            state['projects'].append(result); save()
        state['phase'] = 'all_project_copies_verified_raw_copy_and_relink_pending'
        state['finished_at'] = datetime.now(timezone.utc).isoformat(); save()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        if STARTED:
            state.update(phase='failed_safe', error_type=type(error).__name__)
            save()
        raise
