"""Unlink only the explicitly approved, unchanged 2023_2 source manifest.

Run on compute only after catalog_operations retirement gates have passed.
Native unlink bypasses per-file CIFS latency; no recursive delete is used.
"""
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

WORK=Path(__file__).resolve().parent
RAW=WORK/'raw-retirement'
ORIGINAL=Path('/home/charvin-admin/storage-transfers/alexander-2023-20261008')
APPROVAL='Oui, retirer ces doublons vérifiés de /data'
NATIVE=r'''
import sys,json,os,stat,hashlib,shutil,time
from pathlib import Path
request=json.load(sys.stdin)
root=Path('/volume1/DATA/Alexander/data/2023_2')
assert request['approved_source_root']==str(root)
assert request['approval']==APPROVAL
assert root.resolve()==root and root.is_dir() and not root.is_symlink()
assert request['project_original_preserved'] and request['linux_windows_project_validated']
rows=request['files'];expected={r['path']:r for r in rows}
assert len(expected)==len(rows)==425112 and sum(r['bytes'] for r in rows)==3583194799325
for rel in expected:
 p=Path(rel);assert not p.is_absolute() and '..' not in p.parts and '.' not in p.parts
ignored={'@eaDir','#recycle','#snapshot','.rsync-partial'}
observed=set();directories=[];excluded=[]
def fail(error):raise error
for parent,dirs,files in os.walk(root,followlinks=False,onerror=fail):
 excluded.extend(str((Path(parent)/x).relative_to(root)) for x in dirs if x in ignored)
 dirs[:]=sorted(d for d in dirs if d not in ignored)
 for name in dirs:assert not (Path(parent)/name).is_symlink(),'Directory symlink requires review'
 directories.append(Path(parent))
 for name in sorted(files):
  if name in ignored:continue
  p=Path(parent)/name;rel=str(p.relative_to(root));assert rel in expected,'Unexpected source file: '+rel
  s=p.lstat();r=expected[rel]
  assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_size==r['bytes'] and s.st_mtime_ns==r['mtime_ns'],'Source changed: '+rel
  observed.add(rel)
assert observed==set(expected)
before=shutil.disk_usage(root).free
print(json.dumps({'event':'preflight_passed','files':len(rows),'bytes':sum(r['bytes'] for r in rows),'free_bytes_before':before,'excluded_system_directories':excluded}),flush=True)
# Open every parent relative to a no-follow root fd, protecting against an
# ancestor being replaced by a symlink between preflight and unlink.
rootfd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
parentfd=None;current=None;count=size=0
try:
 for row in rows:
  rel=Path(row['path']);parent=rel.parent
  if parent!=current:
   if parentfd is not None:os.close(parentfd)
   parentfd=os.dup(rootfd)
   for component in parent.parts:
    nextfd=os.open(component,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parentfd)
    os.close(parentfd);parentfd=nextfd
   current=parent
  s=os.stat(rel.name,dir_fd=parentfd,follow_symlinks=False)
  assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_size==row['bytes'] and s.st_mtime_ns==row['mtime_ns'],'Changed before unlink: '+row['path']
  os.unlink(rel.name,dir_fd=parentfd);count+=1;size+=s.st_size
  print(json.dumps({'event':'unlinked','path':row['path'],'bytes':s.st_size}),flush=False)
  if count%1000==0:sys.stdout.flush()
finally:
 if parentfd is not None:os.close(parentfd)
removed_dirs=0;retained_dirs=[]
try:
 for directory in sorted(directories,key=lambda p:len(p.parts),reverse=True):
  # Keep the source root itself. Remove only empty manifest subdirectories,
  # again resolving ancestors through no-follow descriptors.
  if directory==root:continue
  rel=directory.relative_to(root);fd=os.dup(rootfd)
  try:
   for component in rel.parent.parts:
    nextfd=os.open(component,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
    os.close(fd);fd=nextfd
   try:os.rmdir(rel.name,dir_fd=fd);removed_dirs+=1
   except OSError as error:
    import errno
    if error.errno not in (errno.ENOTEMPTY,errno.EEXIST):raise
    retained_dirs.append(str(directory))
  finally:os.close(fd)
finally:os.close(rootfd)
remaining=[]
for parent,dirs,files in os.walk(root,followlinks=False,onerror=fail):
 dirs[:]=[d for d in dirs if d not in ignored]
 for name in dirs:assert not (Path(parent)/name).is_symlink(),'New directory symlink after retirement'
 remaining.extend(str((Path(parent)/n).relative_to(root)) for n in files if n not in ignored)
assert not remaining,'New source files remain; preserve catalog source references for review'
os.sync()
after=shutil.disk_usage(root.parent).free
print(json.dumps({'event':'complete','files_unlinked':count,'logical_bytes_unlinked':size,'free_bytes_before':before,'free_bytes_after':after,
 'measured_free_bytes_increase':after-before,'empty_directories_removed':removed_dirs,'retained_nonempty_directories':retained_dirs,
 'source_root_exists':root.exists(),'source_root_kept_intentionally':True,'remaining_source_payload_files':0,
 'excluded_system_directories':excluded,'source_project_preserved':True}),flush=True)
'''.replace('APPROVAL',repr(APPROVAL))


def main():
    os.umask(0o077)
    with (ORIGINAL/'transfer.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        gate=json.loads((WORK/'retirement-execution-gates.json').read_text())
        assert gate['approval']==APPROVAL and gate['project_catalog_switched']
        assert gate['linux_windows_project_validated'] and gate['api_dependencies_resolved']
        assert (datetime.now(timezone.utc)-datetime.fromisoformat(gate['checked_at'])).total_seconds()<300
        inventory=json.loads((RAW/'state.json').read_text())
        assert inventory['phase']=='metadata_and_prior_checksum_gates_passed'
        assert inventory['source_inventory']['sha256']=='bfe6b368e2c8d0a2de897c6351733060e2e0e62870b30a391afd14a40f52e063'
        assert inventory['source_regular_files_unchanged'] and inventory['destination_paths_and_sizes_match']
        assert inventory['previous_full_content_verification_passed'] and inventory['destination_mtime_differences']['over_1000ns_count']==0
        assert hashlib.sha256((RAW/'source-current.jsonl').read_bytes()).hexdigest()==inventory['source_inventory']['sha256']
        assert hashlib.sha256((RAW/'destination-current.jsonl').read_bytes()).hexdigest()==inventory['destination_inventory']['sha256']
        files=[r for line in (RAW/'source-current.jsonl').read_text().splitlines() if (r:=json.loads(line))['kind']=='file']
        request={'approved_source_root':'/volume1/DATA/Alexander/data/2023_2','approval':APPROVAL,'files':files,
                 'project_original_preserved':True,'linux_windows_project_validated':True,'gates':gate}
        request_path=RAW/'retirement-request.json';assert not request_path.exists()
        request_path.write_text(json.dumps(request))
        state={'phase':'retiring_verified_source_files','started_at':datetime.now(timezone.utc).isoformat(),'files_unlinked':0,'bytes_unlinked':0,
               'approval':APPROVAL,'gate_batch_id':gate['batch_id'],'source_manifest_sha256':inventory['source_inventory']['sha256']}
        def save():
            state['heartbeat_at']=datetime.now(timezone.utc).isoformat()
            p=RAW/'retirement-state.tmp';p.write_text(json.dumps(state,indent=2)+'\n');p.replace(RAW/'retirement-state.json')
        save();launcher='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
        args=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-o','IdentitiesOnly=yes',
              '-i','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519','Gilles@10.20.11.250',shlex.join(['nice','-n','19','python3','-u','-c',launcher])]
        complete=None;last=time.monotonic()
        try:
            with request_path.open('rb') as source,(RAW/'unlink-receipt.jsonl').open('x') as receipt,(RAW/'unlink.errors.log').open('xb') as errors:
                p=subprocess.Popen(args,stdin=source,stdout=subprocess.PIPE,stderr=errors)
                for line in p.stdout:
                    row=json.loads(line);receipt.write(line.decode())
                    if row['event']=='unlinked':state['files_unlinked']+=1;state['bytes_unlinked']+=row['bytes']
                    elif row['event']=='complete':complete=row
                    else:state['native_preflight']=row
                    if time.monotonic()-last>5:receipt.flush();save();last=time.monotonic()
                assert p.wait()==0,'Native retirement failed; inspect receipt and error log before any retry'
            assert complete and complete['files_unlinked']==425112 and complete['logical_bytes_unlinked']==3583194799325
            state.update(phase='source_manifest_retired_catalog_finalization_pending',result=complete,
                         receipt_sha256=hashlib.sha256((RAW/'unlink-receipt.jsonl').read_bytes()).hexdigest());save()
        except Exception as exc:
            state.update(phase='retirement_stopped_requires_review',error=type(exc).__name__+': '+str(exc));save();raise


if __name__=='__main__':main()
