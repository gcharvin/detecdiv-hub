"""Fresh native NAS metadata inventories for 2023_1; no content writes/cutover."""
from datetime import datetime, timezone
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

WORK = Path(__file__).resolve().parent
ORIGINAL = Path('/home/charvin-admin/storage-transfers/alexander-2023-20261008')
SOURCE = '/volume1/DATA/Alexander/data/2023_1'
DEST = '/volume1/homes/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_1'
NATIVE = r'''
import json,os,stat,sys
from pathlib import Path
root=Path(sys.argv[1])
assert str(root) in ALLOWED
assert root.is_dir() and root.resolve()==root
ignored={'@eaDir','#recycle','#snapshot','.rsync-partial'}
def fail(exc):raise exc
for parent,dirs,files in os.walk(root,followlinks=False,onerror=fail):
 dirs[:]=sorted(d for d in dirs if d not in ignored)
 for name in dirs:
  if (Path(parent)/name).is_symlink():raise RuntimeError('Directory symlink requires review')
 print(json.dumps({'kind':'directory','path':str(Path(parent).relative_to(root))},sort_keys=True))
 for name in sorted(files):
  if name in ignored:continue
  p=Path(parent)/name;s=p.lstat()
  if not stat.S_ISREG(s.st_mode):raise RuntimeError('Non-regular payload requires review')
  print(json.dumps({'kind':'file','path':str(p.relative_to(root)),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns},sort_keys=True))
'''.replace('ALLOWED', repr([SOURCE, DEST]))
state = {'started_at':datetime.now(timezone.utc).isoformat(), 'pid':os.getpid(), 'phase':'preflight',
         'content_files_read':False, 'source_deleted':False, 'database_changed':False}

def save():
    state['heartbeat_at'] = datetime.now(timezone.utc).isoformat()
    temporary = WORK/'state.tmp'
    temporary.write_text(json.dumps(state,indent=2)+'\n')
    temporary.replace(WORK/'state.json')

def inventory(label, host, key, root):
    launcher = "import base64;exec(base64.b64decode('"+base64.b64encode(NATIVE.encode()).decode()+"'))"
    native = shlex.join(['nice','-n','19','python3','-u','-c',launcher,root])
    command = ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','-o','ServerAliveInterval=15',
               '-o','ServerAliveCountMax=3','-o','IdentitiesOnly=yes','-i',key,host,native]
    output=WORK/(label+'.jsonl');errors=WORK/(label+'.errors.log')
    assert not output.exists(), 'Inventory output already exists; inspect before rerunning'
    state.update(phase='inventory_'+label, files=0, bytes=0)
    save()
    digest=hashlib.sha256();count=size=0;last=time.monotonic()
    with output.open('xb') as stream,errors.open('xb') as error:
        p=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=error)
        state['native_ssh_pid']=p.pid
        try:
            for line in p.stdout:
                row=json.loads(line);stream.write(line);digest.update(line)
                if row['kind']=='file':count+=1;size+=row['bytes']
                if time.monotonic()-last>10:
                    state.update(files=count,bytes=size);save();last=time.monotonic()
            rc=p.wait()
            assert rc==0, label+' native inventory failed; see error log'
        finally:
            if p.poll() is None:p.terminate();p.wait()
    return {'files':count,'bytes':size,'sha256':digest.hexdigest()}

def records(path):
    files={};dirs=set()
    with path.open() as stream:
        for line in stream:
            row=json.loads(line)
            if row['kind']=='file':
                assert row['path'] not in files
                files[row['path']]={'bytes':row['bytes'],'mtime_ns':row['mtime_ns']}
            else:dirs.add(row['path'])
    return files,dirs

def difference(before,after):
    removed=sorted(before.keys()-after.keys());added=sorted(after.keys()-before.keys())
    modified=sorted(k for k in before.keys()&after.keys() if before[k]!=after[k])
    return {'added_count':len(added),'removed_count':len(removed),'modified_count':len(modified),
            'added_sample':added[:25],'removed_sample':removed[:25],'modified_sample':modified[:25]}

def main():
    os.umask(0o077)
    with (ORIGINAL/'transfer.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        save()
        source=inventory('source-current','Gilles@10.20.11.250',
                         '/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519',SOURCE)
        before,old_dirs=records(ORIGINAL/'2023_1-before.jsonl')
        current,new_dirs=records(WORK/'source-current.jsonl')
        source_diff=difference(before,current)
        source_unchanged=not any(source_diff[k] for k in ('added_count','removed_count','modified_count'))
        state.update(source_inventory=source,source_file_difference=source_diff,
                     source_regular_files_unchanged=source_unchanged,
                     source_directory_changes={'added':len(new_dirs-old_dirs),'removed':len(old_dirs-new_dirs)})
        save()
        if not source_unchanged:
            state['phase']='source_changed_requires_review';save();return
        target=inventory('destination-current','charvin-admin@10.20.8.250',
                         '/home/charvin-admin/.ssh/detecdiv_backup_ed25519',DEST)
        copied,target_dirs=records(WORK/'destination-current.jsonl')
        size_diff=difference({k:v['bytes'] for k,v in current.items()},
                             {k:v['bytes'] for k,v in copied.items()})
        equivalent=not any(size_diff[k] for k in ('added_count','removed_count','modified_count'))
        time_diff=[{'path':k,'nanoseconds':copied[k]['mtime_ns']-v['mtime_ns']} for k,v in current.items()
                   if k in copied and copied[k]['mtime_ns']!=v['mtime_ns']]
        significant=[x for x in time_diff if abs(x['nanoseconds'])>1000]
        prior=json.loads((ORIGINAL/'state.json').read_text())['years']['2023_1'];receipt=prior['phase']=='verified_in_staging' and prior['source_stability_verified'] and prior['source_inventory']==source and (ORIGINAL/'2023_1-verify.log').stat().st_size==0
        state.update(destination_inventory=target,destination_file_size_difference=size_diff,
                     destination_paths_and_sizes_match=equivalent,
                     destination_mtime_differences={'count':len(time_diff),'over_1000ns_count':len(significant),'over_1000ns_sample':significant[:25]},
                     previous_full_content_verification_passed=receipt,
                     destination_directory_changes={'added':len(target_dirs-new_dirs),'removed':len(new_dirs-target_dirs)})
        state['phase']='metadata_and_prior_checksum_gates_passed' if equivalent and not significant and receipt else 'destination_requires_review'
        state['finished_at']=datetime.now(timezone.utc).isoformat();save()

if __name__=='__main__':
    try:main()
    except Exception as exc:
        state.update(phase='failed_safe',error=type(exc).__name__+': '+str(exc));save();raise
