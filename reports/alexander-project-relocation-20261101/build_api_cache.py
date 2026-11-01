"""Preserve only previews/display JSON on the API-visible primary NAS.

The 5.87-GB projection avoids an API mount/deployment change while retiring
3.58 TB of raw duplicates. All copied files are independently content-checked.
"""
import base64
from datetime import datetime,timezone
import json
from pathlib import Path
import shlex
import subprocess
import time

WORK=Path(__file__).resolve().parent
NATIVE=r'''
import json,sys,os,stat,fcntl,hashlib,shutil,errno
from pathlib import Path
files=json.load(sys.stdin)
source=Path('/volume1/DATA/Alexander/data/2023_2')
dest=Path('/volume1/DATA/.detecdiv-hub-api-cache/alexander-2023_2')
assert not dest.exists();dest.mkdir(mode=0o750,parents=True)
def digest(path):
 h=hashlib.sha1()
 with path.open('rb') as f:
  for data in iter(lambda:f.read(4*1024*1024),b''):h.update(data)
 return h.hexdigest()
for row in files:
 rel=Path(row['path']);assert not rel.is_absolute() and '..' not in rel.parts
 assert rel.suffix in ('.mp4','.json')
 src=source/rel;dst=dest/rel;s=src.lstat()
 assert stat.S_ISREG(s.st_mode) and s.st_size==row['bytes'] and s.st_mtime_ns==row['mtime_ns']
 dst.parent.mkdir(mode=0o750,parents=True,exist_ok=True)
 method='independent_reflink'
 with src.open('rb') as source_stream,dst.open('xb') as target:
  try:fcntl.ioctl(target.fileno(),0x40049409,source_stream.fileno())
  except OSError as error:
   if error.errno not in (errno.EXDEV,errno.EOPNOTSUPP,errno.ENOTTY,errno.EINVAL):raise
   method='full_copy';source_stream.seek(0);target.seek(0);shutil.copyfileobj(source_stream,target,4*1024*1024)
 os.chmod(dst,stat.S_IMODE(s.st_mode));os.utime(dst,ns=(s.st_atime_ns,s.st_mtime_ns))
 assert (src.stat().st_dev,src.stat().st_ino)!=(dst.stat().st_dev,dst.stat().st_ino)
 a=digest(src);b=digest(dst);assert a==b
 print(json.dumps({'path':row['path'],'bytes':s.st_size,'sha1':a,'method':method,'cache_path':'/data/.detecdiv-hub-api-cache/alexander-2023_2/'+row['path'],'source_preserved':True}),flush=True)
'''

def main():
    request=WORK/'api-cache-plan.json';rows=json.loads(request.read_text())['rows']
    assert len(rows)==354 and sum(r['bytes'] for r in rows)==5874728850
    state={'phase':'copy_and_verify','files':0,'bytes':0,'source_deleted':False}
    def save():
        state['heartbeat_at']=datetime.now(timezone.utc).isoformat()
        tmp=WORK/'api-cache-state.tmp';tmp.write_text(json.dumps(state,indent=2)+'\n');tmp.replace(WORK/'api-cache-state.json')
    save()
    launcher='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
    args=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-o','IdentitiesOnly=yes',
          '-i','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519','Gilles@10.20.11.250',shlex.join(['nice','-n','19','python3','-u','-c',launcher])]
    inputs=WORK/'api-cache-request.json';inputs.write_text(json.dumps(rows))
    count=size=0;last=time.monotonic()
    try:
        with inputs.open('rb') as input_file,(WORK/'api-cache-receipt.jsonl').open('x') as receipt,(WORK/'api-cache.errors.log').open('xb') as errors:
            p=subprocess.Popen(args,stdin=input_file,stdout=subprocess.PIPE,stderr=errors)
            try:
                for line in p.stdout:
                    row=json.loads(line);receipt.write(line.decode());count+=1;size+=row['bytes']
                    if time.monotonic()-last>5:state.update(files=count,bytes=size);save();last=time.monotonic()
                assert p.wait()==0,'API projection copy failed; inspect error log'
            finally:
                if p.poll() is None:p.terminate();p.wait()
        assert count==354 and size==5874728850
        state.update(phase='verified_ready_for_api_reference_update',files=count,bytes=size,cache_root='/data/.detecdiv-hub-api-cache/alexander-2023_2');save()
    except Exception as exc:state.update(phase='failed_safe',error=str(exc));save();raise

if __name__=='__main__':main()
