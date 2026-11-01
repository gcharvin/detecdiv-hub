"""Keep verified preview/settings content API-visible before source retirement."""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import shutil

WORK=Path(__file__).resolve().parent
PLAN=WORK/'api-media-plan.json'


def main():
    plan=json.loads(PLAN.read_text());assert plan['year']=='2023_1'
    source=Path('/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_1')
    destination=Path('/data/.detecdiv-hub-api-cache/alexander-2023_1')
    assert not destination.exists(),'Inspect an existing cache before any retry'
    destination.mkdir(mode=0o750)
    state={'phase':'copying_api_projection','files':0,'bytes':0,'source_deleted':False}
    def save():
        state['heartbeat_at']=datetime.now(timezone.utc).isoformat()
        p=WORK/'api-media-state.tmp';p.write_text(json.dumps(state,indent=2)+'\n');p.replace(WORK/'api-media-state.json')
    save()
    try:
        with (WORK/'api-media-receipt.jsonl').open('x') as receipt:
            for row in plan['files']:
                relative=Path(row['path']);assert not relative.is_absolute() and '..' not in relative.parts
                assert relative.suffix.lower() in ['.mp4','.json']
                src=source/relative;dst=destination/relative
                before=src.stat();assert src.is_file() and not src.is_symlink() and before.st_size==row['bytes']
                dst.parent.mkdir(mode=0o750,parents=True,exist_ok=True)
                h=hashlib.sha1()
                with src.open('rb') as a,dst.open('xb') as b:
                    for block in iter(lambda:a.read(4*1024*1024),b''):h.update(block);b.write(block)
                after=src.stat();assert (after.st_size,after.st_mtime_ns)==(before.st_size,before.st_mtime_ns)
                os.utime(dst,ns=(before.st_atime_ns,before.st_mtime_ns))
                other=hashlib.sha1()
                with dst.open('rb') as stream:
                    for block in iter(lambda:stream.read(4*1024*1024),b''):other.update(block)
                assert h.digest()==other.digest()
                receipt.write(json.dumps({'path':row['path'],'bytes':before.st_size,'sha1':h.hexdigest(),'api_path':str(dst),'source_preserved':True})+'\n')
                state['files']+=1;state['bytes']+=before.st_size
                if state['files']%25==0:receipt.flush();save()
        assert state['files']==len(plan['files']) and state['bytes']==sum(r['bytes'] for r in plan['files'])
        state.update(phase='verified_ready_for_api_reference_update',finished_at=datetime.now(timezone.utc).isoformat());save()
    except Exception as error:state.update(phase='failed_safe',error=type(error).__name__+': '+str(error));save();raise


if __name__=='__main__':main()
