"""Prepare a three-way verified API cache; never update URIs or delete sources."""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import stat

WORK=Path(__file__).resolve().parent
STATE={'phase':'preflight','files':0,'bytes':0,'source_deleted':False,'catalog_changed':False}
STARTED=False


def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    f=WORK/'state.tmp';f.write_text(json.dumps(STATE,indent=2)+'\n');f.replace(WORK/'state.json')


def sha1(path):
    h=hashlib.sha1()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
    return h.hexdigest()


def main():
    global STARTED
    os.umask(0o077)
    assert not (WORK/'state.json').exists(),'Do not replay over a partial cache'
    request=json.loads((WORK/'request.json').read_text())
    assert request['year']=='2023_3' and len(request['files'])==798
    assert sum(x['bytes'] for x in request['files'])==12055348763
    primary=Path('/data/Alexander/data/2023_3')
    homes=Path('/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_3')
    cache=Path('/data/.detecdiv-hub-api-cache/alexander-2023_3')
    assert not cache.exists(),'Inspect an existing API cache before any retry'
    free=os.statvfs('/data');assert free.f_bavail*free.f_frsize>12055348763+2_000_000_000_000
    STARTED=True;STATE['phase']='copying_verified_api_projection';save();cache.mkdir(mode=0o750)
    with (WORK/'receipt.jsonl').open('x') as receipt:
        for row in request['files']:
            rel=Path(row['path']);assert not rel.is_absolute() and '..' not in rel.parts and rel.suffix.lower() in {'.json','.mp4'}
            a=primary/rel;b=homes/rel;c=cache/rel;old=[]
            for src in [a,b]:
                s=src.lstat();assert stat.S_ISREG(s.st_mode) and s.st_size==row['bytes']
                assert abs(s.st_mtime_ns-row['mtime_ns'])<=100,'Input media timestamp differs from the source manifest'
                old.append((s.st_size,s.st_mtime_ns))
            expected=sha1(a);c.parent.mkdir(mode=0o750,parents=True,exist_ok=True);h=hashlib.sha1()
            with b.open('rb') as source,c.open('xb') as dest:
                for block in iter(lambda:source.read(4*1024*1024),b''):h.update(block);dest.write(block)
            assert h.hexdigest()==expected and sha1(c)==expected,'Primary, homes2 and API projection differ'
            for src,prior in zip([a,b],old):
                s=src.stat();assert (s.st_size,s.st_mtime_ns)==prior,'Input media changed during projection'
            receipt.write(json.dumps({'path':row['path'],'bytes':row['bytes'],'sha1':expected,'primary_homes2_cache_equal':True,'cache_path':str(c),'sources_preserved':True})+'\n')
            STATE['files']+=1;STATE['bytes']+=row['bytes']
            if STATE['files']%25==0:receipt.flush();save()
    assert STATE['files']==798 and STATE['bytes']==12055348763
    STATE.update(phase='verified_ready_for_api_reference_update',finished_at=datetime.now(timezone.utc).isoformat());save()


if __name__=='__main__':
    try:main()
    except Exception as error:
        if STARTED:STATE.update(phase='failed_safe_cache_unpublished',error=type(error).__name__+': '+str(error));save()
        raise
