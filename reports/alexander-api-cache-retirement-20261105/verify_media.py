"""Read-only full cache/home comparison or actual content-handler streaming."""
import asyncio
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import uuid

from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Artifact,RawDataset,User,StorageMigrationBatch
from api.routes_raw_datasets import get_raw_preview_artifact_content,get_raw_dataset_display_settings

WORK=Path(__file__).resolve().parent
ROOTS={'2023_1':'/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_1',
       '2023_2':'/homes2/maliavko/DetecdivHub/raw/2023_2',
       '2023_3':'/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_3'}
CACHE='/data/.detecdiv-hub-api-cache/alexander-'
STATE={'phase':'preflight','files_verified':0,'bytes_verified':0,'filesystem_deletion_performed':False}


def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=WORK/'state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/'state.json')


def hash_file(path):
    path=Path(path);parts=path.parts;fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        for part in parts[1:-1]:
            child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd);os.close(fd);fd=child
        file=os.open(parts[-1],os.O_RDONLY|os.O_NOFOLLOW,dir_fd=fd)
    finally:os.close(fd)
    try:
        before=os.fstat(file);assert stat.S_ISREG(before.st_mode)
        h=hashlib.sha1();size=0
        while True:
            block=os.read(file,4*1024*1024)
            if not block:break
            h.update(block);size+=len(block)
        after=os.fstat(file)
        assert (before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns) and size==before.st_size,'Media changed during read'
        return h.hexdigest(),size,before.st_mtime_ns
    finally:os.close(file)


async def stream_response(response):
    h=hashlib.sha1();count=0;started=False
    async def receive():return {'type':'http.request','body':b'','more_body':False}
    async def send(message):
        nonlocal count,started
        if message['type']=='http.response.start':assert message['status']==200;started=True
        elif message['type']=='http.response.body':
            block=message.get('body',b'');count+=len(block);h.update(block)
        else:raise ValueError('Unexpected content-response message')
    await response({'type':'http','method':'GET','headers':[],'path':'/media-storage-audit','asgi':{'version':'3.0'},'extensions':{}},receive,send)
    assert started
    return h.hexdigest(),count


def main():
    os.umask(0o077);assert not (WORK/'state.json').exists() and not (WORK/'receipt.jsonl').exists()
    request=json.loads((WORK/'request.json').read_text())
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==request['runner_sha256']
    mode=request['mode'];assert mode in {'pairs','handlers','post-retirement'}
    refs=request['plan']['references'];assert len(refs)==1926
    seen=set()
    for row in refs:
        key=(row['year'],row['relative_path']);assert key not in seen;seen.add(key)
        assert row['year'] in ROOTS and row['relative_path'] and not Path(row['relative_path']).is_absolute() and '..' not in Path(row['relative_path']).parts
        assert row['before']==CACHE+row['year']+'/'+row['relative_path']
        assert row['after']==ROOTS[row['year']]+'/'+row['relative_path']
    expected={(r['year'],r['relative_path']):r for r in request.get('pairs',[])}
    if mode!='pairs':assert len(expected)==1926
    STATE['mode']=mode;save()
    with SessionLocal() as db:
        db.execute(text('SET TRANSACTION READ ONLY'))
        operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
        if mode!='pairs':
            batch=db.get(StorageMigrationBatch,uuid.UUID(request['batch_id']))
            assert batch.status==('cache_retirement_prepared' if mode=='post-retirement' else 'home_uris_switched_verification_pending')
    STATE['phase']='comparing_full_cache_and_home_contents' if mode=='pairs' else 'streaming_actual_home_content_handlers';save()
    with (WORK/'receipt.jsonl').open('x') as out:
        for row in refs:
            with SessionLocal() as db:
                db.execute(text('SET TRANSACTION READ ONLY'))
                raw=db.get(RawDataset,uuid.UUID(row['raw_id']));assert raw.owner.user_key=='alexander'
                obj=db.get(Artifact if row['kind']=='artifact' else RawDataset,uuid.UUID(row['id']))
                assert (obj.uri if row['kind']=='artifact' else obj.display_settings_uri)==row['before' if mode=='pairs' else 'after']
                if row['kind']=='artifact':assert obj.job.raw_dataset_id==raw.id
                if mode!='pairs':
                    operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
                    response=get_raw_preview_artifact_content(obj.id,db,operator) if row['kind']=='artifact' else get_raw_dataset_display_settings(obj.id,db,operator)
                    assert str(response.path)==row['after']
            if mode=='pairs':
                a,size,cache_mtime=hash_file(row['before']);b,target_size,home_mtime=hash_file(row['after'])
                assert a==b and size==target_size,'Existing cache and homes2 media differ'
                result={'year':row['year'],'relative_path':row['relative_path'],'bytes':size,'sha1':a,'cache_mtime_ns':cache_mtime,'home_mtime_ns':home_mtime,'cache_and_home_equal':True}
            else:
                digest,size=asyncio.run(stream_response(response));prior=expected[row['year'],row['relative_path']]
                assert digest==prior['sha1'] and size==prior['bytes'],'API content differs from the verified media'
                result={'year':row['year'],'relative_path':row['relative_path'],'kind':row['kind'],'id':row['id'],'bytes':size,'sha1':digest,'actual_home_handler_stream_verified':True}
            out.write(json.dumps(result)+'\n');STATE['files_verified']+=1;STATE['bytes_verified']+=size
            if STATE['files_verified']%25==0:out.flush();save()
    assert STATE['files_verified']==1926 and STATE['bytes_verified']==28981723684
    STATE.update(phase={'pairs':'all_cache_home_pairs_verified','handlers':'all_home_handlers_verified','post-retirement':'post_retirement_home_handlers_verified'}[mode],success=True,receipt_sha256=hashlib.sha256((WORK/'receipt.jsonl').read_bytes()).hexdigest(),finished_at=datetime.now(timezone.utc).isoformat());save()


if __name__=='__main__':
    try:main()
    except Exception as error:
        STATE.update(phase='failed_safe_preserve_caches',success=False,error=type(error).__name__+': '+str(error));save();raise
