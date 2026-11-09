"""Stream all current year1 API handlers and compare full homes2 contents; no updates."""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time
import uuid
from sqlalchemy import select, text
from api.db import SessionLocal
from api.models import Artifact, RawDataset, User
from api.routes_raw_datasets import get_raw_preview_artifact_content, get_raw_dataset_display_settings

WORK=Path(__file__).resolve().parent
STATE={'phase':'preflight','success':False,'handlers_verified':0,'source_deleted':False,'uri_changed':False}

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(4*1024*1024),b''):
            assert time.time()<CONTROL['deadline_unix'];h.update(block)
    return h.hexdigest()

def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=WORK/'state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/'state.json')

async def streamed(response):
    h=hashlib.sha256();count=0;started=False
    async def receive():return {'type':'http.request','body':b'','more_body':False}
    async def send(message):
        nonlocal count,started
        assert time.time()<CONTROL['deadline_unix']
        if message['type']=='http.response.start':assert message['status']==200;started=True
        elif message['type']=='http.response.body':
            value=message.get('body',b'');count+=len(value);h.update(value)
        else:raise ValueError('Unexpected ASGI response message')
    await response({'type':'http','method':'GET','headers':[],'path':'/readonly-year1-media-verification','asgi':{'version':'3.0'},'extensions':{}},receive,send)
    assert started;return h.hexdigest(),count

def reference_set(db):
    prefix='/data/Alexander/data/2024_1/'
    return {(kind,str(identifier),uri) for kind,rows in [
        ('artifact',db.execute(select(Artifact.id,Artifact.uri).where(Artifact.uri.startswith(prefix)))),
        ('display_settings',db.execute(select(RawDataset.id,RawDataset.display_settings_uri).where(RawDataset.display_settings_uri.startswith(prefix))))]
        for identifier,uri in rows}

def main():
    os.umask(0o077);os.nice(19)
    assert not (WORK/'state.json').exists() and not (WORK/'receipt.jsonl').exists(),'No replay'
    assert sha(Path(__file__))==CONTROL['runner_sha256'] and sha(WORK/'plan.json')==CONTROL['plan_sha256']
    plan=json.loads((WORK/'plan.json').read_text());rows=plan['references']
    assert plan['raw_year1_checksum_and_stability_passed'] and plan['native_source_manifest_sha256']=='8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e'
    assert len(rows)==1204 and len({x['relative_path'] for x in rows})==1204
    expected={(x['kind'],x['id'],x['original_uri']) for x in rows}
    route=Path(__import__('api.routes_raw_datasets',fromlist=['x']).__file__);route_hash=sha(route)
    save()
    with SessionLocal() as db,(WORK/'receipt.jsonl').open('x') as out:
        db.execute(text('SET TRANSACTION READ ONLY'));assert reference_set(db)==expected
        operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
        STATE['phase']='streaming_real_source_handlers_and_homes2_files';save();total=0
        for row in rows:
            assert time.time()<CONTROL['deadline_unix']
            relative=row['relative_path'];source=Path(row['original_uri']);target=Path(row['proposed_homes2_uri'])
            assert source==Path('/data/Alexander/data/2024_1')/relative and target==Path('/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_1')/relative
            assert not Path(relative).is_absolute() and '..' not in Path(relative).parts and source.resolve()==source and target.resolve()==target
            before=[p.stat() for p in [source,target]];assert all(p.is_file() and not p.is_symlink() for p in [source,target])
            assert before[0].st_size==before[1].st_size==row['native_source']['bytes']
            if row['kind']=='artifact':
                obj=db.get(Artifact,uuid.UUID(row['id']));db.refresh(obj);assert obj.uri==row['original_uri'];response=get_raw_preview_artifact_content(obj.id,db,operator)
            else:
                obj=db.get(RawDataset,uuid.UUID(row['id']));db.refresh(obj);assert obj.display_settings_uri==row['original_uri'];response=get_raw_dataset_display_settings(obj.id,db,operator)
            assert Path(response.path)==source
            digest,count=asyncio.run(streamed(response));target_hash=sha(target);assert digest==target_hash and count==before[0].st_size
            after=[p.stat() for p in [source,target]]
            assert all((x.st_ino,x.st_size,x.st_mtime_ns)==(y.st_ino,y.st_size,y.st_mtime_ns) for x,y in zip(before,after))
            db.refresh(obj);assert (obj.uri if row['kind']=='artifact' else obj.display_settings_uri)==row['original_uri']
            evidence={'kind':row['kind'],'id':row['id'],'relative_path':relative,'bytes':count,'sha256':digest,'real_source_handler_stream_verified':True,'api_homes2_full_content_verified':True,'inputs_stable':True}
            line=json.dumps(evidence)+'\n';out.write(line);out.flush();print(line,end='',flush=True)
            total+=count;STATE.update(handlers_verified=STATE['handlers_verified']+1,bytes_verified=total);save()
        assert reference_set(db)==expected and sha(route)==route_hash and total==10858665796
        STATE.update(phase='source_api_handlers_and_homes2_content_verified_no_uri_update',success=True,receipt_sha256=sha(WORK/'receipt.jsonl'),route_source_sha256=route_hash,finished_at=datetime.now(timezone.utc).isoformat());save()
        (WORK/'result.json').write_text(json.dumps(STATE,indent=2)+'\n');print(json.dumps({'kind':'terminal_result',**STATE}),flush=True)

CONTROL=json.loads((WORK/'controller-request.json').read_text()) if __name__=='__main__' else {}
if __name__=='__main__':
    try:main()
    except Exception as error:
        STATE.update(phase='failed_safe_readonly',error_type=type(error).__name__);save();raise
