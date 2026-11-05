"""Stream every real 2023_3 content handler and verify its complete SHA1."""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import uuid

from sqlalchemy import select, text
from api.db import SessionLocal
from api.models import User, Artifact, RawDataset, StorageMigrationBatch
from api.routes_raw_datasets import get_raw_preview_artifact_content, get_raw_dataset_display_settings

WORK = Path(__file__).resolve().parent
STATE = {'phase': 'preflight', 'handlers_verified': 0, 'bytes_verified': 0, 'source_deleted': False}


def save():
    STATE['heartbeat_at'] = datetime.now(timezone.utc).isoformat()
    tmp=WORK/'state.tmp';tmp.write_text(json.dumps(STATE,indent=2)+'\n');tmp.replace(WORK/'state.json')


async def stream_response(response):
    h=hashlib.sha1();count=0;started=False
    async def receive():
        return {'type':'http.request','body':b'','more_body':False}
    async def send(message):
        nonlocal count,started
        if message['type']=='http.response.start':
            assert message['status']==200;started=True
        elif message['type']=='http.response.body':
            block=message.get('body',b'');count+=len(block);h.update(block)
        else:
            raise ValueError('Unexpected response message')
    scope={'type':'http','method':'GET','headers':[],'path':'/migration-content-verification','asgi':{'version':'3.0'},'extensions':{}}
    await response(scope,receive,send)
    assert started
    return h.hexdigest(),count


def main():
    os.umask(0o077)
    assert not (WORK/'state.json').exists() and not (WORK/'receipt.jsonl').exists()
    request=json.loads((WORK/'request.json').read_text())
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==request['runner_sha256']
    refs=request['plan']['references'];media={x['path']:x for x in request['media']}
    phase=request.get('verification_phase','before_source_retirement')
    assert phase in {'before_source_retirement','after_source_retirement'}
    if phase=='after_source_retirement':assert len(request['source_retirement_receipt_sha256'])==64
    STATE['verification_phase']=phase
    if phase=='after_source_retirement':STATE['source_retirement_receipt_sha256']=request['source_retirement_receipt_sha256']
    assert len(refs)==len(media)==798
    assert sum(x['bytes'] for x in media.values())==12055348763
    save()
    try:
        with SessionLocal() as db, (WORK/'receipt.jsonl').open('x') as receipt:
            db.execute(text('SET TRANSACTION READ ONLY'))
            batch=db.get(StorageMigrationBatch,uuid.UUID(request['batch_id']))
            assert batch.status==('source_retirement_prepared' if phase=='after_source_retirement' else 'catalog_cutover_complete_api_content_verification_pending')
            operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
            STATE['phase']='streaming_real_api_content_handlers';save()
            for row in refs:
                expected='/data/.detecdiv-hub-api-cache/alexander-2023_3/'+row['relative_path']
                if row['kind']=='artifact':
                    obj=db.get(Artifact,uuid.UUID(row['id']));assert obj.uri==expected
                    response=get_raw_preview_artifact_content(obj.id,db,operator)
                else:
                    obj=db.get(RawDataset,uuid.UUID(row['id']));assert obj.display_settings_uri==expected
                    response=get_raw_dataset_display_settings(obj.id,db,operator)
                assert str(response.path)==expected
                digest,count=asyncio.run(stream_response(response))
                evidence=media[row['relative_path']]
                assert digest==evidence['sha1'] and count==evidence['bytes'],'API response differs from verified homes2 media'
                receipt.write(json.dumps({'kind':row['kind'],'id':row['id'],'relative_path':row['relative_path'],'sha1':digest,'bytes':count,'actual_handler_stream_verified':True})+'\n')
                STATE['handlers_verified']+=1;STATE['bytes_verified']+=count
                if STATE['handlers_verified']%25==0:receipt.flush();save()
        assert STATE['handlers_verified']==798 and STATE['bytes_verified']==12055348763
        STATE.update(phase='all_real_api_content_handlers_verified',success=True,receipt_sha256=hashlib.sha256((WORK/'receipt.jsonl').read_bytes()).hexdigest(),finished_at=datetime.now(timezone.utc).isoformat())
        save()
    except Exception as error:
        STATE.update(phase='failed_safe_sources_retained',success=False,error=type(error).__name__+': '+str(error));save();raise


if __name__=='__main__':main()
