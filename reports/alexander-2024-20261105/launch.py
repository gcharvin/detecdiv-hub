"""One-shot copy launch; originals and preferred locations stay untouched."""
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import uuid

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import api, remote


def main():
    admission=json.loads((HERE/'catalog-admission-summary.json').read_text())
    assert (datetime.now(timezone.utc)-datetime.fromisoformat(admission['checked_at'])).total_seconds()<1800
    assert admission['raws']==115 and admission['raw_owners']==['alexander']
    assert not any(j['affected_raw'] for j in admission['active_jobs']) and not admission['indexing']
    snapshot=(HERE/'runtime/catalog-before.json').read_bytes()
    script=(HERE/'copy_raws.py').read_bytes()
    request={'batch_name':'alexander-2024-20261105','batch_id':str(uuid.uuid4()),'years':['2024_1','2024_2'],
             'bandwidth_kib':61440,'estimated_total_bytes':11034331124398,
             'initial_measurements':{'2024_1':{'files':732579,'bytes':5177814875236},'2024_2':{'files':974112,'bytes':5856516249162}},
             'source_deletion_allowed':False,'catalog_cutover_allowed':False,
             'runner_sha256':hashlib.sha256(script).hexdigest(),
             'catalog_snapshot_sha256':hashlib.sha256(snapshot).hexdigest(),
             'approved_scope':'Copy 2024_1 then 2024_2; discover/relink associated project copies, retain originals; retire /data only after complete verification.',
             'minimum_free_margin_bytes':2000000000000}
    with (HERE/'request.json').open('x',encoding='utf-8') as f:json.dump(request,f,indent=2);f.write('\n')
    batchcode=r'''
import json,uuid
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import User,StorageMigrationBatch,Job,IndexingJob
request=REQUEST
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='5s'"))
 assert not list(db.scalars(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==request['batch_name'])))
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  assert '/data/Alexander/data/2024' not in json.dumps(j.params_json or {}),'Affected raw path job appeared'
 owner=db.scalars(select(User).where(User.user_key=='alexander')).one()
 b=StorageMigrationBatch(id=uuid.UUID(request['batch_id']),owner_user_id=owner.id,batch_name=request['batch_name'],source_kind='raw_data',
  source_path='/data/Alexander/data/2024_{1,2}',strategy='copy_verify_relink_then_retire_verified_sources',status='copy_preflight',
  metadata_json={'request':request,'original_projects_must_be_preserved':True},summary_json={'raw_datasets':115,'source_deleted':False,'catalog_cutover_performed':False})
 db.add(b)
 print(json.dumps({'batch_id':str(b.id),'status':b.status,'owner':'alexander','catalog_locations_changed':False}))
'''.replace('REQUEST',repr(request))
    batch=api(batchcode)
    (HERE/'batch-created.json').write_text(json.dumps(batch,indent=2)+'\n',encoding='utf-8')
    code=r'''
import base64,hashlib,json,os,shlex,subprocess
from pathlib import Path
os.umask(0o077)
work=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
stage=Path('/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105')
task='alex2024-copy-20261105'
assert not work.exists() and not stage.exists(), 'Existing work requires inspection, never overwrite'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
request=REQUEST
script=base64.b64decode(SCRIPT);snapshot=base64.b64decode(SNAPSHOT)
assert hashlib.sha256(script).hexdigest()==request['runner_sha256']
assert hashlib.sha256(snapshot).hexdigest()==request['catalog_snapshot_sha256']
assert Path('/homes2/maliavko/DetecdivHub/raw').is_dir()
work.mkdir(mode=0o700);stage.mkdir(mode=0o700)
(work/'copy_raws.py').write_bytes(script);(work/'catalog-before.json').write_bytes(snapshot)
(work/'request.json').write_text(json.dumps(request,indent=2)+'\n')
args=['/home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python','-u',str(work/'copy_raws.py')]
shell=shlex.join(args)+' > '+shlex.quote(str(work/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(work/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(work),'staging':str(stage),'batch_id':request['batch_id'],
 'runner_sha256':request['runner_sha256'],'source_deleted':False,'catalog_locations_changed':False,'years':request['years']}))
'''.replace('REQUEST',repr(request)).replace('SCRIPT',repr(base64.b64encode(script).decode())).replace('SNAPSHOT',repr(base64.b64encode(snapshot).decode()))
    result=remote('linux',code,timeout=60)
    (HERE/'launch.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
