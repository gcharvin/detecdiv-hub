"""Readonly comparison with a selected relink admission; never release leases."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from audit import remote_json,SSH

CODE=r'''
import json,sys,uuid,hashlib
from datetime import datetime,timezone
from sqlalchemy import inspect
from api.db import SessionLocal
from api.models import Project,ProjectLock,StorageMigrationBatch
v=json.load(sys.stdin)
def columns(x):return {p.key:getattr(x,p.key) for p in inspect(type(x)).column_attrs}
def digest(x):return hashlib.sha256(json.dumps(x,sort_keys=True,default=str).encode()).hexdigest()
with SessionLocal() as db:
 rows=[]
 for old in v['before_projects']:
  p=db.get(Project,uuid.UUID(old['project_id']));now={'identity_sha256':digest({'key':p.project_key,'owner':str(p.owner_user_id),'visibility':p.visibility,'notes':p.notes,
   'acl':[columns(x) for x in p.acl_entries],'groups':[columns(x) for x in p.group_memberships],'note_entries':[columns(x) for x in p.note_entries],'raw_links':[columns(x) for x in p.raw_links]}),
   'metadata_sha256':digest(p.metadata_json),'locations_sha256':digest([columns(x) for x in p.locations])}
  assert all(now[k]==old[k] for k in now),'Catalog changed: requires examined comparison'
  rows.append({'project_id':str(p.id),'identity_and_metadata_unchanged':True,'all_locations_unchanged':True})
 for lease_id in v['lease_ids']:
  l=db.get(ProjectLock,uuid.UUID(lease_id));assert l.holder_key==v['holder'] and l.status!='active'
 b=db.get(StorageMigrationBatch,uuid.UUID(v['batch_id']));assert b.metadata_json['holder']==v['holder']
 assert b.status=='copied_paths_relinked_pending_raw_and_host_gates'
 print(json.dumps({'success':True,'batch_id':str(b.id),'projects':rows,'child_batch_status':b.status,'own_leases_released':len(v['lease_ids']),
  'catalog_read_only':True,'checked_at':datetime.now(timezone.utc).isoformat()}))
'''

if __name__=='__main__':
    number=int(sys.argv[1]);assert number in [1,2,3,4]
    admission=json.loads((HERE/f'runtime/relink-batch{number}-admission.json').read_text())
    final=json.loads((HERE/f'relink-batch{number}-lease-finalization.json').read_text());assert final['success'] and final['batch_id']==admission['batch_id']
    result=remote_json(SSH,'webserver-labo',CODE,admission,container=True)
    (HERE/f'relink-batch{number}-catalog-preservation-verification.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'success':True,'projects':len(result['projects']),'own_leases_released':result['own_leases_released']}))
