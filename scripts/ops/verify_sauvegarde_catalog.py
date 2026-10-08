"""Read back the applied catalog manifest; do not inspect or mutate workers."""
import json
from pathlib import Path
from register_alexander_2023_manifest import remote_json

VERIFY = r'''
import json,sys
from uuid import UUID
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Project
expected=json.load(sys.stdin)
ids=[UUID(row['project_id']) for row in expected]
assert len(ids)==1486 and len(set(ids))==1486
by_id={row['project_id']:row for row in expected}
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 projects=list(db.scalars(select(Project).where(Project.id.in_(ids))))
 assert len(projects)==1486
 counts={}; kinds={}; links=[]
 for p in projects:
  assert p.visibility=='private' and p.health_status=='warning'
  assert p.owner.user_key==by_id[str(p.id)]['owner']
  assert len(p.locations)==1 and p.locations[0].access_mode=='readonly'
  assert p.locations[0].storage_root.host_scope=='detecdiv-server'
  assert p.locations[0].storage_root.path_prefix.startswith('/mnt/detecdiv-secondary-sauvegarde/')
  key=p.owner.user_key; counts[key]=counts.get(key,0)+1
  kind=(p.metadata_json.get('sauvegarde_ingestion') or {}).get('project_kind','detecdiv_shallow')
  kinds[kind]=kinds.get(kind,0)+1
  links.extend({'project_id':str(p.id),'raw_id':str(l.raw_dataset_id)} for l in p.raw_links)
 assert counts=={'alexander':150,'Basile':558,'sandrine':778}
 assert len(links)==1 and links[0]['raw_id']=='d43dff09-d53f-4d27-9264-7eef08ebb444'
 print(json.dumps({'verified_projects':len(projects),'owners':counts,'formats':kinds,
                   'all_private':True,'all_source_locations_readonly':True,
                   'previous_exact_raw_link_preserved':True,'raw_links_total':len(links)}))
'''

repo=Path(__file__).resolve().parents[2]
manifest=json.loads((repo/'reports/sauvegarde-registration-20261001.json').read_text(encoding='utf-8'))
assert manifest['complete'] and manifest['applied'] and not manifest['pending_headers']
result=remote_json('C:/Windows/System32/OpenSSH/ssh.exe','webserver-labo',VERIFY,manifest['projects'],container=True)
(repo/'reports/sauvegarde-catalog-verification-20261001.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result))
