"""Catalog header-valid Sauvegarde projects without raw linking or file changes.

Owner mapping is explicit; unknown formats remain in the review report.
Existing canonical file locations are preserved, including the 2023 pilot.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path, PurePosixPath
from register_alexander_2023_manifest import remote_json


OWNERS = {
    'Alexander': ('alexander', 'alexander-sauvegarde'),
    'Alumni/Sandrine': ('sandrine', 'sandrine-sauvegarde'),
    'Alumni/basile': ('Basile', 'basile-sauvegarde'),
}

PROBE = r'''
import json,sys
from pathlib import Path
rows=json.load(sys.stdin)
root=Path('/mnt/detecdiv-secondary-sauvegarde')
mount=next(line for line in Path('/proc/self/mountinfo').read_text().splitlines()
           if line.split(' - ')[0].split()[4]==str(root))
assert 'ro' in mount.split(' - ')[0].split()[5].split(',')
assert mount.split(' - ')[1].split()[:2]==['cifs','//10.20.8.250/Sauvegarde']
for row in rows:
 path=root/row['relative_path']
 path.relative_to(root)
 assert '..' not in Path(row['relative_path']).parts and not Path(row['relative_path']).is_absolute()
 st=path.stat()
 assert st.st_size==row['mat_bytes'], 'Source size changed: '+str(path)
 assert abs(st.st_mtime_ns-row['mtime_ns'])<1000, 'Source time changed: '+str(path)
 if row['paired_directory_present']: assert path.with_suffix('').is_dir()
print(json.dumps({'verified_files':len(rows),'source_readonly':True}))
'''

REGISTER = r'''
import json,sys
from datetime import datetime,timezone
from pathlib import Path
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Project,ProjectLocation,StorageRoot,User
from api.routes_projects import register_project_path_record
from api.schemas import ProjectPathRegistrationRequest
from api.services.path_resolution import compose_storage_path
data=json.load(sys.stdin); results=[]
with SessionLocal() as db:
 if data['apply']:
  db.execute(text("SELECT pg_advisory_xact_lock(hashtext('sauvegarde-header-ingestion'))"))
 else:
  db.execute(text('SET TRANSACTION READ ONLY'))
 operator=db.scalar(select(User).where(User.user_key=='gilles',User.role=='admin'))
 assert operator is not None
 owners={key:db.scalar(select(User).where(User.user_key==key)) for key in data['owners']}
 assert all(value is not None for value in owners.values()), 'Never create users from folder names'
 known={}
 for location in db.scalars(select(ProjectLocation)):
  path=compose_storage_path(location.storage_root.path_prefix,location.relative_path,location.project_file_name)
  if path.startswith('/mnt/detecdiv-secondary-sauvegarde/'):
   assert path not in known, 'Duplicate source location already in catalog: '+path
   known[path]=location
 for row in data['projects']:
  path=row['project_mat_path']
  if path in known:
   location=known[path]
   assert location.project.owner.user_key==row['owner'], 'Unexpected existing owner'
   results.append({'path':row['relative_path'],'owner':row['owner'],
                   'project_id':str(location.project_id),'action':'unchanged_existing'})
   continue
  root=db.scalar(select(StorageRoot).where(StorageRoot.name==row['root_name']))
  assert root is None or (root.path_prefix==row['root_path'] and root.host_scope=='detecdiv-server')
  if not data['apply']:
   results.append({'path':row['relative_path'],'owner':row['owner'],'action':'would_register_readonly'})
   continue
  payload=ProjectPathRegistrationRequest(project_mat_path=path,project_dir_path=row['project_dir_path'],
      root_path=row['root_path'],storage_root_name=row['root_name'],host_scope='detecdiv-server',
      root_type='project_root',owner_user_key=row['owner'],visibility='private',
      metadata_json={'manifest':'sauvegarde-project-discovery-20261001.json',
                    'source_mtime_ns':row['mtime_ns'],'source_file_bytes':row['mat_bytes']})
  project=register_project_path_record(db,payload=payload,current_user=operator)
  metadata=dict(project.metadata_json or {})
  metadata.update({'source':'ops_readonly_manifest_registration',
    'storage_is_shared_with_raw_dataset':row['project_kind']=='legacy_matlab_timelapse',
    'sauvegarde_ingestion':{'batch':'sauvegarde-all-years-20261001',
       'operator':'gilles','transport':'authenticated_admin_ssh',
       'registered_at':datetime.now(timezone.utc).isoformat(),
       'project_kind':row['project_kind'],'mat_header_verified':True,
       'paired_directory_present':row['paired_directory_present'],
       'source_preserved':True,'raw_link_status':'not_attempted',
       'directory_inventory_status':'not_measured','scientific_jobs_queued':False}})
  project.metadata_json=metadata
  project.health_status='warning'
  project.project_mat_bytes=row['mat_bytes']
  project.total_bytes=row['mat_bytes']
  project.notes='Catalogage de Sauvegarde en lecture seule. Inventaire du dossier et associations aux raws non effectués.'
  db.flush()
  locations=list(db.scalars(select(ProjectLocation).where(ProjectLocation.project_id==project.id)))
  assert len(locations)==1 and locations[0].storage_root.path_prefix==row['root_path']
  locations[0].access_mode='readonly'
  known[path]=locations[0]
  results.append({'path':row['relative_path'],'owner':row['owner'],
                  'project_id':str(project.id),'action':'registered_readonly'})
 if data['apply']: db.commit()
 else: db.rollback()
print(json.dumps({'applied':data['apply'],'projects':results,
                  'raw_links_created':0,'scientific_files_changed':False,'jobs_queued':False}))
'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--allow-partial', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    discovery = json.loads((repo/'reports/sauvegarde-project-discovery-20261001.json').read_text(encoding='utf-8'))
    headers = json.loads((repo/'reports/sauvegarde-project-headers-20261001.json').read_text(encoding='utf-8'))
    assert discovery['complete'] and not discovery['errors']
    assert args.allow_partial or (headers['complete'] and len(discovery['candidates'])==len(headers['candidates']))
    inspected = {row['relativePath']:row for row in headers['candidates']}
    source_root = PurePosixPath('/mnt/detecdiv-secondary-sauvegarde')
    selected=[]; excluded=[]; pending=[]
    for item in discovery['candidates']:
        row = inspected.get(item['relative_path'])
        if row is None:
            pending.append(item['relative_path'])
            continue
        assert int(row['matBytes'])==item['mat_bytes']
        if row['status']!='valid_project_header':
            excluded.append({'path':item['relative_path'],'status':row['status'],'reason':row['review']})
            continue
        relative = PurePosixPath(item['relative_path'])
        assert not relative.is_absolute() and '..' not in relative.parts
        owner_prefix = next(key for key in OWNERS if relative.is_relative_to(PurePosixPath(key)))
        owner,root_name = OWNERS[owner_prefix]
        root_relative = PurePosixPath(owner_prefix)
        if relative.is_relative_to(PurePosixPath('Alexander/analysis')):
            root_relative=PurePosixPath('Alexander/analysis')
            root_name='alexander-sauvegarde-analysis'
        selected.append({**item,'owner':owner,'root_name':root_name,
            'root_path':str(source_root/root_relative),'project_kind':row['projectKind'],
            'project_mat_path':str(source_root/relative),
            'project_dir_path':str(source_root/item['project_dir_relative'])})
    ssh='C:/Windows/System32/OpenSSH/ssh.exe'
    evidence=remote_json(ssh,'detecdiv-server',PROBE,selected)
    # Bound each database transaction and SSH response, retaining an audit checkpoint.
    result={'captured_at':datetime.now(timezone.utc).isoformat(),'applied':args.apply,
            'source_evidence':evidence,'projects':[],'excluded_review':excluded,'pending_headers':pending,
            'raw_links_created':0,'scientific_files_changed':False,'jobs_queued':False,'complete':False}
    for start in range(0,len(selected),100):
        batch=remote_json(ssh,'webserver-labo',REGISTER,{'apply':args.apply,
            'projects':selected[start:start+100],'owners':sorted({r['owner'] for r in selected})},container=True)
        result['projects'].extend(batch['projects'])
        args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
        print(json.dumps({'processed':len(result['projects']),'selected':len(selected),'excluded':len(excluded)}),flush=True)
    result['complete']=not pending and headers['complete']
    args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')


if __name__ == '__main__':
    main()
