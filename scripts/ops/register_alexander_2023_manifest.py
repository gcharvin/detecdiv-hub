"""Register the explicit, header-checked pilot as read-only catalog entries.

No recursive scan, scientific job, raw auto-link, file copy or service restart.
Run locally on Windows; --apply is required to commit the catalog transaction.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import subprocess


PROBE = r'''
import json
from pathlib import Path
names=json.load(__import__('sys').stdin)
root=Path('/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis')
lines=Path('/proc/self/mountinfo').read_text().splitlines()
assert any(line.split(' - ')[0].split()[4]==str(root.parent.parent)
           and 'ro' in line.split(' - ')[0].split()[5].split(',')
           and line.split(' - ')[1].split()[:2]==['cifs','//10.20.8.250/Sauvegarde']
           for line in lines), 'Source must be the read-only NAS mount'
rows=[]
for name in names:
 assert Path(name).name==name and name.endswith('.mat')
 p=root/name
 st=p.stat()
 rows.append({'name':name,'bytes':st.st_size,'mtime_ns':st.st_mtime_ns,
              'paired_directory_present':p.with_suffix('').is_dir()})
print(json.dumps(rows))
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
from api.services.project_indexing import build_project_key
data=json.load(sys.stdin)
apply=data['apply']
root='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis'
root_name='alexander-sauvegarde-analysis'
results=[]
with SessionLocal() as db:
 if not apply:
  db.execute(text('SET TRANSACTION READ ONLY'))
 else:
  db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2023-readonly-pilot'))"))
 operator=db.scalar(select(User).where(User.user_key=='gilles',User.role=='admin'))
 assert operator is not None, 'Expected existing Gilles administrator account'
 assert db.scalar(select(User).where(User.user_key=='alexander')) is not None
 named=db.scalar(select(StorageRoot).where(StorageRoot.name==root_name))
 assert named is None or (named.path_prefix==root and named.host_scope=='detecdiv-server')
 for row in data['projects']:
  name=row['file_name']; path=str(Path(root)/name); stem=Path(name).stem
  key=build_project_key(path,'',stem)
  existing=db.scalar(select(Project).where(Project.project_key==key))
  if existing is not None:
   assert existing.owner.user_key=='alexander', 'Unexpected existing owner'
   results.append({'file':name,'id':str(existing.id),'action':'unchanged_existing'})
   continue
  if not apply:
   results.append({'file':name,'action':'would_register_readonly'})
   continue
  payload=ProjectPathRegistrationRequest(project_mat_path=path,
      project_dir_path=str(Path(root)/stem),root_path=root,storage_root_name=root_name,
      host_scope='detecdiv-server',root_type='project_root',owner_user_key='alexander',
      visibility='private',metadata_json={'manifest':data['manifest'],
      'source_file_bytes':row['mat_bytes'],'source_mtime_ns':row['mtime_ns']})
  project=register_project_path_record(db,payload=payload,current_user=operator)
  metadata=dict(project.metadata_json or {})
  metadata.update({'source':'ops_readonly_manifest_registration',
      'migration_preflight':{'batch':'alexander-2023-20261001','operator':'gilles',
      'transport':'authenticated_admin_ssh','registered_at':datetime.now(timezone.utc).isoformat(),
      'mat_header_verified':True,'paired_directory_verified':True,
      'lineage_status':'pending_internal_path_reconciliation',
      'inventory_status':'mat_only_directory_size_pending','migration_status':'not_copied',
      'source_preserved':True,'worker_jobs_queued':False}})
  project.metadata_json=metadata
  project.health_status='warning'
  project.project_mat_bytes=row['mat_bytes']
  project.total_bytes=row['mat_bytes']
  project.notes='Catalogage 2023 en lecture seule. Liens raw et inventaire du dossier à vérifier avant utilisation ou migration.'
  db.flush()
  locations=list(db.scalars(select(ProjectLocation).where(ProjectLocation.project_id==project.id)))
  assert len(locations)==1
  for location in locations:
   assert location.storage_root.path_prefix==root
   location.access_mode='readonly'
  results.append({'file':name,'id':str(project.id),'action':'registered_readonly'})
 if apply:
  db.commit()
 else:
  db.rollback()
print(json.dumps({'applied':apply,'projects':results,'raw_links_created':0,
                  'scientific_files_changed':False,'worker_jobs_queued':False}))
'''


def remote_json(ssh_exe: str, host: str, code: str, payload: object, *, container: bool = False) -> object:
    launcher = "import base64;exec(base64.b64decode('" + base64.b64encode(code.encode()).decode() + "'))"
    command = ['python', '-c', launcher] if container else ['python3', '-c', launcher]
    if container:
        command = ['docker', 'exec', '-i', 'detecdiv-hub-api', *command]
    completed = subprocess.run([ssh_exe, '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                                host, shlex.join(command)], input=json.dumps(payload),
                               text=True, capture_output=True, check=True, timeout=60)
    return json.loads(completed.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--ssh-exe', default='C:/Windows/System32/OpenSSH/ssh.exe')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    manifest_path = repo / 'reports/alexander-2023-project-pilot-20261001.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    headers = json.loads((repo / 'reports/alexander-2023-mat-inspection-20261001.json').read_text(encoding='utf-8'))
    assert manifest['owner_hub'] == 'alexander' and manifest['source_preserved']
    by_name = {Path(row['file']).name: row for row in headers['candidates']}
    selected = []
    excluded = []
    for row in manifest['candidates']:
        header = by_name[row['file_name']]
        if row['manual_review'] or header['review']:
            excluded.append(row['file_name'])
            continue
        variables = header['variables']
        if isinstance(variables, dict):
            variables = [variables]
        assert any(v['class'] == 'shallow' and v['size'] == [1, 1] for v in variables)
        selected.append(dict(row))
    assert len(selected) == 18 and len(excluded) == 2

    evidence = remote_json(args.ssh_exe, 'detecdiv-server', PROBE, [r['file_name'] for r in selected])
    observed = {r['name']: r for r in evidence}
    for row in selected:
        probe = observed[row['file_name']]
        assert probe['bytes'] == row['mat_bytes'] and probe['paired_directory_present']
        row['mtime_ns'] = probe['mtime_ns']
    result = remote_json(args.ssh_exe, 'webserver-labo', REGISTER, {'apply': args.apply, 'projects': selected,
                    'manifest':manifest_path.name}, container=True)
    result.update({'captured_at':datetime.now(timezone.utc).isoformat(),
                   'excluded_manual_review':excluded,'source_evidence':evidence})
    args.output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'applied':args.apply, 'projects':len(result['projects']),
                      'excluded':len(excluded),'report':str(args.output)}))


if __name__ == '__main__':
    main()
