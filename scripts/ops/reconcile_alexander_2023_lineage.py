"""Read scientific sources only; optionally record catalog inspections and exact links."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path, PurePosixPath

from register_alexander_2023_manifest import remote_json


CATALOG = r'''
import json
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import RawDataset,User
from api.services.path_resolution import compose_storage_path
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 rows=[]
 for raw in db.scalars(select(RawDataset).join(User).where(User.user_key=='alexander')):
  paths=[compose_storage_path(l.storage_root.path_prefix,l.relative_path) for l in raw.locations]
  rows.append({'id':str(raw.id),'label':raw.acquisition_label,'locations':paths})
 print(json.dumps(rows))
'''

PROBE = r'''
import json,sys
from pathlib import Path
paths=json.load(sys.stdin); result={}
for value in paths:
 assert value.startswith(('/data/','/homes/maliavko/','/data_sv/Alexander/',
                         '/mnt/detecdiv-secondary-sauvegarde/Alexander/'))
 p=Path(value)
 try:
  result[value]={'is_directory':p.is_dir(),'resolved':str(p.resolve())}
 except OSError as exc:
  result[value]={'is_directory':False,'error':str(exc)}
print(json.dumps(result))
'''

APPLY = r'''
import json,sys
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Project,ProjectRawLink,RawDataset,User
from api.services.path_resolution import compose_storage_path
report=json.load(sys.stdin); links=[]
with SessionLocal() as db:
 db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2023-readonly-pilot'))"))
 projects=list(db.scalars(select(Project).join(User).where(User.user_key=='alexander',
     Project.metadata_json['migration_preflight']['batch'].astext=='alexander-2023-20261001')))
 assert len(projects)==18
 by_name={p.project_name+'.mat':p for p in projects}
 for row in report['projects']:
  p=by_name[row['project']]
  assert p.health_status=='warning' and all(l.access_mode=='readonly' for l in p.locations)
  p.fov_count=row['fov_count']
  metadata=dict(p.metadata_json or {})
  preflight=dict(metadata['migration_preflight'])
  preflight.update({'mat_loaded_readonly':True,'lineage_inspected_at':report['captured_at'],
      'saved_raw_paths':[entry['saved'] for entry in row['paths']],
      'lineage_report':'alexander-2023-raw-reconciliation-20261001.json'})
  direct_ids=set()
  for entry in row['paths']:
   for path, evidence in entry['candidates'].items():
    if evidence['rule']!='exact_saved_server_path' or not evidence['is_directory']:
     continue
    assert path==entry['saved'] and len(evidence['raw_ids'])==1
    raw=db.get(RawDataset,evidence['raw_ids'][0])
    assert raw.owner.user_key=='alexander'
    assert any(path.startswith(compose_storage_path(l.storage_root.path_prefix,l.relative_path).rstrip('/')+'/')
               for l in raw.locations)
    direct_ids.add(raw.id)
  for raw_id in direct_ids:
   existing=db.scalar(select(ProjectRawLink).where(ProjectRawLink.project_id==p.id,
                                                 ProjectRawLink.raw_dataset_id==raw_id))
   if existing is None:
    db.add(ProjectRawLink(project_id=p.id,raw_dataset_id=raw_id,link_type='source'))
    links.append({'project':p.project_name,'raw_id':str(raw_id)})
  preflight['exact_server_raw_ids']=[str(value) for value in sorted(direct_ids,key=str)]
  preflight['lineage_status']='exact_server_link_recorded' if direct_ids else 'legacy_alias_review_required'
  metadata['migration_preflight']=preflight
  p.metadata_json=metadata
 db.commit()
print(json.dumps({'updated_projects':len(projects),'new_exact_raw_links':links}))
'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Record inspections and exact server-path links only')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    report_path = repo / 'reports/alexander-2023-lineage-20261001.json'
    lineage = json.loads(report_path.read_text(encoding='utf-8'))
    assert lineage['complete'] and lineage['readOnly']
    ssh = 'C:/Windows/System32/OpenSSH/ssh.exe'
    catalog = remote_json(ssh, 'webserver-labo', CATALOG, {}, container=True)
    rows = []
    paths_to_probe = set()
    for project in lineage['projects']:
        if project['status'] != 'inspected':
            continue
        row = {'project':project['fileName'], 'fov_count':project['fovCount'], 'paths':[]}
        for saved in project['rawPaths']:
            normalized = saved.replace('\\','/').rstrip('/')
            candidates = {}
            if normalized.startswith('/data/'):
                candidates[normalized] = {'rule':'exact_saved_server_path', 'raw_ids':[]}
            elif normalized.startswith('X:/maliavko/'):
                candidates['/homes/' + normalized[3:]] = {'rule':'explicit_legacy_home_alias', 'raw_ids':[]}
            elif normalized.startswith('//10.20.11.250/data/'):
                candidates['/data/' + normalized.split('/data/',1)[1]] = {'rule':'explicit_main_nas_unc', 'raw_ids':[]}
            elif normalized.startswith('//10.20.8.250/data/Alexander/'):
                candidates['/data_sv/Alexander/' + normalized.split('/Alexander/',1)[1]] = {'rule':'explicit_secondary_data_unc', 'raw_ids':[]}
            elif normalized.startswith('Z:/Alexander/'):
                tail = normalized.split('/Alexander/',1)[1]
                for root in ['/mnt/detecdiv-secondary-sauvegarde/Alexander/', '/data_sv/Alexander/']:
                    candidates[root+tail] = {'rule':'unconfirmed_legacy_Z_alias', 'raw_ids':[]}
            parts = normalized.split('/')
            for raw in catalog:
                for location in raw['locations']:
                    if not location.startswith('/data/Alexander/'):
                        continue
                    raw_part = PurePosixPath(location).name
                    if raw_part not in parts:
                        continue
                    suffix = '/'.join(parts[parts.index(raw_part)+1:])
                    candidate = location.rstrip('/') + ('/'+suffix if suffix else '')
                    match = candidates.setdefault(candidate, {'rule':'exact_acquisition_component_candidate', 'raw_ids':[]})
                    if raw['id'] not in match['raw_ids']:
                        match['raw_ids'].append(raw['id'])
            row['paths'].append({'saved':saved,'candidates':candidates})
            paths_to_probe.update(candidates)
        rows.append(row)
    observed = remote_json(ssh, 'detecdiv-server', PROBE, sorted(paths_to_probe))
    for row in rows:
        for entry in row['paths']:
            for path, details in entry['candidates'].items():
                details.update(observed[path])
    report = {'captured_at':datetime.now(timezone.utc).isoformat(), 'scientific_files_read_only':True,
              'catalog_write_applied':args.apply,
              'raw_links_created':0, 'projects':rows,
              'rule':'Directory presence and acquisition-name matches are candidates, not proof of relocated identity. Only exact saved server paths may be linked directly.'}
    if args.apply:
        report['catalog_update'] = remote_json(ssh, 'webserver-labo', APPLY, report, container=True)
        report['raw_links_created'] = len(report['catalog_update']['new_exact_raw_links'])
    output = repo / 'reports/alexander-2023-raw-reconciliation-20261001.json'
    output.write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({'projects':len(rows),'paths_probed':len(paths_to_probe),'report':str(output)}))


if __name__ == '__main__':
    main()
