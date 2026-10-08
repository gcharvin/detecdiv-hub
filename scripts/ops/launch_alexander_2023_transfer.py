"""Snapshot the live catalog, journal the approved batch and launch COPY only.

No preferred location, raw/project ID, owner, link or source file is changed.
Stages versioned standalone tools outside the worker checkout; no restarts.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

from register_alexander_2023_manifest import remote_json

SSH='C:/Windows/System32/OpenSSH/ssh.exe'
SCP='C:/Windows/System32/OpenSSH/scp.exe'
BATCH='alexander-2023-20261008'

SNAPSHOT = r'''
import json
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import RawDataset,Job,Project
from api.services.path_resolution import compose_storage_path
prefixes=['/data/Alexander/data/'+y for y in ('2023_1','2023_2','2023_3')]
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 raws=[]; project_ids=set()
 for r in db.scalars(select(RawDataset)):
  matching=[]
  for l in r.locations:
   path=compose_storage_path(l.storage_root.path_prefix,l.relative_path)
   if any(path==p or path.startswith(p+'/') for p in prefixes): matching.append(path)
  if not matching: continue
  row={'id':str(r.id),'external_key':r.external_key,'owner_user_id':str(r.owner_user_id),
    'owner_key':r.owner.user_key if r.owner else None,'acquisition_label':r.acquisition_label,
    'updated_at':r.updated_at.isoformat(),'total_bytes':r.total_bytes,'source_paths':sorted(set(matching)),
    'metadata_json':r.metadata_json,'locations':[], 'project_links':[], 'experiment_links':[]}
  for l in r.locations:
   row['locations'].append({'id':l.id,'root_id':l.storage_root_id,'root_name':l.storage_root.name,
    'root_prefix':l.storage_root.path_prefix,'host_scope':l.storage_root.host_scope,
    'relative_path':l.relative_path,'is_preferred':l.is_preferred,'access_mode':l.access_mode})
  for l in r.project_links:
   row['project_links'].append({'id':l.id,'project_id':str(l.project_id),'link_type':l.link_type})
   project_ids.add(l.project_id)
  for l in r.experiment_links:
   row['experiment_links'].append({'id':l.id,'experiment_id':str(l.experiment_project_id)})
  raws.append(row)
 projects=[]
 for p in db.scalars(select(Project).where(Project.id.in_(project_ids))):
  projects.append({'id':str(p.id),'key':p.project_key,'owner':str(p.owner_user_id),
   'metadata_json':p.metadata_json,'locations':[{'id':l.id,'root_id':l.storage_root_id,
    'relative':l.relative_path,'file':l.project_file_name,'preferred':l.is_preferred} for l in p.locations]})
 jobs=[{'id':str(j.id),'project_id':str(j.project_id),'raw_id':str(j.raw_dataset_id),'status':j.status}
       for j in db.scalars(select(Job).where(Job.status.in_(['queued','running'])))]
 print(json.dumps({'raws':raws,'linked_projects':projects,'active_jobs_at_plan':jobs}))
'''

JOURNAL = r'''
import json,sys
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import StorageMigrationBatch,StorageMigrationItem,User,UserStorageAccount,StorageProvider
data=json.load(sys.stdin)
with SessionLocal() as db:
 db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2023-20261008'))"))
 owner=db.scalar(select(User).where(User.user_key=='alexander'))
 assert owner is not None and db.scalar(select(User).where(User.user_key=='gilles',User.role=='admin')) is not None
 account=db.scalars(select(UserStorageAccount).join(User).join(StorageProvider).where(User.user_key=='alexander',StorageProvider.provider_key=='synology-secondary')).unique().one()
 assert account.provider_user_key=='maliavko' and account.quota_bytes==60000000000000
 existing=db.scalar(select(StorageMigrationBatch).where(StorageMigrationBatch.batch_name==data['batch_name']))
 assert existing is None, 'Batch already exists; inspect/resume, never duplicate the launch'
 batch=StorageMigrationBatch(owner_user_id=owner.id,batch_name=data['batch_name'],
   source_kind='raw_root',source_path=data['source_root'],storage_root_name='user-homes2',
   host_scope='detecdiv-server',root_type='raw_root',strategy='copy_verify_only',status='copying',
   metadata_json={'approved_by':'gilles','alexander_consent_reported_at':'2026-10-08',
    'target_root':data['destination_root'],'snapshot_sha256':data['catalog_snapshot_sha256'],
    'git_revision':data['git_revision'],'raw_ids':data['raw_ids'],
    'source_deletion_allowed':False,'database_cutover_allowed':False,
    'next_gate':'full content verification, source stability, project relinking and client-path tests'},
   summary_json={'raw_record_count':len(data['raw_ids']),'physical_tree_count':3})
 db.add(batch); db.flush()
 for year in data['years']:
  db.add(StorageMigrationItem(batch_id=batch.id,item_type='raw_tree',
    legacy_path=data['source_root']+'/'+year,display_name=year,status='copying',action='copy_verify_only',
    metadata_json={'destination':data['destination_root']+'/'+year,'original_location_retained':True}))
 db.commit()
 print(json.dumps({'batch_id':str(batch.id)}))
'''

STAGE = r'''
import json,sys,os,hashlib
from pathlib import Path
data=json.load(sys.stdin); os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2023-20261008')
assert not root.exists(), 'Launch directory exists; inspect before resume'
root.mkdir(parents=True,mode=0o700)
for name,value in [('catalog-before.json',data['snapshot']),('manifest.json',data['manifest'])]:
 with (root/name).open('x') as f: json.dump(value,f,indent=2); f.write('\n')
print(json.dumps({'root':str(root)}))
'''


def main() -> None:
    repo=Path(__file__).resolve().parents[2]
    runner=repo/'scripts/ops/alexander_2023_transfer.py'
    revision=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    assert not subprocess.check_output(['git','diff','HEAD','--',str(runner)],text=True).strip(), 'Commit runner before launch'
    # Missing/untrusted mount must fail before even a database journal is created.
    guard=runner.read_text().split('def main() -> None:',1)[0]+'\nrequire_mounts(); print(json.dumps({"mounts_verified":True}))\n'
    remote_json(SSH,'detecdiv-server',guard,{})
    snapshot=remote_json(SSH,'webserver-labo',SNAPSHOT,{},container=True)
    assert snapshot['raws'] and all(len(r['source_paths'])==1 for r in snapshot['raws']), 'Ambiguous physical identity; inspect first'
    manifest={'schema_version':1,'batch_name':BATCH,'years':['2023_1','2023_2','2023_3'],
        'source_root':'/data/Alexander/data','destination_root':'/homes2/maliavko/DetecdivHub/raw',
        'bandwidth_kib':61440,'minimum_free_bytes':2_000_000_000_000,
        'source_deletion_allowed':False,'database_cutover_allowed':False,'git_revision':revision,
        'runner_sha256':hashlib.sha256(runner.read_bytes()).hexdigest(),
        'raw_ids':[r['id'] for r in snapshot['raws']],
        'catalog_snapshot_sha256':hashlib.sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest()}
    receipt=remote_json(SSH,'webserver-labo',JOURNAL,manifest,container=True)
    manifest.update(receipt)
    stage=remote_json(SSH,'detecdiv-server',STAGE,{'snapshot':snapshot,'manifest':manifest})
    root=stage['root']
    subprocess.run([SCP,str(runner),'detecdiv-server:'+root+'/runner.py'],check=True)
    launch=r'''
import hashlib,json,subprocess
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2023-20261008')
manifest=json.loads((root/'manifest.json').read_text())
assert hashlib.sha256((root/'runner.py').read_bytes()).hexdigest()==manifest['runner_sha256']
assert subprocess.run(['tmux','has-session','-t','alex2023-transfer'],capture_output=True).returncode!=0
command=f'python3 {root}/runner.py --manifest {root}/manifest.json > {root}/runner.log 2>&1'
subprocess.run(['tmux','new-session','-d','-s','alex2023-transfer',command],check=True)
print(json.dumps({'session':'alex2023-transfer','work_directory':str(root)}))
'''
    started=remote_json(SSH,'detecdiv-server',launch,{})
    result={**started,**receipt,'git_revision':revision,'raw_records':len(snapshot['raws']),
            'physical_datasets':len({p for r in snapshot['raws'] for p in r['source_paths']}),
            'started_at':datetime.now(timezone.utc).isoformat(),'database_paths_unchanged':True}
    (repo/'reports/alexander-2023-transfer-launch-20261008.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result))


if __name__=='__main__':
    main()
