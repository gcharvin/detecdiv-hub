"""Finalize the verified existing project copy; primary retirement is separate."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from audit import remote_json,SSH
from activate_windows_mapping import remote


def main():
    admission=json.loads((HERE/'project1-relink-admission.json').read_text())
    win=json.loads((HERE/'windows-status.json').read_text())['project1-session-verification.json']
    assert win['success'] and win['verification']['fovs']==28 and win['verification']['rois']==14386
    linux=remote('linux',r'''
from pathlib import Path
import json,subprocess,shlex,base64
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
assert (w/'project1-relink.exit').read_text().strip()=='0'
r=json.loads((w/'project1-linux-relink-verification.json').read_text());assert r['success'] and r['fovs']==28 and r['rois']==14386
native=r"""
from pathlib import Path
import os,stat,hashlib,json
stem='anof_190423_nh_sir2_tween';container=Path('/volume1/homes/maliavko/DetecdivHub/projects')/stem;root=container/stem;file=container/(stem+'.mat')
count=size=0
for parent,dirs,files in os.walk(root,followlinks=False):
 dirs[:]=[d for d in dirs if d not in ('@eaDir','#recycle','#snapshot')]
 for d in dirs:assert not (Path(parent)/d).is_symlink()
 for n in files:
  if n in ('@eaDir','#recycle','#snapshot'):continue
  p=Path(parent)/n;s=p.lstat();assert stat.S_ISREG(s.st_mode);count+=1;size+=s.st_size
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for block in iter(lambda:f.read(4*1024*1024),b''):h.update(block)
 return h.hexdigest()
original=Path('/volume1/Sauvegarde/Alexander/analysis')/(stem+'.mat')
a=digest(original);assert a=='c7c6b30c1882d2ad7f1fb91276b2565bfac0e38177b45e31c5712dc8b9cea874'
assert count>=43158
print(json.dumps({'directory_files':count,'directory_bytes':size,'mat_bytes':file.stat().st_size,'mat_sha256':digest(file),'original_mat_sha256_unchanged':a}))
"""
loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(native.encode()).decode())+'))'
args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['nice','-n','19','python3','-c',loader])]
p=subprocess.run(args,capture_output=True,text=True,timeout=90);assert p.returncode==0,p.stderr
print(json.dumps({'verification':r,'footprint':json.loads(p.stdout)}))
''',timeout=110)
    (HERE/'project1-linux-final-verification.json').write_text(json.dumps(linux,indent=2)+'\n')
    for a,b in zip(linux['verification']['samples'],win['verification']['samples']):
        assert a['roi']==b['roi'] and a['imageClass']==b['imageClass'] and a['imageSize']==b['imageSize']
    code=r'''
import json,sys,uuid,hashlib
from datetime import datetime,timezone
from sqlalchemy import select,text,inspect
from api.db import SessionLocal
from api.models import Project,ProjectLock,Job,StorageMigrationBatch,StorageMigrationItem
from api.services.project_locks import release_project_lock
from api.routes_projects import project_detail_view
payload=json.load(sys.stdin);a=payload['admission'];linux=payload['linux'];win=payload['windows'];fp=linux['footprint']
def columns(x):return {p.key:getattr(x,p.key) for p in inspect(type(x)).column_attrs}
def digest(x):return hashlib.sha256(json.dumps(x,sort_keys=True,default=str).encode()).hexdigest()
def identity(p):
 return {'owner':str(p.owner_user_id),'key':p.project_key,'visibility':p.visibility,'notes':p.notes,
  'acl':digest([columns(x) for x in p.acl_entries]),'groups':digest([columns(x) for x in p.group_memberships]),
  'notes_entries':digest([columns(x) for x in p.note_entries]),'raw_links':digest([columns(x) for x in p.raw_links])}
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 p=db.get(Project,uuid.UUID(a['project_id']),with_for_update=True);lease=db.get(ProjectLock,uuid.UUID(a['lease_id']),with_for_update=True)
 assert lease.status=='active' and lease.holder_key=='alex-remaining-20261101-project1'
 assert lease.expires_at>datetime.now(timezone.utc),'Own project lease expired; require a fresh admission check'
 assert not list(db.scalars(select(Job.id).where(Job.project_id==p.id,Job.status.in_(['queued','running','cancelling']))))
 assert p.metadata_json==a['before_metadata'] and p.project_key==a['project_key']
 before=identity(p)
 preferred=[l for l in p.locations if l.is_preferred];assert len(preferred)==1 and preferred[0].id==4246
 assert any(l.id==2761 and not l.is_preferred and l.access_mode=='readonly' for l in p.locations)
 stem='anof_190423_nh_sir2_tween';container='/homes2/maliavko/DetecdivHub/projects/'+stem
 meta=dict(p.metadata_json);meta['project_dir_abs']=container+'/'+stem
 meta['project_rel_from_root']='maliavko/DetecdivHub/projects/'+stem+'/'+stem
 meta['path_verification_20261101']={'linux_windows_verified':True,'fovs':28,'rois':14386,'mat_sha256':fp['mat_sha256'],
  'raw_root':'/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_1',
  'original_project_preserved':True,'raw_primary_retirement_deferred':'Dependent Gilles project MAT location is not yet validated.',
  'evidence':'reports/alexander-2023-remaining-20261101/'}
 p.metadata_json=meta;p.fov_count=28;p.roi_count=14386;p.project_mat_bytes=fp['mat_bytes'];p.project_dir_bytes=fp['directory_bytes']
 p.total_bytes=fp['mat_bytes']+fp['directory_bytes'];p.last_size_scan_at=datetime.now(timezone.utc);p.health_status='ok'
 name='alexander-2023-1-project-path-verification-20261101'
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==name)) is None
 batch=StorageMigrationBatch(owner_user_id=p.owner_user_id,batch_name=name,source_kind='detecdiv_project',source_path=a['source_mat'],
  storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='project_root',strategy='relink_verified_existing_copy_keep_original',
  status='verified_complete_primary_retirement_deferred',metadata_json={'operator':'gilles','linux':linux,'windows':win,'before_identity':before,
   'before_metadata':a['before_metadata'],'original_location_id':2761,'primary_source_deleted':False,'external_dependency':'Gilles project MAT location unresolved'},
  summary_json={'projects':1,'fovs':28,'rois':14386,'original_project_preserved':True,'primary_source_deleted':False})
 db.add(batch);db.flush()
 db.add(StorageMigrationItem(batch_id=batch.id,item_type='detecdiv_project',legacy_path=a['source_mat'],legacy_key=p.project_key,
  display_name=p.project_name,status='verified_complete',action='rebase_existing_copied_project_paths',metadata_json={'project_id':str(p.id),'location_id':4246,'original_location_id':2761}))
 assert identity(p)==before
 release_project_lock(db,lock=lease);db.flush()
 detail=project_detail_view(p);assert next(l.project_mat_path for l in detail.locations if l.is_preferred)==a['target_mat']
 print(json.dumps({'success':True,'committed_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),
  'project_id':str(p.id),'fovs':28,'rois':14386,'preferred_location_id':4246,'original_location_id':2761,'identity_unchanged':before,
  'mat_sha256':fp['mat_sha256'],'total_bytes':p.total_bytes,'project_lease_released':True,'primary_source_deleted':False,
  'primary_retirement_deferred':'Dependent Gilles project MAT location is not yet validated.'}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'admission':admission,'linux':linux,'windows':win},container=True)
    (HERE/'project1-catalog-finalization.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
