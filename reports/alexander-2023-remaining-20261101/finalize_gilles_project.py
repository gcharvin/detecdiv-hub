"""Record verified JSON raw paths while retaining the existing Hub MAT alias."""
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent/'homes2-access-audit-20261101'))
from audit import remote_json, SSH
from activate_windows_mapping import remote


def main():
    admission = json.loads((HERE/'gilles-json-relink-admission.json').read_text())
    linux = json.loads((HERE/'gilles-json-linux-after-verification.json').read_text())
    windows = json.loads((HERE/'gilles-windows-status.json').read_text())['gilles-json-session-verification.json']
    assert linux['success'] and linux['baseline_images_and_raw_reads_unchanged']
    assert windows['success'] and windows['verification']['fovs'] == linux['fovs'] == 30
    assert windows['verification']['rois'] == linux['rois'] == 15281
    for a, b in zip(linux['samples'], windows['verification']['samples']):
        assert a['id'] == b['id'] and a['rawPixelsSha256'] == b['rawPixelsSha256']
        assert a['imageFilesPresent'] == b['imageFilesPresent']
        if a['roiSamplePresent']: assert a['roiPixelsSha256'] == b['roiPixelsSha256']
    footprint = remote('linux', r'''
import json,base64,subprocess,shlex
native=r"""
from pathlib import Path
import json,hashlib,os,stat
root=Path('/volume1/DATA/Gilles/analysis/projects');stem='190423_nh_sir2_tween'
backup=root/'.detecdiv-path-backups'/('20261101-homes2-'+stem)
m=json.loads((backup/'manifest.json').read_text());assert (backup/'applied.json').is_file()
for row in m['files']:
 p=root/row['relative'];s=p.stat();expected=row['after_sha256'] if row['changes'] else row['before_sha256']
 assert hashlib.sha256(p.read_bytes()).hexdigest()==expected
 assert hashlib.sha256((backup/'original'/row['relative']).read_bytes()).hexdigest()==row['before_sha256']
 assert (s.st_ino,s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode))==(row['inode'],row['uid'],row['gid'],row['mode'])
count=size=h5count=h5bytes=0
for parent,dirs,files in os.walk(root/stem,followlinks=False):
 dirs[:]=[d for d in dirs if d not in ('@eaDir','#recycle','#snapshot')]
 for d in dirs:assert not (Path(parent)/d).is_symlink()
 for name in files:
  if name in ('@eaDir','#recycle','#snapshot'):continue
  p=Path(parent)/name;s=p.lstat();assert stat.S_ISREG(s.st_mode);count+=1;size+=s.st_size
  if p.suffix=='.h5':h5count+=1;h5bytes+=s.st_size
top=root/(stem+'.json')
print(json.dumps({'files':count,'directory_bytes':size,'json_bytes':top.stat().st_size,'json_sha256':hashlib.sha256(top.read_bytes()).hexdigest(),'h5_count':h5count,'h5_bytes':h5bytes,'all_original_metadata_backups_verified':True,'live_inodes_owners_modes_preserved':True}))
"""
loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(native.encode()).decode())+'))'
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519','Gilles@10.20.11.250',shlex.join(['nice','-n','19','python3','-c',loader])],capture_output=True,text=True,timeout=90);assert p.returncode==0,p.stderr
print(p.stdout.strip())
''', timeout=105)
    (HERE/'gilles-json-final-footprint.json').write_text(json.dumps(footprint, indent=2)+'\n')
    code = r'''
import json,sys,uuid,hashlib
from datetime import datetime,timezone
from sqlalchemy import select,text,inspect
from api.db import SessionLocal
from api.models import Project,ProjectLock,Job,StorageMigrationBatch,StorageMigrationItem
from api.routes_projects import project_detail_view
v=json.load(sys.stdin);a=v['admission'];fp=v['footprint']
def digest(x):return hashlib.sha256(json.dumps(x,sort_keys=True,default=str).encode()).hexdigest()
def columns(x):return {c.key:getattr(x,c.key) for c in inspect(type(x)).column_attrs}
def identity(p):return {'owner':str(p.owner_user_id),'key':p.project_key,'name':p.project_name,'visibility':p.visibility,'notes':p.notes,'acl':digest([columns(x) for x in p.acl_entries]),'groups':digest([columns(x) for x in p.group_memberships]),'notes_entries':digest([columns(x) for x in p.note_entries]),'raw_links':digest([columns(x) for x in p.raw_links]),'locations':digest([columns(x) for x in p.locations])}
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 p=db.get(Project,uuid.UUID(a['project_id']),with_for_update=True);l=db.get(ProjectLock,uuid.UUID(a['lease_id']),with_for_update=True)
 assert p.owner.user_key=='gilles' and p.metadata_json==a['before_metadata'] and p.project_key==a['before_key']
 assert l.status=='active' and l.holder_key=='alex-remaining-20261101-gilles-json' and l.expires_at>datetime.now(timezone.utc)
 assert not list(db.scalars(select(Job.id).where(Job.project_id==p.id,Job.status.in_(['queued','running','cancelling']))))
 before=identity(p);name='alexander-2023-1-gilles-json-raw-paths-20261101'
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==name)) is None
 meta=dict(p.metadata_json);meta['project_json_abs']='/data/Gilles/analysis/projects/190423_nh_sir2_tween.json';meta['project_manifest_format']='json_v3'
 meta['raw_path_verification_20261101']={'linux_windows_verified':True,'raw_root':'/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_1','fovs':30,'rois':15281,'existing_h5_files':fp['h5_count'],'metadata_backup':'/data/Gilles/analysis/projects/.detecdiv-path-backups/20261101-homes2-190423_nh_sir2_tween','project_retained_under_gilles':True,'historical_mat_alias_resolves_json':True,'existing_partial_extraction_preserved':True,'manifest_sha256':fp['json_sha256']}
 p.metadata_json=meta;p.fov_count=30;p.roi_count=15281;p.h5_count=fp['h5_count'];p.h5_bytes=fp['h5_bytes'];p.project_mat_bytes=fp['json_bytes'];p.project_dir_bytes=fp['directory_bytes'];p.total_bytes=fp['json_bytes']+fp['directory_bytes'];p.last_size_scan_at=datetime.now(timezone.utc);p.health_status='ok'
 batch=StorageMigrationBatch(owner_user_id=p.owner_user_id,batch_name=name,source_kind='detecdiv_project',source_path=meta['project_json_abs'],storage_root_name='hub_gilles_10ef868b',host_scope='server',root_type='project_root',strategy='relink_raw_paths_keep_gilles_project_in_place',status='verified_complete_primary_retirement_pending',metadata_json={'operator':'gilles','linux':v['linux'],'windows':v['windows'],'footprint':fp,'before_identity':before,'before_metadata':a['before_metadata'],'lease_id':str(l.id),'lease_retained_for_source_retirement':True,'project_mat_alias_retained_for_existing_registration_contract':True},summary_json={'fovs':30,'rois':15281,'path_changes':301,'raw_source_deleted':False,'project_retained_under_gilles':True})
 db.add(batch);db.flush();db.add(StorageMigrationItem(batch_id=batch.id,item_type='detecdiv_project',legacy_path=meta['project_json_abs'],legacy_key=p.project_key,display_name=p.project_name,status='verified_complete',action='relink_json_raw_paths_preserve_all_other_values',metadata_json={'project_id':str(p.id),'location_id':2746,'metadata_files_modified':32,'backups_verified':True}))
 assert identity(p)==before
 detail=project_detail_view(p);assert next(x.project_mat_path for x in detail.locations if x.is_preferred)=='/data/Gilles/analysis/projects/190423_nh_sir2_tween.mat'
 print(json.dumps({'success':True,'committed_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),'project_id':str(p.id),'identity_unchanged':before,'fovs':30,'rois':15281,'manifest_sha256':fp['json_sha256'],'lease_retained':str(l.id),'raw_source_deleted':False,'project_retained_under_gilles':True}))
'''
    result = remote_json(SSH, 'webserver-labo', code, {'admission': admission, 'linux': linux, 'windows': windows, 'footprint': footprint}, container=True)
    (HERE/'gilles-json-catalog-finalization.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'identity_unchanged'}))


if __name__ == '__main__':
    main()
