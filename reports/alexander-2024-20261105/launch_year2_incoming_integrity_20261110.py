"""Admit a new readonly remaining-copy scope; never publish before raw year2 gates."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
from audit import remote_json,SSH

ADMIT=r'''
import json,sys,uuid
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Project,Job,ProjectLock,IndexingJob
v=json.load(sys.stdin);ids={uuid.UUID(x['project_id']) for x in v};assert len(ids)==17
now=datetime.now(timezone.utc);busy={};ready=[]
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running'])))),'Indexing active; defer readonly scope'
 for job in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  params=json.dumps(job.params_json or {},default=str)
  for row in v:
   i=uuid.UUID(row['project_id'])
   if job.project_id==i or str(i) in params or row['name'] in params:busy[str(i)]='active_job'
 for lock in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(ids),ProjectLock.status=='active')):
  if lock.expires_at is None or lock.expires_at>now:busy[str(lock.project_id)]='active_lease'
 for row in v:
  p=db.get(Project,uuid.UUID(row['project_id']));assert p is not None and p.project_name==row['name'] and p.owner.user_key in ['alexander','gilles']
  if row['project_id'] not in busy:ready.append(row)
 print(json.dumps({'read_only':True,'checked_at':now.isoformat(),'ready_projects':ready,'deferred_projects':[{'project_id':i,'reason':reason} for i,reason in sorted(busy.items())],'leases_acquired':0}))
'''

LAUNCH=r'''
import base64,json,hashlib,os,subprocess,shlex
from pathlib import Path
os.umask(0o077);root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');work=root/'year2-incoming-integrity-20261110';task='alex2024-year2-incoming-integrity-20261110'
assert not work.exists() and task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
changes={}
sources=[('relink-batch1-20261106','e2f0b82b394bb7f7355a7e0ec503bbe0ee45b5c1cae49d60db1f4892993c4cea'),
 ('relink-batch2-20261107','42637ffb72b75ed3af7cd96b97c5d16b1caeeace1137f26d4f0db19e0fc09f93'),
 ('relink-batch3-20261107','99f8c0d9dd7eeb7d63cb9182add2eff2c6d402da8f628a77945d3af83dab230e'),
 ('relink-batch4-20261107','302d43985081831ff1e2b0906744463e08dee0f57dfec95fe51fc298b66d5b7b')]
for folder,sha in sources:
 w=root/folder;assert (w/'controller.exit').read_text().strip()=='0';raw=(w/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()==sha
 result=json.loads(raw);assert result['success']
 for f in result['files']:
  key=(f['project_id'],f['relative_path']);assert key not in changes;changes[key]=f['after_sha256']
partial=root/'relink-remaining-mains-20261107';assert (partial/'controller.exit').read_text().strip()=='1'
for name in ['modified-01.json','modified-02.json']:
 f=json.loads((partial/name).read_text());assert f['scientific_values_types_and_annotations_preserved'] and f['save_reopen_verified'];key=(f['project_id'],f['relative_path']);assert key not in changes;changes[key]=f['after_sha256']
g=root/'relink-gilles-recovery-20261107';raw=(g/'result.json').read_bytes()
assert (g/'controller.exit').read_text().strip()=='0' and hashlib.sha256(raw).hexdigest()=='c3ec6316207d4fa6b70cd51da2b8fc0f4f7b5cb0e69f2bff8a20ddd669a74b72'
d=json.loads(raw);assert d['success'] and d['json_files']==9
for f in d['files']:
 key=(f['project_id'],f['relative_path']);assert key not in changes;changes[key]=f['after_sha256']
json_receipts=list(g.glob('modified-json-*.json'));assert len(json_receipts)==9
for path in json_receipts:
 f=json.loads(path.read_text());assert f['scientific_values_types_and_annotations_preserved'] and f['save_reopen_verified'] and f['frozen_contexts_unchanged']
 key=(f['project_id'],f['relative_path']);assert key in changes and changes[key]==f['after_sha256'],'JSON receipt differs from pinned successful result'
projects=[]
for p in SELECTION:
 stem=p['name'];pid=p['project_id'];special=stem in ['qp_250724_dbud4stest','281124_Alex'];old=root/('exception-copies-20261106' if special else 'project-copies')/stem
 native=(old/'source-before.jsonl').read_bytes();assert native==(old/'source-after.jsonl').read_bytes()
 inventory=[json.loads(line) for line in native.splitlines()];original={x['path']:x for x in inventory if x['kind']=='file'}
 if special:
  copied={x['path']:x for x in map(json.loads,(old/'destination.jsonl').read_text().splitlines()) if x['kind']=='file'}
  assert set(copied)==set(original) and all(copied[k]['sha256']==original[k]['sha256'] for k in original)
  verified=[{'path':x['path'],'sha256':x['sha256']} for x in original.values()]
 else:
  verified=[json.loads(line) for line in (old/'verify-receipt.jsonl').open()]
  assert len(verified)==len(original) and all(x['source_destination_hash_equal'] and x['owner']=='maliavko' for x in verified)
 files=[]
 for f in verified:
  key=(pid,f['path']);relinked=key in changes;files.append({'path':f['path'],'source_bytes':original[f['path']]['bytes'],'metadata_relinked':relinked,'expected_sha256':changes[key] if relinked else f['sha256']})
 assert (pid,stem+'.mat') in changes and {x['path'] for x in files}==set(original)
 suffix='20261106' if special else '20261105';parent=Path('/homes2/maliavko/DetecdivHub/projects');assert (parent/('.incoming-'+stem+'-'+suffix)).is_dir() and not os.path.lexists(parent/stem)
 projects.append({'name':stem,'project_id':pid,'files':files,'directories':[x['path'] for x in inventory if x['kind']=='directory'],'source_inventory_sha256':hashlib.sha256(native).hexdigest(),'source_receipt_relative':str((old/'source-before.jsonl').relative_to(root)),'source_role':'primary' if stem=='281124_Alex' else 'secondary','stage_suffix':suffix})
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
work.mkdir(mode=0o700);(work/'audit_year2_incoming_integrity_20261110.py').write_bytes(script)
(work/'request.json').write_text(json.dumps({'projects':projects,'expected_projects':len(projects),'read_only':True,'runner_sha256':SHA,'admission':ADMISSION,'publication_allowed':False,'source_deletion_allowed':False,'raw_year2_full_checksum_and_stability_still_required':True})+'\n')
shell=shlex.join(['nice','-n','19','ionice','-c','3','/home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python','-u',str(work/'audit_year2_incoming_integrity_20261110.py')])+' > '+shlex.quote(str(work/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(work/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'work':str(work),'session':task,'runner_sha256':SHA,'projects':len(projects),'files':sum(len(p['files']) for p in projects),'deferred_projects':ADMISSION['deferred_projects'],'read_only':True,'leases_acquired':0,'publication_allowed':False,'source_deletion_allowed':False,'raw_year2_sha1_and_stability_not_replaced':True}))
'''

if __name__=='__main__':
    output=HERE/'year2-incoming-integrity-launch-verification.json';assert not output.exists(),'Inspect existing actor instead of replay'
    plan=json.loads((HERE/'year1-next-stage-candidate-plan-20261109.json').read_text());selected=[p for p in plan['projects'] if p['metadata_relink_completed'] and '2024_2' in p['candidate_raw_years']];assert len(selected)==17
    admission=remote_json(SSH,'webserver-labo',ADMIT,selected,container=True)
    (HERE/'runtime/year2-incoming-integrity-admission.json').write_text(json.dumps(admission,indent=2)+'\n',encoding='utf-8')
    if not admission['ready_projects']:
        print(json.dumps({'launched':False,'reason':'No currently idle project; no work or lease created','deferred_projects':admission['deferred_projects']}));raise SystemExit(0)
    script=(HERE/'audit_year2_incoming_integrity_20261110.py').read_bytes()
    result=remote('linux','SELECTION='+repr(admission['ready_projects'])+'\nADMISSION='+repr(admission)+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\nSHA='+repr(hashlib.sha256(script).hexdigest())+'\n'+LAUNCH,timeout=90)
    with output.open('x',encoding='utf-8') as stream:json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(result))
