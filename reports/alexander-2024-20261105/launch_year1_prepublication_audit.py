"""Admit and start a new readonly integrity scope for 16 year1 project copies."""
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
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Project,Job,ProjectLock,IndexingJob
v=json.load(sys.stdin);ids={uuid.UUID(x['project_id']) for x in v};assert len(ids)==16
now=datetime.now(timezone.utc)
with SessionLocal() as db:
 for i in ids:assert db.get(Project,i).owner.user_key=='alexander'
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  params=json.dumps(j.params_json or {},default=str)
  assert j.project_id not in ids and not any(str(i) in params for i in ids),'Affected project occupied'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running'])))),'Indexing active'
 for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(ids),ProjectLock.status=='active')):
  assert l.expires_at is not None and l.expires_at<=now,'Affected project lease active'
 print(json.dumps({'read_only':True,'checked_at':now.isoformat(),'project_ids':sorted(map(str,ids)),'leases_acquired':0,'jobs_indexing_and_leases_clear':True}))
'''

LAUNCH=r'''
import base64,json,hashlib,os,subprocess,shlex
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');work=root/'year1-prepublication-audit-20261109'
task='alex2024-year1-prepublication-audit-20261109'
assert not work.exists() and task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
state=json.loads((root/'state.json').read_text());assert state['years']['2024_1']['full_sha1_passed'] and state['years']['2024_1']['source_stability_verified']
changes={}
sources=[('relink-batch1-20261106','e2f0b82b394bb7f7355a7e0ec503bbe0ee45b5c1cae49d60db1f4892993c4cea'),
 ('relink-batch2-20261107','42637ffb72b75ed3af7cd96b97c5d16b1caeeace1137f26d4f0db19e0fc09f93'),
 ('relink-batch3-20261107','99f8c0d9dd7eeb7d63cb9182add2eff2c6d402da8f628a77945d3af83dab230e'),
 ('relink-batch4-20261107','302d43985081831ff1e2b0906744463e08dee0f57dfec95fe51fc298b66d5b7b')]
for folder,sha in sources:
 w=root/folder;assert (w/'controller.exit').read_text().strip()=='0';raw=(w/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()==sha
 d=json.loads(raw);assert d['success']
 for f in d['files']:
  key=(f['project_id'],f['relative_path']);assert key not in changes;changes[key]=f['after_sha256']
partial=root/'relink-remaining-mains-20261107';assert (partial/'controller.exit').read_text().strip()=='1'
for name in ['modified-01.json','modified-02.json']:
 f=json.loads((partial/name).read_text());key=(f['project_id'],f['relative_path']);assert key not in changes;changes[key]=f['after_sha256']
projects=[]
for p in SELECTION:
 stem=p['name'];pid=p['project_id'];old=root/'project-copies'/stem
 native=(old/'source-before.jsonl').read_bytes();assert native==(old/'source-after.jsonl').read_bytes()
 inventory=[json.loads(line) for line in native.splitlines()];original={r['path']:r for r in inventory if r['kind']=='file'}
 verified=[json.loads(line) for line in (old/'verify-receipt.jsonl').open()]
 assert len(verified)==len(original) and all(r['source_destination_hash_equal'] and r['owner']=='maliavko' for r in verified)
 files=[]
 for f in verified:
  key=(pid,f['path']);relinked=key in changes
  files.append({'path':f['path'],'source_bytes':original[f['path']]['bytes'],'metadata_relinked':relinked,'expected_sha256':changes[key] if relinked else f['sha256']})
 assert (pid,stem+'.mat') in changes and {f['path'] for f in files}==set(original)
 projects.append({'name':stem,'project_id':pid,'files':files,'directories':[r['path'] for r in inventory if r['kind']=='directory'],'source_inventory_sha256':hashlib.sha256(native).hexdigest()})
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
work.mkdir(mode=0o700);(work/'audit_year1_prepublication.py').write_bytes(script)
(work/'request.json').write_text(json.dumps({'projects':projects,'read_only':True,'runner_sha256':SHA,'admission':ADMISSION,'publication_allowed':False,'source_deletion_allowed':False})+'\n')
python='/home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python'
shell=shlex.join(['nice','-n','19','ionice','-c','3',python,'-u',str(work/'audit_year1_prepublication.py')])+' > '+shlex.quote(str(work/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(work/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'work':str(work),'session':task,'runner_sha256':SHA,'projects':16,'files':sum(len(p['files']) for p in projects),'read_only':True,'leases_acquired':0,'publication_allowed':False,'source_deletion_allowed':False}))
'''

if __name__=='__main__':
    output=HERE/'year1-prepublication-audit-launch.json';assert not output.exists(),'Inspect current audit instead of replay'
    plan=json.loads((HERE/'year1-next-stage-candidate-plan-20261109.json').read_text())
    selected=[p for p in plan['projects'] if p['metadata_relink_completed'] and p['candidate_raw_years']==['2024_1']];assert len(selected)==16
    admission=remote_json(SSH,'webserver-labo',ADMIT,selected,container=True)
    (HERE/'runtime/year1-prepublication-admission.json').write_text(json.dumps(admission,indent=2)+'\n')
    script=(HERE/'audit_year1_prepublication.py').read_bytes()
    result=remote('linux','SELECTION='+repr(selected)+'\nADMISSION='+repr(admission)+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\nSHA='+repr(hashlib.sha256(script).hexdigest())+'\n'+LAUNCH,timeout=90)
    with output.open('x') as stream:json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(result))
