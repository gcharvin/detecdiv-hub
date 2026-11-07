"""New readonly scope: 14 available fields plus target parity with reused FOV1 pixels."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote,api

ADMISSION=r'''
import json,uuid
from datetime import datetime,timezone
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Project,Job,IndexingJob,ProjectLock
i=uuid.UUID('de636b9e-684a-4ab1-bb4a-0bc64d435020');now=datetime.now(timezone.utc)
with SessionLocal() as db:
 p=db.get(Project,i);assert p is not None and p.owner.user_key=='alexander'
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  assert j.project_id!=i and str(i) not in json.dumps(j.params_json or {},default=str),'Project occupied'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 assert not [l for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id==i,ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
 print(json.dumps({'read_only':True,'project_id':str(i),'checked_at':now.isoformat(),'jobs_indexing_leases_clear':True,'leases_acquired':0}))
'''

CODE=r'''
import base64,hashlib,json,os,shlex,subprocess
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');w=root/'lox-available-fields-20261107'
task='alex2024-lox-available-fields-20261107'
assert not w.exists() and task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
base=root/'availability-variants-identity-v2-20261107';prior=base/'project-06.partial.json'
assert (base/'controller.exit').read_text().strip()=='0'
assert hashlib.sha256(prior.read_bytes()).hexdigest()=='23574f8c50dfecf20eb165c9c319f0ba03ca1d3a0aa5075b2711c5a7da08c5cf'
parity=root/'pending-reference-parity-20261107/result.json'
assert hashlib.sha256(parity.read_bytes()).hexdigest()=='5534c9800b12487e4a58b9cf29fc36999c87e4ff8df5651069594ef4489b59b5'
assert (parity.parent/'controller.exit').read_text().strip()=='0' and json.loads(parity.read_text())['success']
project=next(p for p in json.loads((base/'request.json').read_text())['projects'] if p['project_id']=='de636b9e-684a-4ab1-bb4a-0bc64d435020')
assert project['name']=='anof_290424_loxP15' and project['mat_sha256']=='c1a32a8cca1847fe72937423501a1ae8ddbb0c9d1b03460d33f081b5b6558898'
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'verify_lox_available_fields.m').write_bytes(script)
(w/'request.json').write_text(json.dumps({'project':project,'prior_fov_file':str(prior),'prior_fov_sha256':hashlib.sha256(prior.read_bytes()).hexdigest(),
 'native_parity_sha256':hashlib.sha256(parity.read_bytes()).hexdigest(),'script_sha256':SHA,'admission':ADMISSION,'missing_fields_remain_unresolved':True})+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');verify_lox_available_fields('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'script_sha256':SHA,'read_only':True,'new_source_fovs':14,'prior_fov1_source_reads_reused':True,'strict_project_access_gate_passed':False,'scientific_files_saved':False}))
'''

if __name__=='__main__':
    assert not (HERE/'lox-available-fields-launch.json').exists(),'Inspect existing task instead of replay'
    admission=api(ADMISSION)
    (HERE/'runtime/lox-available-fields-admission.json').write_text(json.dumps(admission,indent=2)+'\n')
    script=(HERE/'verify_lox_available_fields.m').read_bytes()
    result=remote('linux','SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\nADMISSION='+repr(admission)+'\n'+CODE,timeout=90)
    with (HERE/'lox-available-fields-launch.json').open('x') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result))
