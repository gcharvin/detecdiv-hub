"""Fresh read-only admission and one-shot candidate copy preparation."""
import base64
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import api,remote


def main():
    status=json.loads((HERE/'runtime/latest-status.json').read_text())
    assert status['project_discovery_exit'].strip()=='0' and status['project_discovery_result']['success']
    selected=[p for p in status['project_candidates'] if p['owner']=='alexander' and
              p['file']=='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis/'+p['name']+'.mat']
    assert len(selected)==32 and all(p['source_metadata_stable'] for p in selected)
    projects=[{'project_id':p['project_id'],'name':p['name'],'owner':p['owner'],'source_mat':p['file'],
               'candidate_acquisitions':p['matching_acquisitions'],'fovs':p['fovs'],'rois':p['rois']} for p in selected]
    ids=[p['project_id'] for p in projects]
    code=r'''
import json,uuid
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Job,ProjectLock,Project,IndexingJob
ids={uuid.UUID(x) for x in IDS}
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 now=datetime.now(timezone.utc)
 busy=[]
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  params=json.dumps(j.params_json or {},default=str)
  if j.project_id in ids or any(str(i) in params for i in ids):busy.append(str(j.id))
 assert not busy,'Affected project is active'
 locks=[str(l.id) for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(ids),ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
 assert not locks,'Affected project is leased'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 assert all(p.owner.user_key=='alexander' for p in db.scalars(select(Project).where(Project.id.in_(ids))))
 print(json.dumps({'checked_at':now.isoformat(),'project_count':len(ids),'affected_jobs':busy,'affected_locks':locks,'read_only':True,'catalog_changed':False}))
'''
    admission=api('IDS='+repr(ids)+'\n'+code)
    with (HERE/'project-copy-admission.json').open('x',encoding='utf-8') as f:json.dump(admission,f,indent=2);f.write('\n')
    script=(HERE/'copy_projects.py').read_bytes()
    request={'projects':projects,'runner_sha256':hashlib.sha256(script).hexdigest(),'admission':admission,
             'source_deletion_allowed':False,'catalog_cutover_allowed':False,'project_paths_changed':False,
             'candidate_matches_are_not_scientific_identity_proof':True}
    with (HERE/'project-copy-request.json').open('x',encoding='utf-8') as f:json.dump(request,f,indent=2);f.write('\n')
    code=r'''
import os,json,hashlib,base64,shlex,subprocess
from pathlib import Path
from datetime import datetime,timezone
os.umask(0o077)
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/project-copies')
assert not w.exists(),'Inspect partial copies, never replay'
task='alex2024-project-copies-20261105'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
assert (datetime.now(timezone.utc)-datetime.fromisoformat(REQUEST['admission']['checked_at'])).total_seconds()<900
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==REQUEST['runner_sha256']
for p in REQUEST['projects']:
 root=Path('/homes2/maliavko/DetecdivHub/projects')
 assert not (root/p['name']).exists() and not (root/('.incoming-'+p['name']+'-20261105')).exists()
w.mkdir(mode=0o700);(w/'copy_projects.py').write_bytes(script);(w/'request.json').write_text(json.dumps(REQUEST)+'\n')
args=['/home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python','-u',str(w/'copy_projects.py')]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':len(REQUEST['projects']),'runner_sha256':REQUEST['runner_sha256'],
 'originals_preserved':True,'source_deleted':False,'project_paths_changed':False,'catalog_changed':False}))
'''
    result=remote('linux','REQUEST='+repr(request)+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code,timeout=60)
    (HERE/'project-copy-launch.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
