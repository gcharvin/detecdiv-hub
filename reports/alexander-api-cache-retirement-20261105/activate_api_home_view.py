"""Add only a readonly API bind, preserving the running image and workers."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
HERE.mkdir(exist_ok=True)
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote,api


def main():
    admission=api(r'''
import json,uuid
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Artifact,RawDataset,ProjectRawLink,Job,IndexingJob,WorkerInstance
prefix='/data/.detecdiv-hub-api-cache/alexander-2023_'
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 rids={a.job.raw_dataset_id for a in db.scalars(select(Artifact).where(Artifact.uri.startswith(prefix)))}|{r.id for r in db.scalars(select(RawDataset).where(RawDataset.display_settings_uri.startswith(prefix)))}
 assert len(rids)==109
 pids={p.project_id for p in db.scalars(select(ProjectRawLink).where(ProjectRawLink.raw_dataset_id.in_(rids)))}
 jobs=[]
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  params=json.dumps(j.params_json or {},default=str)
  assert j.raw_dataset_id not in rids and j.project_id not in pids and not any(str(id) in params for id in rids) and prefix not in params,'Affected job is active'
  jobs.append({'id':str(j.id),'status':j.status,'project_id':str(j.project_id)})
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 workers=[{'instance':w.worker_instance,'pid':w.process_id,'fingerprint':w.code_fingerprint,'current_job_id':str(w.current_job_id) if w.current_job_id else None} for w in db.scalars(select(WorkerInstance))]
 print(json.dumps({'checked_at':datetime.now(timezone.utc).isoformat(),'affected_jobs':0,'active_indexing':0,'unrelated_jobs':jobs,'workers':workers}))
''')
    code='ADMISSION='+repr(admission)+'\n'+r'''
import os,json,copy,hashlib,stat,subprocess,time,urllib.request
from datetime import datetime,timezone
from pathlib import Path
import yaml
os.umask(0o077)
assert (datetime.now(timezone.utc)-datetime.fromisoformat(ADMISSION['checked_at'])).total_seconds()<120
root=Path('/home/charvin-admin/repos/detecdiv-hub-webvm');directory=root/'ops/compose/webserver-labo';compose=directory/'compose.yaml'
work=Path('/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105');assert not work.exists();work.mkdir(mode=0o700)
(work/'activation-admission.json').write_text(json.dumps(ADMISSION,indent=2)+'\n')
def run(args,timeout=45):
 p=subprocess.run(args,cwd=directory,capture_output=True,text=True,timeout=timeout)
 if p.returncode:raise RuntimeError('Command failed: '+args[0]+' '+str(p.returncode)+'; inspect private activation log')
 return p.stdout.strip()
def inspect():
 value=run(['docker','inspect','detecdiv-hub-api','--format','{{.Id}}|{{.Image}}|{{.Config.Image}}|{{json .Mounts}}'])
 id,image,tag,mounts=value.split('|',3);return {'id':id,'image':image,'tag':tag,'mounts':json.loads(mounts)}
def health():
 with urllib.request.urlopen('http://127.0.0.1:8000/health',timeout=5) as response:return response.status==200
mount=json.loads(run(['findmnt','-J','-M','/homes2/maliavko/DetecdivHub','-o','SOURCE,TARGET,FSTYPE,OPTIONS']))['filesystems'][0]
assert mount['source']=='charvin-admin@10.20.11.100:/homes2/maliavko/DetecdivHub' and 'ro' in mount['options'].split(',')
assert Path('/homes2/maliavko/DetecdivHub/raw').is_dir()
before=inspect();assert before['image']=='sha256:2326824f70336590f16c2256da7e213a7322d7fa626a50760be38543f2a3516b'
assert run(['docker','image','inspect',before['tag'],'--format','{{.Id}}'])==before['image']
pg=run(['docker','inspect','detecdiv-hub-postgres','--format','{{.Id}}'])
# Preserve full old content-handler receipts before replacing the container.
archive=work/'prior-api-receipts';archive.mkdir(mode=0o700)
for name in ['detecdiv-alexander-2023-3-content-20261105','detecdiv-alexander-2023-3-content-20261105-post-retirement']:
 run(['docker','cp','detecdiv-hub-api:/tmp/'+name,str(archive/name)])
 assert hashlib.sha256((archive/name/'receipt.jsonl').read_bytes()).hexdigest()=='dffd5663adaca24daa5e72e265a06b785425eea6c935a10761ae7a1ee4abc667'
raw=compose.read_bytes();assert hashlib.sha256(raw).hexdigest()=='a365f389a63aff80c0f3b116f6311b6cd9ae74f598028697c46b18e37fb548d8'
(work/'compose-before.yaml').write_bytes(raw)
old=yaml.safe_load(raw);new=copy.deepcopy(old)
bind='/homes2/maliavko/DetecdivHub:/homes2/maliavko/DetecdivHub:ro'
assert new['services']['api']['volumes']==['/data:/data:ro']
new['services']['api']['volumes'].append(bind)
comparison=copy.deepcopy(new);comparison['services']['api']['volumes'].remove(bind);assert comparison==old
staged=directory/'compose.homes2-prepared.yaml';assert not staged.exists()
staged.write_text(yaml.safe_dump(new,sort_keys=False));os.chmod(staged,stat.S_IMODE(compose.stat().st_mode))
run(['docker-compose','-f',str(staged),'config','--quiet'])
written=False
try:
 assert compose.read_bytes()==raw and inspect()['id']==before['id']
 staged.replace(compose);written=True
 p=subprocess.run(['docker-compose','up','-d','--no-deps','--no-build','--force-recreate','api'],cwd=directory,capture_output=True,text=True,timeout=120)
 (work/'activation.log').write_text(p.stdout+p.stderr);assert p.returncode==0,'Inspect private activation log'
 for attempt in range(60):
  try:
   if health():break
  except Exception:pass
  time.sleep(1)
 else:raise RuntimeError('API health did not recover')
 after=inspect();assert after['image']==before['image'] and after['id']!=before['id']
 assert run(['docker','inspect','detecdiv-hub-postgres','--format','{{.Id}}'])==pg
 assert len(after['mounts'])==len(before['mounts'])+1
 assert any(m['Destination']=='/homes2/maliavko/DetecdivHub' and not m['RW'] for m in after['mounts'])
 run(['docker','exec','detecdiv-hub-api','test','-d','/homes2/maliavko/DetecdivHub/raw'])
 result={'success':True,'finished_at':datetime.now(timezone.utc).isoformat(),'before':before,'after':after,'postgres_container_unchanged':True,'worker_restart_commands_issued':False,'compose_before_sha256':hashlib.sha256(raw).hexdigest(),'compose_after_sha256':hashlib.sha256(compose.read_bytes()).hexdigest(),'api_health_ok':True,'prior_api_receipts_archived':True,'uri_changes':0,'cache_files_deleted':0}
 (work/'activation-result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
except BaseException:
 if written:
  tmp=directory/'compose.rollback.tmp';tmp.write_bytes(raw);os.chmod(tmp,stat.S_IMODE(compose.stat().st_mode));tmp.replace(compose)
  p=subprocess.run(['docker-compose','up','-d','--no-deps','--no-build','--force-recreate','api'],cwd=directory,capture_output=True,text=True,timeout=120)
  (work/'rollback.log').write_text(p.stdout+p.stderr)
 raise
'''
    result=remote('api',code,timeout=240)
    (HERE/'api-bind-activation.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    (HERE/'api-bind-admission.json').write_text(json.dumps(admission,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result))


if __name__=='__main__':main()
