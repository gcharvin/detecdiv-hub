import json
import ops

paths=json.loads((ops.OUT/'fixture-paths.json').read_text())
result=ops.api(f'''
import json,os,time,requests
from datetime import datetime,timedelta,timezone
from pathlib import Path
from sqlalchemy import select
from api.db import SessionLocal
from api.models import User,Project,ExecutionTarget,Job
from api.services.auth import issue_user_session
state_path=Path('/tmp/{ops.ROLLOUT}-smoke.json')
assert not state_path.exists(),'Smoke state exists; inspect to avoid duplicate jobs'
with SessionLocal.begin() as s:
 user=s.scalars(select(User).where(User.user_key=='gilles',User.is_active.is_(True))).one()
 assert user.role=='admin'
 project=Project(project_name='OPS - Test arrêt forcé 2026-10-20',owner_user_id=user.id,
  visibility='private',status='indexed',notes='Synthetic deployment smoke test. Empty project, no microscope data. Retained for cancellation audit.',
  metadata_json={{'synthetic':True,'purpose':'job_force_stop_deployment_smoke','rollout':{ops.ROLLOUT!r}}},backup_excluded=True)
 s.add(project);s.flush();project_id=str(project.id)
 auth,token=issue_user_session(s,user=user,client_label={ops.ROLLOUT!r})
 auth.expires_at=datetime.now(timezone.utc)+timedelta(minutes=15)
 session_id=str(auth.id)
with SessionLocal.begin() as s:
 target=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='detecdiv-server').with_for_update()).one()
 meta=dict(target.metadata_json or {{}})
 assert not meta.get('drain_new_jobs'),'Target already drained'
 previous_present='drain_new_jobs' in meta;previous=meta.get('drain_new_jobs')
 meta['drain_new_jobs']=True;meta['force_stop_smoke_admission']={{'rollout':{ops.ROLLOUT!r}}};target.metadata_json=meta
 target_id=str(target.id)
state={{'project_id':project_id,'auth_session_id':session_id,'token':token,'target_id':target_id}}
state_path.write_text(json.dumps(state));os.chmod(state_path,0o600)
try:
 payload={{'project_id':project_id,'execution_target_id':target_id,'requested_mode':'server',
  'priority':100,'requested_by':'gilles','requested_from_host':'ops-force-stop-smoke',
  'project_ref':{{'project_mat_path':{paths['project_json']!r}}},
  'pipeline_ref':{{'pipeline_json_path':{paths['pipeline_json']!r}}},
  'run_request':{{'run_id':'force_stop_smoke_20261020','selected_nodes':['force_stop_probe'],
    'gpu':{{'mode':'force_cpu'}},'description':'Synthetic stop test without cancellation checkpoints'}},
  'execution':{{'save_project':False,'allow_gui':False,'interactive':False,'matlab_max_threads':1}},
  'client_context':{{'synthetic':True,'rollout':{ops.ROLLOUT!r}}}}}
 r=requests.post('http://127.0.0.1:8000/pipeline-runs',headers={{'Authorization':'Bearer '+token}},json=payload,timeout=30)
 assert r.status_code==201,(r.status_code,r.text[:1500])
 job_id=r.json()['id'];state['job_id']=job_id;state_path.write_text(json.dumps(state))
 with SessionLocal.begin() as s:
  job=s.get(Job,job_id,with_for_update=True);assert job.status=='queued'
  params=dict(job.params_json or {{}});params['resources']={{'cpu_cores':1,'memory_mb':4096}}
  job.params_json=params
finally:
 with SessionLocal.begin() as s:
  target=s.get(ExecutionTarget,target_id,with_for_update=True)
  meta=dict(target.metadata_json or {{}});owner=meta.pop('force_stop_smoke_admission',None)
  assert owner=={{'rollout':{ops.ROLLOUT!r}}}
  if previous_present:meta['drain_new_jobs']=previous
  else:meta.pop('drain_new_jobs',None)
  target.metadata_json=meta
print(json.dumps({{k:v for k,v in state.items() if k!='token'}}))
''')
ops.save('smoke-job.json',result)
print(result)
