import json
import ops

# Only remove PID markers from the disposable synthetic fixture, after checking
# that every corresponding process from the first test has exited.
print(ops.remote('linux', '''
import json,psutil
from pathlib import Path
root=Path.cwd().resolve();folder=(root/'.deploy-staging/job-force-stop-20261020/smoke').resolve()
assert root in folder.parents
for name in ('matlab.pid','python-0.pid','python-1.pid'):
 p=folder/name
 if p.exists():
  assert not psutil.pid_exists(int(p.read_text())),name
  p.unlink()
print(json.dumps({'old_smoke_markers_cleared':True}))
'''))
result=ops.api('''
import json,requests
from pathlib import Path
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,ExecutionTarget
p=Path('/tmp/job-force-stop-20261020-smoke.json');state=json.loads(p.read_text())
assert 'first_job_id' not in state,'Second test already submitted'
with SessionLocal() as s:
 first=s.get(Job,state['job_id']);assert first.status=='cancelled' and first.force_stopped_at
 params=first.params_json
 payload={'project_id':state['project_id'],'execution_target_id':state['target_id'],'requested_mode':'server','priority':100,
  'requested_by':'gilles','requested_from_host':'ops-force-stop-ui-smoke',
  'project_ref':params['project_ref'],'pipeline_ref':params['pipeline_ref'],
  'run_request':{**params['run_request'],'run_id':'force_stop_ui_20261020'},
  'execution':{'allow_gui':False,'interactive':False,'save_project':False,'matlab_max_threads':1,'code_commit':params['execution']['code_commit']},
  'client_context':{'synthetic':True,'rollout':'job-force-stop-20261020','validation':'live_browser_confirmation'}}
with SessionLocal.begin() as s:
 t=s.get(ExecutionTarget,state['target_id'],with_for_update=True);m=dict(t.metadata_json or {})
 assert not m.get('drain_new_jobs')
 present='drain_new_jobs' in m;previous=m.get('drain_new_jobs')
 m['drain_new_jobs']=True;m['force_stop_smoke_admission']={'rollout':'job-force-stop-20261020'};t.metadata_json=m
try:
 r=requests.post('http://127.0.0.1:8000/pipeline-runs',headers={'Authorization':'Bearer '+state['token']},json=payload,timeout=30)
 assert r.status_code==201,(r.status_code,r.text[:1500])
 state['first_job_id']=state['job_id'];state['job_id']=r.json()['id'];p.write_text(json.dumps(state))
 with SessionLocal.begin() as s:
  j=s.get(Job,state['job_id'],with_for_update=True);assert j.status=='queued'
  params=dict(j.params_json or {});params['resources']={'cpu_cores':1,'memory_mb':4096};j.params_json=params
finally:
 with SessionLocal.begin() as s:
  t=s.get(ExecutionTarget,state['target_id'],with_for_update=True);m=dict(t.metadata_json or {})
  assert m.pop('force_stop_smoke_admission')=={'rollout':'job-force-stop-20261020'}
  if present:m['drain_new_jobs']=previous
  else:m.pop('drain_new_jobs',None)
  t.metadata_json=m
print(json.dumps({k:v for k,v in state.items() if k!='token'}))
''')
ops.save('smoke-ui-job.json',result)
print(result)
