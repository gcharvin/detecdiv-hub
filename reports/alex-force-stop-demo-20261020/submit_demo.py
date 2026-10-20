import json
from common import ops, OUT, OLD

paths=json.loads((OUT/'fixture-paths.json').read_text())
runtime=json.loads((OLD/'fixture-runtime.json').read_text())
commit=runtime['release'].rsplit('/',1)[-1]
v=ops.api(f'''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import User,Project,ExecutionTarget,Job
from api.schemas import PipelineRunCreateRequest
from api.services.pipeline_runs import create_pipeline_run_job
from api.services.users import user_can_edit_project
KEY='alex_force_stop_demo_admission'
with SessionLocal.begin() as s:
 actor=s.scalars(select(User).where(User.user_key=='gilles',User.is_active.is_(True))).one();assert actor.role=='admin'
 alex=s.scalars(select(User).where(User.user_key=='alexander',User.is_active.is_(True))).one()
 existing=s.scalars(select(Project).where(Project.project_key=='ops-force-stop-demo-alexander-20261020')).first()
 assert existing is None,'Demo already provisioned; inspect instead of creating a duplicate'
 project=Project(project_key='ops-force-stop-demo-alexander-20261020',project_name='Force-stop demonstration - Alexander',
  owner_user_id=alex.id,visibility='private',status='indexed',backup_excluded=True,
  notes='Demonstration only. No microscope data. In Run Monitor, select alex_force_stop_demo_20261020, click Force stop, and confirm. The demo completes automatically after 30 minutes.',
  metadata_json={{'synthetic':True,'purpose':'force_stop_training','created_by_user_key':'gilles','demo_for':'alexander'}})
 s.add(project);s.flush();project_id=project.id
 target=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='detecdiv-server').with_for_update()).one()
 meta=dict(target.metadata_json or {{}});assert not meta.get('drain_new_jobs') and KEY not in meta
 previous={{'present':'drain_new_jobs' in meta,'value':meta.get('drain_new_jobs')}}
 meta['drain_new_jobs']=True;meta[KEY]=previous;target.metadata_json=meta;target_id=target.id
try:
 with SessionLocal() as s:
  alex=s.scalars(select(User).where(User.user_key=='alexander',User.is_active.is_(True))).one()
  payload=PipelineRunCreateRequest(project_id=project_id,execution_target_id=target_id,requested_mode='server',priority=100,
   requested_by='alexander',requested_from_host='ops-demo-on-behalf-of-alexander',
   project_ref={{'project_mat_path':{paths['project_json']!r}}},
   pipeline_ref={{'pipeline_json_path':{paths['pipeline_json']!r}}},
   run_request={{'run_id':'alex_force_stop_demo_20261020','selected_nodes':['force_stop_demo'],
    'gpu':{{'mode':'force_cpu'}},'description':'Force-stop demonstration: empty project, no experimental data; auto-completes after 30 minutes.'}},
   execution={{'code_commit':{commit!r},'allow_gui':False,'interactive':False,'save_project':False,'matlab_max_threads':1}},
   client_context={{'synthetic':True,'initiated_by_user_key':'gilles','on_behalf_of':'alexander','purpose':'user_force_stop_demonstration'}})
  job=create_pipeline_run_job(s,payload=payload,current_user=alex,submitted_via='ops_demo_on_behalf')
  assert job.status=='queued'
  params=dict(job.params_json or {{}});params['resources']={{'cpu_cores':1,'memory_mb':4096}};job.params_json=params
  s.commit()
  assert job.requested_by=='alexander'
  assert user_can_edit_project(s.get(Project,project_id),alex)
  result={{'project_id':str(project_id),'job_id':str(job.id),'owner':'alexander','alex_can_edit':True,
   'run_id':'alex_force_stop_demo_20261020','auto_complete_minutes':30,'resources':params['resources'],
   'project_url':'http://detecdiv-hub.detecdiv.internal/web/project.html?id='+str(project_id)}}
finally:
 with SessionLocal.begin() as s:
  target=s.get(ExecutionTarget,target_id,with_for_update=True);meta=dict(target.metadata_json or {{}})
  owner=meta.pop(KEY);assert owner==previous
  if owner['present']:meta['drain_new_jobs']=owner['value']
  else:meta.pop('drain_new_jobs',None)
  target.metadata_json=meta
print(json.dumps(result))
''')
ops.save('demo-job.json',v)
print(json.dumps(v,indent=2))
