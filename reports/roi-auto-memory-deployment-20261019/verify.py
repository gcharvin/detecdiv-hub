import json
import ops
from rollout import snapshot_code

before = json.loads((ops.OUT / 'before-activation.json').read_text())
after = ops.api(snapshot_code)
old_jobs = {j['id']: j for j in before['jobs']}
new_jobs = {j['id']: j for j in after['jobs']}
for key, original in old_jobs.items():
    assert key in new_jobs, 'Active job unexpectedly left running state: ' + key
    current = new_jobs[key]
    assert current['params_hash'] == original['params_hash']
    assert current['allocation'] == original['allocation']
    assert current['started_at'] == original['started_at']
    assert current['heartbeat'] > original['heartbeat']
for original in before['workers']:
    if original['job']:
        current = next(w for w in after['workers'] if w['target'] == original['target'] and w['instance'] == original['instance'])
        assert current['pid'] == original['pid'] and current['job'] == original['job']
for t in after['targets'].values():
    assert not t['drain_new_jobs'] and not t['roi_memory_rollout']
ops.save('after-activation.json', after)
print(json.dumps({'jobs_preserved': list(old_jobs), 'workers': after['workers']}))

for label in ops.TARGETS:
    result = ops.remote(label, r'''
import json,os,subprocess,sys,hashlib
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
root=Path.cwd();stage=root/'.deploy-staging/roi-auto-memory-20261019'
platform=PLATFORM
if platform=='linux':
 pid=subprocess.check_output(['systemctl','show','detecdiv-worker@1.service','-p','MainPID','--value'],text=True).strip()
 env=dict(item.split('=',1) for item in Path('/proc',pid,'environ').read_text().split(chr(0)) if '=' in item)
 os.environ.update({k:v for k,v in env.items() if k.startswith('DETECDIV_HUB_')})
else:
 from dotenv import load_dotenv
 load_dotenv(root/'.env',override=False)
 sys.path.insert(0,str(stage/'runtime'))
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import ExecutionTarget
from api.services.job_priority_settings import resolve_job_resource_runtime_config
import api.services.job_priority_settings as priority_settings
priority_settings.ensure_system_settings_table=lambda session: None
from worker.job_resources import resolve_job_resource_allocation,allocation_fits,resource_config_for_target
from worker.memory_resources import memory_policy,worker_allocation_fits
source=stage/'probe-pipeline.json'
source.write_text(json.dumps({'nodes':[{'id':'extract','type':'roiExtract','pkg':'roiExtract','func':'roiExtract.process'}]}))
with SessionLocal() as s:
 s.execute(text('SET TRANSACTION READ ONLY'))
 key='detecdiv-server' if platform=='linux' else 'windows-10-20-11-56'
 target=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key==key)).one()
 config=resource_config_for_target(resolve_job_resource_runtime_config(s),target)
 policy=memory_policy(target)
 totals={'cpu_cores':0,'memory_mb':0,'memory_capacity_mb':policy['capacity_mb'],'disk_io_units':0,'gpu_jobs':0,'gpu_vram_mb':0,'gpu_exclusive':False,'allocations':{}}
 job=SimpleNamespace(id=uuid4(),project_id=None,pipeline_id=None,execution_target_id=target.id,requested_mode='server',params_json={'job_kind':'pipeline_run','pipeline_ref':{'pipeline_json_path':str(source)},'run_request':{'selected_nodes':['extract']}})
 results=[]
 for busy in [0,48*1024,55*1024]:
  totals['memory_mb']=busy
  a=resolve_job_resource_allocation(s,job=job,target=target,config=config,totals=totals)
  results.append({'busy_mb':busy,'memory_mb':a['memory_mb'],'mode':a.get('memory_allocation_mode'),'fit':allocation_fits(a,totals=totals,config=config),'worker_fit':worker_allocation_fits(a,policy=policy),'inspection':a['pipeline_gpu_profile'].get('roi_extraction')})
 assert all(r['inspection']['eligible'] for r in results),results
 if platform=='linux':
  assert [r['memory_mb'] for r in results]==[49152,30720,24576],results
  assert [r['fit'][0] for r in results]==[True,True,False],results
 else:
  assert not policy['dynamic']
  assert all(r['mode'] is None for r in results)
  assert len({r['memory_mb'] for r in results})==1
 manifest=json.loads((stage/'manifest.json').read_text())
 hashes={name:hashlib.sha256((root/name).read_bytes()).hexdigest() if (root/name).exists() else None for name in manifest['files']}
 print(json.dumps({'policy':policy,'checks':results,'live_hashes':hashes,'expected_fingerprint':manifest['expected_fingerprint'],'activation':json.loads((stage/'activation-last.json').read_text())}))
'''.replace('PLATFORM', repr(label)))
    ops.save(label + '-policy-verification.json', result)
    print(json.dumps({'host': label, **result}))
