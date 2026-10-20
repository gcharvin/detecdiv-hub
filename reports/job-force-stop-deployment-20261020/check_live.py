import json
import ops

v = ops.save('after-first-activation.json', ops.snapshot())
print(json.dumps(v['workers'], indent=2))
print(json.dumps({x['target_key']:{k:v for k,v in (x['metadata_json'] or {}).items()
                                    if 'rollout' in k or k=='drain_new_jobs'} for x in v['targets']},indent=2))
details = ops.api('''
import json
from api.config import get_settings
from api.db import SessionLocal
from sqlalchemy import select
from api.models import User
from api.services.matlab_code_versions import default_commit
with SessionLocal() as s:
 print(json.dumps({'release':default_commit(s),'admins':[u.user_key for u in s.scalars(select(User).where(User.role=='admin',User.is_active.is_(True)))],'legacy_auth':get_settings().allow_legacy_user_key_auth}))
''')
ops.save('smoke-context.json', details)
print(details)
for label in ('linux','windows'):
    result=ops.remote(label, '''
import json,psutil
from pathlib import Path
stage=Path('.deploy-staging/job-force-stop-20261020')
v=json.loads((stage/'activation-last.json').read_text()) if (stage/'activation-last.json').exists() else None
workers=[]
for p in psutil.process_iter(['pid','name','cmdline','create_time']):
 try:
  cmd=' '.join(p.info['cmdline'] or [])
  if 'python' in (p.info['name'] or '').lower() and ('-m worker.run_worker' in cmd or cmd.endswith('worker/run_worker.py')):
   workers.append({'pid':p.pid,'created':p.info['create_time'],'children':[{'pid':c.pid,'name':c.name(),'created':c.create_time()} for c in p.children(recursive=True)]})
 except (psutil.NoSuchProcess,psutil.AccessDenied):pass
print(json.dumps({'activation':v,'trees':workers}))
''')
    ops.save(label+'-live-trees.json',result)
    print(label, result['activation'], 'worker roots',[(p['pid'],len(p['children'])) for p in result['trees']])
