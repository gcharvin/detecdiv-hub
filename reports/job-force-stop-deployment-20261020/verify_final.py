import json
import ops

expected=json.loads((ops.OUT/'api-staging.json').read_text())['expected_fingerprint']
v=ops.api('''
import json,requests
from sqlalchemy import select
from api.db import SessionLocal
from api.models import User
from api.routes_assistant import get_assistant_status
h=requests.get('http://127.0.0.1:8000/health',timeout=10);h.raise_for_status()
openapi=requests.get('http://127.0.0.1:8000/openapi.json',timeout=10).json()
assert '/pipeline-runs/{job_id}/force-stop' in openapi['paths']
html=requests.get('http://127.0.0.1:8000/web/project.html',timeout=10).text
assert 'src="/web/app.js?v=20261020-force-stop-en1"' in html
js=requests.get('http://127.0.0.1:8000/web/app.js?v=20261020-force-stop-en1',timeout=10).text
assert 'Force stop requested for job' in js and 'Forcer' not in js
with SessionLocal() as s:
 user=s.scalars(select(User).where(User.user_key=='gilles')).one()
 status=get_assistant_status(current_user=user,db=s)
print(json.dumps({'health':h.json(),'force_stop_route':True,'english_ui':True,'assistant_status':status.message}))
''')
assert v['health']['database_status']=='ok'
assert v['health']['code_fingerprint']==expected,(expected,v)
ops.save('api-final-verification.json',v)
print('API verified:',expected,v['assistant_status'])
v=ops.remote('api','''
import json,hashlib
from pathlib import Path
root=Path.cwd();m=json.loads((root/'.deploy-staging/job-force-stop-20261020/manifest.json').read_text())
for name,item in m['files'].items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==item['after'],name
print(json.dumps({'files_verified':len(m['files']),'fingerprint':m['expected_fingerprint']}))
''')
ops.save('api-final-files.json',v)
print(v)
