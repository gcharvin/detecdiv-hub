import json
import ops
state=json.loads((ops.OUT/'smoke-job.json').read_text())
result=ops.api(f'''
import json,os
from pathlib import Path
from datetime import datetime,timedelta,timezone
from sqlalchemy import select
from api.db import SessionLocal
from api.models import User,UserSession
from api.services.auth import issue_user_session
state={state!r}
with SessionLocal.begin() as s:
 old=s.get(UserSession,state['auth_session_id']);old.status='revoked'
 user=s.scalars(select(User).where(User.user_key=='gilles',User.is_active.is_(True))).one()
 auth,token=issue_user_session(s,user=user,client_label='job-force-stop-20261020-ui')
 auth.expires_at=datetime.now(timezone.utc)+timedelta(minutes=10)
 state['auth_session_id']=str(auth.id);state['token']=token
p=Path('/tmp/job-force-stop-20261020-smoke.json');p.write_text(json.dumps(state));os.chmod(p,0o600)
print(json.dumps({{k:v for k,v in state.items() if k!='token'}}))
''')
ops.save('smoke-active-auth.json',result)
print({'refreshed':True})
