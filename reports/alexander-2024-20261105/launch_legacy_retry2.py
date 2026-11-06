"""Read only 136 deleted graphics-handle cases with a non-dereferencing GUI reader."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import api,remote


def main():
    busy=api(r'''
import json
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Job,ProjectLock
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'));now=datetime.now(timezone.utc)
 ids={str(j.project_id) for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))) if j.project_id}
 ids.update(str(l.project_id) for l in db.scalars(select(ProjectLock).where(ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now)
 print(json.dumps({'busy_project_ids':sorted(ids),'checked_at':now.isoformat(),'read_only':True}))
''')
    script=(HERE/'review_legacy_paths_v3.m').read_bytes()
    code=r'''
import base64,hashlib,json,os,shlex,subprocess
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
old=root/'legacy-path-review-retry-20261106'
assert (old/'controller.exit').read_text().strip()=='0'
raw=(old/'receipt.jsonl').read_bytes();assert hashlib.sha256(raw).hexdigest()=='b11c9f9f58e6f21cd6e1599cf2097ff104a6422c5211b9f97054f35b3f89beec'
rows=[json.loads(line) for line in raw.splitlines()]
errors={'Invalid or deleted object.'}
ids={r['id'] for r in rows if r.get('error') in errors};assert len(ids)==136
request=json.loads((old/'request.json').read_text())
request['projects']=[p for p in request['projects'] if p['id'] in ids]
request.update(busy_project_ids=BUSY['busy_project_ids'],admission=BUSY,script_sha256=SHA,
 retry_reason='Only 136 invalid graphics-Line handle errors: inspect accessible metadata; count but do not dereference deleted GUI handles; no source writes')
w=root/'legacy-path-review-retry2-20261106';assert not w.exists()
task='alex2024-legacy-path-retry2-20261106'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'request.json').write_text(json.dumps(request)+'\n');(w/'review_legacy_paths_v3.m').write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');review_legacy_paths_v3('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':len(ids),'script_sha256':SHA,'read_only':True,'original_receipt_preserved':True}))
'''
    result=remote('linux','BUSY='+repr(busy)+'\nSHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code,timeout=60)
    with (HERE/'legacy-retry2-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
