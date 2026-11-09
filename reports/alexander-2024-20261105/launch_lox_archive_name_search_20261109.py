"""Fresh readonly Lox admission, then one detached archive-directory name scan."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
from audit import remote_json,SSH

def main():
    assert not (HERE/'lox-archive-name-search-launch-verification.json').exists(),'No replay'
    code=r'''
import json,uuid
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Job,ProjectLock,IndexingJob
i=uuid.UUID('de636b9e-684a-4ab1-bb4a-0bc64d435020')
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'));now=datetime.now(timezone.utc)
 assert not list(db.scalars(select(ProjectLock.id).where(ProjectLock.project_id==i,ProjectLock.status=='active',ProjectLock.expires_at>now)))
 for job in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):assert job.project_id!=i and str(i) not in json.dumps(job.params_json or {})
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 print(json.dumps({'read_only':True,'jobs_leases_indexing_clear':True,'checked_at':now.isoformat(),'lease_acquired':False}))
'''
    admission=remote_json(SSH,'webserver-labo',code,{},container=True)
    script=(HERE/'search_lox_archive_names_20261109.py').read_bytes();request={'read_only':True,'source_deletion_allowed':False,'matlab_loading_allowed':False,'runner_sha256':hashlib.sha256(script).hexdigest(),'admission':admission,'scope':'New directory/main-file name search for Lox archives only; no content recovery or identity claim'}
    setup=r'''
import json,base64,shlex,subprocess
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/lox-archive-name-search-20261109');assert not w.exists();w.mkdir(mode=0o700)
(w/'search_lox_archive_names_20261109.py').write_bytes(base64.b64decode(SCRIPT));(w/'request.json').write_text(json.dumps(REQUEST,indent=2)+'\n')
shell=shlex.join(['/home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python','-u',str(w/'search_lox_archive_names_20261109.py')])+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s','alex2024-lox-archive-name-search-20261109','bash','-c',shell],check=True)
print(json.dumps({'launched':True,'work':str(w),'read_only':True,'source_deleted':False,'project_saved':False,'cleanup_gate_passed':False}))
'''
    result=remote('linux','SCRIPT='+repr(base64.b64encode(script).decode())+'\nREQUEST='+repr(request)+'\n'+setup,timeout=45)
    result.update(request)
    with (HERE/'lox-archive-name-search-launch-verification.json').open('x',encoding='utf-8') as stream:json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(result))

if __name__=='__main__':main()
