"""Separate read-only review of new JSON alternatives and unknown legacy MATs."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import api,remote


def main():
    admission=api(r'''
import json
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Job,ProjectLock
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'));now=datetime.now(timezone.utc)
 busy={str(j.project_id) for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))) if j.project_id}
 busy.update(str(l.project_id) for l in db.scalars(select(ProjectLock).where(ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now)
 print(json.dumps({'checked_at':now.isoformat(),'busy_project_ids':sorted(busy),'read_only':True}))
''')
    script=(HERE/'review_legacy_paths.m').read_bytes()
    code=r'''
import json,os,base64,hashlib,shlex,subprocess
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
assert (root/'project-discovery.exit').read_text().strip()=='0'
original=json.loads((root/'project-discovery-request.json').read_text())
rows=[json.loads(line) for line in (root/'project-discovery-receipt.jsonl').open()]
review=json.loads((root/'project-discovery-format-review.json').read_text())
w=root/'legacy-path-review';assert not w.exists();w.mkdir(mode=0o700)
acquisitions=original['acquisitions']
jsonrows=[]
def strings(v):
 if isinstance(v,str):yield v
 elif isinstance(v,list):
  for i in v:yield from strings(i)
 elif isinstance(v,dict):
  for i in v.values():yield from strings(i)
for p in review['missing_json_alternatives']:
 row={'id':p['id'],'name':p['name'],'owner':p['owner'],'files':p['files'],'matches':[],'read_only':True}
 try:
  for file in p['files']:
   f=Path(file);before=f.stat();document=json.loads(f.read_text(encoding='utf-8-sig'))
   matches=[]
   for value in strings(document):
    normalized='/'+value.replace('\\','/').strip('/')+'/'
    for acq in acquisitions:
     if '/'+acq['name']+'/' in normalized:matches.append({'acquisition':acq['year']+'/'+acq['name'],'value':value})
   after=f.stat();assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
   row['matches'].extend(matches)
  row['status']='json_paths_inspected'
 except Exception as e:row.update(status='requires_review',error=type(e).__name__+': '+str(e))
 jsonrows.append(row)
(w/'json-alternatives-receipt.json').write_text(json.dumps(jsonrows,indent=2)+'\n')
projects=[{'id':r['project_id'],'name':r['name'],'owner':r['owner'],'file':r['file']} for r in rows if r.get('error')=='Main MAT is not a recognized shallowObj layout']
request={'projects':projects,'acquisitions':acquisitions,'busy_project_ids':ADMISSION['busy_project_ids'],'admission':ADMISSION,'read_only':True,'script_sha256':SHA}
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
(w/'request.json').write_text(json.dumps(request)+'\n');(w/'review_legacy_paths.m').write_bytes(script)
task='alex2024-legacy-path-review-20261105'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');review_legacy_paths('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'legacy_projects':len(projects),'script_sha256':SHA,'read_only':True,
 'json_alternatives_inspected':len(jsonrows),'json_candidate_projects':sum(bool(r['matches']) for r in jsonrows),'json_reviews_remaining':sum(r['status']=='requires_review' for r in jsonrows),
 'missing_main_files_still_unresolved':len(review['still_missing'])}))
'''
    code='ADMISSION='+repr(admission)+'\nSHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code
    result=remote('linux',code,timeout=90)
    with (HERE/'legacy-format-review-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
