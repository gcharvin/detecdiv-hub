"""Inspect verified small archive members as a new read-only legacy scope."""

import base64
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import api, remote

ADMISSION = r'''
import json
from sqlalchemy import select, text
from api.db import SessionLocal
from api.models import IndexingJob, Job, ProjectLock
with SessionLocal() as db:
    db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    busy = {str(row.project_id) for row in db.scalars(select(Job).where(
        Job.status.in_(['queued', 'running', 'cancelling']))) if row.project_id}
    busy.update(str(row.project_id) for row in db.scalars(select(ProjectLock).where(
        ProjectLock.status == 'active', (ProjectLock.expires_at.is_(None) | (ProjectLock.expires_at > __import__('sqlalchemy').func.now())))))
    indexing = list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued', 'running']))))
    print(json.dumps({'busy_project_ids': sorted(busy), 'indexing_count': len(indexing), 'read_only': True}))
'''

LAUNCH = r'''
import base64,hashlib,json,os,re,shlex,stat,subprocess,sys,zipfile
from datetime import datetime,timezone
from pathlib import Path,PurePosixPath
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
w=root/'legacy-archived-main-path-review-20261112'
task='alex2024-legacy-archived-main-path-review-20261112'
assert not w.exists()
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
members=(root/'legacy-registered-zip-main-header-scope-20261112/members.json').read_bytes()
assert hashlib.sha256(members).hexdigest()=='9f8ee544954ada1d32e2032a8644b2fe81bcd3a515e50e2fd2f851cd0130f41f'
scan=(root/'legacy-registered-zip-main-bounded-reference-scan-20261112/receipt.json').read_bytes()
assert hashlib.sha256(scan).hexdigest()=='b10e8c0759f84757978d21b22c466e8be0a97d8a254f93566951c7109a43a88d'
plan=(root/'project-discovery-format-review.json').read_bytes()
assert hashlib.sha256(plan).hexdigest()=='93636f5c65e545e24f2e7e52b5d92d700033212ecc4fabc50c009fa29c47de6f'
baseline={x['id']:x for x in json.loads(plan)['still_missing']}
fingerprints={(x['archive'],x['member']):x for x in json.loads(scan)}
def normalized(value):return re.sub('[^a-z0-9]+','',value.casefold())
members=json.loads(members);assert len(members)==len(fingerprints)==37
assignments=[]
for member in members:
    stamp=fingerprints[(member['archive'],member['member'])]
    name=normalized(PurePosixPath(member['member']).stem)
    ids=[i for i in member['projects'] if any(normalized(Path(p).stem)==name for p in baseline[i]['files'])]
    assert len(ids)==1 and stamp['bytes']==member['bytes']<=22874
    assignments.append((ids[0],member,stamp))
assert len({x[0] for x in assignments})==37
assert BUSY['indexing_count']==0,'Defer while indexing is active'
release=Path('/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7')
assert subprocess.check_output(['git','-C',str(release),'rev-parse','HEAD'],text=True).strip()==release.name
os.umask(0o077);w.mkdir(mode=0o700);(w/'inputs').mkdir(mode=0o700)
rows=[];extraction=[]
for project,member,stamp in assignments:
    if project in BUSY['busy_project_ids']:
        extraction.append({'project_id':project,'status':'deferred_busy'});continue
    path=Path(member['archive']);assert path.parts[:2]==('/','archive') and path.resolve(strict=True)==path
    with os.fdopen(os.open(path,os.O_RDONLY|os.O_NOFOLLOW),'rb') as stream:
        before=os.fstat(stream.fileno());assert stat.S_ISREG(before.st_mode)
        with zipfile.ZipFile(stream,'r') as z:
            info=z.getinfo(member['member']);assert info.file_size==member['bytes'] and f'{info.CRC:08x}'==member['crc32']
            with z.open(info) as f:content=f.read(info.file_size+1)
        after=os.fstat(stream.fileno())
    assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
    assert len(content)==stamp['bytes'] and hashlib.sha256(content).hexdigest()==stamp['sha256']
    target=w/'inputs'/(project+'.mat')
    with target.open('xb') as f:f.write(content);f.flush();os.fsync(f.fileno())
    old=baseline[project]
    rows.append({'id':project,'name':old['name'],'owner':old['owner'],'file':str(target)})
    extraction.append({'project_id':project,'status':'copied_for_readonly_inspection','archive':str(path),
        'member':member['member'],'bytes':len(content),'sha256':stamp['sha256'],'archive_metadata_stable':True})
assert rows,'All scoped projects are occupied'
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
(w/'review_legacy_paths_v3.m').write_bytes(script)
original=json.loads((root/'project-discovery-request.json').read_bytes())
request={'projects':rows,'acquisitions':original['acquisitions'],'busy_project_ids':[],
         'read_only':True,'script_sha256':SHA,'admission':BUSY,'source_saved':False}
(w/'request.json').write_text(json.dumps(request)+'\n')
(w/'archive-inputs.json').write_text(json.dumps(extraction,indent=2)+'\n')
expression="maxNumCompThreads(1);addpath('"+str(w)+"');review_legacy_paths_v3('"+str(w)+"','"+str(release)+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'work':w.name,'projects':len(rows),'deferred':37-len(rows),
 'script_sha256':SHA,'read_only':True,'source_saved':False,'started_at':datetime.now(timezone.utc).isoformat()}))
'''


def main():
    launch = HERE / 'archived-legacy-readonly-launch-verification.json'
    assert not launch.exists()
    admission = api(ADMISSION)
    script = (HERE / 'review_legacy_paths_v3.m').read_bytes()
    prefix = (f'BUSY={admission!r}\nSHA={hashlib.sha256(script).hexdigest()!r}\n'
              f'SCRIPT={base64.b64encode(script).decode()!r}\n')
    result = remote('linux', prefix + LAUNCH, timeout=60)
    with launch.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
