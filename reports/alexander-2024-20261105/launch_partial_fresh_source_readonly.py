"""Launch a one-shot read-only native inventory comparison for 2024 candidates."""

import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import api
from audit import SSH, remote_json
from verify_retirement_manifest import VERIFIED

ADMISSION = r'''
import datetime,json,uuid
from sqlalchemy import select,text,or_,func
from api.db import SessionLocal
from api.models import Job,ProjectLock,IndexingJob
projects=[uuid.UUID(x) for x in PROJECT_IDS];raws=[uuid.UUID(x) for x in RAW_IDS]
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
 active=('queued','running','cancelling')
 jobs=db.scalars(select(Job.id).where(Job.status.in_(active),or_(Job.project_id.in_(projects),Job.raw_dataset_id.in_(raws)))).all()
 locks=db.scalars(select(ProjectLock.id).where(ProjectLock.project_id.in_(projects),ProjectLock.status=='active',or_(ProjectLock.expires_at.is_(None),ProjectLock.expires_at>func.now()))).all()
 indexing=db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(active))).all()
 print(json.dumps({'checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'projects':len(projects),'raws':len(raws),'active_jobs':len(jobs),'active_locks':len(locks),'active_indexing':len(indexing),'read_only':True}))
'''

LAUNCH = r'''
import base64,hashlib,json,os,pathlib,shlex,subprocess,sys
v=json.load(sys.stdin);base=pathlib.Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work=base/'partial-source-freshness-20261116';session='alex2024-partial-source-freshness-20261116'
assert not work.exists()
sessions=subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
assert session not in sessions
assert (base/'controller.exit').read_text().strip()=='0'
assert hashlib.sha256((base/'copy_raws.py').read_bytes()).hexdigest()=='a0576fffa3c1d43d232f54567c6c9f7153aed40b436ace98f26079af72432b99'
script=base64.b64decode(v['script'],validate=True);assert hashlib.sha256(script).hexdigest()==v['script_sha256']
assert v['request']['read_only'] and not v['request']['eligible_for_unlink'] and v['request']['script_sha256']==v['script_sha256']
os.umask(0o077);work.mkdir(mode=0o700);(work/'partial_fresh_source_readonly.py').write_bytes(script)
payload=(json.dumps(v['request'],sort_keys=True)+'\n').encode();(work/'request.json').write_bytes(payload);request_sha=hashlib.sha256(payload).hexdigest()
args=['nice','-n','19','ionice','-c','3',sys.executable,'-u',str(work/'partial_fresh_source_readonly.py'),request_sha]
command=shlex.join(args)+' > '+shlex.quote(str(work/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(work/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',session,'bash','-c',command],check=True)
print(json.dumps({'launched':True,'work':work.name,'session':session,'request_sha256':request_sha,'script_sha256':v['script_sha256'],'read_only':True,'eligible_for_unlink':False}))
'''


def read_pinned(path, expected):
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == expected
    return json.loads(data)


def main():
    output = HERE / 'partial-source-freshness-launch-verification.json'
    assert not output.exists(), 'Inspect existing launch instead of replaying'
    runtime = HERE / 'runtime'
    subsets = read_pinned(runtime / 'retirement-partial-subset-digests-20261116.json',
                          '55c98657fa6c97399228afbb9d4192bc030ef5e5e554612b59ed4ad3e5059163')
    evidence = read_pinned(runtime / 'retirement-partial-evidence-matrix-20261116.json',
                           '95698aa0df74341569718db789813a785d9df25aebedbd3de6338a7c2f2c5fd1')
    media = read_pinned(runtime / 'retirement-partial-media-proof-map-20261116.json',
                        'e674ff86e7803d9b643e872c7bced6255edcfbdb0d6e52f9de83b4e2847e32aa')
    projects = sorted({item for row in evidence['rows'] for item in row['project_ids']})
    raws = sorted({item for row in media['roots'] for item in row['raw_ids']})
    assert len(projects) == 19 and len(raws) == 64 and len(evidence['rows']) == 17
    expected = {(row['year'], row['acquisition']) for row in evidence['rows']}
    actual = {(year, name) for year, item in subsets['years'].items() for name in item['roots']}
    assert actual == expected and len(actual) == 17
    admission = api(ADMISSION.replace('PROJECT_IDS', repr(projects)).replace('RAW_IDS', repr(raws)))
    assert admission['read_only'] and not any(admission[key] for key in ('active_jobs', 'active_locks', 'active_indexing'))
    assert (admission['projects'], admission['raws']) == (19, 64)
    script = (HERE / 'partial_fresh_source_readonly.py').read_bytes()
    sha = hashlib.sha256(script).hexdigest()
    fixture = json.loads((HERE / 'partial-fresh-source-v2-fixture-verification.json').read_bytes())
    assert fixture['exit'] == 0 and fixture['scripts_sha256']['partial_fresh_source_readonly.py'] == sha
    years = {}
    for year, value in subsets['years'].items():
        count, size, digest = VERIFIED[year]
        years[year] = {'files': count, 'bytes': size, 'manifest_sha256': digest, 'roots': value['roots']}
    request = {'read_only': True, 'eligible_for_unlink': False, 'script_sha256': sha,
               'years': years, 'admission': admission}
    result = remote_json(SSH, 'detecdiv-server', LAUNCH,
                         {'request': request, 'script_sha256': sha, 'script': base64.b64encode(script).decode()},
                         container=False)
    result['started_at'] = datetime.now(timezone.utc).isoformat()
    with output.open('x', encoding='utf8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
