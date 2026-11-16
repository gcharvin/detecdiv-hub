"""Launch a new, selected-root dry-run SHA1 comparison after fresh source proof."""

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
from launch_partial_fresh_source_readonly import ADMISSION

LAUNCH = r'''
import base64,hashlib,json,os,pathlib,shlex,subprocess,sys
v=json.load(sys.stdin);base=pathlib.Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work=base/'partial-target-sha1-20261116';session='alex2024-partial-target-sha1-20261116'
source=base/'partial-source-freshness-20261116'
assert not work.exists() and (source/'controller.exit').read_text().strip()=='0'
assert json.loads((source/'result.json').read_bytes())['phase']=='success'
sessions=subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
assert session not in sessions and 'alex2024-partial-source-freshness-20261116' not in sessions
assert hashlib.sha256((base/'copy_raws.py').read_bytes()).hexdigest()=='a0576fffa3c1d43d232f54567c6c9f7153aed40b436ace98f26079af72432b99'
script=base64.b64decode(v['script'],validate=True);assert hashlib.sha256(script).hexdigest()==v['script_sha256']
assert v['request']['read_only'] and not v['request']['eligible_for_unlink'] and v['request']['script_sha256']==v['script_sha256']
os.umask(0o077);work.mkdir(mode=0o700);(work/'partial_target_sha1_readonly.py').write_bytes(script)
payload=(json.dumps(v['request'],sort_keys=True)+'\n').encode();(work/'request.json').write_bytes(payload);request_sha=hashlib.sha256(payload).hexdigest()
args=['nice','-n','19','ionice','-c','3',sys.executable,'-u',str(work/'partial_target_sha1_readonly.py'),request_sha]
command=shlex.join(args)+' > '+shlex.quote(str(work/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(work/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',session,'bash','-c',command],check=True)
print(json.dumps({'launched':True,'work':work.name,'session':session,'request_sha256':request_sha,'script_sha256':v['script_sha256'],'read_only':True,'eligible_for_unlink':False}))
'''


def pinned(path, digest):
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == digest
    return json.loads(data)


def main():
    output = HERE / 'partial-target-sha1-launch-verification.json'
    assert not output.exists(), 'Inspect the existing launch instead of replaying'
    runtime = HERE / 'runtime'
    subsets = pinned(runtime / 'retirement-partial-subset-digests-20261116.json',
                     '55c98657fa6c97399228afbb9d4192bc030ef5e5e554612b59ed4ad3e5059163')
    evidence = pinned(runtime / 'retirement-partial-evidence-matrix-20261116.json',
                      '95698aa0df74341569718db789813a785d9df25aebedbd3de6338a7c2f2c5fd1')
    media = pinned(runtime / 'retirement-partial-media-proof-map-20261116.json',
                   'e674ff86e7803d9b643e872c7bced6255edcfbdb0d6e52f9de83b4e2847e32aa')
    fresh_path = HERE / 'partial-fresh-source-completion-verification.json'
    fresh_bytes = fresh_path.read_bytes()
    fresh = json.loads(fresh_bytes)
    assert fresh['success'] and fresh['controller_exit'] == 0 and not fresh['eligible_for_unlink']
    assert sum(row['roots'] for row in fresh['years'].values()) == 17
    finished = datetime.fromisoformat(fresh['finished_at'])
    assert 0 <= (datetime.now(timezone.utc) - finished).total_seconds() < 3600
    projects = sorted({item for row in evidence['rows'] for item in row['project_ids']})
    raws = sorted({item for row in media['roots'] for item in row['raw_ids']})
    assert len(projects) == 19 and len(raws) == 64 and len(evidence['rows']) == 17
    expected = {(row['year'], row['acquisition']) for row in evidence['rows']}
    actual = {(year, name) for year, item in subsets['years'].items() for name in item['roots']}
    assert actual == expected and len(actual) == 17
    admission = api(ADMISSION.replace('PROJECT_IDS', repr(projects)).replace('RAW_IDS', repr(raws)))
    assert admission['read_only'] and not any(admission[key] for key in ('active_jobs', 'active_locks', 'active_indexing'))
    script = (HERE / 'partial_target_sha1_readonly.py').read_bytes()
    sha = hashlib.sha256(script).hexdigest()
    fixture = json.loads((HERE / 'partial-target-sha1-fixture-verification.json').read_bytes())
    assert fixture['exit'] == 0 and fixture['scripts_sha256']['partial_target_sha1_readonly.py'] == sha
    actor_fixture = json.loads((HERE / 'partial-target-sha1-actor-fixture-verification.json').read_bytes())
    assert actor_fixture['exit'] == 0 and actor_fixture['scripts_sha256']['partial_target_sha1_readonly.py'] == sha
    roots = [{'year': year, 'name': name, **{k: info[k] for k in ('files', 'bytes', 'directories')}}
             for year, item in sorted(subsets['years'].items()) for name, info in sorted(item['roots'].items())]
    request = {'read_only': True, 'eligible_for_unlink': False, 'script_sha256': sha,
               'fresh_source_proof_sha256': hashlib.sha256(fresh_bytes).hexdigest(),
               'roots': roots, 'admission': admission}
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
