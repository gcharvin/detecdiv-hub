"""Corroborate terminal selected-root SHA1 receipts without replaying the actor."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from audit import SSH, remote_json

CHECK = r'''
import hashlib,json,pathlib,subprocess,sys
v=json.load(sys.stdin)
base=pathlib.Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
w=base/'partial-target-sha1-20261116'
assert (w/'controller.exit').read_text().strip()=='0'
assert subprocess.run(['tmux','has-session','-t','alex2024-partial-target-sha1-20261116'],capture_output=True).returncode!=0
for line in subprocess.run(['ps','-eo','comm=,args='],capture_output=True,text=True,check=True).stdout.splitlines():
 parts=line.strip().split(None,1)
 if len(parts)==2 and parts[0].startswith('python') and '/partial-target-sha1-20261116/partial_target_sha1_readonly.py' in parts[1]:
  raise AssertionError('Selected-root actor still active')
script=(w/'partial_target_sha1_readonly.py').read_bytes();raw=(w/'request.json').read_bytes()
assert hashlib.sha256(script).hexdigest()==v['script_sha256'] and hashlib.sha256(raw).hexdigest()==v['request_sha256']
request=json.loads(raw);result=json.loads((w/'result.json').read_bytes());state=json.loads((w/'state.json').read_bytes())
assert result==state and result['phase']=='success' and result['read_only'] and not result['eligible_for_unlink']
assert request['read_only'] and not request['eligible_for_unlink'] and request['script_sha256']==v['script_sha256']
assert request['fresh_source_proof_sha256']==v['source_proof_sha256']
assert request['roots']==v['roots'] and len(request['roots'])==len(result['roots'])==17
logs=sorted(p.name for p in w.glob('root-*-sha1.log'))
assert logs==['root-'+str(i).zfill(2)+'-sha1.log' for i in range(1,18)]
for i,(expected,row) in enumerate(zip(request['roots'],result['roots']),1):
 assert all(row[k]==expected[k] for k in ('year','name','files','bytes','directories'))
 assert row['sha1_compare_exit']==0 and row['log_bytes']==0
 assert (w/('root-'+str(i).zfill(2)+'-sha1.log')).stat().st_size==0
 for kind in ('source_metadata','target_metadata'):
  assert all(row[kind][k]==expected[k] for k in ('files','bytes','directories'))
print(json.dumps({'success':True,'controller_exit':0,'read_only':True,'eligible_for_unlink':False,
 'roots':17,'files':sum(x['files'] for x in result['roots']),'logical_bytes':sum(x['bytes'] for x in result['roots']),
 'terminal_log_bytes':0,'per_root_sha1_exit_codes_all_zero':True,
 'before_after_metadata_equality_established_by_pinned_actor_control_flow':True,
 'separate_after_metadata_receipts_present':False,'fresh_native_source_after_sha1_required':True,
 'fresh_dependency_and_retirement_gates_required':True,'request_sha256':v['request_sha256'],
 'script_sha256':v['script_sha256'],'source_proof_sha256':v['source_proof_sha256'],
 'finished_at':result['finished_at']}))
'''


def main():
    output = HERE / 'partial-target-sha1-completion-verification.json'
    assert not output.exists(), 'Inspect the existing proof instead of replaying'
    launch = json.loads((HERE / 'partial-target-sha1-launch-verification.json').read_bytes())
    script = (HERE / 'partial_target_sha1_readonly.py').read_bytes()
    assert hashlib.sha256(script).hexdigest() == launch['script_sha256']
    source = (HERE / 'partial-fresh-source-completion-verification.json').read_bytes()
    source_proof = json.loads(source)
    assert source_proof['success'] and not source_proof['eligible_for_unlink']
    subset_path = HERE / 'runtime/retirement-partial-subset-digests-20261116.json'
    subset_bytes = subset_path.read_bytes()
    assert hashlib.sha256(subset_bytes).hexdigest() == '55c98657fa6c97399228afbb9d4192bc030ef5e5e554612b59ed4ad3e5059163'
    subsets = json.loads(subset_bytes)
    roots = [{'year': year, 'name': name, **{k: info[k] for k in ('files', 'bytes', 'directories')}}
             for year, item in sorted(subsets['years'].items()) for name, info in sorted(item['roots'].items())]
    result = remote_json(SSH, 'detecdiv-server', CHECK,
                         {'script_sha256': launch['script_sha256'], 'request_sha256': launch['request_sha256'],
                          'source_proof_sha256': hashlib.sha256(source).hexdigest(), 'roots': roots}, container=False)
    assert result['success'] and not result['eligible_for_unlink']
    result['checked_at'] = datetime.now(timezone.utc).isoformat()
    with output.open('x', encoding='utf8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
