"""Corroborate an existing terminal fresh-source actor without replaying it."""

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
w=base/'partial-source-freshness-20261116'
assert (w/'controller.exit').read_text().strip()=='0'
assert subprocess.run(['tmux','has-session','-t','alex2024-partial-source-freshness-20261116'],capture_output=True).returncode!=0
for line in subprocess.run(['ps','-eo','comm=,args='],capture_output=True,text=True,check=True).stdout.splitlines():
 parts=line.strip().split(None,1)
 if len(parts)==2 and parts[0].startswith('python') and '/partial-source-freshness-20261116/partial_fresh_source_readonly.py' in parts[1]:
  raise AssertionError('Fresh-source actor still active')
script=(w/'partial_fresh_source_readonly.py').read_bytes();raw=(w/'request.json').read_bytes()
assert hashlib.sha256(script).hexdigest()==v['script_sha256'] and hashlib.sha256(raw).hexdigest()==v['request_sha256']
request=json.loads(raw);result=json.loads((w/'result.json').read_bytes());state=json.loads((w/'state.json').read_bytes())
assert result==state and result['phase']=='success' and result['read_only'] and not result['eligible_for_unlink']
assert request['read_only'] and not request['eligible_for_unlink'] and request['script_sha256']==v['script_sha256']
assert set(result['years'])==set(request['years'])=={'2024_1','2024_2'}
out={}
for year in ('2024_1','2024_2'):
 row=result['years'][year];expected=request['years'][year]
 assert row['phase']=='success' and row['roots']==expected['roots']
 assert (row['files'],row['bytes'],row['manifest_sha256'])==(expected['files'],expected['bytes'],expected['manifest_sha256'])
 h=hashlib.sha256()
 with (base/year/'source-before.jsonl').open('rb') as f:
  for block in iter(lambda:f.read(4*1024*1024),b''):h.update(block)
 assert h.hexdigest()==expected['manifest_sha256']
 out[year]={'files':row['files'],'bytes':row['bytes'],'manifest_sha256':row['manifest_sha256'],
            'roots':len(row['roots']),'finished_at':row['finished_at'],
            'ssh_error_log_bytes':(w/(year+'-ssh-errors.log')).stat().st_size}
assert sum(x['roots'] for x in out.values())==17
print(json.dumps({'success':True,'controller_exit':0,'read_only':True,'source_deleted':False,
                  'eligible_for_unlink':False,'request_sha256':v['request_sha256'],
                  'script_sha256':v['script_sha256'],'finished_at':result['finished_at'],'years':out}))
'''


def main():
    output = HERE / 'partial-fresh-source-completion-verification.json'
    assert not output.exists(), 'Inspect existing completion proof instead of replaying'
    launch = json.loads((HERE / 'partial-source-freshness-launch-verification.json').read_bytes())
    script = (HERE / 'partial_fresh_source_readonly.py').read_bytes()
    assert hashlib.sha256(script).hexdigest() == launch['script_sha256']
    result = remote_json(SSH, 'detecdiv-server', CHECK,
                         {'script_sha256': launch['script_sha256'], 'request_sha256': launch['request_sha256']},
                         container=False)
    assert result['success'] and not result['eligible_for_unlink']
    with output.open('x', encoding='utf8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
