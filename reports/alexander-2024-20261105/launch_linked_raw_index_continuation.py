"""Inspect only ZIP indexes deferred by the earlier fixed read budgets."""

import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from audit import SSH, remote_json

LAUNCH = r'''
import base64,hashlib,json,os,shlex,subprocess,sys
from pathlib import Path
v=json.load(sys.stdin)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
previous=root/'legacy-linked-raw-zip-indexes-20261112'
raw=(previous/'receipt.jsonl').read_bytes()
request_bytes=(previous/'request.json').read_bytes()
assert hashlib.sha256(raw).hexdigest()=='9a0374bc56c80af6774689e84c369fd6bc7c80e2844a3da2203910900ff7ac0d'
assert hashlib.sha256(request_bytes).hexdigest()=='c433fea5fdfe3bb7a9b75b79498f4ca526db7a0c10824cf763f85bf6e546f2fe'
assert (previous/'controller.exit').read_text().strip()=='0'
rows=[json.loads(line) for line in raw.splitlines()]
selected={r['archive'] for r in rows if r['status']=='IndexBudgetExceeded'}
assert len(rows)==66 and len(selected)==2
request=json.loads(request_bytes)
request['archives']=[r for r in request['archives'] if r['path'] in selected]
assert len(request['archives'])==2 and sum(len(r['projects']) for r in request['archives'])==2
script=base64.b64decode(v['script'],validate=True);assert hashlib.sha256(script).hexdigest()==v['sha256']
request['script_sha256']=v['sha256']
work=root/'legacy-linked-raw-zip-indexes-continuation-20261112';task='alex2024-legacy-linked-raw-zip-indexes-continuation-20261112'
assert not work.exists()
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
os.umask(0o077);work.mkdir(mode=0o700)
(work/'request.json').write_text(json.dumps(request)+'\n')
(work/'scan_legacy_bundle_indexes.py').write_bytes(script)
args=['nice','-n','19','ionice','-c','3',sys.executable,'-u',str(work/'scan_legacy_bundle_indexes.py')]
shell=shlex.join(args)+' > '+shlex.quote(str(work/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(work/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'started':True,'work':work.name,'archives':2,'projects':2,
 'script_sha256':v['sha256'],'read_only':True,'member_payload_reads':0}))
'''


def main():
    output = HERE / 'legacy-linked-raw-index-continuation-launch-verification.json'
    assert not output.exists()
    script = (HERE / 'scan_linked_raw_archive_indexes_continuation.py').read_bytes()
    result = remote_json(SSH, 'detecdiv-server', LAUNCH, {
        'script': base64.b64encode(script).decode(), 'sha256': hashlib.sha256(script).hexdigest(),
    }, container=False)
    result['started_at'] = datetime.now(timezone.utc).isoformat()
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
