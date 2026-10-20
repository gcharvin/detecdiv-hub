"""Prepare isolated runtime mirrors and guarded idle activators; do not activate."""
import base64
import json

import operations as ops

ROLLOUT = 'hub-reconciled-20261020'
TESTS = [
    'tests/test_job_force_stop.py', 'tests/test_pipeline_run_resource_defaults.py',
    'tests/test_custom_processor_gpu_resources.py', 'tests/test_nvidia_runtime.py',
    'tests/test_project_deletion.py', 'tests/test_matlab_code_checkout.py',
]
TESTS = [p for p in TESTS if (ops.ROOT / p).is_file()]

source = (ops.ROOT / 'reports/job-force-stop-deployment-20261020/activate_when_idle.py').read_text()
source = source.replace('job-force-stop-20261020', ROLLOUT)
source = source.replace('force_stop_rollout', 'hub_reconciliation_rollout')
source = source.replace('detecdiv-force-stop-activation-20261020',
                        'detecdiv-hub-reconciled-activation-20261020')
compile(source, 'activate_when_idle.py', 'exec')
(ops.OUT / 'activate_when_idle.py').write_text(source, encoding='utf-8')

for label in ['api', 'linux', 'windows']:
    folder = ops.OUT / (label + '-reconciliation')
    manifest = json.loads((folder / 'manifest.json').read_text())
    files = {p: {'before': v['before'], 'after': v['after']} for p, v in
             manifest['files'].items() if v['changed']}
    payload = {p: base64.b64encode((folder / 'files' / p).read_bytes()).decode() for p in files}
    tests = {p: base64.b64encode((ops.ROOT / p).read_bytes()).decode() for p in TESTS}
    result = ops.remote(label, f'''
import ast,base64,hashlib,json,shutil
from pathlib import Path
from api.services.runtime_version import _compute_code_fingerprint,_iter_fingerprint_paths
root=Path.cwd();stage=root/'.deploy-staging/{ROLLOUT}';mirror=stage/'root'
assert not (stage/'manifest.json').exists(),'Staging already exists; inspect before reuse'
expected={files!r};payload={payload!r}
for name,item in expected.items():
 p=root/name;actual=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
 assert actual==item['before'],'Concurrent change: '+name
for p in _iter_fingerprint_paths(root):
 q=mirror/p.relative_to(root);q.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,q)
for name,encoded in payload.items():
 data=base64.b64decode(encoded);assert hashlib.sha256(data).hexdigest()==expected[name]['after']
 if name.endswith('.py'):compile(data,name,'exec')
 for target in [stage/'files'/name,mirror/name]:
  target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
for name,encoded in {tests!r}.items():
 p=mirror/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(base64.b64decode(encoded))
manifest={{'commit':{ops.COMMIT!r},'role':{manifest['role']!r},'files':expected,
 'before_fingerprint':_compute_code_fingerprint(root),
 'expected_fingerprint':_compute_code_fingerprint(mirror)}}
(stage/'activate_when_idle.py').write_bytes(base64.b64decode({base64.b64encode(source.encode()).decode()!r}))
(stage/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\\n')
print(json.dumps(manifest))
''', timeout=90)
    ops.save(label + '-staging.json', result)
    print(label, result['before_fingerprint'], '->', result['expected_fingerprint'],
          'files', len(result['files']), flush=True)
