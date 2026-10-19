import base64
import hashlib
import json
import re
from pathlib import Path
import subprocess

import ops

for label in ops.TARGETS:
    before = json.loads((ops.OUT / (label + '-before.json')).read_text())
    folder = ops.OUT / (label + '-intended')
    folder.mkdir(exist_ok=True)
    files = {}
    for name in ops.FILES:
        intended = (ops.ROOT / name).read_bytes()
        old = before['files'][name]
        if label == 'windows' and name == 'worker/run_worker.py':
            base = subprocess.check_output(['git', 'show', 'HEAD:' + name], cwd=ops.ROOT)
            # Preserve this host's compatibility checks and claim diagnostics.
            inputs = [base64.b64decode(old['data']), base, intended]
            paths = []
            for i, data in enumerate(inputs):
                path = folder / f'merge-{i}.txt'
                path.write_bytes(data.replace(b'\r\n', b'\n'))
                paths.append(path)
            result = subprocess.run(['git', 'merge-file', '-p', *map(str, paths)], capture_output=True)
            if result.returncode == 1:
                (folder / 'merge-conflict.txt').write_bytes(result.stdout)
                value = result.stdout.decode()
                conflicts = re.findall(r'<<<<<<<[^\n]*\n(.*?)=======\n(.*?)>>>>>>>[^\n]*\n', value, re.S)
                assert len(conflicts) == 1, 'Unexpected Windows conflict'
                left, right = conflicts[0]
                assert left.startswith('                report_claim_blocker(candidate, reason or "target resource capacity")\n')
                assert right.startswith('                waiting_allocation = minimum_allocation(allocation)\n')
                value = re.sub(r'<<<<<<<[^\n]*\n.*?=======\n.*?>>>>>>>[^\n]*\n',
                               lambda match: left.splitlines(True)[0] + right, value, flags=re.S)
                intended = value.encode()
            else:
                assert result.returncode == 0
                intended = result.stdout
            assert b'can_claim_automatic_pipeline' in intended
            assert b'report_claim_blocker' in intended
        compile(intended, name, 'exec')
        (folder / Path(name).name).write_bytes(intended)
        files[name] = {'data': base64.b64encode(intended).decode(),
                       'before': old['sha256'] if old else None,
                       'after': hashlib.sha256(intended).hexdigest()}
    tests = {}
    for name in ['tests/test_roi_extraction_resources.py', 'tests/test_worker_memory_resources.py',
                 'tests/test_custom_processor_gpu_resources.py', 'tests/test_target_gpu_capacity.py',
                 'tests/test_worker_scaling.py', 'tests/test_worker_scale_api.py',
                 'tests/test_job_priority_settings.py', 'tests/test_worker_resource_reporting.py',
                 'tests/test_worker_recovery.py', 'tests/test_pipeline_runs.py',
                 'tests/test_pipeline_terminal_status.py', 'tests/test_pipeline_dependency_preflight.py',
                 'tests/test_pipeline_worker_path_context.py', 'tests/test_windows_worker_routing.py']:
        tests[name] = base64.b64encode((ops.ROOT / name).read_bytes()).decode()
    spec = base64.b64encode(json.dumps({'files': files, 'tests': tests}).encode()).decode()
    result = ops.remote(label, f'''
import base64,hashlib,json,shutil,sys,subprocess,os,importlib.util
from pathlib import Path
root=Path.cwd()
stage=root/'.deploy-staging/roi-auto-memory-20261019'
stage.mkdir(parents=True,exist_ok=True)
spec=json.loads(base64.b64decode({spec!r}))
manifest={{'files':{{}}}}
for name,item in spec['files'].items():
 p=root/name
 digest=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
 assert digest==item['before'], 'Live source changed: '+name
 dest=stage/'files'/name
 dest.parent.mkdir(parents=True,exist_ok=True)
 dest.write_bytes(base64.b64decode(item['data']))
 assert hashlib.sha256(dest.read_bytes()).hexdigest()==item['after']
 manifest['files'][name]={{'before':digest,'after':item['after']}}
runtime=stage/'runtime'
runtime.mkdir(exist_ok=True)
for package in ['api','worker']:
 shutil.copytree(root/package,runtime/package,dirs_exist_ok=True,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
for name in spec['files']:
 shutil.copy2(stage/'files'/name,runtime/name)
for name,data in spec['tests'].items():
 dest=runtime/name; dest.parent.mkdir(parents=True,exist_ok=True)
 dest.write_bytes(base64.b64decode(data))
test_env=dict(os.environ)
if importlib.util.find_spec('pytest') is None:
 packages=stage/'test-packages'
 if not (packages/'pytest').exists():
  install=subprocess.run([sys.executable,'-m','pip','install','--target',str(packages),'pytest==8.4.2'],capture_output=True,text=True)
  (stage/'test-dependency-install.log').write_text(install.stdout+install.stderr,encoding='utf-8')
  assert install.returncode==0,'Isolated test dependency install failed'
 test_env['PYTHONPATH']=str(runtime)+os.pathsep+str(packages)
selected=[name for name in spec['tests'] if Path(name).name not in ['test_worker_resource_reporting.py','test_pipeline_terminal_status.py','test_windows_worker_routing.py']]
result=subprocess.run([sys.executable,'-m','pytest',*selected,'-q'],cwd=runtime,env=test_env,capture_output=True,text=True)
(stage/'pytest.log').write_text(result.stdout+result.stderr,encoding='utf-8')
assert result.returncode==0,result.stdout[-3000:]+result.stderr[-1000:]
from api.services.runtime_version import _iter_fingerprint_paths
paths=_iter_fingerprint_paths(root)
paths += [root/name for name in spec['files'] if not (root/name).exists()]
ordered=[]
for part in ['api','worker']:
 ordered.extend(sorted(p for p in paths if p.relative_to(root).parts[0]==part))
ordered.extend(p for p in paths if p.relative_to(root).parts[0] not in ['api','worker'])
digest=hashlib.sha1()
for p in ordered:
 name=p.relative_to(root).as_posix()
 data=(stage/'files'/name).read_bytes() if name in spec['files'] else p.read_bytes()
 digest.update(name.encode());digest.update(bytes([0]));digest.update(data);digest.update(bytes([0]))
manifest['expected_fingerprint']=digest.hexdigest()[:12]
manifest['pytest']=result.stdout.strip()
(stage/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
print(json.dumps(manifest))
''', timeout=90)
    ops.save(label + '-staging.json', result)
    print(label, result['expected_fingerprint'], result['pytest'])
