"""Run synthetic tests on the exact isolated worker mirrors."""
import concurrent.futures

import operations as ops

TEST_PATHS = ['tests/test_job_force_stop.py', 'tests/test_pipeline_run_resource_defaults.py',
              'tests/test_custom_processor_gpu_resources.py', 'tests/test_nvidia_runtime.py',
              'tests/test_project_deletion.py', 'tests/test_matlab_code_checkout.py']


def check(label):
    value = ops.remote(label, f'''
import json,os,subprocess,sys
from pathlib import Path
stage=Path.cwd()/'.deploy-staging/hub-reconciled-20261020'
manifest=json.loads((stage/'manifest.json').read_text())
deps=stage/'test-deps'
if not (deps/'pytest').is_dir():
 p=subprocess.run([sys.executable,'-m','pip','install','--quiet','--disable-pip-version-check',
  '--target',str(deps),'pytest==9.1.1'],capture_output=True,text=True,timeout=120)
 assert p.returncode==0,p.stderr[-1200:]
files=[p for p in {TEST_PATHS!r} if (stage/'root'/p).exists()]
env={{**os.environ,'PYTHONPATH':str(deps)}}
p=subprocess.run([sys.executable,'-m','pytest','-q',*files,
 '--junitxml='+str(stage/'tests.xml')],cwd=stage/'root',env=env,capture_output=True,text=True,timeout=120)
(stage/'tests.log').write_text(p.stdout+p.stderr)
print(json.dumps({{'exit':p.returncode,'output':p.stdout[-8500:],'stderr':p.stderr[-1500:],
 'expected_fingerprint':manifest['expected_fingerprint'],'test_files':files}}))
''', timeout=270)
    ops.save(label + '-staging-tests.json', value)
    print(label, value['exit'], value['output'], flush=True)
    return value


if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(check, ['linux', 'windows']))
    raise SystemExit(any(v['exit'] for v in results))
