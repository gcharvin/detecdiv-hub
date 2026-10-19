"""Run-scoped deployment transport; never includes credentials in reports."""
import base64
import difflib
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'scripts'))
import deploy_detecdiv_release as transport

LINUX_ROOT = '/home/charvin-admin/repos/detecdiv-hub-webvm'
WINDOWS_ROOT = 'C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub'
FILES = ['worker/job_resources.py', 'worker/memory_resources.py', 'worker/run_worker.py',
         'worker/autoscaling.py', 'worker/roi_extraction_resources.py']
TARGETS = {
 'linux': {'ssh_host': 'detecdiv-server'},
 'windows': {'ssh_host': '10.20.11.56', 'ssh_user': 'detecdiv-ops',
             'ssh_key': str(Path.home() / '.ssh/id_ed25519_detecdiv_windows')},
}


def remote(label, code, timeout=60):
    command = (f'cd {LINUX_ROOT} && .venv/bin/python -' if label == 'linux' else
               f'cd /d "{WINDOWS_ROOT}" && ".venv\\Scripts\\python.exe" -')
    result = subprocess.run([*transport.ssh_args(TARGETS[label]), command], input=code,
                            capture_output=True, encoding='utf-8', errors='replace', timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stdout[-2000:] + result.stderr[-2000:])
    return json.loads(result.stdout.splitlines()[-1])


def api(code):
    config = json.loads(transport.DEFAULT_CONFIG.read_text(encoding='utf-8'))
    return transport.api_code(config, code)


def save(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')
    return value


def inventory():
    for label in TARGETS:
        result = remote(label, f'''
import json,base64,hashlib,subprocess
from pathlib import Path
from api.services.runtime_version import get_runtime_version_info
files={{}}
for name in {FILES!r}:
 p=Path(name)
 files[name]={{'data':base64.b64encode(p.read_bytes()).decode(),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}} if p.exists() else None
print(json.dumps({{'files':files,'version':get_runtime_version_info(),'git':subprocess.run(['git','status','--short'],capture_output=True,text=True).stdout}}))
''')
        save(label + '-before.json', result)
        diffs = []
        for name, data in result['files'].items():
            if not data:
                continue
            before = base64.b64decode(data['data']).decode('utf-8').replace('\r\n', '\n')
            base = subprocess.check_output(['git', 'show', 'HEAD:' + name], cwd=ROOT).decode().replace('\r\n', '\n')
            diffs.extend(difflib.unified_diff(base.splitlines(True), before.splitlines(True),
                                             fromfile='local-HEAD/' + name, tofile=label + '/' + name))
        (OUT / (label + '-adaptations.diff')).write_text(''.join(diffs), encoding='utf-8')
        print(label, result['version'], 'diff_lines', len(diffs))


if __name__ == '__main__':
    inventory()
