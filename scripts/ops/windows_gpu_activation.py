"""Run-scoped Windows GPU activation helpers (2026-10-16).

Remote code is transferred as encoded data, never interpolated as shell source.
"""
import base64
import json
from pathlib import Path
import subprocess
import sys
import uuid

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'reports/windows-gpu-activation-20261016'
OUT.mkdir(exist_ok=True)
SSH = 'C:/Windows/System32/OpenSSH/ssh.exe'
SCP = 'C:/Windows/System32/OpenSSH/scp.exe'
CONFIG = json.loads((ROOT/'ops/detecdiv_release_targets.json').read_text())
WINDOWS = CONFIG['windows']
WIN = ['-o', 'IdentitiesOnly=yes', '-i', str(Path(WINDOWS['ssh_key']).expanduser()), '-l', WINDOWS['ssh_user'], WINDOWS['ssh_host']]

def command(args, **kwargs):
    r = subprocess.run(args, capture_output=True, text=True, encoding='utf-8', errors='replace', **kwargs)
    if r.returncode:
        raise RuntimeError(f'Exit {r.returncode}: {r.stdout[-5000:]}\n{r.stderr[-3000:]}')
    return r.stdout.strip()

def api(code):
    return command([SSH, '-o', 'BatchMode=yes', CONFIG['api']['ssh_host'], 'docker exec -i '+CONFIG['api']['container']+' python -'], input=code, timeout=60)

def linux(code):
    return command([SSH, '-o', 'BatchMode=yes', CONFIG['linux']['ssh_host'], 'python3 -'], input=code, timeout=60)

def win(code, timeout=60):
    script = "$ErrorActionPreference='Stop'\n$ProgressPreference='SilentlyContinue'\n"+code
    encoded = base64.b64encode(script.encode('utf-16le')).decode()
    if len(encoded) > 6000:
        name = 'remote-' + uuid.uuid4().hex + '.ps1'
        local = OUT/name
        local.write_text(script, encoding='utf-8')
        remote = 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/' + name
        upload(local, remote)
        return command([SSH, '-o', 'BatchMode=yes', *WIN, f'powershell.exe -NoProfile -ExecutionPolicy Bypass -File {remote}'], timeout=timeout)
    return command([SSH, '-o', 'BatchMode=yes', *WIN, f'powershell.exe -NoProfile -EncodedCommand {encoded}'], timeout=timeout)

def upload(local, remote):
    # scp uses user@host syntax rather than ssh's -l option.
    destination = WINDOWS['ssh_user']+'@'+WINDOWS['ssh_host']+':'+remote
    return command([SCP, '-o', 'BatchMode=yes', *WIN[:-3], str(local), destination], timeout=60)

if __name__ == '__main__':
    mode, source = sys.argv[1:3]
    result = globals()[mode](Path(source).read_text(encoding='utf-8-sig'))
    print(result)
