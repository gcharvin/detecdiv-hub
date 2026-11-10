"""Read only the two fixed fixture paths in the actual limited worker session."""
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path
import subprocess

WORK = Path(__file__).resolve().parent


def main():
    assert os.name == 'nt' and not (WORK / 'result.json').exists(), 'No fixture reader replay'
    control = json.loads((WORK / 'request.json').read_text(encoding='utf-8'))
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == control['runner_sha256']
    identity = subprocess.check_output(['whoami'], text=True).strip()
    session = wintypes.DWORD()
    assert ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session))
    assert identity.casefold() == 'gmgm\\charvin-admin' and session.value == 1 and ctypes.windll.shell32.IsUserAnAdmin() == 0
    root = Path('//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub/.raw-namespace-alias-fixture-20261110')
    expected = hashlib.sha256(b'DetecDiv namespace compatibility fixture 20261110\n').hexdigest()
    assert expected == control['expected_sha256']
    rows = []
    for relative in ['.incoming-fixture/2024_2/payload.bin', '2024_2/payload.bin']:
        path = root / relative
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
        assert hashlib.sha256(raw).hexdigest() == expected and len(raw) == before.st_size == 50
        assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
        rows.append({'fixture_relative_path': relative, 'bytes': len(raw), 'sha256': expected})
    result = {'success': True, 'windows_identity': identity, 'session_id': session.value, 'limited_token_verified': True,
              'fixture_paths_verified': rows, 'dataset_paths_changed': False, 'scientific_files_read_or_saved': False,
              'worker_modified': False, 'fixture_only': True}
    (WORK / 'result.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        (WORK / 'error.json').write_text(json.dumps({'success': False, 'error_type': type(error).__name__, 'error': str(error)}) + '\n', encoding='utf-8')
        raise
