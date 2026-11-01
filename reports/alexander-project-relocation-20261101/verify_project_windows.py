"""Read-only MATLAB validation under the worker's real limited user token."""
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess

WORK = Path('C:/ProgramData/DetecDivHub/homes2-connection-private')
RELEASE = 'C:/Users/Charvin-Admin/Documents/GitHub/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
receipt = {'started_at': datetime.now(timezone.utc).isoformat(), 'success': False, 'project_saved': False}
try:
    identity = subprocess.check_output(['whoami'], text=True).strip()
    session = wintypes.DWORD()
    assert ctypes.windll.kernel32.ProcessIdToSessionId(__import__('os').getpid(), ctypes.byref(session))
    assert identity.casefold() == 'gmgm\\charvin-admin' and session.value == 1
    receipt.update(windows_identity=identity, session_id=session.value)
    batch = f"addpath('{WORK.as_posix()}');verify_project_windows('{WORK.as_posix()}','{RELEASE}')"
    with (WORK/'project-verification-matlab.log').open('wb') as log:
        p = subprocess.run(['C:/Program Files/MATLAB/R2025b/bin/matlab.exe','-singleCompThread','-batch',batch],
            cwd=WORK, stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW, timeout=1500)
    receipt['matlab_exit_code'] = p.returncode
    assert p.returncode == 0, 'MATLAB validation failed; inspect the private diagnostic log'
    data = json.loads((WORK/'windows-project-verification.json').read_text(encoding='utf-8-sig'))
    assert data['success'] and not data['project_saved'] and data['fovs'] == 12 and data['rois'] == 5724
    receipt.update(success=True, verification=data)
except Exception as error:
    receipt['error'] = f'{type(error).__name__}: {error}'
finally:
    receipt['finished_at'] = datetime.now(timezone.utc).isoformat()
    (WORK/'project-session-verification.json').write_text(json.dumps(receipt, indent=2)+'\n',encoding='utf-8')
