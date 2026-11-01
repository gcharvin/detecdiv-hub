"""Run in the worker's limited logon session; never read a credential blob."""
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path('C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub')
PRIVATE = Path('C:/ProgramData/DetecDivHub/homes2-connection-private')
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
RESULT = PRIVATE/'mapping-session-verification.json'
result = {'utc':datetime.now(timezone.utc).isoformat(), 'success':False}
try:
    from api.config import get_settings
    from worker.path_mappings import parse_worker_path_mappings, map_worker_path
    identity = subprocess.check_output(['whoami'], text=True).strip()
    assert identity.casefold() == 'gmgm\\charvin-admin', 'Unexpected user'
    session = wintypes.DWORD()
    assert ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session))
    assert session.value == 1
    result.update(windows_identity=identity, session_id=session.value)

    class CREDENTIAL(ctypes.Structure):
        _fields_ = [('Flags',wintypes.DWORD),('Type',wintypes.DWORD),
            ('TargetName',wintypes.LPWSTR),('Comment',wintypes.LPWSTR),
            ('LastWritten',wintypes.FILETIME),('CredentialBlobSize',wintypes.DWORD),
            ('CredentialBlob',ctypes.c_void_p),('Persist',wintypes.DWORD),
            ('AttributeCount',wintypes.DWORD),('Attributes',ctypes.c_void_p),
            ('TargetAlias',wintypes.LPWSTR),('UserName',wintypes.LPWSTR)]
    advapi = ctypes.WinDLL('advapi32', use_last_error=True)
    advapi.CredReadW.argtypes = [wintypes.LPCWSTR,wintypes.DWORD,wintypes.DWORD,ctypes.POINTER(ctypes.POINTER(CREDENTIAL))]
    advapi.CredReadW.restype = wintypes.BOOL
    advapi.CredFree.argtypes = [ctypes.c_void_p]
    cred = ctypes.POINTER(CREDENTIAL)()
    assert advapi.CredReadW('srv-data-pb-lan1.u-strasbg.fr',2,0,ctypes.byref(cred)), 'Stored worker credential unavailable'
    try:
        result['stored_credential'] = {'target':cred.contents.TargetName,'user':cred.contents.UserName,
                                       'type':cred.contents.Type,'persist':cred.contents.Persist}
        assert cred.contents.UserName == 'maliavko' and cred.contents.Persist == 2
    finally:
        advapi.CredFree(cred)

    mappings = parse_worker_path_mappings(get_settings().worker_path_mappings)
    home = '/homes2/maliavko/DetecdivHub'
    unc = '//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub'
    assert map_worker_path(home, mappings).replace(chr(92),'/') == unc
    assert map_worker_path('/homes2/another-user/DetecdivHub',mappings) == '/homes2/another-user/DetecdivHub'
    root = Path(map_worker_path(home, mappings))
    assert root.is_dir()
    checks = []
    request = json.loads((PRIVATE/'mapping-validation-paths.json').read_text())
    for raw in request['raws']:
        mapped = map_worker_path(raw['canonical_path'], mappings)
        exists = Path(mapped).is_dir()
        checks.append({'raw_id':raw['raw_id'],'exists':exists})
        assert exists, 'Raw directory unavailable: '+raw['raw_id']
    assert len(checks) == 21
    for year in ('2023_1','2023_2'):
        assert (root/'raw'/'.incoming-alexander-2023-20261008'/year).is_dir()
    sample = root/'raw'/'.incoming-alexander-2023-20261008'/'2023_2'/'010723_sf1.48_ramm'/'27062023_sf1.48_ramm'/'gfluo.png'
    with sample.open('rb') as stream:
        header = stream.read(8)
    assert header == b'\x89PNG\r\n\x1a\n', 'Copied sample PNG header mismatch'
    probe_name = '.detecdiv-windows-home-probe-'+uuid.uuid4().hex+'.tmp'
    probe = root/probe_name
    payload = b'DetecDiv post-restart Windows access probe'
    with probe.open('xb') as stream:
        stream.write(payload)
    assert probe.read_bytes() == payload
    result.update(success=True, mapping_count=len(mappings), mapping={'source':home,'target':unc},
                  raw_directory_checks=checks, sample_file_header_verified=True,
                  write_read_probe_passed=True, probe_name=probe_name, probe_bytes=len(payload),
                  nas_owner_verified=False)
except Exception as exc:
    result['error'] = type(exc).__name__+': '+str(exc)
RESULT.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
sys.exit(0 if result['success'] else 1)
