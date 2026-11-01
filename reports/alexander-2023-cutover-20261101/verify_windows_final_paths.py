"""Read-only final-path checks in the existing worker user session."""
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT=Path('C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub')
PRIVATE=Path('C:/ProgramData/DetecDivHub/homes2-connection-private')
os.chdir(ROOT);sys.path.insert(0,str(ROOT))
result={'success':False,'checks':[],'fov_checks':[],'scientific_files_changed':False}
try:
    identity=subprocess.check_output(['whoami'],text=True).strip()
    assert identity.casefold()=='gmgm\\charvin-admin'
    session=wintypes.DWORD()
    assert ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(),ctypes.byref(session))
    assert session.value==1
    result.update(windows_identity=identity,session_id=session.value)
    from api.config import get_settings
    from worker.path_mappings import parse_worker_path_mappings,map_worker_path
    mappings=parse_worker_path_mappings(get_settings().worker_path_mappings)
    request=json.loads((PRIVATE/'cutover-validation-paths.json').read_text())
    for r in request['raws']:
        final=Path(map_worker_path(r['new_path'],mappings))
        old=Path(map_worker_path(r['source_path'],mappings))
        assert str(final).startswith('\\\\srv-data-pb-lan1.u-strasbg.fr\\home\\DetecdivHub\\raw\\2023_2\\')
        assert final.is_dir() and old.is_dir(), 'Raw source or final copy is inaccessible: '+r['id']
        result['checks'].append({'raw_id':r['id'],'final_exists':True,'source_retained_and_readable':True})
    assert len(result['checks'])==21
    for r in request['project_paths']:
        assert Path(map_worker_path(r['source'],mappings)).is_dir()
        assert Path(map_worker_path(r['final'],mappings)).is_dir()
        result['fov_checks'].append({'source':r['source'],'final':r['final'],'both_readable':True})
    assert len(result['fov_checks'])==12
    sample=Path(map_worker_path('/homes2/maliavko/DetecdivHub/raw/2023_2/010723_sf1.48_ramm/27062023_sf1.48_ramm/gfluo.png',mappings))
    with sample.open('rb') as f:assert f.read(8)==b'\x89PNG\r\n\x1a\n'
    result.update(success=True,sample_file_header_verified=True,mapping_count=len(mappings))
except Exception as exc:
    result['error']=type(exc).__name__+': '+str(exc)
(PRIVATE/'cutover-windows-final-verification.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
sys.exit(0 if result['success'] else 1)
