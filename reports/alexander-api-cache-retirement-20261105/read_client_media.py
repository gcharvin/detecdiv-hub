"""Six representative media reads in the actual Linux/Windows client view."""
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess

WORK=Path(__file__).resolve().parent


def main():
    request=json.loads((WORK/'request.json').read_text());assert len(request['files'])==6
    assert len(request['cache_retirement_receipt_sha256'])==64
    result={'success':True,'host':'windows' if os.name=='nt' else 'linux','cache_retirement_receipt_sha256':request['cache_retirement_receipt_sha256'],'files':[]}
    root='/homes2/maliavko/DetecdivHub'
    if os.name=='nt':
        identity=subprocess.check_output(['whoami'],text=True).strip();session=wintypes.DWORD()
        assert ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(),ctypes.byref(session))
        assert identity.casefold()=='gmgm\\charvin-admin' and session.value==1
        result.update(windows_identity=identity,session_id=session.value)
    for row in request['files']:
        assert row['path'].startswith(root+'/raw/') and '..' not in Path(row['path']).parts
        path=row['path']
        if os.name=='nt':path='//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub'+path[len(root):]
        p=Path(path);before=p.stat();assert stat.S_ISREG(before.st_mode) and before.st_size==row['bytes']
        h=hashlib.sha1()
        with p.open('rb') as f:
            for block in iter(lambda:f.read(4*1024*1024),b''):h.update(block)
        assert h.hexdigest()==row['sha1'] and (p.stat().st_size,p.stat().st_mtime_ns)==(before.st_size,before.st_mtime_ns)
        result['files'].append({'year':row['year'],'kind':row['kind'],'bytes':row['bytes'],'sha1':h.hexdigest()})
    (WORK/'result.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__':
    try:main()
    except Exception as error:
        (WORK/'failure.json').write_text(json.dumps({'success':False,'error':type(error).__name__+': '+str(error)})+'\n',encoding='utf-8');raise
