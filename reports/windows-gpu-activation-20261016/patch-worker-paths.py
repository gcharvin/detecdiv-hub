from pathlib import Path
import hashlib
import json
import shutil

root=Path('C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub')
target=root/'worker/path_mappings.py'
backup=root/'.deploy-backups/windows-gpu-20261016/worker/path_mappings.py'
assert not backup.exists(),'Backup already exists'
value=target.read_text()
old='    lowered = key.lower()\n    return any(part in lowered for part in ("path", "file", "directory", "folder", "uri"))'
new='    lowered = key.lower()\n    if lowered in {"custompackageroot", "custompackagedir"}:\n        return True\n    return any(part in lowered for part in ("path", "file", "directory", "folder", "uri"))'
assert value.count(old)==1,'Live code changed; inspect before patch'
before=hashlib.sha256(target.read_bytes()).hexdigest()
value=value.replace(old,new,1)
compile(value,str(target),'exec')
backup.parent.mkdir(parents=True,exist_ok=True)
shutil.copy2(target,backup)
target.write_text(value,encoding='utf-8',newline='\n')
from api.services.runtime_version import get_runtime_version_info
print(json.dumps({'file':'worker/path_mappings.py','before':before,'after':hashlib.sha256(target.read_bytes()).hexdigest(),'fingerprint':get_runtime_version_info()['code_fingerprint']}))
