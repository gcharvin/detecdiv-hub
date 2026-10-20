from pathlib import Path
import hashlib
import json
import shutil

root=Path('C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub')
stage=root/'.deploy-staging/windows-gpu-20261016'
backup=root/'.deploy-backups/windows-gpu-20261016'
manifest=json.loads((stage/'manifest.json').read_text())
for relative, hashes in manifest.items():
    target=root/relative
    source=stage/Path(relative).name
    assert hashlib.sha256(target.read_bytes()).hexdigest()==hashes['before'], 'Live source changed'
    assert hashlib.sha256(source.read_bytes()).hexdigest()==hashes['after'], 'Staged source changed'
    compile(source.read_text(),relative,'exec')
for relative, hashes in manifest.items():
    target=root/relative
    saved=backup/relative
    saved.parent.mkdir(parents=True,exist_ok=True)
    assert not saved.exists(),'Backup exists; inspect before retry'
    shutil.copy2(target,saved)
    shutil.copy2(stage/Path(relative).name,target)
from api.services.runtime_version import get_runtime_version_info
print(json.dumps({'fingerprint':get_runtime_version_info()['code_fingerprint'],'files':manifest}))
