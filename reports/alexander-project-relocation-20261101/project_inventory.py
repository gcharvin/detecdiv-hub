"""Native, read-only inventory of the one explicitly selected project tree."""
import json
import os
from pathlib import Path
import stat

BASE=Path('/volume1/Sauvegarde/Alexander/analysis')
STEM='anof_26102023_yam76s_100ms'
ignored={'@eaDir','#recycle','#snapshot'}

def emit(path):
    s=path.lstat()
    if not stat.S_ISREG(s.st_mode):raise RuntimeError('Non-regular project payload requires review: '+str(path))
    print(json.dumps({'kind':'file','path':str(path.relative_to(BASE)),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns},sort_keys=True),flush=True)

emit(BASE/(STEM+'.mat'))
root=BASE/STEM
assert root.is_dir() and root.resolve()==root
def fail(exc):raise exc
for parent,dirs,files in os.walk(root,followlinks=False,onerror=fail):
    dirs[:]=sorted(d for d in dirs if d not in ignored)
    for d in dirs:
        if (Path(parent)/d).is_symlink():raise RuntimeError('Project directory symlink requires review')
    print(json.dumps({'kind':'directory','path':str(Path(parent).relative_to(BASE))},sort_keys=True),flush=True)
    for name in sorted(files):
        if name not in ignored:emit(Path(parent)/name)
