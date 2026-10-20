import base64
import difflib
import json
from pathlib import Path
import subprocess
import ops

snapshot = ops.save("before.json", ops.snapshot())
print(json.dumps({k:snapshot[k] for k in ("jobs","workers","indexing")}, indent=2))
for label in ops.TARGETS:
    result = ops.remote(label, f'''
import base64,json,hashlib,subprocess
from pathlib import Path
files={{}}
names={ops.FILES!r}
pending=Path('.deploy-staging/roi-auto-memory-20261019/manifest.json')
manifest=json.loads(pending.read_text()) if pending.exists() else None
if manifest:
 names += [name for name in manifest['files'] if name not in names]
for name in names:
 p=Path(name)
 files[name]={{'data':base64.b64encode(p.read_bytes()).decode(),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}} if p.exists() else None
pending_files={{}}
if manifest:
 for name in manifest['files']:
  p=Path('.deploy-staging/roi-auto-memory-20261019/files')/name
  pending_files[name]={{'data':base64.b64encode(p.read_bytes()).decode(),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}}
print(json.dumps({{'files':files,'pending_roi_manifest':manifest,'pending_roi_files':pending_files,
 'git':subprocess.run(['git','status','--short'],capture_output=True,text=True).stdout}}))
''')
    ops.save(label + "-before.json", result)
    differences=[]
    for name, item in result["files"].items():
        if not item:
            continue
        base=subprocess.run(["git","show","HEAD:"+name],capture_output=True,cwd=ops.ROOT)
        if base.returncode:
            continue
        differences.extend(difflib.unified_diff(base.stdout.decode().replace("\r\n","\n").splitlines(True),
            base64.b64decode(item["data"]).decode().replace("\r\n","\n").splitlines(True),
            fromfile="HEAD/"+name,tofile=label+"/"+name))
    (ops.OUT/(label+"-adaptations.diff")).write_text("".join(differences),encoding="utf-8")
    print(label, "diff lines",len(differences), "pending ROI",bool(result["pending_roi_manifest"]))
