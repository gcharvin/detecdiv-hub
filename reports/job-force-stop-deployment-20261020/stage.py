import base64
import difflib
import hashlib
import json
from pathlib import Path
import subprocess
import ops

TESTS = ["tests/test_job_force_stop.py", "tests/test_pipeline_terminal_status.py",
         "tests/test_pipeline_runs.py", "tests/test_pipeline_worker_path_context.py",
         "tests/test_worker_recovery.py", "tests/test_project_locks.py"]
for label in ops.TARGETS:
    before = json.loads((ops.OUT / (label + "-before.json")).read_text())
    folder = ops.OUT / (label + "-intended")
    folder.mkdir(exist_ok=True)
    files = {}
    names = list(ops.FILES)
    if label == "api":
        names = [name for name in names if not name.startswith("worker/")]
    if label == "windows":
        names += [n for n in before["pending_roi_files"] if n not in names]
    changes = []
    for name in names:
        original = before["files"].get(name)
        current = before["pending_roi_files"].get(name) if label == "windows" else None
        current = current or original
        local = ops.ROOT / name
        if name not in ops.FILES:
            intended = base64.b64decode(current["data"])
        elif not current:
            intended = local.read_bytes()
        else:
            base = subprocess.run(["git", "show", "HEAD:" + name], cwd=ops.ROOT, capture_output=True)
            if base.returncode:
                assert base64.b64decode(current["data"]) == local.read_bytes(), name
                intended = local.read_bytes()
            else:
                sources = [base64.b64decode(current["data"]), base.stdout, local.read_bytes()]
                paths = []
                for i, data in enumerate(sources):
                    p = folder / f"merge-{i}.txt"
                    p.write_bytes(data.replace(b"\r\n", b"\n"))
                    paths.append(str(p))
                merged = subprocess.run(["git", "merge-file", "-p", *paths], capture_output=True)
                if merged.returncode:
                    (folder / (Path(name).name + ".conflict")).write_bytes(merged.stdout)
                    raise RuntimeError("Inspect deployment merge conflict: " + label + " " + name)
                intended = merged.stdout
        if name.endswith(".py"):
            compile(intended, name, "exec")
        target = folder / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(intended)
        files[name] = {"data": base64.b64encode(intended).decode(),
                       "before": original["sha256"] if original else None,
                       "after": hashlib.sha256(intended).hexdigest()}
        old = base64.b64decode(original["data"]).decode().replace("\r\n", "\n") if original else ""
        changes.extend(difflib.unified_diff(old.splitlines(True), intended.decode().replace("\r\n", "\n").splitlines(True),
                                           fromfile="live/" + name, tofile="intended/" + name))
    (ops.OUT / (label + "-deployment.diff")).write_text("".join(changes), encoding="utf-8")
    tests = {name: base64.b64encode((ops.ROOT / name).read_bytes()).decode() for name in TESTS}
    payload = base64.b64encode(json.dumps({"files": files, "tests": tests}).encode()).decode()
    result = ops.remote(label, f'''
import base64,hashlib,json,shutil,subprocess,os,sys,importlib.util
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/{ops.ROLLOUT}'
stage.mkdir(parents=True,exist_ok=True)
spec=json.loads(base64.b64decode({payload!r}))
manifest={{'files':{{}}}}
for name,item in spec['files'].items():
 p=root/name
 actual=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
 assert actual==item['before'],'Live source changed: '+name
 dest=stage/'files'/name;dest.parent.mkdir(parents=True,exist_ok=True)
 dest.write_bytes(base64.b64decode(item['data']))
 assert hashlib.sha256(dest.read_bytes()).hexdigest()==item['after']
 manifest['files'][name]={{'before':actual,'after':item['after']}}
runtime=stage/'runtime';runtime.mkdir(exist_ok=True)
for package in ['api','worker']:
 shutil.copytree(root/package,runtime/package,dirs_exist_ok=True,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
for name in spec['files']:
 dest=runtime/name;dest.parent.mkdir(parents=True,exist_ok=True)
 shutil.copy2(stage/'files'/name,dest)
for name,data in spec['tests'].items():
 dest=runtime/name;dest.parent.mkdir(parents=True,exist_ok=True)
 dest.write_bytes(base64.b64decode(data))
if {label!r}!='api':
 env=dict(os.environ);packages=root/'.deploy-staging/roi-auto-memory-20261019/test-packages'
 if packages.exists():env['PYTHONPATH']=str(runtime)+os.pathsep+str(packages)
 result=subprocess.run([sys.executable,'-m','pytest',*spec['tests'],'-k','not cancel_request_records_authenticated_actor','-q'],cwd=runtime,env=env,capture_output=True,text=True)
 (stage/'pytest.log').write_text(result.stdout+result.stderr,encoding='utf-8')
 assert result.returncode==0,result.stdout[-4000:]+result.stderr[-1500:]
 manifest['pytest']=result.stdout.strip()
paths=[]
for part in ['api','worker']:
 paths.extend(sorted(p for p in (root/part).rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix not in ('.pyc','.pyo')))
paths += [root/'db/schema.sql',root/'pyproject.toml']
paths += [root/name for name in spec['files'] if name.startswith(('api/','worker/')) and not (root/name).exists()]
ordered=[]
for part in ['api','worker']:
 ordered.extend(sorted(p for p in paths if p.relative_to(root).parts[0]==part))
ordered.extend(p for p in paths if p.relative_to(root).parts[0] not in ['api','worker'])
digest=hashlib.sha1()
for p in ordered:
 name=p.relative_to(root).as_posix();data=(stage/'files'/name).read_bytes() if name in spec['files'] else p.read_bytes()
 digest.update(name.encode());digest.update(bytes([0]));digest.update(data);digest.update(bytes([0]))
manifest['expected_fingerprint']=digest.hexdigest()[:12]
(stage/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
print(json.dumps(manifest))
''', timeout=120)
    ops.save(label + "-staging.json", result)
    print(label, result["expected_fingerprint"], result.get("pytest", "API staged for build"), flush=True)
