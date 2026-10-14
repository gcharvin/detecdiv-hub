"""Run the supplementary 4B AWQ pilot with a bounded admission lock and watchdog."""
from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path

from evaluate_qwen_runs import benchmark, remote, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.ssh = "C:/Windows/System32/OpenSSH/ssh.exe"
    args.api_host = "webserver-labo"
    args.compute_host = "detecdiv-server"
    args.base_url = "http://127.0.0.1:8002/v1"
    args.model = "Qwen/Qwen3-4B-AWQ"
    args.label = "qwen3-4b-awq-coexistence"
    result_path = args.output / ("results-" + args.label + ".json")
    if result_path.exists() and json.loads(result_path.read_text(encoding="utf-8"))["status"] == "complete_needs_review":
        print(json.dumps({"status": "already_complete", "results": str(result_path)}))
        return 0
    args.coexistence_owner = "qwen-coex-" + str(uuid.uuid4())
    owner = args.coexistence_owner
    state_path = args.output / "coexistence-controller.json"
    state = {"owner": owner, "status": "preparing", "started_at": time.time()}
    write_json(state_path, state)
    scripts = Path(__file__).parent
    locker_source = (scripts / "qwen_pilot_admission_lock.py").read_text(encoding="utf-8")
    watchdog_source = (scripts / "qwen_pilot_watchdog.py").read_text(encoding="utf-8")
    remote(args, args.api_host, f'''
import pathlib,json,subprocess,sys
root=pathlib.Path('/tmp/qwen-pilot-coexistence-20261014')
root.mkdir(exist_ok=True)
old=root/'state.json'
if old.exists() and json.loads(old.read_text()).get('status')!='released':
 raise RuntimeError('Admission owner already exists')
stop=root/'stop'
if stop.exists(): stop.unlink()
source=root/'admission.py'
source.write_text({locker_source!r})
log=(root/'admission.log').open('a')
p=subprocess.Popen([sys.executable,'-c','exec(open('+repr(str(source))+').read())',str(root),{owner!r}],stdout=log,stderr=log,start_new_session=True)
print(json.dumps({{'pid':p.pid}}))
''', api=True)
    compute_prepared = False
    try:
        time.sleep(2)
        lock = remote(args, args.api_host, "import pathlib; print(pathlib.Path('/tmp/qwen-pilot-coexistence-20261014/state.json').read_text())", api=True)
        if lock.get("owner") != owner or lock.get("status") != "locked" or lock.get("cancel_requested"):
            raise RuntimeError("Admission not available")
        setup = remote(args, args.compute_host, f"""
import pathlib,json,time,hashlib,subprocess,stat
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014')
root.mkdir(exist_ok=True)
status=subprocess.run(['systemctl','is-active','detecdiv-qwen.service'],capture_output=True,text=True).stdout.strip()
if status!='inactive': raise RuntimeError('Existing inference service is not inactive')
launcher=pathlib.Path('/home/charvin-admin/repos/detecdiv-hub-webvm/ops/scripts/run-qwen-server.sh')
original=launcher.read_bytes()
if b'QWEN_COEXISTENCE_PILOT' in original: raise RuntimeError('Existing pilot owns launcher')
backup=root/('launcher-'+{owner!r}+'.sh')
backup.write_bytes(original)
mode=stat.S_IMODE(launcher.stat().st_mode)
stop=root/'stop'
if stop.exists(): stop.unlink()
model='/home/charvin-admin/.cache/detecdiv-qwen/huggingface/hub/models--Qwen--Qwen3-4B-AWQ/snapshots/74d4bd2bd4bff9cafc9345221320bffb08b406a3'
if not list(pathlib.Path(model).glob('*.safetensors')): raise RuntimeError('Checkpoint not cached')
script='''#!/usr/bin/env bash
set -euo pipefail
# QWEN_COEXISTENCE_PILOT
export VLLM_USE_FLASHINFER_SAMPLER=0
export HF_HUB_OFFLINE=1
exec /home/charvin-admin/venvs/detecdiv-qwen/bin/vllm serve '''+model+''' --served-model-name Qwen/Qwen3-4B-AWQ --host 127.0.0.1 --port 8002 --max-model-len 8192 --max-num-seqs 1 --max-num-batched-tokens 512 --gpu-memory-utilization 0.23 --enforce-eager --enable-auto-tool-choice --tool-call-parser hermes
'''
manifest={{'owner':{owner!r},'launcher':str(launcher),'backup':str(backup),'original_mode':mode,
 'original_sha256':hashlib.sha256(original).hexdigest(),'pilot_sha256':hashlib.sha256(script.encode()).hexdigest(),
 'expires_at':time.time()+600,'max_service_mib':5888,'initial_service':status}}
(root/'manifest.json').write_text(json.dumps(manifest))
launcher.write_text(script)
launcher.chmod(mode)
source=root/'watchdog.py'
source.write_text({watchdog_source!r})
log=(root/'watchdog.log').open('a')
p=subprocess.Popen(['python3',str(source),str(root/'manifest.json')],stdout=log,stderr=log,start_new_session=True)
print(json.dumps({{'watchdog_pid':p.pid,'manifest':manifest}}))
""")
        compute_prepared = True
        state["setup"] = setup
        state["status"] = "starting_service"
        write_json(state_path, state)
        time.sleep(2)
        remote(args, args.compute_host, '''
import subprocess,json,pathlib
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014')
s=json.loads((root/'watchdog-state.json').read_text())
if s['status']!='watching': raise RuntimeError('Watchdog not armed')
r=subprocess.run(['sudo','-n','/usr/bin/systemctl','start','detecdiv-qwen.service'],capture_output=True,text=True,timeout=60)
if r.returncode: raise RuntimeError(r.stderr)
print(json.dumps({'started':True}))
''')
        ready = False
        for _ in range(24):
            status = remote(args, args.compute_host, '''
import pathlib,json,urllib.request
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014')
s=json.loads((root/'watchdog-state.json').read_text())
try:
 with urllib.request.urlopen('http://127.0.0.1:8002/v1/models',timeout=2) as r: models=json.load(r)
except OSError: models={}
print(json.dumps({'watchdog':s['status'],'models':models}))
''')
            if status["watchdog"] != "watching":
                raise RuntimeError("Watchdog stopped service during startup")
            if status["models"].get("data"):
                ready = True
                break
            time.sleep(5)
        if not ready:
            raise RuntimeError("Model did not become ready within startup limit")
        state["status"] = "benchmarking"
        write_json(state_path, state)
        state["benchmark_exit"] = benchmark(args)
    except (RuntimeError, OSError, ValueError, KeyError) as exc:
        state["error"] = str(exc)
    finally:
        if compute_prepared:
            remote(args, args.compute_host, "import pathlib,json; pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014/stop').touch(); print('{}')")
            for _ in range(20):
                cleanup = remote(args, args.compute_host, '''
import pathlib,json,subprocess,hashlib
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014')
s=json.loads((root/'watchdog-state.json').read_text())
m=json.loads((root/'manifest.json').read_text())
s['service_status']=subprocess.run(['systemctl','is-active','detecdiv-qwen.service'],capture_output=True,text=True).stdout.strip()
s['original_hash_restored']=hashlib.sha256(pathlib.Path(m['launcher']).read_bytes()).hexdigest()==m['original_sha256']
print(json.dumps(s))
''')
                if cleanup["status"] == "stopped" and cleanup["service_status"] == "inactive" and cleanup["original_hash_restored"]:
                    state["cleanup"] = {key: value for key, value in cleanup.items() if key != "samples"}
                    write_json(args.output / "coexistence-vram-measurements.json", cleanup)
                    break
                time.sleep(3)
            else:
                state["cleanup_error"] = "Managed service/launcher cleanup not verified"
        if not compute_prepared or "cleanup" in state:
            remote(args, args.api_host, "import pathlib; pathlib.Path('/tmp/qwen-pilot-coexistence-20261014/stop').touch(); print('{}')", api=True)
            time.sleep(2)
            state["admission"] = remote(args, args.api_host, "import pathlib; print(pathlib.Path('/tmp/qwen-pilot-coexistence-20261014/state.json').read_text())", api=True)
        state["status"] = "finished"
        state["finished_at"] = time.time()
        write_json(state_path, state)
    print(json.dumps(state, indent=2))
    return 0 if state.get("benchmark_exit") == 0 and "cleanup" in state else 2


if __name__ == "__main__":
    raise SystemExit(main())
