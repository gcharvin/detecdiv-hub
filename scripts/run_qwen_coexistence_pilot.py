"""Run a bounded managed-service Qwen pilot, shared or from the frozen main plan."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import subprocess
import threading
import time
import uuid
from pathlib import Path

if __package__:
    from .evaluate_qwen_runs import benchmark, gpu_guard, remote, write_json
else:
    from evaluate_qwen_runs import benchmark, gpu_guard, remote, write_json


def configure_main_plan(args):
    """Main arms must keep their frozen inference conditions and run exclusively."""
    plan = json.loads(args.main_plan.read_text(encoding="utf-8"))
    candidates = [c for c in plan["candidates"] if c["model"] == args.model]
    if len(candidates) != 1:
        raise ValueError("Model must occur exactly once in the main plan")
    if (args.context_tokens != 8192 or args.kv_cache_mib or args.cpu_offload_gib
            or args.allocator_gc or args.disable_uva or args.max_new_tokens != 1100
            or args.evidence_profile != "standard"):
        raise ValueError("Changed context/cache/offload/generation needs a separate supplementary arm")
    candidate = candidates[0]
    revision = candidate["revision"]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Main plan must pin an exact checkpoint revision")
    args.revision = revision
    args.gpu_memory_utilization = candidate["provisional_gpu_memory_utilization"]
    if not 0 < args.gpu_memory_utilization <= 0.8:
        raise ValueError("Main plan memory cap must leave scientific headroom")
    args.max_service_mib = 22400  # Independent ceiling, with >=2 GiB physical margin.
    args.tool_call_parser = candidate.get("tool_call_parser", "hermes")
    if args.tool_call_parser not in {"hermes", "qwen3_coder"}:
        raise ValueError("Unsupported pilot tool parser")
    args.language_model_only = bool(candidate.get("language_model_only", False))
    if args.label == "qwen3-4b-awq-coexistence":
        args.label = args.model.split("/")[-1].lower().replace("-awq", "-awq-int4")


def validate_resume(previous, model, corpus_sha256, settings):
    if previous["model"] != model or previous["corpus_sha256"] != corpus_sha256:
        raise ValueError("Choose a new label for a different checkpoint or corpus")
    if previous.get("cases") and previous.get("inference_settings") not in (None, settings):
        raise ValueError("Resume must preserve the original inference settings")


def request_compute_stop(args, owner):
    return remote(args, args.compute_host, f'''
import pathlib,json
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014')
manifest=json.loads((root/'manifest.json').read_text())
if manifest['owner']=={owner!r}:
 (root/'stop').touch()
print(json.dumps({{'owned':manifest['owner']=={owner!r}}}))
''', timeout=15)


def watch_admission(args, owner, finished, state):
    """Preempt even during a slow in-flight inference, without releasing admission."""
    while not finished.is_set():
        try:
            admission = remote(args, args.api_host, '''
import pathlib
print(pathlib.Path('/tmp/qwen-pilot-coexistence-20261014/state.json').read_text())
''', api=True, timeout=15)
            if (admission.get("owner") != owner or admission.get("status") != "locked"
                    or admission.get("cancel_requested")
                    or time.time() - admission.get("checked_at", 0) > 10):
                state["preemption_reason"] = "Scientific request or lost admission ownership/freshness"
                request_compute_stop(args, owner)
                return
        except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as exc:
            state["preemption_reason"] = type(exc).__name__ + ": admission check failed"
            try:
                request_compute_stop(args, owner)
            except (RuntimeError, OSError, ValueError, subprocess.SubprocessError):
                pass  # Independent compute watchdog still enforces its lease.
            return
        finished.wait(2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", choices=["Qwen/Qwen3-4B-AWQ", "Qwen/Qwen3-8B-AWQ",
                                          "Qwen/Qwen3-14B-AWQ", "Qwen/Qwen3-8B-FP8",
                                          "Qwen/Qwen3-14B-FP8",
                                          "Alennnndy/Qwen3.6-27B-AWQ-4bit"], default="Qwen/Qwen3-4B-AWQ")
    parser.add_argument("--main-plan", type=Path, help="Frozen main plan; requires no scientific GPU reservation/process")
    parser.add_argument("--label", default="qwen3-4b-awq-coexistence")
    parser.add_argument("--context-tokens", type=int, default=8192)
    parser.add_argument("--kv-cache-mib", type=int, default=0)
    parser.add_argument("--cpu-offload-gib", type=float, default=0)
    parser.add_argument("--allocator-gc", type=float, default=0)
    parser.add_argument("--disable-uva", action="store_true")
    parser.add_argument("--max-new-tokens", type=int, default=1100)
    parser.add_argument("--evidence-profile", choices=["standard", "compact4096"], default="standard")
    parser.add_argument("--startup-timeout-seconds", type=int, default=120,
                        help="Bounded readiness window within the independent 600-second watchdog lease")
    args = parser.parse_args()
    if not 30 <= args.startup_timeout_seconds <= 450:
        parser.error("Startup timeout must be between 30 and 450 seconds")
    revisions = {"Qwen/Qwen3-4B-AWQ": "74d4bd2bd4bff9cafc9345221320bffb08b406a3",
                 "Qwen/Qwen3-8B-AWQ": "4da05a8edb55c6046cce958586c33b61da07bb79"}
    args.revision = revisions.get(args.model)
    args.gpu_memory_utilization = 0.23
    args.max_service_mib = 5888
    args.tool_call_parser = "hermes"
    args.language_model_only = False
    if args.main_plan:
        try:
            configure_main_plan(args)
        except ValueError as exc:
            parser.error(str(exc))
    elif not args.revision:
        parser.error("This model requires a pinned --main-plan and exclusive GPU admission")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.label):
        parser.error("Use a simple output label")
    args.ssh = "C:/Windows/System32/OpenSSH/ssh.exe"
    args.api_host = "webserver-labo"
    args.compute_host = "detecdiv-server"
    args.base_url = "http://127.0.0.1:8002/v1"
    args.coexistence_owner = None
    corpus_sha256 = hashlib.sha256((args.output / "corpus.json").read_bytes()).hexdigest()
    settings = {key: getattr(args, key) for key in (
        "revision", "context_tokens", "kv_cache_mib", "cpu_offload_gib", "allocator_gc",
        "disable_uva", "max_new_tokens", "evidence_profile", "gpu_memory_utilization", "max_service_mib")}
    if args.tool_call_parser != "hermes" or args.language_model_only:
        settings.update(tool_call_parser=args.tool_call_parser,
                        language_model_only=args.language_model_only)
    args.inference_settings = settings
    result_path = args.output / ("results-" + args.label + ".json")
    if result_path.exists():
        previous = json.loads(result_path.read_text(encoding="utf-8"))
        try:
            validate_resume(previous, args.model, corpus_sha256, settings)
        except ValueError as exc:
            parser.error(str(exc))
        if previous["status"] == "complete_needs_review":
            print(json.dumps({"status": "already_complete", "results": str(result_path)}))
            return 0
    if args.main_plan:
        preflight = gpu_guard(args)
        if not preflight["available"]:
            write_json(args.output / "main-admission-check.json", preflight)
            print(json.dumps({"status": "waiting_for_scientific_reservations", "guard": preflight}))
            return 2  # No lock, launcher mutation or service startup while science owns GPU.
    args.coexistence_owner = "qwen-coex-" + str(uuid.uuid4())
    owner = args.coexistence_owner
    suffix = "" if args.label == "qwen3-4b-awq-coexistence" else "-" + args.label
    state_path = args.output / ("coexistence-controller" + suffix + ".json")
    state = {"owner": owner, "status": "preparing", "started_at": time.time(),
             "model": args.model, "context_tokens": args.context_tokens,
             "kv_cache_mib": args.kv_cache_mib, "cpu_offload_gib": args.cpu_offload_gib,
             "allocator_gc": args.allocator_gc,
             "disable_uva": args.disable_uva,
             "max_new_tokens": args.max_new_tokens, "evidence_profile": args.evidence_profile,
             "corpus_sha256": corpus_sha256, "inference_settings": settings,
             "exclusive_gpu": bool(args.main_plan),
             "startup_timeout_seconds": args.startup_timeout_seconds}
    if args.main_plan:
        for prefix in ("coexistence-controller", "coexistence-vram-measurements"):
            old = args.output / (prefix + suffix + ".json")
            if old.exists():
                archive = old.with_name(old.stem + "-wave-" + owner + ".json")
                archive.write_bytes(old.read_bytes())
    write_json(state_path, state)
    scripts = Path(__file__).parent
    locker_source = (scripts / "qwen_pilot_admission_lock.py").read_text(encoding="utf-8")
    watchdog_source = (scripts / "qwen_pilot_watchdog.py").read_text(encoding="utf-8")
    checkpoint = "/home/charvin-admin/.cache/detecdiv-qwen/huggingface/hub/models--" + args.model.replace("/", "--") + "/snapshots/" + args.revision
    command = ["/home/charvin-admin/venvs/detecdiv-qwen/bin/vllm", "serve", checkpoint,
               "--served-model-name", args.model, "--host", "127.0.0.1", "--port", "8002",
               "--max-model-len", str(args.context_tokens), "--max-num-seqs", "1",
               "--max-num-batched-tokens", "256", "--gpu-memory-utilization", str(args.gpu_memory_utilization),
               "--enforce-eager", "--enable-auto-tool-choice", "--tool-call-parser", args.tool_call_parser]
    if args.language_model_only:
        command.append("--language-model-only")
    if args.kv_cache_mib:
        command.extend(["--kv-cache-memory-bytes", str(args.kv_cache_mib * 1024**2)])
    if args.cpu_offload_gib:
        command.extend(["--cpu-offload-gb", str(args.cpu_offload_gib)])
    launcher_text = ("#!/usr/bin/env bash\nset -euo pipefail\n# QWEN_COEXISTENCE_PILOT\n"
                     "export VLLM_USE_FLASHINFER_SAMPLER=0\nexport HF_HUB_OFFLINE=1\n"
                     + ("export VLLM_WEIGHT_OFFLOADING_DISABLE_UVA=1\n" if args.disable_uva else "")
                     + ("export PYTORCH_ALLOC_CONF=" + shlex.quote(
                         f"garbage_collection_threshold:{args.allocator_gc},max_split_size_mb:64") + "\n"
                        if args.allocator_gc else "") + "exec "
                     + shlex.join(command) + "\n")
    remote(args, args.api_host, f'''
import pathlib,json,subprocess,sys,os
if os.environ.get('DETECDIV_HUB_ASSISTANT_GPU_ARBITRATION_ENABLED','').lower() not in ('true','1','yes'):
 raise RuntimeError('API GPU arbitration must remain enabled')
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
    preempt_finished = threading.Event()
    preempt_thread = None
    try:
        time.sleep(2)
        lock = remote(args, args.api_host, "import pathlib; print(pathlib.Path('/tmp/qwen-pilot-coexistence-20261014/state.json').read_text())", api=True)
        if lock.get("owner") != owner or lock.get("status") != "locked" or lock.get("cancel_requested"):
            raise RuntimeError("Admission not available")
        args.coexistence_owner = None
        initial_guard = gpu_guard(args)
        args.coexistence_owner = owner
        if args.main_plan and not initial_guard["available"]:
            raise RuntimeError("Scientific GPU activity appeared before admission was locked")
        if (any(job["status"] == "queued" for job in initial_guard["gpu_jobs"])
                or sum(job.get("gpu_vram_mb") or 24576 for job in initial_guard["gpu_jobs"]) + args.max_service_mib + 2048 > 24576):
            raise RuntimeError("Scientific reservations leave insufficient pilot headroom")
        state["preflight_guard"] = initial_guard
        setup = remote(args, args.compute_host, f"""
import pathlib,json,time,hashlib,subprocess,stat
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014')
root.mkdir(exist_ok=True)
status=subprocess.run(['systemctl','is-active','detecdiv-qwen.service'],capture_output=True,text=True).stdout.strip()
if status!='inactive': raise RuntimeError('Existing inference service is not inactive')
for proc in pathlib.Path('/proc').iterdir():
 if not proc.name.isdigit(): continue
 try:
  if b'worker/run_worker.py' not in (proc/'cmdline').read_bytes(): continue
  env=(proc/'environ').read_bytes().split(b'\\0')
  values=[v.split(b'=',1)[1].lower() for v in env if v.startswith(b'DETECDIV_HUB_ASSISTANT_GPU_ARBITRATION_ENABLED=')]
  if values not in ([b'true'],[b'1'],[b'yes']): raise RuntimeError('Worker GPU arbitration must remain enabled')
 except (FileNotFoundError,ProcessLookupError): continue
launcher=pathlib.Path('/home/charvin-admin/repos/detecdiv-hub-webvm/ops/scripts/run-qwen-server.sh')
original=launcher.read_bytes()
if b'QWEN_COEXISTENCE_PILOT' in original: raise RuntimeError('Existing pilot owns launcher')
backup=root/('launcher-'+{owner!r}+'.sh')
backup.write_bytes(original)
mode=stat.S_IMODE(launcher.stat().st_mode)
stop=root/'stop'
if stop.exists(): stop.unlink()
model={checkpoint!r}
if not list(pathlib.Path(model).glob('*.safetensors')): raise RuntimeError('Checkpoint not cached')
index=pathlib.Path(model)/'model.safetensors.index.json'
if index.exists() and any(not (pathlib.Path(model)/name).is_file() for name in set(json.loads(index.read_text())['weight_map'].values())):
 raise RuntimeError('Checkpoint cache is incomplete')
script={launcher_text!r}
manifest={{'owner':{owner!r},'launcher':str(launcher),'backup':str(backup),'original_mode':mode,
 'original_sha256':hashlib.sha256(original).hexdigest(),'pilot_sha256':hashlib.sha256(script.encode()).hexdigest(),
 'expires_at':time.time()+600,'max_service_mib':{args.max_service_mib},'initial_service':status,
 'exclusive_gpu':{bool(args.main_plan)!r}}}
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
        preempt_thread = threading.Thread(target=watch_admission,
            args=(args, owner, preempt_finished, state), daemon=True)
        preempt_thread.start()
        ready = False
        for _ in range(args.startup_timeout_seconds // 5):
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
    except (RuntimeError, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        state["error"] = str(exc)
    finally:
        preempt_finished.set()
        if preempt_thread:
            preempt_thread.join(timeout=35)
        if compute_prepared:
            request_compute_stop(args, owner)
            for _ in range(20):
                cleanup = remote(args, args.compute_host, f'''
import pathlib,json,subprocess,hashlib
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014')
s=json.loads((root/'watchdog-state.json').read_text())
m=json.loads((root/'manifest.json').read_text())
s['service_status']=subprocess.run(['systemctl','is-active','detecdiv-qwen.service'],capture_output=True,text=True).stdout.strip()
s['original_hash_restored']=hashlib.sha256(pathlib.Path(m['launcher']).read_bytes()).hexdigest()==m['original_sha256']
s['owned']=s['owner']==m['owner']=={owner!r}
print(json.dumps(s))
''')
                if cleanup["owned"] and cleanup["status"] == "stopped" and cleanup["service_status"] == "inactive" and cleanup["original_hash_restored"]:
                    state["cleanup"] = {key: value for key, value in cleanup.items() if key != "samples"}
                    write_json(args.output / ("coexistence-vram-measurements" + suffix + ".json"), cleanup)
                    break
                time.sleep(3)
            else:
                state["cleanup_error"] = "Managed service/launcher cleanup not verified"
        if not compute_prepared or "cleanup" in state:
            remote(args, args.api_host, f'''
import pathlib,json
root=pathlib.Path('/tmp/qwen-pilot-coexistence-20261014')
s=json.loads((root/'state.json').read_text())
if s['owner']=={owner!r}: (root/'stop').touch()
print(json.dumps({{'owned':s['owner']=={owner!r}}}))
''', api=True)
            time.sleep(2)
            state["admission"] = remote(args, args.api_host, "import pathlib; print(pathlib.Path('/tmp/qwen-pilot-coexistence-20261014/state.json').read_text())", api=True)
        state["status"] = "finished"
        state["finished_at"] = time.time()
        write_json(state_path, state)
    print(json.dumps(state, indent=2))
    return 0 if state.get("benchmark_exit") == 0 and "cleanup" in state else 2


if __name__ == "__main__":
    raise SystemExit(main())
