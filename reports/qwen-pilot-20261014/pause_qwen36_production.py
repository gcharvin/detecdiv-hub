"""Pause only the idle original managed service between authorized pilot waves."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root.parent.parent / 'scripts'))
from evaluate_qwen_runs import remote, gpu_guard, write_json, now

args = SimpleNamespace(ssh='C:/Windows/System32/OpenSSH/ssh.exe', api_host='webserver-labo',
                       compute_host='detecdiv-server', coexistence_owner=None)
guard = gpu_guard(args)
assert not guard['gpu_jobs'], 'Scientific reservation has priority'
result = remote(args, args.compute_host, '''
import pathlib,json,subprocess,hashlib,urllib.request,urllib.error
launcher=pathlib.Path('/home/charvin-admin/repos/detecdiv-hub-webvm/ops/scripts/run-qwen-server.sh')
assert hashlib.sha256(launcher.read_bytes()).hexdigest()=='0e681f73e64b5bc571b96b513d47789cef25e366b9059b08e0144e98cb33824c'
state=json.loads(pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014/watchdog-state.json').read_text());assert state['status']=='stopped'
status=subprocess.run(['systemctl','is-active','detecdiv-qwen.service'],capture_output=True,text=True).stdout.strip()
out={'initial_service':status}
if status=='inactive':print(json.dumps(out));raise SystemExit()
assert status=='active'
pid=subprocess.check_output(['systemctl','show','detecdiv-qwen.service','-p','MainPID','--value'],text=True).strip()
command=(pathlib.Path('/proc')/pid/'cmdline').read_bytes().decode(errors='replace')
assert 'Qwen3-14B-AWQ' in command and ('8001' in command or '8001' in subprocess.check_output(['systemctl','show','detecdiv-qwen.service','-p','ExecStart'],text=True))
processes=subprocess.check_output(['nvidia-smi','--query-compute-apps=pid,process_name,used_gpu_memory','--format=csv,noheader'],text=True)
for line in processes.splitlines():
 p=line.split(',')[0].strip();assert 'detecdiv-qwen.service' in (pathlib.Path('/proc')/p/'cgroup').read_text()
try:
 metrics=urllib.request.urlopen('http://127.0.0.1:8001/metrics',timeout=5).read().decode()
 lines=[l for l in metrics.splitlines() if not l.startswith('#') and ('num_requests_running{' in l or 'num_requests_waiting{' in l)]
 assert len(lines)==2 and all(float(l.rsplit(' ',1)[1])==0 for l in lines)
 out['requests']=lines
except urllib.error.URLError:
 out['requests']='API not listening during original model startup; no accepted inference request'
subprocess.run(['sudo','-n','systemctl','stop','detecdiv-qwen.service'],check=True)
out.update(original_pid=pid,service=subprocess.run(['systemctl','is-active','detecdiv-qwen.service'],capture_output=True,text=True).stdout.strip())
print(json.dumps(out))
''')
record = {'checked_at': now(), 'guard': guard, **result}
path = root / ('qwen36-production-pause-' + str(result.get('original_pid', 'already-inactive')) + '.json')
write_json(path, record)
print(json.dumps(record))
