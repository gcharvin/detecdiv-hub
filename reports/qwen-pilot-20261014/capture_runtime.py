"""Capture bounded local runtime evidence for one private principal arm."""
import datetime
import json
import sys
from pathlib import Path
from types import SimpleNamespace

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root.parent.parent / 'scripts'))
from evaluate_qwen_runs import remote, redact, write_json, now

label = sys.argv[1]
controller = json.loads((root / ('coexistence-controller-' + label + '.json')).read_text(encoding='utf-8'))
since = datetime.datetime.fromtimestamp(controller['started_at'], datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
args = SimpleNamespace(ssh='C:/Windows/System32/OpenSSH/ssh.exe')
code = """
import json,pathlib,subprocess
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-coexistence-20261014')
cmd=['journalctl','-u','detecdiv-qwen.service','--since',SINCE,'-n','900','--no-pager','-o','json']
if UNTIL:cmd.extend(['--until',UNTIL])
p=subprocess.run(cmd,capture_output=True,text=True)
events=[]
for line in p.stdout.splitlines():
 try:
  item=json.loads(line);events.append({'time':int(item['__REALTIME_TIMESTAMP'])/1000000,'message':str(item.get('MESSAGE',''))})
 except (ValueError,KeyError):pass
s=json.loads((root/'watchdog-state.json').read_text())
print(json.dumps({'journal':'\n'.join(e['message'] for e in events),'journal_records':events,'watchdog':{k:v for k,v in s.items() if k!='samples'}}))
""".replace("'\n'", "'\\n'").replace('SINCE', repr(since)).replace('UNTIL', repr(datetime.datetime.fromtimestamp(controller['finished_at'], datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') if controller.get('finished_at') else None))
record = remote(args, 'detecdiv-server', code)
if record['watchdog'].get('owner') != controller['owner']:
    record['watchdog'] = controller.get('cleanup', {'owner': controller['owner'], 'status': 'unavailable'})
record.update(checked_at=now(), label=label, since=since)
record['journal'] = redact(record['journal'])
for event in record.get('journal_records', []):
    event['message'] = redact(event['message'])
write_json(root / ('runtime-' + label + '.json'), record)
keywords = ('kernel', 'Marlin', 'memory', 'cache', 'ERROR', 'Error', 'failed', 'unsupported', 'Loading weights', 'Model loading', 'FP8', 'fp8')
lines = [line for line in record['journal'].splitlines() if any(k in line for k in keywords) and len(line) < 1200]
print(json.dumps({'watchdog': record['watchdog'], 'runtime_lines': lines}, ensure_ascii=True))
