"""Detached read-only native manifest preflight. Never invokes unlink mode."""
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess

WORK = Path(__file__).resolve().parent
STATE = {'phase': 'preflight', 'source_deleted': False, 'catalog_changed': False}


def save():
    STATE['heartbeat_at'] = datetime.now(timezone.utc).isoformat()
    tmp = WORK / 'state.tmp'
    tmp.write_text(json.dumps(STATE, indent=2)+'\n')
    tmp.replace(WORK / 'state.json')


def main():
    os.umask(0o077)
    assert not (WORK / 'state.json').exists(), 'Inspect existing read-only preparation'
    with (WORK.parent / 'project3-native-retirement-preflight.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raw = json.loads((WORK.parent / '2023_3/state.json').read_text())
        assert raw['full_sha1_passed'] and raw['source_stability_verified']
        script = (WORK / 'retire_project3_manifest_native.py').read_bytes()
        request = json.loads((WORK / 'request.json').read_text())
        assert hashlib.sha256(script).hexdigest() == request['native_script_sha256']
        assert request['mode'] == 'preflight' and request['source_deletion_allowed'] is False
        manifest = (WORK.parent / '2023_3/source-before.jsonl').read_bytes()
        assert hashlib.sha256(manifest).hexdigest() == '4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3'
        # The timed-out Nov3 probe was read-only. Check for a surviving matching
        # native process without printing any other process's command line.
        probe = r'''
import os,json,base64,re
from pathlib import Path
found=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit() or int(p.name)==os.getpid():continue
 try:
  if p.stat().st_uid!=os.getuid():continue
  cmd=(p/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace')
  parts=[cmd]
  for token in re.findall(r"b64decode\(['\"]([A-Za-z0-9+/=]+)['\"]",cmd):
   try:parts.append(base64.b64decode(token).decode(errors='replace'))
   except Exception:pass
  if any('4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3' in x and 'def retire(' in x for x in parts):found.append(int(p.name))
 except (OSError,ValueError):pass
print(json.dumps({'matching_preflight_pids':found}))
'''
        prefix = ['ssh', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-i', '/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519', 'Gilles@10.20.11.250']
        loader = 'import base64;exec(base64.b64decode('+repr(base64.b64encode(probe.encode()).decode())+'))'
        p = subprocess.run(prefix+[shlex.join(['python3','-c',loader])], capture_output=True, text=True, timeout=45)
        assert p.returncode == 0, p.stderr
        prior = json.loads(p.stdout)
        assert not prior['matching_preflight_pids'], 'Existing native preflight requires review'
        STATE.update(phase='checking_native_source_manifest', prior_native_processes=prior)
        save()
        loader = 'import base64;exec(compile(base64.b64decode('+repr(base64.b64encode(script).decode())+'),"retire_project3_manifest_native.py","exec"))'
        args = prefix+[shlex.join(['nice','-n','19','python3','-u','-c',loader])]
        stdin = (json.dumps({'mode':'preflight','source_root':'/volume1/DATA/Alexander/data/2023_3'})+'\n').encode()+manifest
        with (WORK/'native-receipt.jsonl').open('xb') as out, (WORK/'native-errors.log').open('xb') as err:
            p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=out, stderr=err)
            STATE['ssh_pid'] = p.pid
            save()
            p.communicate(stdin, timeout=7200)
        STATE['native_exit_code'] = p.returncode
        assert p.returncode == 0, 'Native read-only scan failed; preserve source and inspect logs'
        events = [json.loads(x) for x in (WORK/'native-receipt.jsonl').read_text().splitlines()]
        assert len(events) == 1 and events[0]['event'] == 'preflight_passed' and events[0]['files'] == 708590
        STATE.update(phase='native_source_manifest_preflight_verified_readonly', result=events[0], finished_at=datetime.now(timezone.utc).isoformat())
        save()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        STATE.update(phase='failed_safe_readonly', error=type(error).__name__+': '+str(error))
        save()
        raise
