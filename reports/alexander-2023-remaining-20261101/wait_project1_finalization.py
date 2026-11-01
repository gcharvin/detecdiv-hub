"""One-shot continuation of running validation, with no raw-data deletion."""
from datetime import datetime, timezone
import json
import msvcrt
from pathlib import Path
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

OBSERVE = r'''
from pathlib import Path
import json
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
p=w/'project1-relink.exit'
r={'exit':p.read_text().strip() if p.exists() else None}
if (w/'project1-relink-progress.json').exists():r['progress']=json.loads((w/'project1-relink-progress.json').read_text())
print(json.dumps(r))
'''


def main():
    runtime = HERE/'runtime'
    runtime.mkdir(exist_ok=True)
    # Stop before the existing own lease expires, without acquiring others.
    deadline = datetime(2026, 11, 1, 22, 33, tzinfo=timezone.utc)
    with (runtime/'project1-finalizer.lock').open('a+b') as lock:
        lock.seek(0); lock.write(b'0'); lock.flush(); lock.seek(0)
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        while datetime.now(timezone.utc) < deadline:
            if (HERE/'project1-catalog-finalization.json').exists():
                return
            try:
                state = remote('linux', OBSERVE, timeout=60)
            except Exception as error:
                print(json.dumps({'observation_error_type': type(error).__name__}), flush=True)
                time.sleep(60)
                continue
            (runtime/'project1-finalizer-status.json').write_text(json.dumps(state, indent=2)+'\n')
            if state['exit'] is None:
                time.sleep(60)
                continue
            if state['exit'] != '0':
                print(json.dumps({'stopped': 'linux_validation_failed', 'source_deleted': False}), flush=True)
                return
            # Existing finalizer requires BOTH complete Linux and actual Windows
            # receipts, unchanged original hash/identity and its unexpired lease.
            # It only updates project metadata/audit and releases that own lease.
            result = subprocess.run([sys.executable, str(HERE/'finalize_project1.py')],
                                    cwd=HERE.parents[1], capture_output=True, text=True, timeout=230)
            if result.returncode == 0:
                print(json.dumps({'project_catalog_finalized': True, 'source_deleted': False}), flush=True)
            else:
                # Retain private evidence; never print transport/configuration errors.
                (runtime/'project1-finalization-error.txt').write_text(result.stderr)
                print(json.dumps({'stopped': 'finalization_failed_requires_review', 'source_deleted': False}), flush=True)
            return
        print(json.dumps({'stopped': 'own_lease_deadline', 'source_deleted': False}), flush=True)


if __name__ == '__main__':
    main()
