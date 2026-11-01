"""One-shot local status bridge for the detached copy; never changes data.

No scheduler/service is installed. If this workstation sleeps or reboots, the
NAS copy continues on compute and this optional bridge can be resumed later.
"""
import json
import msvcrt
from pathlib import Path
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import api,remote

BATCH='a8c8e0da-72b6-42c5-9644-2028709c9707'
OBSERVE=r'''
import json,re
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/2023_3')
s=json.loads((w/'state.json').read_text());p=w/'copy.log';transferred=0
if p.exists():
 with p.open('rb') as f:f.seek(max(0,p.stat().st_size-8192));tail=f.read()
 values=re.findall(rb'([0-9.]+)\s+[0-9]+%',tail)
 if values:transferred=int(values[-1].replace(b'.',b''))
print(json.dumps({'state':s,'copied_bytes_from_rsync':transferred}))
'''


def synchronize(value):
    code=r'''
import json,uuid
from datetime import datetime,timezone
from sqlalchemy import text
from api.db import SessionLocal
from api.models import StorageMigrationBatch
v=PAYLOAD;s=v['state'];phase=s['phase']
allowed={'copying','inventory_source','verifying_full_content','checking_source_stability','failed_safe','verified_in_staging_project_and_catalog_gates_pending'}
assert phase in allowed
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 b=db.get(StorageMigrationBatch,uuid.UUID(BATCH),with_for_update=True)
 assert b.batch_name=='alexander-2023-3-copy-resume-20261101' and b.source_path=='/data/Alexander/data/2023_3'
 assert b.status in allowed,'Later migration action superseded this copy-only reporter'
 assert not s['source_deleted'] and not s['catalog_changed']
 b.status=phase
 m=dict(b.metadata_json or {});m['standalone_copy_progress']={'state_path':'/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/2023_3/state.json',
  'phase':phase,'heartbeat_at':s['heartbeat_at'],'copied_bytes_from_rsync':v['copied_bytes_from_rsync'],'full_sha1_passed':s.get('full_sha1_passed',False)}
 b.metadata_json=m
 summary=dict(b.summary_json or {});summary.update(source_deleted=False,catalog_cutover_performed=False,
  total_bytes=s.get('source_inventory',{}).get('bytes'),total_files=s.get('source_inventory',{}).get('files'),
  copied_bytes=v['copied_bytes_from_rsync'],full_sha1_passed=s.get('full_sha1_passed',False),project_path_gates_pending=True)
 b.summary_json=summary
 print(json.dumps({'updated':True,'phase':phase,'checked_at':datetime.now(timezone.utc).isoformat()}))
'''.replace('PAYLOAD',repr(value)).replace('BATCH',repr(BATCH))
    return api(code)


def main():
    runtime=HERE/'runtime';runtime.mkdir(exist_ok=True)
    with (runtime/'copy3-reporter.lock').open('a+b') as lock:
        lock.seek(0);lock.write(b'0');lock.flush();lock.seek(0)
        msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
        while True:
            try:
                value=remote('linux',OBSERVE,timeout=60);result=synchronize(value)
                record={'observation':value,'catalog_update':result}
                (runtime/'copy3-status.json').write_text(json.dumps(record,indent=2)+'\n')
                print(json.dumps(result),flush=True)
                if value['state']['phase'] in {'failed_safe','verified_in_staging_project_and_catalog_gates_pending'}:
                    (runtime/'copy3-reporter-complete.json').write_text(json.dumps(record,indent=2)+'\n');return
            except Exception as error:
                # Do not copy transport/configuration exception details into logs.
                print(json.dumps({'status_bridge_error_type':type(error).__name__,'copy_process_unchanged':True}),flush=True)
            time.sleep(300)


if __name__=='__main__':main()
