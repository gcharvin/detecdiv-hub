"""Resumable 2023_3 copy/full verification after the existing 2023_1 copy gate.

This standalone compute-host runner never deletes sources, publishes a year,
rewrites a project, or changes catalog locations. Later gates remain mandatory.
"""
from datetime import datetime,timezone
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path

WORK=Path(__file__).resolve().parent
ORIGINAL=Path('/home/charvin-admin/storage-transfers/alexander-2023-20261008')
SOURCE=Path('/data/Alexander/data/2023_3')
DEST=Path('/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_3')


def main():
    os.umask(0o077)
    spec=importlib.util.spec_from_file_location('copy_helpers',ORIGINAL/'runner.py')
    helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
    assert hashlib.sha256((ORIGINAL/'runner.py').read_bytes()).hexdigest()=='d55e58f62b120e1ebc9a64226db9b343bc07c2bb34d76362856d2873474d5126'
    with (ORIGINAL/'transfer.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        gate=json.loads((WORK.parent/'2023_1/copy-preservation-gates.json').read_text())
        assert gate['original_full_sha1_passed'] and gate['all_original_destination_files_preserved']
        assert gate['current_source_is_unchanged_subset'] and gate['destination_core_files']==750255
        request=json.loads((WORK/'request.json').read_text())
        assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==request['runner_sha256'],'Runner changed after launch plan'
        assert request['year']=='2023_3' and request['bandwidth_kib']==61440
        assert request['source_deletion_allowed'] is False and request['catalog_cutover_allowed'] is False
        state_path=WORK/'state.json'
        state=json.loads(state_path.read_text()) if state_path.exists() else {
            'started_at':datetime.now(timezone.utc).isoformat(),'year':'2023_3','phase':'preflight',
            'source_deleted':False,'catalog_changed':False,'project_relink_required_before_retirement':True}
        assert state.get('year')=='2023_3'
        def save():
            state['heartbeat_at']=datetime.now(timezone.utc).isoformat();state['runner_pid']=os.getpid()
            helper.atomic_json(state_path,state)
        def heartbeat(pid):state['child_pid']=pid;save()
        def progress(value):state['inventory_progress']=value;save()
        save()
        try:
            helper.require_mounts()
            assert SOURCE.is_dir() and not SOURCE.is_symlink()
            assert not Path('/homes2/maliavko/DetecdivHub/raw/2023_3').exists()
            before_path=WORK/'source-before.jsonl'
            if before_path.exists():
                assert 'source_inventory' in state,'Partial inventory requires manual review'
                assert hashlib.sha256(before_path.read_bytes()).hexdigest()==state['source_inventory']['sha256']
                before=state['source_inventory']
            else:
                state['phase']='inventory_source';save()
                before=helper.inventory('2023_3',before_path,progress)
                state['source_inventory']=before;save()
            free=os.statvfs(DEST.parent)
            assert free.f_bavail*free.f_frsize>=before['bytes']+2_000_000_000_000,'Insufficient capacity including 2-TB margin'
            if not DEST.exists():DEST.mkdir(mode=0o700)
            assert DEST.is_dir() and not DEST.is_symlink()
            state.update(phase='copying',staging_path=str(DEST),bandwidth_kib=61440);save()
            result=helper.run_process(helper.rsync_command(SOURCE,DEST,61440,verify=False),WORK/'copy.log',heartbeat)
            assert result==0,'Copy failed: rsync exit '+str(result)
            helper.require_mounts();state['phase']='verifying_full_content';save()
            # Every attempt has a separate log; never reuse a previous discrepancy receipt.
            verify=WORK/('verify-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'.log')
            state['verification_log']=str(verify);save()
            result=helper.run_process(helper.rsync_command(SOURCE,DEST,61440,verify=True),verify,heartbeat)
            assert result==0 and verify.stat().st_size==0,'Full content comparison failed; do not retire source'
            state['phase']='checking_source_stability';save()
            after=helper.inventory('2023_3',WORK/'source-after.jsonl',progress)
            assert before==after,'Source changed; preserve source and catalog for review'
            state.update(phase='verified_in_staging_project_and_catalog_gates_pending',
                full_sha1_passed=True,source_stability_verified=True,finished_at=datetime.now(timezone.utc).isoformat());save()
        except Exception as error:
            state.update(phase='failed_safe',error=type(error).__name__+': '+str(error));save();raise


if __name__=='__main__':main()
