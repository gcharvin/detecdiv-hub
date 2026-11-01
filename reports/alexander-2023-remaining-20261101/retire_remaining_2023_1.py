"""Retire only the verified residual 2023_1 manifest after both project gates.

Uses the previously exercised native unlink routine, with an independent
year-specific lock so the detached 2023_3 copy keeps its global copy lock.
"""
import ast
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

WORK = Path(__file__).resolve().parent
RAW = WORK/'2023_1'
LEGACY = Path('/home/charvin-admin/storage-transfers/alexander-project-relocation-20261101/retire_verified_raws.py')
APPROVAL = 'ok continue avec le transfert de 2023_1 puis 2023_3 sur homes2, et libère la place sur /data dès que les copies et les chemins des projets sont vérifiés'
SOURCE_SHA = 'ea08c7b7411bbf934005b8a8be0b1c0fbf3b1e59a69e11272a32b9c7dbe0c650'


def native_script(source):
    # The deployed wrapper has replacement characters in its old approval
    # text. Its extracted native literal was compared and is byte-identical.
    assert hashlib.sha256(source.encode()).hexdigest() in {
        '05baca5fba08ddb2a1752b109fba1010905f9ddf10b130ebbd2122c248175ad4',
        '50417c1d703b1d7811d53df99357f0537325cd9ee91922debefc223c6397e043'}
    tree = ast.parse(source)
    assignment = next(n for n in tree.body if isinstance(n, ast.Assign) and
                      any(isinstance(t, ast.Name) and t.id == 'NATIVE' for t in n.targets))
    script = ast.literal_eval(assignment.value.func.value)
    for old, new in [("Path('/volume1/DATA/Alexander/data/2023_2')", "Path('/volume1/DATA/Alexander/data/2023_1')"),
                     ('==425112', '==87364'), ('==3583194799325', '==737182660223')]:
        assert script.count(old) == 1
        script = script.replace(old, new)
    assert script.count('==APPROVAL') == 1
    script = script.replace('==APPROVAL', '=='+repr(APPROVAL))
    compile(script, 'native_residual_retirement', 'exec')
    return script


def main():
    os.umask(0o077)
    with (RAW/'retirement.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        gate = json.loads((WORK/'source1-retirement-execution-gates.json').read_text())
        assert gate['retirement_runner_sha256'] == hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        assert gate['approval'] == APPROVAL and gate['both_projects_linux_windows_validated']
        assert gate['api_dependencies_resolved'] and gate['original_projects_preserved']
        assert gate['gilles_project_retained_under_gilles'] and gate['retirement_project_leases_active']
        assert 0 <= (datetime.now(timezone.utc)-datetime.fromisoformat(gate['checked_at'])).total_seconds() < 300
        copy = json.loads((RAW/'copy-preservation-gates.json').read_text())
        assert copy['original_full_sha1_passed'] and copy['all_original_destination_files_preserved']
        assert copy['current_source_is_unchanged_subset']
        target = json.loads((RAW/'residual-destination-revalidation.json').read_text())
        assert target['success'] and target['files'] == 87364 and target['bytes'] == 737182660223
        assert target['all_owned_by_maliavko'] and target['source_manifest_sha256'] == SOURCE_SHA
        manifest = RAW/'source-current.jsonl'
        assert hashlib.sha256(manifest.read_bytes()).hexdigest() == SOURCE_SHA
        rows = [x for line in manifest.read_text().splitlines() if (x := json.loads(line))['kind'] == 'file']
        assert len(rows) == 87364 and sum(r['bytes'] for r in rows) == 737182660223
        assert not (RAW/'retirement-state.json').exists(), 'Inspect the prior one-shot retirement before any retry'
        request = {'approved_source_root': '/volume1/DATA/Alexander/data/2023_1', 'approval': APPROVAL,
                   'files': rows, 'project_original_preserved': True, 'linux_windows_project_validated': True,
                   'gates': gate}
        request_path = RAW/'retirement-request.json'
        assert not request_path.exists(); request_path.write_text(json.dumps(request))
        script = native_script(LEGACY.read_text())
        loader = 'import base64;exec(base64.b64decode('+repr(base64.b64encode(script.encode()).decode())+'))'
        args = ['ssh', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'ServerAliveInterval=15',
                '-o', 'ServerAliveCountMax=3', '-i', '/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519',
                'Gilles@10.20.11.250', shlex.join(['nice', '-n', '19', 'python3', '-u', '-c', loader])]
        state = {'phase': 'retiring_verified_residual_manifest', 'files_unlinked': 0, 'bytes_unlinked': 0,
                 'batch_id': gate['batch_id'], 'source_manifest_sha256': SOURCE_SHA}

        def save():
            state['heartbeat_at'] = datetime.now(timezone.utc).isoformat()
            temporary = RAW/'retirement-state.tmp'
            temporary.write_text(json.dumps(state, indent=2)+'\n'); temporary.replace(RAW/'retirement-state.json')

        save(); complete = None; last = time.monotonic()
        try:
            with request_path.open('rb') as data, (RAW/'unlink-receipt.jsonl').open('x') as receipt, (RAW/'unlink.errors.log').open('xb') as errors:
                child = subprocess.Popen(args, stdin=data, stdout=subprocess.PIPE, stderr=errors)
                for line in child.stdout:
                    row = json.loads(line); receipt.write(line.decode())
                    if row['event'] == 'unlinked':
                        state['files_unlinked'] += 1; state['bytes_unlinked'] += row['bytes']
                    elif row['event'] == 'complete': complete = row
                    else: state['native_preflight'] = row
                    if time.monotonic()-last > 5: receipt.flush(); save(); last = time.monotonic()
                assert child.wait() == 0, 'Native unlink stopped; inspect private receipt before any retry'
            assert complete and complete['files_unlinked'] == 87364 and complete['logical_bytes_unlinked'] == 737182660223
            state.update(phase='source_manifest_retired_catalog_finalization_pending', result=complete,
                         receipt_sha256=hashlib.sha256((RAW/'unlink-receipt.jsonl').read_bytes()).hexdigest())
            save()
        except Exception as error:
            state.update(phase='retirement_stopped_requires_review', error_type=type(error).__name__)
            save(); raise


if __name__ == '__main__':
    main()
