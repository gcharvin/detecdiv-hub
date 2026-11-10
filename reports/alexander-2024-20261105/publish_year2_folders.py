"""Prepared, not yet fixture-validated: future 17-project publication.

No admission, catalog cutover, source removal or metadata rewriting is performed
here. The two nonstandard sources and their actual staging suffixes are explicit.
"""
import ast
import base64
import ctypes
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import time

WORK = Path(__file__).resolve().parent
ROOT = WORK.parent
STATE = {'phase': 'preflight', 'published_projects': [], 'source_deleted': False, 'catalog_changed': False}
STARTED = False
SPECIAL = {'qp_250724_dbud4stest', '281124_Alex'}
INTEGRITY_SHA = '3d619d2a8c6bad75ec3118e333cbafd87550b0e8f5fa16326cf21c09c9a2ee29'
REQUEST_SHA = 'f4573231522cdf88b0624ea635ffae314ea744d0f9e1d44903a136b6d19b4c63'
RAW2_MANIFEST = '004fb3404b4af8fe09b6063a09e3a162488f689499ddb51087513daf3d9f4fad'

TARGET_GUARD = r'''
import hashlib,json,os,pwd,stat,sys,time
from pathlib import Path
v=json.load(sys.stdin);assert v['namespace'] in ['incoming','final'] and len(v['projects'])==17
parent=Path('/volume1/homes/maliavko/DetecdivHub/projects');assert parent.resolve()==parent
uid=pwd.getpwnam('maliavko').pw_uid;ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','.rsync-partial'}
def deadline():assert time.time()<v['own_lease_deadline_unix']
def fail(error):raise error
out=[]
for p in v['projects']:
 deadline();stem=p['name'];suffix='20261106' if stem in ['qp_250724_dbud4stest','281124_Alex'] else '20261105'
 assert p['stage_suffix']==suffix
 incoming=parent/('.incoming-'+stem+'-'+suffix);final=parent/stem
 root,other=(incoming,final) if v['namespace']=='incoming' else (final,incoming)
 assert root.resolve()==root and root.is_dir() and not os.path.lexists(other) and root.lstat().st_uid==uid
 expected={row['path']:row for row in p['target_rows']};assert len(expected)==len(p['target_rows'])
 seen=set();directories=set();total=main=rehashes=0
 for directory,dirs,files in os.walk(root,followlinks=False,onerror=fail):
  deadline();dirs[:]=sorted(name for name in dirs if name not in ignored)
  if Path(directory)!=root:directories.add(str(Path(directory).relative_to(root)))
  for name in dirs:assert not (Path(directory)/name).is_symlink()
  for name in files:
   if name in ignored:continue
   path=Path(directory)/name;relative=str(path.relative_to(root));assert relative in expected and path.parent.resolve()==path.parent
   row=expected[relative];before=path.lstat()
   assert stat.S_ISREG(before.st_mode) and before.st_uid==uid and before.st_nlink==1
   assert (before.st_ino,before.st_size,before.st_mtime_ns)==(row['inode'],row['bytes'],row['mtime_ns']),'Target changed since full hash'
   if relative in p['relinked_paths']:
    h=hashlib.sha256()
    with path.open('rb') as stream:
     for block in iter(lambda:stream.read(4*1024*1024),b''):deadline();h.update(block)
    assert h.hexdigest()==row['sha256'];after=path.lstat()
    assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns);rehashes+=1
   seen.add(relative);total+=before.st_size
   if relative==stem+'.mat':main=before.st_size
 assert seen==set(expected) and directories==set(p['directories']) and main>0
 assert rehashes==len(p['relinked_paths'])
 out.append({'project_id':p['project_id'],'name':stem,'files':len(seen),'total_bytes':total,'project_mat_bytes':main,'project_dir_bytes':total-main,'metadata_hashes_rechecked':rehashes,'native_owner':'maliavko'})
print(json.dumps({'success':True,'namespace':v['namespace'],'projects':out}))
'''


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save():
    STATE['heartbeat_at'] = datetime.now(timezone.utc).isoformat()
    path = WORK / 'state.tmp'
    path.write_text(json.dumps(STATE, indent=2) + '\n')
    path.replace(WORK / 'state.json')


def rename_directory_noreplace(parent_fd, source_name, destination_name):
    if not re.fullmatch(r'[A-Za-z0-9_]+', destination_name):
        raise ValueError('Unexpected destination scope')
    suffix = '20261106' if destination_name in SPECIAL else '20261105'
    if source_name != '.incoming-' + destination_name + '-' + suffix:
        raise ValueError('Wrong stage suffix or traversal')
    before = os.stat(source_name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISDIR(before.st_mode):
        raise ValueError('Source is not a no-follow directory')
    libc = ctypes.CDLL(None, use_errno=True)
    call = libc.renameat2
    call.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    call.restype = ctypes.c_int
    if call(parent_fd, source_name.encode(), parent_fd, destination_name.encode(), 1) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    after = os.stat(destination_name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISDIR(after.st_mode) or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise ValueError('Renamed object changed')
    return {'source_inode': before.st_ino, 'destination_inode': after.st_ino, 'no_overwrite': True, 'directory_no_follow': True, 'stage_suffix': suffix}


def native(code, value, path, role, deadline):
    assert role in ['primary', 'secondary'] and not path.exists() and time.time() < deadline
    host, key = ('Gilles@10.20.11.250', 'detecdiv_main_nas_ed25519') if role == 'primary' else ('charvin-admin@10.20.8.250', 'detecdiv_backup_ed25519')
    loader = 'import base64;exec(base64.b64decode(' + repr(base64.b64encode(code.encode()).decode()) + '))'
    args = ['ssh', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3', '-i', '/home/charvin-admin/.ssh/' + key, host, shlex.join(['nice', '-n', '19', 'python3', '-u', '-c', loader])]
    with path.open('xb') as output, Path(str(path) + '.errors.log').open('xb') as errors:
        result = subprocess.run(args, input=json.dumps(value).encode(), stdout=output, stderr=errors, timeout=min(7200, max(1, deadline - time.time())))
        output.flush()
        os.fsync(output.fileno())
    assert result.returncode == 0 and time.time() < deadline, 'Native guard failed; preserve receipts and inspect'


def source_guards(projects, standard, special, phase, deadline):
    for project in projects:
        stem = project['name']
        STATE.update(phase='fresh_original_' + phase, current_project=stem)
        save()
        code = special if stem in SPECIAL else standard
        request = {'name': stem, 'role': 'source'} if stem in SPECIAL else {'name': stem}
        role = 'primary' if stem == '281124_Alex' else 'secondary'
        path = WORK / ('source-' + phase + '-' + stem + '.jsonl')
        native(code, request, path, role, deadline)
        assert digest(path) == project['source_inventory_sha256'], 'Original inventory/content changed; no overwrite or waiver'


def literal(path, name):
    tree = ast.parse(path.read_text())
    return ast.literal_eval(next(node.value for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)))


def verified_scope(request):
    """Reconstruct scope from pinned successful evidence, not caller-supplied paths."""
    audit = ROOT / 'year2-incoming-integrity-20261110'
    assert (audit / 'controller.exit').read_text().strip() == '0'
    assert digest(audit / 'result.json') == INTEGRITY_SHA and digest(audit / 'request.json') == REQUEST_SHA
    original = json.loads((audit / 'request.json').read_text())
    proof = json.loads((audit / 'result.json').read_text())
    assert proof['success'] and len(original['projects']) == len(proof['projects']) == len(request['projects']) == 17
    for passed, frozen, supplied in zip(proof['projects'], original['projects'], request['projects']):
        assert passed['project_id'] == frozen['project_id'] == supplied['project_id'] and passed['name'] == frozen['name'] == supplied['name']
        for key in ['name', 'project_id', 'directories', 'source_inventory_sha256', 'stage_suffix']:
            assert frozen[key] == supplied[key]
        assert supplied['stage_suffix'] == ('20261106' if supplied['name'] in SPECIAL else '20261105')
        receipt = audit / frozen['name'] / 'target-full-hashes.jsonl'
        assert digest(receipt) == passed['target_receipt_sha256']
        rows = [json.loads(line) for line in receipt.open()]
        assert rows[-1]['kind'] == 'complete' and supplied['target_rows'] == rows[:-1]
        assert supplied['relinked_paths'] == [file['path'] for file in frozen['files'] if file['metadata_relinked']]
    assert sum(len(p['target_rows']) for p in request['projects']) == 103243
    assert len({p['project_id'] for p in request['projects']}) == 17


def main():
    global STARTED
    os.umask(0o077)
    with (WORK / 'publication.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert not (WORK / 'state.json').exists(), 'Never replay partial publication'
        STARTED = True
        save()
        q = json.loads((WORK / 'request.json').read_text())
        assert digest(Path(__file__)) == q['runner_sha256']
        fixtures = json.loads((ROOT / 'year2-publication-fixtures-20261110/fixture-result.json').read_text())
        assert fixtures['success'] is True and fixtures['publication_started'] is False and fixtures['production_paths_used'] is False
        assert len(fixtures['tests']) == 21 and fixtures['publisher_sha256'] == q['runner_sha256'], 'Exact publisher bytes must pass isolated fixtures first'
        assert q['source_deletion_allowed'] is False and q['catalog_cutover_allowed'] is False
        assert q['integrity_result_sha256'] == INTEGRITY_SHA
        verified_scope(q)
        admission = q['admission']
        assert admission['holder'] == 'alexander-2024-year2-final-publication-20261110'
        assert len(admission['leases']) == len({p['project_id'] for p in admission['leases']}) == 17
        assert {p['project_id'] for p in q['projects']} == {p['project_id'] for p in admission['leases']}
        deadline = q['own_lease_deadline_unix']
        assert deadline == min(datetime.fromisoformat(p['expires_at']).timestamp() for p in admission['leases'])
        assert time.time() < deadline and 0 <= (datetime.now(timezone.utc) - datetime.fromisoformat(admission['checked_at'])).total_seconds() < 120
        assert digest(ROOT / 'copy_raws.py') == 'a0576fffa3c1d43d232f54567c6c9f7153aed40b436ace98f26079af72432b99'
        raw = json.loads((ROOT / 'state.json').read_text())['years']['2024_2']
        assert raw['full_sha1_passed'] is True and raw['source_stability_verified'] is True
        assert digest(ROOT / '2024_2/source-before.jsonl') == digest(ROOT / '2024_2/source-after.jsonl') == RAW2_MANIFEST
        assert (ROOT / '2024_2/verify-full-sha1.log').stat().st_size == 0
        audit_source = ROOT / 'year2-incoming-integrity-20261110/audit_year2_incoming_integrity_20261110.py'
        special_source = ROOT / 'exception-copies-20261106/exception_copies.py'
        assert digest(audit_source) == '05639082a0b5400f5ad25d88b62303c60624de757fc918908b2098622ebc9045'
        assert digest(special_source) == '65bbbd8d6160107c02f9c0753a0838767ef2e2c4ad5c570d0c2b759668b95ee9'
        source_guards(q['projects'], literal(audit_source, 'SOURCE_STANDARD'), literal(special_source, 'NATIVE'), 'before', deadline)
        payload = {'projects': q['projects'], 'own_lease_deadline_unix': deadline, 'namespace': 'incoming'}
        native(TARGET_GUARD, payload, WORK / 'native-before.json', 'secondary', deadline)
        STATE['phase'] = 'publishing_verified_folders'
        save()
        parent = Path('/homes2/maliavko/DetecdivHub/projects')
        assert parent.resolve() == parent
        mount = json.loads(subprocess.check_output(['findmnt', '-J', '-T', str(parent), '-o', 'SOURCE,TARGET,OPTIONS'], text=True))['filesystems'][0]
        assert mount['source'] == '//10.20.8.250/home' and mount['target'] == '/homes2/maliavko'
        assert {'username=maliavko', 'rw'}.issubset(set(mount['options'].split(',')))
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            with (WORK / 'renames.jsonl').open('x') as output:
                for project in q['projects']:
                    assert time.time() < deadline
                    stem = project['name']
                    row = rename_directory_noreplace(fd, '.incoming-' + stem + '-' + project['stage_suffix'], stem)
                    row.update(project_id=project['project_id'], name=stem, utc=datetime.now(timezone.utc).isoformat())
                    output.write(json.dumps(row) + '\n')
                    output.flush()
                    os.fsync(output.fileno())
                    STATE['published_projects'].append(row)
                    save()
        finally:
            os.close(fd)
        payload['namespace'] = 'final'
        STATE['phase'] = 'verifying_native_final_folders'
        save()
        native(TARGET_GUARD, payload, WORK / 'native-after.json', 'secondary', deadline)
        source_guards(q['projects'], literal(audit_source, 'SOURCE_STANDARD'), literal(special_source, 'NATIVE'), 'after', deadline)
        report = json.loads((WORK / 'native-after.json').read_text())
        assert report['success'] and report['namespace'] == 'final' and len(report['projects']) == 17
        result = {'success': True, 'projects': report['projects'], 'filesystem_published': True, 'all_original_inventories_unchanged': True,
                  'source_deleted': False, 'catalog_changed': False, 'final_host_and_catalog_gates_pending': True, 'renames_sha256': digest(WORK / 'renames.jsonl')}
        (WORK / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        STATE.update(phase='filesystem_published_final_host_gates_pending', success=True, finished_at=datetime.now(timezone.utc).isoformat())
        save()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        if STARTED:
            STATE.update(phase='failed_safe_partial_publication_requires_review', error_type=type(error).__name__, error=str(error))
            save()
        raise
