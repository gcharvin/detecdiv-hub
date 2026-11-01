"""Native primary-NAS JSON path update, preserving live inodes/owners/ACLs.

Prepare first, validate the original project, then apply under its Hub lease.
No ROI arrays, historical pipeline records, or raw source files are written.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time

ROOT = Path('/volume1/DATA/Gilles/analysis/projects')
STEM = '190423_nh_sir2_tween'
BACKUP = ROOT/'.detecdiv-path-backups'/('20261101-homes2-'+STEM)
OLD = '/data/Alexander/data/2023_1'
NEW = '/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_1'
PROTECTED = {'notes', 'comments', 'description', 'history', 'tag', 'message', 'label'}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def mapped(value, source, destination, changes, path='$', protected=False):
    if isinstance(value, dict):
        return {k: mapped(v, source, destination, changes, path+'.'+k,
                          protected or k.casefold() in PROTECTED) for k, v in value.items()}
    if isinstance(value, list):
        return [mapped(v, source, destination, changes, path+'['+str(i)+']', protected)
                for i, v in enumerate(value)]
    if isinstance(value, str) and not protected and (value == source or value.startswith(source+'/')):
        assert value.startswith(source+'/190423_nh_sir2_tween/'), 'Unexpected raw acquisition'
        result = destination+value[len(source):]
        changes.append({'field': path, 'before': value, 'after': result})
        return result
    return value


def canonical(value, prefix):
    return mapped(value, prefix, '<VERIFIED_RAW_ROOT>', [])


def write_live(path, data, expected, before):
    # In-place write is intentional: a replacement would assign the operator's
    # NAS owner and could alter quotas/ACLs. The originals are already backed up.
    fd = os.open(str(path), os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'r+b') as stream:
        current = os.fstat(stream.fileno())
        assert current.st_ino == before['inode'] and current.st_uid == before['uid']
        assert current.st_gid == before['gid'] and stat.S_IMODE(current.st_mode) == before['mode']
        original = stream.read()
        assert sha(original) == expected, 'Concurrent metadata write'
        try:
            stream.seek(0); stream.write(data); stream.truncate(); stream.flush(); os.fsync(stream.fileno())
        except OSError:
            stream.seek(0); stream.write(original); stream.truncate(); stream.flush(); os.fsync(stream.fileno())
            raise
        assert sha(path.read_bytes()) == sha(data)
        after = path.stat()
        assert (after.st_ino, after.st_uid, after.st_gid, stat.S_IMODE(after.st_mode)) == (
            before['inode'], before['uid'], before['gid'], before['mode'])


def prepare():
    assert not (BACKUP/'manifest.json').exists(), 'Completed preparation already exists; inspect it'
    top = ROOT/(STEM+'.json')
    project = json.loads(top.read_text())
    assert project['schemaVersion'] == 3 and project['projectName'] == STEM
    assert project['projectId'] == '67556ff2-8555-478c-a957-1c952a4f49d7'
    paths = [top]
    for fov in project['fovs']:
        relative = Path(fov['metadataPath'])
        assert not relative.is_absolute() and '..' not in relative.parts
        paths.append(ROOT/STEM/relative)
    paths.append(ROOT/STEM/project['runProfilesPath'])
    assert len(paths) == 32 and len(set(paths)) == 32
    BACKUP.mkdir(mode=0o700, parents=True, exist_ok=True)
    files = []; roi_count = 0
    for p in paths:
        s = p.lstat(); assert stat.S_ISREG(s.st_mode)
        original = p.read_bytes(); value = json.loads(original); changes = []
        if p.name.startswith('fov_'): roi_count += len(value.get('rois', []))
        proposed = mapped(value, OLD, NEW, changes)
        assert canonical(value, OLD) == canonical(proposed, NEW), 'Non-path metadata changed'
        rewritten = json.dumps(proposed, ensure_ascii=False, indent=2, allow_nan=False).encode()+b'\n'
        relative = str(p.relative_to(ROOT))
        for kind, data in [('original', original), ('proposed', rewritten)]:
            target = BACKUP/kind/relative
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not target.exists():
                with target.open('xb') as f: f.write(data)
            assert target.read_bytes() == data
        files.append({'relative': relative, 'before_sha256': sha(original), 'after_sha256': sha(rewritten),
                      'changes': len(changes), 'examples': changes[:3], 'inode': s.st_ino,
                      'uid': s.st_uid, 'gid': s.st_gid, 'mode': stat.S_IMODE(s.st_mode), 'mtime_ns': s.st_mtime_ns})
    result = {'phase': 'prepared_originals_unchanged', 'backup': str(BACKUP), 'project_saved': False,
              'fovs': len(project['fovs']), 'rois': roi_count, 'files': files,
              'path_changes': sum(f['changes'] for f in files), 'raw_source_deleted': False}
    assert result['fovs'] == 30 and roi_count == 15281 and result['path_changes'] >= 300
    (BACKUP/'manifest.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result))


def apply():
    gates = json.loads(sys.argv[2])
    assert gates['project_id'] == '0e485efc-6b23-4faa-a316-4faea283477d'
    assert gates['lease_id'] == '68ce3140-5d35-4737-8c55-906184e9948c'
    assert gates['linux_baseline_passed'] and gates['source_copy_preserved'] and gates['own_lease_active']
    assert 0 <= time.time()-gates['checked_at_epoch'] < 120, 'Stale admission check'
    manifest = json.loads((BACKUP/'manifest.json').read_text())
    assert not (BACKUP/'applied.json').exists(), 'Already applied; inspect receipts'
    for f in manifest['files']:
        p = ROOT/f['relative']; s = p.stat()
        assert sha(p.read_bytes()) == f['before_sha256'] and s.st_mtime_ns == f['mtime_ns']
        assert sha((BACKUP/'original'/f['relative']).read_bytes()) == f['before_sha256']
        assert sha((BACKUP/'proposed'/f['relative']).read_bytes()) == f['after_sha256']
    applied = []
    try:
        for f in manifest['files']:
            if not f['changes']: continue
            data = (BACKUP/'proposed'/f['relative']).read_bytes()
            write_live(ROOT/f['relative'], data, f['before_sha256'], f)
            applied.append(f)
    except Exception:
        # Restore completed writes; the currently interrupted write is separately
        # detectable by its original/proposed hash and preserved backup.
        for f in reversed(applied):
            write_live(ROOT/f['relative'], (BACKUP/'original'/f['relative']).read_bytes(), f['after_sha256'], f)
        raise
    result = {'phase': 'paths_updated_validation_pending', 'files_modified': len(applied),
              'path_changes': manifest['path_changes'], 'backup': str(BACKUP),
              'project_retained_under_gilles': True, 'live_inodes_owners_modes_preserved': True,
              'roi_arrays_modified': False, 'historical_pipeline_records_modified': False,
              'raw_source_deleted': False}
    (BACKUP/'applied.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result))


if __name__ == '__main__':
    os.umask(0o077)
    {'prepare': prepare, 'apply': apply}[sys.argv[1]]()
