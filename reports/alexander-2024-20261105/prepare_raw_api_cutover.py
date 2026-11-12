"""Reconcile both 2024 raw and media scopes with the archived content checks."""

import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "homes2-access-audit-20261101"))
from audit import SSH, remote_json

HOST = r'''
import hashlib, json, sys
from pathlib import Path

root = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
inputs = json.load(sys.stdin)
years = {}
for year, config in inputs.items():
    folder = root / config['folder'] / 'container-reports'
    receipt = (folder / 'receipt.jsonl').read_bytes()
    assert hashlib.sha256(receipt).hexdigest() == config['receipt_sha256']
    rows = [json.loads(line) for line in receipt.splitlines()]
    archived = {(row['kind'], row['id']): row for row in rows}
    assert len(archived) == len(rows) == config['handlers']
    assert sum(row['bytes'] for row in rows) == config['bytes']
    plan_bytes = (folder / 'plan.json').read_bytes()
    assert hashlib.sha256(plan_bytes).hexdigest() == config['plan_sha256']
    plan = json.loads(plan_bytes)
    refs = []
    for row in plan['references']:
        verified = archived[(row['kind'], row['id'])]
        assert row['relative_path'] == verified['relative_path']
        assert row['raw_dataset_id'] == verified.get('raw_dataset_id', row['raw_dataset_id'])
        uri = row.get('uri', row.get('original_uri'))
        assert uri == '/data/Alexander/data/' + year + '/' + row['relative_path']
        refs.append({**{key: row[key] for key in ('kind', 'id', 'raw_dataset_id', 'relative_path')},
                     'source': uri, 'bytes': verified['bytes'], 'sha256': verified['sha256']})
    assert {(row['kind'], row['id']) for row in refs} == set(archived)
    years[year] = {'raws': plan['raws'], 'refs': refs, 'route_sha256': config['route_sha256']}
print(json.dumps(years))
'''

API = r'''
import hashlib, json, stat, sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID
from sqlalchemy import func, or_, select, text
from api.db import SessionLocal
from api.models import Artifact, IndexingJob, Job, ProjectLock, ProjectRawLink, RawDataset, RawDatasetLocation, StorageRoot
from api.services.path_resolution import compose_storage_path
import api.routes_raw_datasets as routes

HOME = Path('/homes2/maliavko/DetecdivHub/raw')
OLD_NAMESPACE = HOME / '.incoming-alexander-2024-20261105'

def paths(year):
    return f'/data/Alexander/data/{year}', HOME / year, OLD_NAMESPACE / year

def in_tree(value, prefix):
    return value == prefix or value.startswith(prefix + '/')

def relative(value, prefix):
    assert in_tree(value, prefix) and value.startswith(prefix + '/')
    suffix = value[len(prefix) + 1:]
    assert all(part not in ('', '.', '..') for part in suffix.split('/'))
    assert '\\' not in suffix and '\x00' not in suffix
    return suffix

def verify_path(path, expected, size=None):
    # The API's SSHFS view can expose the native alias as a directory.
    assert expected.resolve(strict=True) == expected
    assert path.resolve(strict=True) in (expected, path)
    info = path.lstat()
    original = expected.lstat()
    assert stat.S_IFMT(info.st_mode) == stat.S_IFMT(original.st_mode)
    if size is None:
        assert stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)
    else:
        assert stat.S_ISREG(info.st_mode) and info.st_size == original.st_size == size

def media_rows(db, prefixes):
    settings = func.regexp_replace(RawDataset.display_settings_uri, r'^[[:space:]]+', '')
    artifact = func.regexp_replace(Artifact.uri, r'^[[:space:]]+', '')
    statements = {
        'display_settings': select(RawDataset.id, RawDataset.display_settings_uri, RawDataset.id).where(
            or_(*(settings.startswith(prefix + '/', autoescape=True) for prefix in prefixes))),
        'artifact': select(Artifact.id, Artifact.uri, Job.raw_dataset_id).outerjoin(Job, Job.id == Artifact.job_id).where(
            or_(*(artifact.startswith(prefix + '/', autoescape=True) for prefix in prefixes))),
    }
    return {(kind, str(row_id), uri, str(raw_id) if raw_id is not None else None)
            for kind, stmt in statements.items() for row_id, uri, raw_id in db.execute(stmt)}

def locations(db):
    rows = defaultdict(list)
    statement = select(RawDatasetLocation, StorageRoot.path_prefix).join(StorageRoot).order_by(RawDatasetLocation.id)
    for location, prefix in db.execute(statement):
        rows[str(location.raw_dataset_id)].append({'id': location.id, 'preferred': location.is_preferred,
            'path': compose_storage_path(prefix, location.relative_path), 'root_id': location.storage_root_id,
            'relative_path': location.relative_path, 'access_mode': location.access_mode})
    return rows

def admission(db, raw_ids, project_ids):
    raw_ids = [UUID(raw) for raw in raw_ids]
    linked = set(db.scalars(select(ProjectRawLink.project_id).where(ProjectRawLink.raw_dataset_id.in_(raw_ids))))
    projects = linked | {UUID(project) for project in project_ids}
    active_status = ('queued', 'running', 'cancelling')
    params = func.cast(Job.params_json, type_=RawDataset.display_settings_uri.type)
    job_scope = or_(Job.raw_dataset_id.in_(raw_ids), Job.project_id.in_(projects),
                    params.contains('/data/Alexander/data/2024_', autoescape=True),
                    params.contains('.incoming-alexander-2024-20261105', autoescape=True))
    busy = db.scalars(select(Job.id).where(Job.status.in_(active_status), job_scope)).all()
    leases = db.scalars(select(ProjectLock.id).where(ProjectLock.project_id.in_(projects),
        ProjectLock.status == 'active', or_(ProjectLock.expires_at.is_(None), ProjectLock.expires_at > func.now()))).all()
    indexing = db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(active_status))).all()
    return {'affected_projects': len(projects), 'active_jobs': len(busy), 'active_leases': len(leases),
            'active_indexing': len(indexing)}

def inventory(db, years, project_ids, check_files=True):
    root = db.scalars(select(StorageRoot).where(StorageRoot.name == 'user-homes2')).one()
    assert root.path_prefix == '/homes2'
    live = locations(db)
    route_sha = hashlib.sha256(Path(routes.__file__).read_bytes()).hexdigest()
    output = {}
    for year, previous in years.items():
        assert previous['route_sha256'] == route_sha
        source, target, old = paths(year)
        previous_raws = {row['id']: row for row in previous['raws']}
        scoped = {raw_id for raw_id, rows in live.items() if any(in_tree(row['path'], source) for row in rows)}
        assert scoped == set(previous_raws), 'Raw dataset scope changed'
        owner_ids = {str(raw): str(owner) for raw, owner in db.execute(select(RawDataset.id, RawDataset.owner_user_id)
            .where(RawDataset.id.in_([UUID(raw_id) for raw_id in scoped])))}
        raw_changes = []
        for raw_id in sorted(scoped):
            rows = live[raw_id]
            baseline = previous_raws[raw_id]
            assert owner_ids[raw_id] == baseline['owner_user_id']
            assert [(str(r['id']), r['preferred'], r['path']) for r in rows] == sorted(
                [(str(r['id']), r['preferred'], r['path']) for r in baseline['locations']], key=lambda r: int(r[0]))
            preferred, = [row for row in rows if row['preferred']]
            suffix = relative(preferred['path'], source)
            final = target / suffix
            if check_files:
                verify_path(final, old / suffix)
                assert final.is_dir() == Path(preferred['path']).is_dir()
                assert final.is_file() == Path(preferred['path']).is_file()
            raw_changes.append({'raw_id': raw_id, 'before': rows, 'preferred_id': preferred['id'],
                                'target': str(final), 'target_relative': final.relative_to('/homes2').as_posix()})
        expected_refs = {(row['kind'], row['id'], row['source'], row['raw_dataset_id']) for row in previous['refs']}
        assert media_rows(db, (source, str(target), str(old))) == expected_refs, 'Raw media URI scope changed'
        media = []
        for ref in previous['refs']:
            assert ref['raw_dataset_id'] in scoped
            suffix = relative(ref['source'], source)
            assert suffix == ref['relative_path']
            final = target / suffix
            if check_files:
                verify_path(final, old / suffix, ref['bytes'])
            media.append({**ref, 'target': str(final)})
        output[year] = {'raw_changes': raw_changes, 'media': media}
    raw_ids = [row['raw_id'] for year in output.values() for row in year['raw_changes']]
    assert len(raw_ids) == len(set(raw_ids))
    return {'years': output, 'storage_root_id': root.id, 'route_sha256': route_sha,
            'admission': admission(db, raw_ids, project_ids)}
'''

READ = r'''
inputs = json.load(sys.stdin)
with SessionLocal() as db:
    db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    db.execute(text("SET LOCAL statement_timeout = '30s'"))
    plan = inventory(db, inputs['years'], inputs['project_ids'])
print(json.dumps(plan))
'''


def archive_inputs():
    inputs = {}
    for index, date in ((1, '20261109'), (2, '20261111')):
        proof = json.loads((HERE / f"year{index}-api-media-readonly-completion-verification.json").read_bytes())
        launch = json.loads((HERE / f"year{index}-api-media-readonly-launch-verification.json").read_bytes())
        assert proof['success'] and proof['controller_exit'] == 0 and not proof['uri_changed']
        inputs[f"2024_{index}"] = {
            'folder': f"year{index}-api-media-readonly-{date}", 'receipt_sha256': proof['receipt_sha256'],
            'handlers': proof['handlers_verified'], 'bytes': proof['bytes_verified'],
            'plan_sha256': launch.get('plan_sha256', proof.get('plan_sha256')),
            'route_sha256': proof.get('route_sha256', proof.get('route_source_sha256')),
        }
    return remote_json(SSH, 'webserver-labo', HOST, inputs, container=False)


def project_ids():
    import gzip
    from publish_readable_raw_paths import remote
    first = json.loads(gzip.decompress((HERE / 'runtime/year1-host-request.json.gz').read_bytes()))
    second = remote('linux', r'''
import hashlib,json
from pathlib import Path
path = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year2-final-host-inputs-20261110/request.json')
payload = path.read_bytes()
assert hashlib.sha256(payload).hexdigest() == '4824f2b1a22e26d048f4614077aee7843483d261d116e69956a74eb7e3e7b7fa'
data = json.loads(payload)
print(json.dumps({'projects': [row['project_id'] for row in data['projects']]}))
''', timeout=40)['projects']
    ids = [row['project_id'] for row in first['projects']] + second
    assert len(ids) == len(set(ids)) == 33
    return ids


def main():
    path = HERE / 'runtime/raw-api-cutover-preflight-20261112.json'
    assert not path.exists(), 'Inspect the existing preflight before rerunning'
    assert json.loads((HERE / 'readable-raw-paths-verification.json').read_bytes())['success']
    years = archive_inputs()
    ids = project_ids()
    result = remote_json(SSH, 'webserver-labo', API + READ,
                         {'years': years, 'project_ids': ids}, container=True)
    encoded = (json.dumps({'plan': result, 'project_ids': ids}, indent=2) + '\n').encode()
    with path.open('xb') as stream:
        stream.write(encoded)
    print(json.dumps({'success': True, 'raws': {year: len(data['raw_changes']) for year, data in result['years'].items()},
                      'media': {year: len(data['media']) for year, data in result['years'].items()},
                      'admission': result['admission'], 'plan_sha256': hashlib.sha256(encoded).hexdigest()}))


if __name__ == '__main__':
    main()
