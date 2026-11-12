"""Switch the 2024 raw preferences and exact verified media URIs together."""

import argparse
import hashlib
import json
from pathlib import Path

from prepare_raw_api_cutover import API, HERE, SSH, remote_json
from publish_readable_raw_paths import ROOT, UNC, remote, write
from raw_api_media_operations import PLAN, operate

RESULT = HERE / 'raw-api-cutover-result.json'
METADATA = HERE / 'runtime/raw-api-cutover-metadata-20261112.json'

CATALOG = r'''
from sqlalchemy.orm import load_only
from api.models import User

def unchanged_metadata_digest(metadata, path_field):
    other_fields = {key: value for key, value in metadata.items() if key != path_field}
    return hashlib.sha256(json.dumps(other_fields, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

def path_metadata(db, plan, switched=False, lock=False):
    groups = [('raw_dataset', RawDataset, 'dataset_dir_abs', {}), ('artifact', Artifact, 'absolute_path', {})]
    for scope in plan['years'].values():
        for row in scope['raw_changes']:
            preferred, = [r for r in row['before'] if r['preferred']]
            groups[0][3][UUID(row['raw_id'])] = preferred['path'], row['target']
        for row in scope['media']:
            if row['kind'] == 'artifact':
                groups[1][3][UUID(row['id'])] = row['source'], row['target']
    evidence, records = {}, []
    for kind, model, field, expected in groups:
        query = select(model).where(model.id.in_(expected)).order_by(model.id)
        if lock:
            query = query.with_for_update()
        seen = set()
        for obj in db.scalars(query):
            prior, target = expected[obj.id]
            assert obj.metadata_json[field] == (target if switched else prior), (kind, field)
            evidence[f'{kind}:{obj.id}'] = unchanged_metadata_digest(obj.metadata_json, field)
            records.append((obj, field, target))
            seen.add(obj.id)
        assert seen == set(expected)
    assert len(evidence) == 2085
    return evidence, records

def before(db, plan, project_ids):
    live = locations(db)
    all_raws = []
    for year, scope in plan['years'].items():
        source, target, old = paths(year)
        raw_ids = {row['raw_id'] for row in scope['raw_changes']}
        assert {raw_id for raw_id, rows in live.items() if any(in_tree(r['path'], source) for r in rows)} == raw_ids
        assert not any(in_tree(row['path'], str(target)) for rows in live.values() for row in rows)
        for change in scope['raw_changes']:
            assert live[change['raw_id']] == change['before']
            old_preferred, = [r for r in change['before'] if r['preferred']]
            assert old_preferred['id'] == change['preferred_id']
            suffix = relative(old_preferred['path'], source)
            assert change['target'] == str(target / suffix)
            assert change['target_relative'] == (target / suffix).relative_to('/homes2').as_posix()
            all_raws.append(change['raw_id'])
        refs = {(r['kind'], r['id'], r['source'], r['raw_dataset_id']) for r in scope['media']}
        assert media_rows(db, (source, str(target), str(old))) == refs
    assert len(all_raws) == len(set(all_raws)) == 115
    result = admission(db, all_raws, project_ids)
    assert result['active_jobs'] == result['active_indexing'] == result['active_leases'] == 0, result
    return result

def after(db, plan, check_routes):
    live = locations(db)
    counts = {'raw_datasets': 0, 'media_uris': 0, 'raw_api_routes': 0}
    if check_routes:
        operator = db.scalars(select(User).options(load_only(User.id, User.role)).where(
            User.user_key == 'gilles', User.role == 'admin', User.is_active.is_(True))).one()
    for year, scope in plan['years'].items():
        source, target, old = paths(year)
        for change in scope['raw_changes']:
            raw_id = change['raw_id']
            previous = {r['id']: r for r in change['before']}
            rows = live[raw_id]
            assert [row for row in rows if row['id'] in previous] == [
                dict(row, preferred=False) for row in change['before']]
            new, = [row for row in rows if row['id'] not in previous]
            assert new['preferred'] and new['path'] == change['target'] and new['root_id'] == plan['storage_root_id']
            assert new['access_mode'] == 'read' and new['relative_path'] == change['target_relative']
            counts['raw_datasets'] += 1
            if check_routes:
                detail = routes.get_raw_dataset(UUID(raw_id), db, operator)
                preferred, = [r for r in detail.locations if r.is_preferred]
                assert str(detail.id) == raw_id and preferred.absolute_path == change['target']
                counts['raw_api_routes'] += 1
        expected = {(r['kind'], r['id'], r['target'], r['raw_dataset_id']) for r in scope['media']}
        assert media_rows(db, (source, str(target), str(old))) == expected
        counts['media_uris'] += len(expected)
    assert counts['raw_datasets'] == 115 and counts['media_uris'] == 2081
    return counts

def apply(db, plan, project_ids, expected_metadata):
    # Keep admission and mutations in one short transaction. Readers can continue.
    db.execute(text('LOCK TABLE indexing_jobs, jobs, project_locks IN SHARE MODE NOWAIT'))
    root = db.get(StorageRoot, plan['storage_root_id'], with_for_update=True)
    assert (root.name, root.path_prefix) == ('user-homes2', '/homes2')
    gate = before(db, plan, project_ids)
    metadata, records = path_metadata(db, plan, lock=True)
    assert metadata == expected_metadata, 'Raw or artifact metadata changed after preflight'
    for obj, field, target in records:
        obj.metadata_json = dict(obj.metadata_json, **{field: target})
    for scope in plan['years'].values():
        for change in scope['raw_changes']:
            original = db.get(RawDatasetLocation, change['preferred_id'], with_for_update=True)
            assert original.is_preferred and str(original.raw_dataset_id) == change['raw_id']
            original.is_preferred = False
            db.add(RawDatasetLocation(raw_dataset_id=original.raw_dataset_id, storage_root_id=root.id,
                relative_path=change['target_relative'], is_preferred=True, access_mode='read'))
        for row in scope['media']:
            if row['kind'] == 'artifact':
                obj = db.get(Artifact, UUID(row['id']), with_for_update=True)
                assert obj.uri == row['source']
                obj.uri = row['target']
            else:
                assert row['kind'] == 'display_settings'
                obj = db.get(RawDataset, UUID(row['id']), with_for_update=True)
                assert obj.display_settings_uri == row['source']
                obj.display_settings_uri = row['target']
    db.flush()
    counts = after(db, plan, False)
    assert path_metadata(db, plan, switched=True)[0] == expected_metadata
    db.commit()
    return {**counts, 'path_metadata_fields': len(expected_metadata), 'nonpath_metadata_preserved': True,
            'admission': gate, 'committed_at': datetime.now(timezone.utc).isoformat()}

request = json.load(sys.stdin)
payload = request['plan_text'].encode()
assert hashlib.sha256(payload).hexdigest() == request['plan_sha256']
prepared = json.loads(payload)
plan, project_ids = prepared['plan'], prepared['project_ids']
assert hashlib.sha256(Path(routes.__file__).read_bytes()).hexdigest() == plan['route_sha256']
with SessionLocal() as db:
    mode = request['mode']
    isolation = 'SERIALIZABLE' if mode == 'apply' else 'REPEATABLE READ, READ ONLY'
    db.execute(text('SET TRANSACTION ISOLATION LEVEL ' + isolation))
    db.execute(text("SET LOCAL lock_timeout = '3s'"))
    db.execute(text("SET LOCAL statement_timeout = '30s'"))
    if mode == 'apply':
        result = apply(db, plan, project_ids, request['metadata'])
    elif mode == 'metadata':
        result = {'metadata': path_metadata(db, plan)[0]}
    elif mode == 'check':
        result = {'admission': before(db, plan, project_ids)}
    else:
        assert mode == 'postflight'
        result = after(db, plan, True)
        assert path_metadata(db, plan, switched=True)[0] == request['metadata']
        result.update(path_metadata_fields=len(request['metadata']), nonpath_metadata_preserved=True)
print(json.dumps({'success': True, 'mode': mode, 'plan_sha256': request['plan_sha256'],
                  'source_deleted': False, **result}))
'''

CHECK_PATHS = r'''
from pathlib import Path
def check(root, pairs):
    for alias, historical, directory in pairs:
        current, old = root / alias, root / historical
        if directory:
            assert current.is_dir() and old.is_dir()
        else:
            assert current.is_file() and old.is_file()
            assert current.stat().st_size == old.stat().st_size
'''


def verify_raw_location_paths(plan):
    pairs = []
    root = Path(ROOT)
    for year, scope in plan['years'].items():
        for row in scope['raw_changes']:
            alias = Path(row['target']).relative_to(root).as_posix()
            old = '.incoming-alexander-2024-20261105/' + alias
            pairs.append((alias, old, (Path(UNC) / old).is_dir()))
    exec(CHECK_PATHS + '\ncheck(Path(root), pairs)', {'root': UNC, 'pairs': pairs})
    code = 'import json\n' + CHECK_PATHS + f'\ncheck(Path({ROOT!r}), {pairs!r})\nprint(json.dumps({{"success": True}}))'
    assert remote('linux', code, timeout=100)['success']


def run(mode):
    payload = PLAN.read_bytes()
    plan_sha = hashlib.sha256(payload).hexdigest()
    request = {'mode': mode, 'plan_text': payload.decode(), 'plan_sha256': plan_sha}
    if mode in ('apply', 'postflight'):
        baseline = json.loads(METADATA.read_bytes())
        assert baseline['plan_sha256'] == plan_sha and len(baseline['metadata']) == 2085
        request['metadata'] = baseline['metadata']
    if mode == 'apply':
        assert not RESULT.exists(), 'Inspect the committed result instead of replaying'
        proof = operate('status', 'targets')
        assert proof['phase'] == 'complete' and proof['plan_sha256'] == plan_sha
        assert proof['verified'] == 2081 and proof['durable_receipts_verified']
        verify_raw_location_paths(json.loads(payload)['plan'])
    result = remote_json(SSH, 'webserver-labo', API + CATALOG, request, container=True)
    if mode == 'apply':
        write(RESULT, result)
    elif mode == 'metadata':
        write(METADATA, result)
        return {'success': True, 'metadata_fields': len(result['metadata']), 'plan_sha256': plan_sha}
    elif mode == 'postflight':
        path = HERE / 'raw-api-cutover-catalog-verification.json'
        if not path.exists():
            write(path, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('check', 'metadata', 'apply', 'postflight'))
    print(json.dumps(run(parser.parse_args().mode)))


if __name__ == '__main__':
    main()
