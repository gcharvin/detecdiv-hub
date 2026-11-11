"""Inventory year2 API media against the pinned initial native manifest."""

import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "homes2-access-audit-20261101"))
from activate_windows_mapping import remote
from audit import SSH, remote_json

SNAPSHOT = r"""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload
from api.db import SessionLocal
from api.models import Artifact, Job, RawDataset, RawDatasetLocation
from api.services.path_resolution import compose_storage_path
import api.routes_raw_datasets as routes

prefix = '/data/Alexander/data/2024_2/'
references = []
raws = []
with SessionLocal() as db:
    db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    query = select(RawDataset).options(
        selectinload(RawDataset.locations).selectinload(RawDatasetLocation.storage_root)
    ).order_by(RawDataset.id)
    for raw in db.scalars(query):
        locations = [
            {'id': str(location.id), 'preferred': location.is_preferred,
             'path': compose_storage_path(location.storage_root.path_prefix, location.relative_path)}
            for location in raw.locations
        ]
        if any(loc['path'].rstrip('/') == prefix.rstrip('/') or
               loc['path'].startswith(prefix) for loc in locations):
            raws.append({'id': str(raw.id), 'owner_user_id': str(raw.owner_user_id),
                         'locations': sorted(locations, key=lambda loc: loc['id'])})
        uri = str(raw.display_settings_uri or '')
        if uri.strip().startswith(prefix):
            references.append({'kind': 'display_settings', 'id': str(raw.id),
                               'raw_dataset_id': str(raw.id), 'uri': uri})
    query = select(Artifact.id, Artifact.uri, Job.raw_dataset_id).outerjoin(
        Job, Job.id == Artifact.job_id
    ).where(Artifact.uri.startswith(prefix) |
            Artifact.uri.regexp_match(r'^[[:space:]]+/data/Alexander/data/2024_2/')).order_by(Artifact.id)
    for artifact_id, uri, raw_id in db.execute(query):
        references.append({'kind': 'artifact', 'id': str(artifact_id),
                           'raw_dataset_id': str(raw_id) if raw_id else None, 'uri': uri})

print(json.dumps({'checked_at': datetime.now(timezone.utc).isoformat(), 'raws': raws,
                  'references': references, 'route_sha256': hashlib.sha256(
                      Path(routes.__file__).read_bytes()).hexdigest()}))
"""

MEMBERSHIP = r"""
import hashlib
import json
from pathlib import Path

root = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work = root / 'year2-api-media-inventory-20261111'
work.mkdir(mode=0o700)
prefix = '/data/Alexander/data/2024_2/'
raw_ids = {row['id'] for row in snapshot['raws']}
paths = set()
for row in snapshot['references']:
    uri = row['uri']
    relative = uri[len(prefix):] if uri.startswith(prefix) else ''
    valid = uri == uri.strip() and all(
        component not in ('', '.', '..') for component in relative.split('/')
    ) and not any(character in relative for character in ('\\', '\x00'))
    row['relative_path'] = relative if valid else None
    if valid:
        paths.add(relative)

digest = hashlib.sha256()
found = {}
files = total = 0
with (root / '2024_2/source-before.jsonl').open('rb') as stream:
    for line in stream:
        digest.update(line)
        entry = json.loads(line)
        if entry['kind'] == 'file':
            files += 1
            total += entry['bytes']
            if entry['path'] in paths:
                assert entry['path'] not in found
                found[entry['path']] = entry['bytes']
assert digest.hexdigest() == '004fb3404b4af8fe09b6063a09e3a162488f689499ddb51087513daf3d9f4fad'
assert (files, total) == (974112, 5856516249162)

for row in snapshot['references']:
    row['initial_native_bytes'] = found.get(row['relative_path'])
    row['raw_location_in_scope'] = row['raw_dataset_id'] in raw_ids
snapshot['initial_manifest_sha256'] = digest.hexdigest()
payload = (json.dumps(snapshot, indent=2) + '\n').encode()
(work / 'plan.json').write_bytes(payload)
summary = {
    'read_only': True,
    'checked_at': snapshot['checked_at'],
    'raw_datasets_with_source_locations': len(raw_ids),
    'artifacts': sum(row['kind'] == 'artifact' for row in snapshot['references']),
    'display_settings': sum(row['kind'] == 'display_settings' for row in snapshot['references']),
    'references_without_safe_relative_path': sum(
        row['relative_path'] is None for row in snapshot['references']),
    'valid_references_missing_from_initial_manifest': sum(
        row['relative_path'] is not None and row['initial_native_bytes'] is None
        for row in snapshot['references']),
    'references_without_in_scope_raw_location': sum(
        not row['raw_location_in_scope'] for row in snapshot['references']),
    'unique_files_in_initial_manifest': len(found),
    'unique_bytes_in_initial_manifest': sum(found.values()),
    'initial_manifest_sha256': digest.hexdigest(),
    'route_sha256': snapshot['route_sha256'],
    'plan_sha256': hashlib.sha256(payload).hexdigest(),
    'handlers_streamed': False,
    'uri_changed': False,
    'source_deleted': False,
}
(work / 'result.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary))
"""


def main():
    output = HERE / "year2-api-media-inventory-preparation-verification.json"
    assert not output.exists(), "Inspect the existing inventory instead of replaying"
    snapshot = remote_json(SSH, "webserver-labo", SNAPSHOT, {}, container=True)
    private_snapshot = HERE / "runtime/year2-api-media-catalog-snapshot-20261111.json"
    with private_snapshot.open("x", encoding="utf-8") as stream:
        json.dump(snapshot, stream, indent=2)
    code = "import json\nsnapshot=json.loads(" + repr(json.dumps(snapshot)) + ")\n" + MEMBERSHIP
    result = remote("linux", code, timeout=90)
    result["script_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
