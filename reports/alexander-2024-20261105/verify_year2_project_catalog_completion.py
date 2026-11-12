"""Verify the committed year2 project locations and recover a lost response."""

import json

from launch_year2_project_publication import HERE, PRIVATE, SSH, read_json, remote_json

CODE = r"""
import hashlib, json, sys
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import inspect, select, text
from sqlalchemy.orm import load_only
from api.db import SessionLocal
from api.models import Project, ProjectLock, StorageMigrationBatch, User
from api.routes_projects import get_project

request = json.load(sys.stdin)
def columns(obj):
    return {column.key: getattr(obj, column.key) for column in inspect(type(obj)).column_attrs}

def canonical(value):
    return json.loads(json.dumps(value, default=str))

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()

def identity(project):
    return {'key': project.project_key, 'owner': str(project.owner_user_id),
            'visibility': project.visibility, 'notes': project.notes,
            'acl': [columns(row) for row in project.acl_entries],
            'groups': [columns(row) for row in project.group_memberships],
            'note_entries': [columns(row) for row in project.note_entries],
            'raw_links': [columns(row) for row in project.raw_links]}

def preserved_columns(value):
    return {key: item for key, item in canonical(value).items() if key not in ('is_preferred', 'updated_at')}

with SessionLocal() as db:
    db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    batch = db.get(StorageMigrationBatch, UUID(request['batch_id']))
    assert batch.batch_name == request['holder'] == 'alexander-2024-year2-final-publication-20261110'
    assert batch.status == 'projects_preferred_on_homes2_sources_retained'
    metadata = batch.metadata_json
    assert metadata['hosts'] == request['hosts'] and metadata['publication_result_sha256'] == request['publication_result_sha256']
    before = {row['project_id']: row for row in metadata['before_projects']}
    after = {row['project_id']: row for row in metadata['cutover_changes']}
    assert set(before) == set(after) == set(request['project_ids']) and len(before) == 17
    assert {row['id'] for row in request['leases']} == set(metadata['own_leases_released'])
    operator = db.scalars(select(User).options(load_only(User.id, User.role)).where(
        User.user_key == 'gilles', User.role == 'admin', User.is_active.is_(True))).one()
    for identifier, old in before.items():
        project = db.get(Project, UUID(identifier))
        assert project.project_name == old['name'] == after[identifier]['name']
        assert digest(identity(project)) == old['identity_sha256']
        current = dict(project.metadata_json)
        prefix = '/homes2/maliavko/DetecdivHub/projects/'
        stem = project.project_name
        mat = prefix + stem + '/' + stem + '.mat'
        assert current['project_mat_abs'] == mat
        assert current['project_dir_abs'] == prefix + stem + '/' + stem
        assert current['project_rel_from_root'] == 'maliavko/DetecdivHub/projects/' + stem + '/' + stem
        old_metadata = old['columns']['metadata_json'] or {}
        for key in ('project_mat_abs', 'project_dir_abs', 'project_rel_from_root'):
            current.pop(key)
            if key in old_metadata:
                current[key] = old_metadata[key]
        assert current == old_metadata
        original_ids = {row['id'] for row in old['locations']}
        actual = {row.id: row for row in project.locations}
        added = after[identifier]['new_location_id']
        assert set(actual) == original_ids | {added}
        assert all(preserved_columns(row) == preserved_columns(columns(actual[row['id']]))
                   for row in old['locations'])
        assert actual[added].is_preferred and not any(actual[key].is_preferred for key in original_ids)
        chosen = [row for row in get_project(project.id, db, operator).locations if row.is_preferred]
        assert len(chosen) == 1 and chosen[0].project_mat_path == mat
    for item in request['leases']:
        lease = db.get(ProjectLock, UUID(item['id']))
        assert lease.holder_key == request['holder'] and str(lease.project_id) == item['project_id']
        assert lease.status == 'released' and lease.released_at is not None
    assert batch.summary_json['source_deleted'] is False and batch.summary_json['raw_catalog_changed'] is False
print(json.dumps({'success': True, 'projects': 17, 'project_api_routes_verified': 17,
                  'identity_and_nonpath_metadata_preserved': True, 'original_locations_preserved': True,
                  'own_leases_released': 17, 'batch_id': request['batch_id'],
                  'checked_at': datetime.now(timezone.utc).isoformat(), 'read_only': True,
                  'source_deleted_by_project_cutover': False}))
"""


def main():
    output = HERE / "year2-project-catalog-preservation-postflight.json"
    assert not output.exists()
    admission = read_json(PRIVATE / "year2-publication-admission.json")
    hosts = read_json(HERE / "year2-final-hosts-completion-verification.json")
    control = read_json(PRIVATE / "year2-final-host-control.json")
    assert hosts["success"] is True and admission["batch_id"] == control["batch_id"]
    result = remote_json(SSH, "webserver-labo", CODE, {
        "batch_id": admission["batch_id"], "holder": admission["holder"], "leases": admission["leases"],
        "project_ids": [row["project_id"] for row in admission["leases"]], "hosts": hosts,
        "publication_result_sha256": control["publication_result_sha256"],
    }, container=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
