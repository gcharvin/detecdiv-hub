"""Set the 17 verified homes2 copies as preferred while retaining originals."""

import json

from year2_final_host_operations import (
    HERE, PRIVATE, PUBLICATION, REQUEST_SHA, fresh_admission, remote, scope_from_baseline,
)
from launch_year2_project_publication import SSH, read_json, remote_json

CODE = r"""
import hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import inspect, select, text
from sqlalchemy.orm import load_only
from api.db import SessionLocal
from api.models import (IndexingJob, Job, Project, ProjectLocation, ProjectLock, RawDatasetLocation,
                        StorageMigrationBatch, StorageRoot, User)
from api.routes_projects import get_project
from api.services.path_resolution import compose_storage_path
from api.services.project_locks import release_project_lock

request = json.load(sys.stdin)
admission = request['admission']
holder = 'alexander-2024-year2-final-publication-20261110'
assert admission['holder'] == holder
published = {UUID(row['project_id']): row for row in request['publication']}
assert len(published) == 17
source_raw = '/data/Alexander/data/2024_2'
target_raw = '/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_2'

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

def preserved(original, current):
    return {key: value for key, value in original.items() if key not in ('is_preferred', 'updated_at')} == {
        key: value for key, value in canonical(current).items() if key not in ('is_preferred', 'updated_at')}

with SessionLocal.begin() as db:
    db.execute(text("SET LOCAL lock_timeout = '10s'"))
    db.execute(text('SELECT pg_advisory_xact_lock(hashtext(:holder))'), {'holder': holder})
    batch = db.get(StorageMigrationBatch, UUID(admission['batch_id']), with_for_update=True)
    assert batch.batch_name == holder and batch.status == 'filesystem_publication_admitted'
    before = {UUID(row['project_id']): row for row in batch.metadata_json['before_projects']}
    leases = {UUID(row['id']): row for row in batch.metadata_json['leases']}
    assert set(before) == set(published) and len(leases) == 17
    assert leases == {UUID(row['id']): row for row in admission['leases']}
    projects = list(db.scalars(select(Project).where(Project.id.in_(before)).order_by(Project.id).with_for_update()))
    assert {project.id for project in projects} == set(before)
    now = datetime.now(timezone.utc)
    locks = list(db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(before),
        ProjectLock.status == 'active').with_for_update()))
    live = {row.id: row for row in locks if row.expires_at is None or row.expires_at > now}
    assert set(live) == set(leases)
    for key, row in live.items():
        assert row.holder_key == holder and row.write_scope == 'storage_relocation'
        assert str(row.project_id) == leases[key]['project_id'] and row.expires_at.isoformat() == leases[key]['expires_at']
    raw_ids = {link.raw_dataset_id for project in projects for link in project.raw_links}
    locations = select(RawDatasetLocation.raw_dataset_id, StorageRoot.path_prefix, RawDatasetLocation.relative_path).join(
        StorageRoot, RawDatasetLocation.storage_root_id == StorageRoot.id)
    for raw_id, prefix, relative in db.execute(locations):
        path = compose_storage_path(prefix, relative).rstrip('/')
        if any(path == root or path.startswith(root + '/') for root in (source_raw, target_raw)):
            raw_ids.add(raw_id)
    needles = (source_raw, target_raw, '/raw/2024_2/', *(str(i) for i in before),
               *(row['name'] for row in before.values()), *(str(i) for i in raw_ids))
    for job in db.execute(select(Job.project_id, Job.raw_dataset_id, Job.params_json).where(
            Job.status.in_(['queued', 'running', 'cancelling']))):
        assert job.project_id not in before and job.raw_dataset_id not in raw_ids
        assert not any(needle in json.dumps(job.params_json or {}, default=str) for needle in needles)
    assert db.scalar(select(IndexingJob.id).where(IndexingJob.status.in_(['queued', 'running'])).limit(1)) is None
    root = db.scalars(select(StorageRoot).where(StorageRoot.name == 'user-homes2')).one()
    assert root.path_prefix == '/homes2' and root.host_scope == 'detecdiv-server'
    operator = db.scalars(select(User).options(load_only(User.id, User.role)).where(
        User.user_key == 'gilles', User.role == 'admin', User.is_active.is_(True))).one()
    changes = []
    for project in projects:
        old, native = before[project.id], published[project.id]
        stem = old['name']
        assert project.project_name == native['name'] == stem and native['native_owner'] == 'maliavko'
        assert old['owner_key'] == ('gilles' if stem == '281124_Alex' else 'alexander')
        assert digest(identity(project)) == old['identity_sha256']
        assert digest(project.metadata_json) == old['metadata_sha256']
        assert digest([columns(row) for row in project.locations]) == old['locations_sha256']
        relative = 'maliavko/DetecdivHub/projects/' + stem
        directory = Path(root.path_prefix) / relative
        mat = directory / (stem + '.mat')
        derived = directory / stem
        assert mat.resolve(strict=True) == mat and mat.is_file() and mat.stat().st_size == native['project_mat_bytes']
        assert derived.resolve(strict=True) == derived and derived.is_dir()
        assert all(not (row.storage_root_id == root.id and row.relative_path == relative) for row in project.locations)
        old_ids = [row.id for row in project.locations]
        for row in project.locations:
            row.is_preferred = False
        location = ProjectLocation(project=project, storage_root=root, relative_path=relative,
            project_file_name=mat.name, access_mode='readwrite', is_preferred=True)
        db.add(location)
        project.metadata_json = {**(project.metadata_json or {}), 'project_mat_abs': str(mat),
                                 'project_dir_abs': str(derived), 'project_rel_from_root': relative + '/' + stem}
        for key in ('project_mat_bytes', 'project_dir_bytes', 'total_bytes'):
            setattr(project, key, native[key])
        project.last_size_scan_at = now
        db.flush()
        db.expire(project, ['locations'])
        preferred = [row for row in get_project(project.id, db, operator).locations if row.is_preferred]
        assert len(preferred) == 1 and preferred[0].project_mat_path == str(mat)
        assert digest(identity(project)) == old['identity_sha256']
        locations_by_id = {row.id: columns(row) for row in project.locations}
        assert set(locations_by_id) == set(old_ids) | {location.id}
        assert all(preserved(row, locations_by_id[row['id']]) for row in old['locations'])
        changes.append({'project_id': str(project.id), 'name': stem, 'original_location_ids': old_ids,
                        'new_location_id': location.id, 'preferred_mat_path': str(mat)})
    assert all(row.expires_at > datetime.now(timezone.utc) for row in live.values())
    for row in live.values():
        release_project_lock(db, lock=row)
    batch.status = 'projects_preferred_on_homes2_sources_retained'
    batch.summary_json = {'projects': 17, 'source_deleted': False, 'original_projects_preserved': True,
                          'project_api_detail_handlers_verified': 17, 'raw_catalog_changed': False}
    batch.metadata_json = {**batch.metadata_json, 'cutover_changes': changes, 'hosts': request['hosts'],
                           'own_leases_released': [str(i) for i in live],
                           'publication_result_sha256': request['publication_result_sha256']}
    result = {'success': True, 'batch_id': str(batch.id), 'projects': 17,
              'original_locations_preserved': True, 'own_leases_released': len(live),
              'project_api_routes_verified': 17, 'source_deleted': False, 'raw_catalog_changed': False}
result['committed_at'] = datetime.now(timezone.utc).isoformat()
print(json.dumps(result))
"""


def main():
    output = HERE / "year2-project-catalog-cutover-result.json"
    assert not output.exists()
    hosts = read_json(HERE / "year2-final-hosts-completion-verification.json")
    assert hosts["success"] is True and hosts["projects"] == 17 and hosts["limited_windows_token_verified"] is True
    control = read_json(PRIVATE / "year2-final-host-control.json")
    report = remote("linux", "request_sha=" + repr(REQUEST_SHA) + "\n" + PUBLICATION, timeout=60)
    assert report["result_sha256"] == control["publication_result_sha256"]
    assert {(row["project_id"], row["name"]) for row in report["projects"]} == {
        (row["project_id"], row["name"]) for row in scope_from_baseline()}
    admission = fresh_admission()
    assert admission["batch_id"] == control["batch_id"]
    result = remote_json(SSH, "webserver-labo", CODE, {
        "admission": admission, "publication": report["projects"],
        "publication_result_sha256": report["result_sha256"], "hosts": hosts,
    }, container=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
