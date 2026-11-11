"""Admit and launch the pinned 17-project filesystem publication."""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
PRIVATE = HERE / "runtime"
HOLDER = "alexander-2024-year2-final-publication-20261110"
PUBLISHER_SHA = "b44f97ccba7736a85e61d1f9d745d8c27d8a2e1abb41c2a857c28497f2de8e44"
INTEGRITY_SHA = "3d619d2a8c6bad75ec3118e333cbafd87550b0e8f5fa16326cf21c09c9a2ee29"
sys.path.insert(0, str(HERE.parent / "homes2-access-audit-20261101"))
from activate_windows_mapping import remote
from audit import SSH, remote_json

CATALOG = r"""
from datetime import datetime, timezone
import hashlib, json, sys
from uuid import UUID

from sqlalchemy import inspect, select, text
from sqlalchemy.orm import load_only
from api.db import SessionLocal
from api.models import (IndexingJob, Job, Project, ProjectLock, RawDatasetLocation,
                        StorageMigrationBatch, StorageRoot, User)
from api.services.path_resolution import compose_storage_path
from api.services.project_locks import acquire_client_edit_lease

request = json.load(sys.stdin)
holder = 'alexander-2024-year2-final-publication-20261110'
projects = {UUID(row['project_id']): row for row in request['projects']}
assert len(projects) == len(request['projects']) == 17
source = '/data/Alexander/data/2024_2'
target = '/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_2'

def columns(obj):
    return {column.key: getattr(obj, column.key) for column in inspect(type(obj)).column_attrs}

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()

def identity(project):
    return {'key': project.project_key, 'owner': str(project.owner_user_id),
            'visibility': project.visibility, 'notes': project.notes,
            'acl': [columns(row) for row in project.acl_entries],
            'groups': [columns(row) for row in project.group_memberships],
            'note_entries': [columns(row) for row in project.note_entries],
            'raw_links': [columns(row) for row in project.raw_links]}

mode = request['mode']
assert mode in ('check', 'admit', 'fresh')
with SessionLocal.begin() as db:
    db.execute(text("SET LOCAL lock_timeout = '10s'"))
    if mode == 'check':
        db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    else:
        db.execute(text('SELECT pg_advisory_xact_lock(hashtext(:holder))'), {'holder': holder})
    batches = list(db.scalars(select(StorageMigrationBatch).where(StorageMigrationBatch.batch_name == holder)))
    if mode == 'fresh':
        assert len(batches) == 1 and str(batches[0].id) == request['batch_id']
        assert batches[0].status == 'filesystem_publication_admitted'
    else:
        assert not batches, 'Existing admission must be inspected'
    query = select(Project).where(Project.id.in_(projects)).order_by(Project.id)
    if mode != 'check':
        query = query.with_for_update()
    current = list(db.scalars(query))
    assert {project.id for project in current} == set(projects)
    before = []
    for project in current:
        expected = projects[project.id]
        owner = db.scalar(select(User.user_key).where(User.id == project.owner_user_id))
        assert project.project_name == expected['name']
        assert owner == ('gilles' if expected['name'] == '281124_Alex' else 'alexander')
        actual = {'identity_sha256': digest(identity(project)), 'metadata_sha256': digest(project.metadata_json),
                  'locations_sha256': digest([columns(row) for row in project.locations])}
        assert all(expected[key] == value for key, value in actual.items()), 'Project catalog has changed'
        before.append(dict(actual, project_id=str(project.id), name=project.project_name, owner_key=owner,
                           columns=columns(project), locations=[columns(row) for row in project.locations]))
    raw_ids = {link.raw_dataset_id for project in current for link in project.raw_links}
    locations = select(RawDatasetLocation.raw_dataset_id, StorageRoot.path_prefix, RawDatasetLocation.relative_path).join(
        StorageRoot, RawDatasetLocation.storage_root_id == StorageRoot.id)
    for raw_id, prefix, relative in db.execute(locations):
        path = compose_storage_path(prefix, relative).rstrip('/')
        if any(path == root or path.startswith(root + '/') for root in (source, target)):
            raw_ids.add(raw_id)
    needles = (source, target, '/raw/2024_2/', *(str(project_id) for project_id in projects),
               *(row['name'] for row in projects.values()), *(str(raw_id) for raw_id in raw_ids))
    jobs = select(Job.project_id, Job.raw_dataset_id, Job.params_json).where(Job.status.in_(['queued', 'running', 'cancelling']))
    for job in db.execute(jobs):
        serialized = json.dumps(job.params_json or {}, default=str)
        assert job.project_id not in projects and job.raw_dataset_id not in raw_ids
        assert not any(needle in serialized for needle in needles), 'Affected job parameters present'
    assert db.scalar(select(IndexingJob.id).where(IndexingJob.status.in_(['queued', 'running'])).limit(1)) is None
    now = datetime.now(timezone.utc)
    locks = [row for row in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(projects),
                                                               ProjectLock.status == 'active'))
             if row.expires_at is None or row.expires_at > now]
    leases = []
    if mode == 'fresh':
        expected = {UUID(row['id']): row for row in batches[0].metadata_json['leases']}
        assert len(expected) == 17 and {row.id for row in locks} == set(expected)
        for row in locks:
            assert row.holder_key == holder and row.write_scope == 'storage_relocation'
            assert row.expires_at and row.expires_at.isoformat() == expected[row.id]['expires_at']
            assert str(row.project_id) == expected[row.id]['project_id']
        leases = list(expected.values())
    else:
        assert not locks, 'Affected leases already active'
    if mode == 'admit':
        operator = db.scalars(select(User).options(load_only(User.id, User.user_key)).where(
            User.user_key == 'gilles', User.role == 'admin', User.is_active.is_(True))).one()
        for project in current:
            lease = acquire_client_edit_lease(db, project_id=project.id, owner=operator,
                holder_key=holder, holder_host='codex-admin-workstation', ttl_seconds=21600,
                write_scope='storage_relocation', reason='Publish verified 2024_2 project copies',
                metadata_json={'source_deletion_allowed': False, 'year': '2024_2'})
            leases.append({'id': str(lease.id), 'project_id': str(project.id),
                           'expires_at': lease.expires_at.isoformat()})
        before = json.loads(json.dumps(before, default=str))
        batch = StorageMigrationBatch(batch_name=holder, source_kind='detecdiv_project',
            source_path='/homes2/maliavko/DetecdivHub/projects', storage_root_name='user-homes2',
            host_scope='detecdiv-server', root_type='project_root',
            strategy='verified_final_project_publication_preserve_originals', status='filesystem_publication_admitted',
            metadata_json={'holder': holder, 'before_projects': before, 'leases': leases,
                           'integrity_result_sha256': request['integrity_result_sha256']},
            summary_json={'projects': 17, 'source_deleted': False, 'preferred_locations_changed': False})
        db.add(batch)
        db.flush()
        batches = [batch]
    result = {'mode': mode, 'holder': holder, 'projects': 17, 'raw_job_scope': len(raw_ids),
              'leases': leases, 'batch_id': str(batches[0].id) if batches else None,
              'checked_at': datetime.now(timezone.utc).isoformat()}
print(json.dumps(result))
"""

COMPUTE = r"""
import base64, hashlib, importlib.util, json, shlex, subprocess
from datetime import datetime, timezone
from pathlib import Path
import sys

root = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work = root / 'year2-final-publication-20261110'
script = work / 'publish_year2_folders.py'
audit = root / 'year2-incoming-integrity-20261110'
raw = json.loads((root / 'state.json').read_text())
year = raw['years']['2024_2']
assert raw['phase'] == 'both_years_verified_project_host_catalog_gates_pending'
assert year['full_sha1_passed'] is True and year['source_stability_verified'] is True
assert (root / 'controller.exit').read_text().strip() == '0'
assert (root / '2024_2/verify-full-sha1.log').stat().st_size == 0
assert year['finished_at'] == inputs['raw_finished_at']
if inputs['mode'] == 'prepare':
    payload = base64.b64decode(inputs['publisher'], validate=True)
    assert hashlib.sha256(payload).hexdigest() == inputs['publisher_sha256']
    assert hashlib.sha256((audit / 'request.json').read_bytes()).hexdigest() == (
        'f4573231522cdf88b0624ea635ffae314ea744d0f9e1d44903a136b6d19b4c63')
    evidence = (audit / 'result.json').read_bytes()
    assert hashlib.sha256(evidence).hexdigest() == inputs['integrity_result_sha256']
    results = json.loads(evidence)['projects']
    projects = []
    for project, result in zip(json.loads((audit / 'request.json').read_text())['projects'], results, strict=True):
        assert project['project_id'] == result['project_id'] and project['name'] == result['name']
        rows = (audit / project['name'] / 'target-full-hashes.jsonl').read_bytes()
        assert hashlib.sha256(rows).hexdigest() == result['target_receipt_sha256']
        decoded = [json.loads(line) for line in rows.splitlines()]
        assert decoded[-1]['kind'] == 'complete' and len(decoded) == result['target_files'] + 1
        keys = ('project_id', 'name', 'source_inventory_sha256', 'stage_suffix', 'directories')
        projects.append(dict({key: project[key] for key in keys}, target_rows=decoded[:-1],
                             relinked_paths=[row['path'] for row in project['files'] if row['metadata_relinked']]))
    scope = sorted((project['project_id'], project['name']) for project in projects)
    assert scope == sorted((row['project_id'], row['name']) for row in inputs['projects'])
    assert len(scope) == 17 and sum(len(project['target_rows']) for project in projects) == 103243
    work.mkdir(mode=0o700)
    with script.open('xb') as out:
        out.write(payload)
    prepared = {'projects': projects, 'integrity_result_sha256': inputs['integrity_result_sha256'],
                'runner_sha256': inputs['publisher_sha256'], 'source_deletion_allowed': False,
                'catalog_cutover_allowed': False}
    staged = work / 'prepared-request.json'
    with staged.open('x') as out:
        json.dump(prepared, out)
    print(json.dumps({'prepared': True, 'projects': len(scope), 'work': str(work),
                      'prepared_sha256': hashlib.sha256(staged.read_bytes()).hexdigest()}))
else:
    assert inputs['mode'] == 'start'
    assert not (work / 'state.json').exists() and not (work / 'controller.exit').exists()
    assert hashlib.sha256(script.read_bytes()).hexdigest() == inputs['publisher_sha256']
    staged = (work / 'prepared-request.json').read_bytes()
    assert hashlib.sha256(staged).hexdigest() == inputs['prepared_sha256']
    request = json.loads(staged)
    admission = inputs['admission']
    assert admission['mode'] == 'fresh' and len(admission['leases']) == 17
    assert 0 <= (datetime.now(timezone.utc) - datetime.fromisoformat(admission['checked_at'])).total_seconds() < 60
    request['admission'] = admission
    request['own_lease_deadline_unix'] = min(datetime.fromisoformat(lease['expires_at']).timestamp()
                                           for lease in admission['leases'])
    with (work / 'request.json').open('x') as out:
        json.dump(request, out)
    command = shlex.join(['nice', '-n', '19', 'ionice', '-c', '3', sys.executable, '-u', str(script)])
    command += ' > ' + shlex.quote(str(work / 'controller.log')) + ' 2>&1'
    command += '; result=$?; printf "%s\\n" "$result" > ' + shlex.quote(str(work / 'controller.exit'))
    session = 'alex2024-year2-final-publication-20261110'
    subprocess.run(['tmux', 'new-session', '-d', '-s', session, 'bash', '-c', command], check=True)
    print(json.dumps({'launched': True, 'work': str(work), 'session': session, 'batch_id': admission['batch_id'],
                      'projects': 17, 'catalog_cutover_performed': False, 'source_deleted': False}))
"""


def read_json(path):
    return json.loads(path.read_bytes())


def write_receipt(name, value):
    with (PRIVATE / name).open("x", encoding="utf-8") as out:
        json.dump(value, out, indent=2)
        out.write("\n")


def scope_from_baseline():
    baseline = PRIVATE / "year2-catalog-prepublication-review.json"
    review = read_json(HERE / "year2-catalog-prepublication-review-verification.json")
    assert hashlib.sha256(baseline.read_bytes()).hexdigest() == review["private_snapshot_sha256"]
    assert review["all_catalog_identity_metadata_locations_unchanged"] is True
    keys = ("project_id", "name", "identity_sha256", "metadata_sha256", "locations_sha256")
    return [{key: row[key] for key in keys} for row in read_json(baseline)["before_projects"]]


def compute(inputs):
    return remote("linux", "import json\ninputs=json.loads(" + repr(json.dumps(inputs)) + ")\n" + COMPUTE, timeout=90)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Check current catalog and jobs in read-only mode")
    args = parser.parse_args()
    scope = scope_from_baseline()
    parameters = {"projects": scope, "integrity_result_sha256": INTEGRITY_SHA}
    if args.check:
        print(json.dumps(remote_json(SSH, "webserver-labo", CATALOG, dict(parameters, mode="check"), container=True)))
        return
    raw = read_json(HERE / "raw-year2-completion-verification.json")
    assert raw["success"] is True and raw["full_sha1_passed"] is True and raw["source_stability_verified"] is True
    assert raw["year"] == "2024_2" and raw["controller_exit"] == 0 and raw["verification_log_bytes"] == 0
    fixture = read_json(HERE / "year2-publication-fixtures-completion-verification.json")
    assert fixture["publisher_sha256"] == PUBLISHER_SHA and fixture["real_cifs_cases_passed"] == 9
    assert fixture["isolated_fixture_cases_passed"] == 21 and fixture["cifs_fixture_cleaned"] is True
    publisher = (HERE / "publish_year2_folders.py").read_bytes()
    assert hashlib.sha256(publisher).hexdigest() == PUBLISHER_SHA
    inputs = dict(parameters, publisher_sha256=PUBLISHER_SHA, raw_finished_at=raw["source_stability_finished_at"])
    prepared = compute(dict(inputs, mode="prepare", publisher=base64.b64encode(publisher).decode("ascii")))
    write_receipt("year2-publication-prepared.json", prepared)
    admission = remote_json(SSH, "webserver-labo", CATALOG, dict(parameters, mode="admit"), container=True)
    assert admission["holder"] == HOLDER and len(admission["leases"]) == 17
    write_receipt("year2-publication-admission.json", admission)
    fresh = remote_json(SSH, "webserver-labo", CATALOG,
                        dict(parameters, mode="fresh", batch_id=admission["batch_id"]), container=True)
    launch = compute(dict(inputs, mode="start", prepared_sha256=prepared["prepared_sha256"], admission=fresh))
    write_receipt("year2-publication-launch.json", launch)
    print(json.dumps(launch))


if __name__ == "__main__":
    main()
