"""Hash the readable target files or stream the live handlers after cutover."""

import asyncio
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.orm import load_only

from api.db import SessionLocal
from api.models import User
import api.routes_raw_datasets as routes
from catalog_support import media_rows, paths, verify_path
from prior_media_verifier import digest_file, digest_response, fingerprint

WORK = Path(__file__).resolve().parent


def check_catalog(db, plan):
    for year, scope in plan['years'].items():
        source, target, old = paths(year)
        expected = {(row['kind'], row['id'], row['target'], row['raw_dataset_id']) for row in scope['media']}
        assert media_rows(db, (source, str(target), str(old))) == expected


def verify(db, plan, mode, deadline, receipt):
    route = Path(routes.__file__)
    assert hashlib.sha256(route.read_bytes()).hexdigest() == plan['route_sha256']
    operator = db.scalars(select(User).options(load_only(User.id, User.role)).where(
        User.user_key == 'gilles', User.role == 'admin', User.is_active.is_(True)
    )).one()
    if mode == 'handlers':
        check_catalog(db, plan)
    count = total = 0
    for year, scope in plan['years'].items():
        _, target, old = paths(year)
        for row in scope['media']:
            assert time.time() < deadline
            path = target / row['relative_path']
            assert str(path) == row['target']
            original = old / row['relative_path']
            verify_path(path, original, row['bytes'])
            before = fingerprint(original)
            if mode == 'handlers':
                identifier = UUID(row['id'])
                if row['kind'] == 'artifact':
                    response = routes.get_raw_preview_artifact_content(identifier, db, operator)
                else:
                    assert row['kind'] == 'display_settings'
                    response = routes.get_raw_dataset_display_settings(identifier, db, operator)
                assert Path(response.path) == path
                digest, size = asyncio.run(digest_response(response, deadline))
            else:
                digest, size = digest_file(path, deadline)
            assert (digest, size) == (row['sha256'], row['bytes'])
            assert fingerprint(original) == before
            line = json.dumps({'year': year, 'kind': row['kind'], 'id': row['id'],
                               'bytes': size, 'sha256': digest})
            receipt.write(line + '\n')
            receipt.flush()
            print(line, flush=True)
            count += 1
            total += size
    if mode == 'handlers':
        check_catalog(db, plan)
    assert hashlib.sha256(route.read_bytes()).hexdigest() == plan['route_sha256']
    assert (count, total) == (2081, 15841239322)
    return count, total


def main():
    mode = sys.argv[1]
    assert mode in ('targets', 'handlers')
    request = json.loads((WORK / 'control.json').read_bytes())
    assert request['mode'] == mode
    payload = (WORK / 'plan.json').read_bytes()
    assert hashlib.sha256(payload).hexdigest() == request['plan_sha256']
    plan = json.loads(payload)['plan']
    deadline = time.time() + 5400
    path = WORK / 'receipt.jsonl'
    with SessionLocal() as db, path.open('x', encoding='utf-8') as receipt:
        db.execute(text('SET TRANSACTION READ ONLY'))
        count, total = verify(db, plan, mode, deadline, receipt)
    result = {'success': True, 'mode': mode, 'verified': count, 'bytes': total,
              'plan_sha256': request['plan_sha256'], 'receipt_sha256': digest_file(path, deadline)[0],
              'finished_at': datetime.now(timezone.utc).isoformat(), 'source_deleted': False}
    with (WORK / 'result.json').open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps({'kind': 'terminal', **result}), flush=True)


if __name__ == '__main__':
    main()
