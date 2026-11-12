"""Stage small, indexed ZIP main candidates and run the protected path reader."""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import stat
import subprocess
import sys
import zipfile


WORK = Path(__file__).resolve().parent
ROOT = WORK.parent
INDEX = ROOT / 'legacy-unspecified-raw-zip-indexes-recovery-20261112'
EXPECTED_INDEX_RECEIPT = '3907868f85326790e990da3436a6ca9b6fe5bfdd462c7e285e0b808515c989bc'
EXPECTED_INDEX_REQUEST = '07e2d9f10578cb2548646c59f1398a7d3abbf5956399cf3f4465d545862f2ae7'
ENGINE = '/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def normalized(value):
    return re.sub('[^a-z0-9]+', '', value.casefold())


def stamp(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def main():
    os.umask(0o077)
    control = json.loads((WORK / 'control.json').read_bytes())
    assert control['read_only'] and not control['source_saved']
    assert digest(Path(__file__).read_bytes()) == control['runner_sha256']
    reader = (WORK / 'review_legacy_paths_v3.m').read_bytes()
    assert digest(reader) == control['matlab_sha256']
    receipt_bytes = (INDEX / 'receipt.jsonl').read_bytes()
    request_bytes = (INDEX / 'request.json').read_bytes()
    assert digest(receipt_bytes) == EXPECTED_INDEX_RECEIPT and digest(request_bytes) == EXPECTED_INDEX_REQUEST
    assert (INDEX / 'controller.exit').read_text().strip() == '0'
    completed = json.loads((INDEX / 'result.json').read_bytes())
    assert completed['counts']['complete_index_only'] == 2
    index_rows = [json.loads(line) for line in receipt_bytes.splitlines()]
    requests = {row['path']: row for row in json.loads(request_bytes)['archives']}
    baseline = json.loads((ROOT / 'project-discovery-format-review.json').read_bytes())['still_missing']
    baseline = {row['id']: row for row in baseline}
    original = json.loads((ROOT / 'project-discovery-request.json').read_bytes())
    inputs = WORK / 'inputs'
    inputs.mkdir(mode=0o700)
    rows = []
    deferred = []
    seen = set()
    with (WORK / 'archive-inputs.jsonl').open('x', encoding='utf-8') as evidence:
        for item in index_rows:
            if item['status'] != 'complete_index_only':
                continue
            archive_path = Path(item['archive'])
            assert archive_path.parts[:3] in (('/', 'archive', 'theo'), ('/', 'archive', 'audrey')) and archive_path.resolve(strict=True) == archive_path
            projects = requests[item['archive']]['projects']
            matches = []
            for candidate in item['candidates']:
                if not candidate['main_name_match']:
                    continue
                stem = normalized(PurePosixPath(candidate['member']).stem)
                project, = [p for p in projects if stem in {normalized(Path(n).stem) for n in p['declared_names']}]
                project_id = project['id']
                assert project_id not in seen and candidate['bytes'] <= 4747
                seen.add(project_id)
                if project_id in control['busy_project_ids']:
                    deferred.append(project_id)
                else:
                    matches.append((project_id, candidate))
            if not matches:
                continue
            with os.fdopen(os.open(archive_path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
                before = os.fstat(stream.fileno())
                assert stat.S_ISREG(before.st_mode)
                with zipfile.ZipFile(stream, 'r') as archive:
                    assert len(archive.infolist()) == item['entries']
                    for project_id, candidate in matches:
                        member = archive.getinfo(candidate['member'])
                        assert member.file_size == candidate['bytes'] and f'{member.CRC:08x}' == candidate['crc32']
                        with archive.open(member) as source:
                            payload = source.read(member.file_size + 1)
                        assert len(payload) == member.file_size
                        target = inputs / (project_id + '.mat')
                        with target.open('xb') as output:
                            output.write(payload)
                            output.flush()
                            os.fsync(output.fileno())
                        old = baseline[project_id]
                        rows.append({'id': project_id, 'name': old['name'], 'owner': old['owner'], 'file': str(target)})
                        evidence.write(json.dumps({'project_id': project_id, 'archive': str(archive_path),
                            'member': candidate['member'], 'bytes': len(payload), 'sha256': digest(payload)}) + '\n')
                        evidence.flush()
                assert stamp(before) == stamp(os.fstat(stream.fileno()))
            if len(rows) % 25 < len(matches):
                print(json.dumps({'kind': 'staging_progress', 'staged': len(rows), 'deferred': len(deferred)}), flush=True)
        os.fsync(evidence.fileno())
    assert len(seen) == len(rows) + len(deferred) == 2 and rows
    request = {'projects': rows, 'acquisitions': original['acquisitions'], 'busy_project_ids': [],
               'read_only': True, 'deferred': deferred, 'source_saved': False}
    (WORK / 'request.json').write_text(json.dumps(request) + '\n')
    (WORK / 'stage-result.json').write_text(json.dumps({'staged': len(rows), 'deferred': len(deferred),
        'inputs_sha256': digest((WORK / 'archive-inputs.jsonl').read_bytes())}) + '\n')
    assert subprocess.check_output(['git', '-C', ENGINE, 'rev-parse', 'HEAD'], text=True).strip() == Path(ENGINE).name
    expression = "maxNumCompThreads(1);addpath('" + str(WORK) + "');review_legacy_paths_v3('" + str(WORK) + "','" + ENGINE + "');"
    args = ['env', 'CUDA_VISIBLE_DEVICES=', 'nice', '-n', '19', 'ionice', '-c', '3', '/usr/local/bin/matlab', '-batch', expression]
    subprocess.run(args, check=True)


if __name__ == '__main__':
    main()
