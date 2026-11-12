"""Run and corroborate the 2024 raw media checks with durable host receipts."""

import argparse
import base64
import hashlib
import json

from prepare_raw_api_cutover import API, HERE, SSH, remote_json

PLAN = HERE / 'runtime/raw-api-cutover-preflight-20261112.json'

HOST = r'''
import base64, hashlib, json, os, shlex, subprocess, sys
from pathlib import Path

request = json.load(sys.stdin)
mode = request['mode']
assert mode in ('targets', 'handlers')
name = 'raw-api-cutover-' + mode + '-20261112'
work = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105') / name
container_work = '/tmp/alexander-' + name
container = 'detecdiv-hub-api'
os.umask(0o077)
if request['operation'] == 'launch':
    work.mkdir(mode=0o700)
    for name, item in request['files'].items():
        data = base64.b64decode(item['data'], validate=True)
        assert hashlib.sha256(data).hexdigest() == item['sha256']
        (work / name).write_bytes(data)
    subprocess.run(['docker', 'exec', container, 'python', '-c',
                    'import os;os.mkdir(' + repr(container_work) + ',0o700)'], check=True)
    for name in request['files']:
        subprocess.run(['docker', 'cp', str(work / name), container + ':' + container_work + '/' + name],
                       check=True, capture_output=True)
    command = shlex.join(['docker', 'exec', container, 'python', '-u',
                          container_work + '/verify_raw_api_cutover_media.py', mode])
    archive = work / 'container-reports'
    archive.mkdir()
    copy = shlex.join(['docker', 'cp', container + ':' + container_work + '/.', str(archive)])
    script = f'{command}\nstatus=$?\nprintf "%s\\n" "$status" > controller.exit\n'
    script += f'{copy}\nprintf "%s\\n" "$?" > archive.exit\nexit "$status"\n'
    (work / 'run.sh').write_text(script)
    with (work / 'controller.log').open('xb') as log:
        p = subprocess.Popen(['bash', 'run.sh'], cwd=work, start_new_session=True,
                             stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
    launch = {'launched': True, 'mode': mode, 'pid': p.pid, 'plan_sha256': request['files']['plan.json']['sha256'],
              'script_sha256': request['files']['verify_raw_api_cutover_media.py']['sha256']}
    (work / 'launch.json').write_text(json.dumps(launch) + '\n')
    print(json.dumps(launch))
else:
    assert request['operation'] == 'status'
    launch = json.loads((work / 'launch.json').read_bytes())
    assert launch['plan_sha256'] == request['plan_sha256']
    exit_file, archive_exit = work / 'controller.exit', work / 'archive.exit'
    log = (work / 'controller.log').read_bytes().splitlines()
    rows = [line for line in log if line.startswith(b'{"year":')]
    if not exit_file.is_file() or not archive_exit.is_file():
        print(json.dumps({'phase': 'running_or_archiving', 'mode': mode, 'verified': len(rows)}))
    else:
        exits = [int(path.read_text()) for path in (exit_file, archive_exit)]
        if exits != [0, 0]:
            print(json.dumps({'phase': 'failed', 'mode': mode, 'verified': len(rows), 'exit_codes': exits}))
        else:
            archive = work / 'container-reports'
            result = json.loads((archive / 'result.json').read_bytes())
            receipt = (archive / 'receipt.jsonl').read_bytes()
            assert hashlib.sha256(receipt).hexdigest() == result['receipt_sha256']
            assert receipt.splitlines() == rows
            assert result['success'] and result['mode'] == mode and result['plan_sha256'] == launch['plan_sha256']
            plan_bytes = (work / 'plan.json').read_bytes()
            assert hashlib.sha256(plan_bytes).hexdigest() == launch['plan_sha256']
            plan = json.loads(plan_bytes)['plan']
            expected = [(year, r['kind'], r['id'], r['bytes'], r['sha256'])
                        for year, scope in plan['years'].items() for r in scope['media']]
            actual = [tuple(json.loads(line)[field] for field in ('year','kind','id','bytes','sha256'))
                      for line in rows]
            assert actual == expected and len(actual) == result['verified'] == 2081
            assert sum(row[3] for row in actual) == result['bytes'] == 15841239322
            print(json.dumps({**result, 'phase': 'complete', 'exit_codes': exits, 'durable_receipts_verified': True}))
'''


def operate(operation, mode):
    plan = PLAN.read_bytes()
    plan_sha = hashlib.sha256(plan).hexdigest()
    args = {'operation': operation, 'mode': mode, 'plan_sha256': plan_sha}
    if operation == 'launch':
        if mode == 'handlers':
            proof = json.loads((HERE / 'raw-api-cutover-result.json').read_bytes())
            assert proof['success'] and proof['plan_sha256'] == plan_sha
        files = {'plan.json': plan, 'control.json': json.dumps({'mode': mode, 'plan_sha256': plan_sha}).encode(),
                 'catalog_support.py': API.encode(),
                 'verify_raw_api_cutover_media.py': (HERE / 'verify_raw_api_cutover_media.py').read_bytes(),
                 'prior_media_verifier.py': (HERE / 'verify_year2_api_media_readonly.py').read_bytes()}
        args['files'] = {name: {'data': base64.b64encode(data).decode(), 'sha256': hashlib.sha256(data).hexdigest()}
                         for name, data in files.items()}
    result = remote_json(SSH, 'webserver-labo', HOST, args, container=False)
    if result.get('phase') == 'complete':
        proof = HERE / f'raw-api-{mode}-verification.json'
        if proof.exists():
            assert json.loads(proof.read_bytes()) == result
        else:
            with proof.open('x', encoding='utf-8') as stream:
                json.dump(result, stream, indent=2)
                stream.write('\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('launch', 'status'))
    parser.add_argument('mode', choices=('targets', 'handlers'))
    args = parser.parse_args()
    print(json.dumps(operate(args.operation, args.mode)))


if __name__ == '__main__':
    main()
