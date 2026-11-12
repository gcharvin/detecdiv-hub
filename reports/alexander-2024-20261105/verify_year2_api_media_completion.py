"""Corroborate the finished year2 media actor from its archived receipts."""

import json

from launch_year2_api_media_readonly import HERE, SSH, RUNNER_SHA256, remote_json

CODE = r"""
import hashlib, json, re
from pathlib import Path

work = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year2-api-media-readonly-20261111')
archive = work / 'container-reports'
assert (work / 'controller.exit').read_text().strip() == '0'
assert (work / 'archive.exit').read_text().strip() == '0'
control = json.loads((archive / 'controller-request.json').read_bytes())
runner = (archive / 'verify_year2_api_media_readonly.py').read_bytes()
assert hashlib.sha256(runner).hexdigest() == control['runner_sha256'] == '584d29a81717fa3d1632ec65b7ae092b9356dae953627373d2e2f398a91e8b50'
raw_proof = (archive / 'raw-year2-completion-verification.json').read_bytes()
assert hashlib.sha256(raw_proof).hexdigest() == control['raw_completion_sha256']
plan = (archive / 'plan.json').read_bytes()
plan_hash = hashlib.sha256(plan).hexdigest()
assert plan_hash == '36329bad1371cf9d95251eab52033ccc8e78eb79ca56a95468cf53bdae53125d'
plan = json.loads(plan)
result = json.loads((archive / 'result.json').read_bytes())
assert result['success'] is True and result['phase'] == 'verified_before_uri_cutover'
assert result['read_only'] is True and result['source_deleted'] is False and result['uri_changed'] is False
assert result['route_sha256'] == plan['route_sha256']
receipt = (archive / 'receipt.jsonl').read_bytes()
assert hashlib.sha256(receipt).hexdigest() == result['receipt_sha256']
rows = [json.loads(line) for line in receipt.splitlines()]
expected = [(row['kind'], row['id'], row['raw_dataset_id'], row['relative_path'], row['initial_native_bytes'])
            for row in plan['references']]
actual = [(row['kind'], row['id'], row['raw_dataset_id'], row['relative_path'], row['bytes']) for row in rows]
assert actual == expected and len(set(actual)) == len(actual) == result['handlers_verified'] == 877
assert all(re.fullmatch('[0-9a-f]{64}', row['sha256']) for row in rows)
assert sum(row['bytes'] for row in rows) == result['bytes_verified'] == 4982573526
host_receipts = []
terminal = []
for line in (work / 'controller.log').read_bytes().splitlines():
    try:
        record = json.loads(line)
    except ValueError:
        continue
    if isinstance(record, dict) and 'raw_dataset_id' in record:
        host_receipts.append(record)
    elif isinstance(record, dict) and record.get('kind') == 'terminal_result':
        terminal.append(record)
assert host_receipts == rows and terminal == [{'kind': 'terminal_result', **result}]
print(json.dumps(dict(result, plan_sha256=plan_hash, controller_exit=0, archive_exit=0,
    all_receipts_match_pinned_plan=True, durable_host_log_matches_archive=True)))
"""


def main():
    output = HERE / "year2-api-media-readonly-completion-verification.json"
    assert not output.exists()
    launch = json.loads((HERE / "year2-api-media-readonly-launch-verification.json").read_bytes())
    assert launch["runner_sha256"] == RUNNER_SHA256
    result = remote_json(SSH, "webserver-labo", CODE, {}, container=False)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
