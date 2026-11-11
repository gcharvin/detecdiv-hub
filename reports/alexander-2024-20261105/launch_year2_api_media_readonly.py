"""Start the year2 readonly media verifier once terminal raw proof exists."""

import base64
import hashlib
import json
from pathlib import Path
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "homes2-access-audit-20261101"))
from activate_windows_mapping import remote
from audit import SSH, remote_json

RUNNER_SHA256 = "584d29a81717fa3d1632ec65b7ae092b9356dae953627373d2e2f398a91e8b50"

COMPUTE_PLAN = r"""
import base64, hashlib, json
from pathlib import Path
root = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
state = json.loads((root / 'state.json').read_text())
year = state['years']['2024_2']
assert year['full_sha1_passed'] is True and year['source_stability_verified'] is True
assert year['sha1_finished_at'] == proof['sha1_finished_at']
assert year['finished_at'] == proof['source_stability_finished_at']
assert state['source_deleted'] is False and (root / 'controller.exit').read_text().strip() == '0'
plan = (root / 'year2-api-media-inventory-20261111/plan.json').read_bytes()
assert hashlib.sha256(plan).hexdigest() == '36329bad1371cf9d95251eab52033ccc8e78eb79ca56a95468cf53bdae53125d'
print(json.dumps({'plan': base64.b64encode(plan).decode()}))
"""

SUPERVISOR = r"""
import json
from pathlib import Path
import subprocess
import sys
import time

work = Path(__file__).resolve().parent
container_path = sys.argv[1]
control = json.loads((work / 'controller-request.json').read_text())
timeout = max(0.1, control['deadline_unix'] - time.time())
try:
    execution = subprocess.run(
        ['docker', 'exec', 'detecdiv-hub-api', 'python', '-u',
         container_path + '/verify_year2_api_media_readonly.py'], timeout=timeout)
    result = execution.returncode
except subprocess.TimeoutExpired:
    result = 124
(work / 'controller.exit').write_text(str(result) + '\n')
reports = work / 'container-reports'
reports.mkdir(mode=0o700)
archived = subprocess.run(['docker', 'cp', 'detecdiv-hub-api:' + container_path + '/.', str(reports)])
(work / 'archive.exit').write_text(str(archived.returncode) + '\n')
sys.exit(result or archived.returncode)
"""

LAUNCH = r"""
import base64, json, os, subprocess, sys
from pathlib import Path

os.umask(0o077)
inputs = json.load(sys.stdin)
work = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year2-api-media-readonly-20261111')
container_path = '/tmp/alexander-year2-api-readonly-20261111'
work.mkdir(mode=0o700)
for name, encoded in inputs['files'].items():
    (work / name).write_bytes(base64.b64decode(encoded, validate=True))
(work / 'supervisor.py').write_text(inputs['supervisor'])
subprocess.run(['docker', 'exec', 'detecdiv-hub-api', 'python', '-c',
                'import os; os.mkdir(' + repr(container_path) + ', 0o700)'], check=True, capture_output=True)
for name in inputs['files']:
    subprocess.run(['docker', 'cp', str(work / name), 'detecdiv-hub-api:' + container_path + '/' + name],
                   check=True, capture_output=True)
with (work / 'controller.log').open('xb') as log:
    actor = subprocess.Popen([sys.executable, str(work / 'supervisor.py'), container_path],
                             stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                             start_new_session=True)
receipt = {'work': str(work), 'container_work': container_path, 'pid': actor.pid, 'read_only': True,
           'source_deleted': False, 'uri_changed': False}
(work / 'launch.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
"""


def encode(data):
    return base64.b64encode(data).decode("ascii")


def main():
    output = HERE / "year2-api-media-readonly-launch-verification.json"
    assert not output.exists(), "Inspect the existing verifier rather than replaying it"
    proof_path = HERE / "raw-year2-completion-verification.json"
    proof_bytes = proof_path.read_bytes()
    proof = json.loads(proof_bytes)
    assert proof["success"] and proof["year"] == "2024_2"
    assert proof["full_sha1_passed"] and proof["source_stability_verified"] and proof["controller_exit"] == 0
    assert proof["verification_log_bytes"] == 0
    assert proof["native_before_after_manifest_sha256"] == (
        "004fb3404b4af8fe09b6063a09e3a162488f689499ddb51087513daf3d9f4fad"
    )
    runner = (HERE / "verify_year2_api_media_readonly.py").read_bytes()
    assert hashlib.sha256(runner).hexdigest() == RUNNER_SHA256
    compute = remote("linux", "import json\nproof=json.loads(" + repr(json.dumps(proof)) + ")\n" + COMPUTE_PLAN,
                     timeout=45)
    control = {"runner_sha256": RUNNER_SHA256, "raw_completion_sha256": hashlib.sha256(proof_bytes).hexdigest(),
               "deadline_unix": time.time() + 7200, "read_only": True,
               "source_deletion_allowed": False, "uri_changes_allowed": False}
    files = {proof_path.name: encode(proof_bytes), "verify_year2_api_media_readonly.py": encode(runner),
             "plan.json": compute["plan"], "controller-request.json": encode(json.dumps(control).encode())}
    receipt = remote_json(SSH, "webserver-labo", LAUNCH, {"files": files, "supervisor": SUPERVISOR})
    receipt.update(control, expected_handlers=877, expected_bytes=4982573526)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2)
        stream.write("\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
