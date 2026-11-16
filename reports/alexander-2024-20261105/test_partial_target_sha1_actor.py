"""Exercise the selected-root actor with fake rsync and temporary fixtures."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import partial_target_sha1_readonly as actor


class FakeProcess:
    def __init__(self, output, changed):
        self.pid = 12345
        self.returncode = 0
        if changed:
            output.write(b'>fc........ x\n')

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode


class TargetActorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='alex2024-target-actor-')
        self.base = Path(self.tmp.name)
        self.work = self.base / 'work'
        self.work.mkdir()
        self.source = self.base / 'fixture-source'
        self.target = self.base / 'fixture-target'
        self.source.mkdir()
        self.target.mkdir()
        roots = []
        for index in range(17):
            name = f'acquisition-{index:02d}'
            for parent in (self.source, self.target):
                (parent / name).mkdir()
                (parent / name / 'x').write_bytes(b'data')
            roots.append({'year': '2024_1', 'name': name, 'files': 1, 'bytes': 4, 'directories': 1})
        original = self.base / 'copy_raws.py'
        original.write_text("def require_mounts(): pass\n"
                            "def rsync(source, target, bandwidth, verify=False):\n"
                            " return ['rsync', '--dry-run', '--checksum-choice=sha1', '--delete']\n")
        self.pin = hashlib.sha256(original.read_bytes()).hexdigest()
        script_sha = hashlib.sha256(Path(actor.__file__).read_bytes()).hexdigest()
        payload = json.dumps({'read_only': True, 'eligible_for_unlink': False,
                              'script_sha256': script_sha, 'roots': roots}).encode()
        (self.work / 'request.json').write_bytes(payload)
        self.request_sha = hashlib.sha256(payload).hexdigest()

    def tearDown(self):
        self.tmp.cleanup()

    def run_actor(self, changed=False):
        def fixture_child(base, year, name):
            return (self.source if str(base).startswith('/data/') else self.target) / name

        def fake_process(args, stdout=None, **kwargs):
            self.assertIn('--dry-run', args)
            self.assertIn('--checksum-choice=sha1', args)
            return FakeProcess(stdout, changed)

        with patch.object(actor, 'BASE', self.base), patch.object(actor, 'WORK', self.work), \
                patch.object(actor, 'PIN', self.pin), patch.object(actor, 'safe_child', side_effect=fixture_child), \
                patch.object(actor.subprocess, 'Popen', side_effect=fake_process):
            actor.main(self.request_sha)

    def test_seventeen_fake_roots_complete_without_mutation(self):
        self.run_actor()
        result = json.loads((self.work / 'result.json').read_bytes())
        self.assertEqual(result['phase'], 'success')
        self.assertEqual(len(result['roots']), 17)
        self.assertFalse(result['eligible_for_unlink'])
        self.assertTrue(all((self.target / row['name'] / 'x').read_bytes() == b'data' for row in result['roots']))

    def test_difference_stops_and_preserves_partial_log(self):
        with self.assertRaises(AssertionError):
            self.run_actor(changed=True)
        self.assertEqual(json.loads((self.work / 'state.json').read_bytes())['phase'], 'failed_safe')
        self.assertTrue((self.work / 'root-01-sha1.log').stat().st_size)
        self.assertFalse((self.work / 'result.json').exists())
        self.assertEqual((self.target / 'acquisition-00' / 'x').read_bytes(), b'data')


if __name__ == '__main__':
    unittest.main()
