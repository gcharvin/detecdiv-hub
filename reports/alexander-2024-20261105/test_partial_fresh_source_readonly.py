"""Isolated fake-SSH tests of the read-only native-inventory actor."""

import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import partial_fresh_source_readonly as actor


class FakeProcess:
    def __init__(self, payload):
        self.stdout = io.BytesIO(payload)
        self.pid = 12345

    def wait(self, timeout=None):
        return 0

    def poll(self):
        return 0


class FreshSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.work = self.base / 'work'
        self.work.mkdir()
        (self.base / '2024_1').mkdir()
        original = self.base / 'copy_raws.py'
        original.write_text("NATIVE = 'fixture only'\n")
        self.pin = hashlib.sha256(original.read_bytes()).hexdigest()
        rows = [{'kind': 'directory', 'path': '.'}, {'kind': 'directory', 'path': 'one'},
                {'kind': 'file', 'path': 'one/a', 'bytes': 3}]
        lines = [(json.dumps(row, sort_keys=True) + '\n').encode() for row in rows]
        self.data = b''.join(lines)
        (self.base / '2024_1' / 'source-before.jsonl').write_bytes(self.data)
        subset = b''.join(lines[1:])
        plan = {'manifest_sha256': hashlib.sha256(self.data).hexdigest(), 'files': 1, 'bytes': 3,
                'roots': {'one': {'files': 1, 'bytes': 3, 'directories': 1,
                                  'manifest_subset_sha256': hashlib.sha256(subset).hexdigest()}}}
        script_sha = hashlib.sha256(Path(actor.__file__).read_bytes()).hexdigest()
        (self.work / 'request.json').write_text(json.dumps({'read_only': True, 'eligible_for_unlink': False,
                                                            'script_sha256': script_sha,
                                                            'years': {'2024_1': plan}}))
        self.request_sha = hashlib.sha256((self.work / 'request.json').read_bytes()).hexdigest()
        self.verified = {'2024_1': (1, 3, plan['manifest_sha256'])}

    def tearDown(self):
        self.tmp.cleanup()

    def run_actor(self, payload):
        with patch.object(actor, 'BASE', self.base), patch.object(actor, 'WORK', self.work), \
                patch.object(actor, 'PIN', self.pin), \
                patch.object(actor, 'VERIFIED', self.verified), \
                patch.object(actor.subprocess, 'Popen', side_effect=lambda *args, **kwargs: FakeProcess(payload)):
            actor.main(self.request_sha)

    def test_exact_inventory_completes_read_only(self):
        self.run_actor(self.data)
        result = json.loads((self.work / 'result.json').read_text())
        self.assertEqual(result['phase'], 'success')
        self.assertFalse(result['eligible_for_unlink'])

    def test_changed_inventory_stops_and_preserves_mismatch(self):
        with self.assertRaises(ValueError):
            self.run_actor(self.data.replace(b'"bytes": 3', b'"bytes": 4'))
        self.assertEqual(json.loads((self.work / 'state.json').read_text())['phase'], 'failed_safe')
        self.assertTrue((self.work / '2024_1-first-mismatch.json').exists())
        self.assertFalse((self.work / 'result.json').exists())

    def test_truncated_inventory_stops(self):
        with self.assertRaises(ValueError):
            self.run_actor(b'')
        self.assertFalse((self.work / 'result.json').exists())

    def test_retained_lox_is_rejected(self):
        path = self.work / 'request.json'
        request = json.loads(path.read_bytes())
        request['years']['2024_1']['roots'] = {'290424_loxP15': {}}
        path.write_text(json.dumps(request))
        self.request_sha = hashlib.sha256(path.read_bytes()).hexdigest()
        with self.assertRaises(AssertionError):
            self.run_actor(self.data)
        self.assertFalse((self.work / 'result.json').exists())

    def test_request_hash_mismatch_rejected(self):
        self.request_sha = '0' * 64
        with self.assertRaises(AssertionError):
            self.run_actor(self.data)
        self.assertFalse((self.work / 'result.json').exists())


if __name__ == '__main__':
    unittest.main()
