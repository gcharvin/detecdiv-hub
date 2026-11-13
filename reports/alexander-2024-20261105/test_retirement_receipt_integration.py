"""Exercise receipt verification against real unlinks in temporary fixtures."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from verify_retirement_manifest import native_rows, open_absolute_directory
from verify_retirement_receipt import _verify
from withdraw_retirement_manifest import withdraw_verified_tree


class ReceiptIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='alex2024-receipt-integration-')
        self.base = Path(self.tmp.name)
        self.root = self.base / 'fixture-data'
        self.root.mkdir()
        (self.root / 'one').write_bytes(b'one')
        (self.root / 'two').write_bytes(b'two')
        self.manifest = self.base / 'manifest.jsonl'
        self.receipt = self.base / 'receipt.jsonl'
        with open_absolute_directory(self.root) as fd:
            rows = list(native_rows(fd))
        data = b''.join((json.dumps(row, sort_keys=True) + '\n').encode() for row in rows)
        self.manifest.write_bytes(data)
        self.expected = (2, 6, hashlib.sha256(data).hexdigest())

    def tearDown(self):
        self.tmp.cleanup()

    def test_completed_fixture_receipt_matches(self):
        with self.manifest.open('rb') as manifest:
            result = withdraw_verified_tree(self.root, manifest, self.expected, self.receipt, lambda: None)
        with self.manifest.open('rb') as manifest, self.receipt.open('rb') as receipt:
            verified = _verify(manifest, receipt, self.expected)
        self.assertEqual(result['files'], verified['files'])
        self.assertEqual(result['available_delta'], verified['available_delta'])
        self.assertEqual(list(self.root.iterdir()), [])

    def test_expired_fixture_admission_remains_incomplete(self):
        calls = 0

        def admission():
            nonlocal calls
            calls += 1
            if calls > 4:
                raise ValueError('fixture admission expired')

        with self.manifest.open('rb') as manifest:
            with self.assertRaises(ValueError):
                withdraw_verified_tree(self.root, manifest, self.expected, self.receipt, admission)
        self.assertEqual(len(list(self.root.iterdir())), 1)
        with self.manifest.open('rb') as manifest, self.receipt.open('rb') as receipt:
            with self.assertRaises(ValueError):
                _verify(manifest, receipt, self.expected)


if __name__ == '__main__':
    unittest.main()
