"""Isolated tests of ordered per-file retirement receipt verification."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from verify_retirement_receipt import _verify, verify_2024_receipt


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.manifest = self.root / 'manifest.jsonl'
        self.receipt = self.root / 'receipt.jsonl'
        self.files = [{'kind': 'file', 'path': 'a', 'bytes': 2},
                      {'kind': 'file', 'path': 'b', 'bytes': 3}]
        payload = b''.join((json.dumps(row) + '\n').encode() for row in [{'kind': 'directory', 'path': '.'}, *self.files])
        self.manifest.write_bytes(payload)
        self.expected = (2, 5, hashlib.sha256(payload).hexdigest())
        self.events = [{'event': 'start', 'files': 2, 'logical_bytes': 5,
                        'manifest_sha256': self.expected[2], 'available_before': 100}]
        for row in self.files:
            self.events.extend([{'event': 'intent', 'file': row}, {'event': 'unlinked', 'file': row}])
        self.events.append({'event': 'complete', 'files': 2, 'logical_bytes': 5,
                            'available_before': 100, 'available_after': 103, 'available_delta': 3})

    def tearDown(self):
        self.tmp.cleanup()

    def check(self):
        self.receipt.write_bytes(b''.join((json.dumps(row) + '\n').encode() for row in self.events))
        with self.manifest.open('rb') as manifest, self.receipt.open('rb') as receipt:
            return _verify(manifest, receipt, self.expected)

    def test_valid_complete(self):
        self.assertEqual(self.check()['available_delta'], 3)

    def test_missing_unlinked_rejected(self):
        del self.events[2]
        with self.assertRaises(ValueError):
            self.check()

    def test_duplicate_record_rejected(self):
        self.events.insert(3, self.events[2])
        with self.assertRaises(ValueError):
            self.check()

    def test_wrong_file_rejected(self):
        self.events[2]['file'] = self.files[1]
        with self.assertRaises(ValueError):
            self.check()

    def test_extra_record_rejected(self):
        self.events.append({'event': 'unexpected'})
        with self.assertRaises(ValueError):
            self.check()

    def test_wrong_measured_delta_rejected(self):
        self.events[-1]['available_delta'] = 5
        with self.assertRaises(ValueError):
            self.check()

    def test_wrong_manifest_digest_rejected(self):
        self.manifest.write_bytes(self.manifest.read_bytes() + b' ')
        with self.assertRaises(ValueError):
            self.check()

    def test_other_year_rejected(self):
        with self.manifest.open('rb') as manifest, self.receipt.open('wb+') as receipt:
            with self.assertRaises(ValueError):
                verify_2024_receipt('2023_1', manifest, receipt)


if __name__ == '__main__':
    unittest.main()
