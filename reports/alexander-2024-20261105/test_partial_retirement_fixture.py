"""Isolated integration tests; fixture code refuses all production paths."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from partial_retirement_fixture import withdraw_fixture
from verify_retirement_manifest import native_rows, open_absolute_directory


class PartialWithdrawalFixtureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='alex2024-partial-fixture-')
        self.base = Path(self.tmp.name)
        self.root = self.base / 'fixture-data'
        (self.root / 'one').mkdir(parents=True)
        (self.root / 'two').mkdir()
        (self.root / 'one' / 'a').write_bytes(b'aaa')
        (self.root / 'one' / 'b').write_bytes(b'bbbb')
        (self.root / 'two' / 'c').write_bytes(b'ccccc')
        with open_absolute_directory(self.root) as fd:
            rows = list(native_rows(fd))
        data = b''.join((json.dumps(row, sort_keys=True) + '\n').encode() for row in rows)
        self.manifest = self.base / 'manifest.jsonl'
        self.manifest.write_bytes(data)
        self.receipt = self.base / 'fixture-receipt.jsonl'
        self.expected = (3, 12, hashlib.sha256(data).hexdigest())

    def tearDown(self):
        self.tmp.cleanup()

    def run_fixture(self, callback=lambda: None):
        with self.manifest.open('rb') as manifest:
            return withdraw_fixture(self.root, manifest, self.expected, ['one'], self.receipt, callback)

    def test_selected_files_removed_and_other_root_preserved(self):
        original = (self.root / 'two' / 'c').stat()
        result = self.run_fixture()
        self.assertEqual((result['files'], result['logical_bytes']), (2, 7))
        self.assertEqual((self.root / 'two' / 'c').read_bytes(), b'ccccc')
        self.assertEqual((self.root / 'two' / 'c').stat().st_ino, original.st_ino)
        self.assertEqual(json.loads(self.receipt.read_text().splitlines()[-1])['event'], 'complete')

    def test_expired_admission_stops_after_first_file(self):
        calls = 0

        def admission():
            nonlocal calls
            calls += 1
            if calls > 3:
                raise ValueError('fixture admission expired')

        with self.assertRaises(ValueError):
            self.run_fixture(admission)
        self.assertEqual(len(list((self.root / 'one').iterdir())), 1)
        self.assertNotEqual(json.loads(self.receipt.read_text().splitlines()[-1])['event'], 'complete')
        self.assertTrue((self.root / 'two' / 'c').exists())

    def test_new_file_between_unlinks_stops(self):
        calls = 0

        def admission():
            nonlocal calls
            calls += 1
            if calls == 4:
                (self.root / 'one' / 'unexpected').write_bytes(b'x')

        with self.assertRaises(ValueError):
            self.run_fixture(admission)
        self.assertTrue((self.root / 'one' / 'b').exists())
        self.assertNotEqual(json.loads(self.receipt.read_text().splitlines()[-1])['event'], 'complete')

    def test_unexpected_file_before_snapshot_stops_without_receipt(self):
        (self.root / 'two' / 'extra').write_bytes(b'x')
        with self.assertRaises(ValueError):
            self.run_fixture()
        self.assertFalse(self.receipt.exists())
        self.assertTrue((self.root / 'one' / 'a').exists())

    def test_non_fixture_root_rejected_before_access(self):
        with self.manifest.open('rb') as manifest:
            with self.assertRaises(ValueError):
                withdraw_fixture(Path('/volume1/DATA/Alexander/data/2024_1'), manifest, self.expected,
                                 ['one'], self.receipt, lambda: None)


if __name__ == '__main__':
    unittest.main()
