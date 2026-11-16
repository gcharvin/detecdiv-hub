"""Isolated Linux fixtures for read-only partial source verification."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from plan_partial_retirement_manifest import _plan
from verify_partial_retirement_source import _verify, verify_2024_source
from verify_retirement_manifest import native_rows, open_absolute_directory


class PartialSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / 'source'
        (self.root / 'one').mkdir(parents=True)
        (self.root / 'two').mkdir()
        (self.root / 'one' / 'a').write_bytes(b'abc')
        (self.root / 'two' / 'b').write_bytes(b'defg')
        with open_absolute_directory(self.root) as fd:
            rows = list(native_rows(fd))
        data = b''.join((json.dumps(row, sort_keys=True) + '\n').encode() for row in rows)
        self.manifest = self.base / 'manifest'
        self.manifest.write_bytes(data)
        self.expected = (2, 7, hashlib.sha256(data).hexdigest())
        with self.manifest.open('rb') as stream:
            self.subsets = _plan(stream, ['one'], self.expected)

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, subsets=None):
        with open_absolute_directory(self.root) as fd, self.manifest.open('rb') as stream:
            return _verify(fd, stream, self.expected, self.subsets if subsets is None else subsets)

    def test_exact_tree_and_selected_evidence(self):
        result = self.check()
        self.assertFalse(result['eligible_for_unlink'])
        self.assertEqual(result['selected_roots']['one']['bytes'], 3)

    def test_added_file_outside_selected_root_rejected(self):
        (self.root / 'two' / 'unexpected').write_bytes(b'x')
        with self.assertRaises(ValueError):
            self.check()

    def test_changed_selected_file_rejected(self):
        (self.root / 'one' / 'a').write_bytes(b'changed')
        with self.assertRaises(ValueError):
            self.check()

    def test_subset_digest_mismatch_rejected(self):
        subsets = json.loads(json.dumps(self.subsets))
        subsets['one']['manifest_subset_sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            self.check(subsets)

    def test_symlink_ancestor_rejected(self):
        os.rename(self.root / 'one', self.root / 'old')
        os.symlink('old', self.root / 'one')
        with self.assertRaises(ValueError):
            self.check()

    def test_empty_subset_rejected(self):
        with self.assertRaises(ValueError):
            self.check({})

    def test_other_year_rejected_before_open(self):
        with self.manifest.open('rb') as stream:
            with self.assertRaises(ValueError):
                verify_2024_source('2023_1', stream, self.subsets)


if __name__ == '__main__':
    unittest.main()
