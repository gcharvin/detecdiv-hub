"""Isolated tests for partial manifest planning; no source deletion."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from plan_partial_retirement_manifest import _plan, plan


class PartialManifestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'manifest'
        rows = [{'kind': 'directory', 'path': '.'},
                {'kind': 'directory', 'path': 'one'},
                {'kind': 'file', 'path': 'one/a', 'bytes': 3},
                {'kind': 'directory', 'path': 'two'},
                {'kind': 'file', 'path': 'two/b', 'bytes': 4}]
        data = b''.join((json.dumps(x) + '\n').encode() for x in rows)
        self.path.write_bytes(data)
        self.expected = (2, 7, hashlib.sha256(data).hexdigest())

    def tearDown(self):
        self.tmp.cleanup()

    def call(self, names, expected=None):
        with self.path.open('rb') as stream:
            return _plan(stream, names, expected or self.expected)

    def test_selected_root_only(self):
        result = self.call(['one'])
        self.assertEqual(set(result), {'one'})
        self.assertEqual((result['one']['files'], result['one']['bytes'], result['one']['directories']), (1, 3, 1))

    def test_multiple_roots(self):
        self.assertEqual(set(self.call(['one', 'two'])), {'one', 'two'})

    def test_retained_lox_rejected(self):
        with self.assertRaises(ValueError):
            self.call(['290424_LOXp15'])

    def test_uncertain_stub_rejected(self):
        with self.assertRaises(ValueError):
            self.call(['130224_dfob_rep_dslog'])

    def test_traversal_rejected(self):
        with self.assertRaises(ValueError):
            self.call(['../one'])

    def test_unknown_root_rejected(self):
        with self.assertRaises(ValueError):
            self.call(['unknown'])

    def test_wrong_full_digest_rejected(self):
        with self.assertRaises(ValueError):
            self.call(['one'], (2, 7, '0' * 64))

    def test_other_year_rejected(self):
        with self.path.open('rb') as stream:
            with self.assertRaises(ValueError):
                plan('2023_1', stream, ['one'])


if __name__ == '__main__':
    unittest.main()
