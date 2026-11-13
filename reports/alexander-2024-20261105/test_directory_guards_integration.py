"""Exercise separate directory-guard prototypes on temporary fixtures only.

These tests do not implement a production controller or an atomic operation.
"""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from retirement_directory_inventory_v2 import check_unchanged_chain, snapshot
from retirement_directory_transition import capture, validate_one_file_removal
from verify_retirement_manifest import native_rows, open_absolute_directory


class DirectoryGuardIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='alex2024-chain-integration-')
        self.base = Path(self.tmp.name)
        self.root = self.base / 'fixture-data'
        self.parent = self.root / 'a' / 'b'
        self.parent.mkdir(parents=True)
        (self.parent / 'one').write_bytes(b'one')
        (self.parent / 'two').write_bytes(b'two')
        with open_absolute_directory(self.root) as fd:
            rows = list(native_rows(fd))
        data = b''.join((json.dumps(row, sort_keys=True) + '\n').encode() for row in rows)
        self.manifest = self.base / 'manifest.jsonl'
        self.manifest.write_bytes(data)
        expected = (2, 6, hashlib.sha256(data).hexdigest())
        with open_absolute_directory(self.root) as fd, self.manifest.open('rb') as stream:
            self.baseline = snapshot(fd, stream, expected)

    def tearDown(self):
        self.tmp.cleanup()

    def remove_fixture_file(self, name):
        check_unchanged_chain(self.root, 'a/b', self.baseline)
        with open_absolute_directory(self.parent) as fd:
            before = capture(fd)
            os.unlink(name, dir_fd=fd)
            after = capture(fd)
            parent = validate_one_file_removal(before, after, name, before['children'][name])
        row = next(row for row in self.baseline['directories'] if row['path'] == 'a/b')
        row.update(parent)
        encoded = json.dumps(self.baseline['directories'], sort_keys=True, separators=(',', ':')).encode()
        self.baseline['directories_sha256'] = hashlib.sha256(encoded).hexdigest()

    def test_two_same_parent_fixture_removals(self):
        self.remove_fixture_file('one')
        self.remove_fixture_file('two')
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_changed_ancestor_stops_before_first_removal(self):
        os.chmod(self.root / 'a', 0o700)
        with self.assertRaises(ValueError):
            self.remove_fixture_file('one')
        self.assertTrue((self.parent / 'one').exists())

    def test_unexplained_parent_change_stops_before_second_removal(self):
        self.remove_fixture_file('one')
        (self.parent / 'unexpected').write_bytes(b'x')
        with self.assertRaises(ValueError):
            self.remove_fixture_file('two')
        self.assertTrue((self.parent / 'two').exists())


if __name__ == '__main__':
    unittest.main()
