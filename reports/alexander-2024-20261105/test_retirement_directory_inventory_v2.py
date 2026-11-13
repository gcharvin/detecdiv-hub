"""Isolated Linux fixtures for the read-only directory baseline."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import retirement_directory_inventory_v2 as inventory
from retirement_directory_inventory_v2 import check_unchanged_chain, snapshot
from verify_retirement_manifest import native_rows, open_absolute_directory


class DirectoryInventoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / 'data'
        (self.root / 'a' / 'b').mkdir(parents=True)
        (self.root / 'a' / 'b' / 'data.bin').write_bytes(b'data')
        with open_absolute_directory(self.root) as fd:
            rows = list(native_rows(fd))
        data = b''.join((json.dumps(row, sort_keys=True) + '\n').encode() for row in rows)
        self.manifest = self.base / 'manifest'
        self.manifest.write_bytes(data)
        self.expected = (1, 4, hashlib.sha256(data).hexdigest())

    def tearDown(self):
        self.tmp.cleanup()

    def capture(self):
        with open_absolute_directory(self.root) as fd, self.manifest.open('rb') as stream:
            return snapshot(fd, stream, self.expected)

    def test_valid_unchanged_chain(self):
        check_unchanged_chain(self.root, 'a/b', self.capture())

    def test_directory_metadata_change_rejected(self):
        baseline = self.capture()
        os.chmod(self.root / 'a', 0o700)
        with self.assertRaises(ValueError):
            check_unchanged_chain(self.root, 'a/b', baseline)

    def test_file_added_after_capture_rejected(self):
        baseline = self.capture()
        (self.root / 'a' / 'b' / 'unexpected').write_bytes(b'x')
        with self.assertRaises(ValueError):
            check_unchanged_chain(self.root, 'a/b', baseline)

    def test_symlink_ancestor_rejected(self):
        baseline = self.capture()
        os.rename(self.root / 'a' / 'b', self.root / 'a' / 'old')
        os.symlink('old', self.root / 'a' / 'b')
        with self.assertRaises((ValueError, OSError)):
            check_unchanged_chain(self.root, 'a/b', baseline)

    def test_tampered_baseline_rejected(self):
        baseline = self.capture()
        baseline['directories'][0]['ino'] += 1
        with self.assertRaises(ValueError):
            check_unchanged_chain(self.root, 'a/b', baseline)

    def test_changed_payload_rejected_before_inventory(self):
        (self.root / 'a' / 'b' / 'data.bin').write_bytes(b'changed')
        with self.assertRaises(ValueError):
            self.capture()

    def test_file_added_after_first_manifest_check_rejected(self):
        original = inventory.verify
        calls = 0

        def mutate_after_first_check(*args):
            nonlocal calls
            result = original(*args)
            calls += 1
            if calls == 1:
                (self.root / 'a' / 'b' / 'unexpected').write_bytes(b'x')
            return result

        with patch.object(inventory, 'verify', side_effect=mutate_after_first_check):
            with self.assertRaises(ValueError):
                self.capture()

    def test_directory_metadata_change_after_second_check_rejected(self):
        original = inventory.verify
        calls = 0

        def mutate_after_second_check(*args):
            nonlocal calls
            result = original(*args)
            calls += 1
            if calls == 2:
                os.chmod(self.root / 'a', 0o700)
            return result

        with patch.object(inventory, 'verify', side_effect=mutate_after_second_check):
            with self.assertRaises(ValueError):
                self.capture()

    def test_parent_traversal_rejected(self):
        with self.assertRaises(ValueError):
            check_unchanged_chain(self.root, '../a', self.capture())


if __name__ == '__main__':
    unittest.main()
