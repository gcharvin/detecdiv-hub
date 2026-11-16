"""Isolated Linux tests for read-only selected-root SHA1 and metadata checks."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import copy_raws
from partial_target_sha1_readonly import inventory, safe_child


class PartialTargetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='alex2024-target-sha-fixture-')
        self.base = Path(self.tmp.name)
        self.source = self.base / 'source'
        self.target = self.base / 'target'
        self.source.mkdir()
        self.target.mkdir()
        (self.source / 'x').write_bytes(b'hello')
        (self.target / 'x').write_bytes(b'hello')

    def tearDown(self):
        self.tmp.cleanup()

    def compare(self):
        args = copy_raws.rsync(self.source, self.target, 0, verify=True)
        self.assertIn('--dry-run', args)
        self.assertIn('--checksum-choice=sha1', args)
        return subprocess.run(args, capture_output=True, check=False)

    def test_identical_files_produce_no_changes(self):
        result = self.compare()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b'')

    def test_same_size_changed_content_detected_without_writes(self):
        path = self.target / 'x'
        before = path.stat()
        path.write_bytes(b'HELLO')
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        result = self.compare()
        self.assertEqual(result.returncode, 0)
        self.assertTrue(result.stdout)
        self.assertEqual(path.read_bytes(), b'HELLO')

    def test_extra_target_detected_but_not_deleted(self):
        extra = self.target / 'extra'
        extra.write_bytes(b'keep')
        result = self.compare()
        self.assertEqual(result.returncode, 0)
        self.assertTrue(result.stdout)
        self.assertEqual(extra.read_bytes(), b'keep')

    def test_inventory_detects_metadata_change(self):
        before = inventory(self.target)
        (self.target / 'x').write_bytes(b'changed')
        self.assertNotEqual(before, inventory(self.target))

    def test_symlink_is_rejected(self):
        os.symlink('x', self.target / 'link')
        with self.assertRaises(ValueError):
            inventory(self.target)

    def test_retained_and_traversal_roots_rejected(self):
        for root in ('290424_LOXp15', '../x'):
            with self.assertRaises(ValueError):
                safe_child(self.base, '2024_1', root)

    def test_symlink_year_root_rejected(self):
        (self.base / 'actual').mkdir()
        os.symlink('actual', self.base / '2024_1')
        with self.assertRaises(ValueError):
            safe_child(self.base, '2024_1', 'candidate')


if __name__ == '__main__':
    unittest.main()
