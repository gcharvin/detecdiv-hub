"""Isolated tests for the read-only directory transition prototype."""

import os
from pathlib import Path
import tempfile
import unittest

from retirement_directory_transition import capture, validate_one_file_removal


class TransitionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / 'one').write_bytes(b'one')
        (self.root / 'two').write_bytes(b'two')
        self.fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        self.before = capture(self.fd)
        self.expected = self.before['children']['one']

    def tearDown(self):
        os.close(self.fd)
        self.tmp.cleanup()

    def test_exact_removal(self):
        (self.root / 'one').unlink()
        self.assertEqual(validate_one_file_removal(self.before, capture(self.fd), 'one', self.expected), capture(self.fd)['parent'])

    def test_additional_removal_rejected(self):
        (self.root / 'one').unlink()
        (self.root / 'two').unlink()
        with self.assertRaises(ValueError):
            validate_one_file_removal(self.before, capture(self.fd), 'one', self.expected)

    def test_additional_file_rejected(self):
        (self.root / 'one').unlink()
        (self.root / 'three').write_bytes(b'three')
        with self.assertRaises(ValueError):
            validate_one_file_removal(self.before, capture(self.fd), 'one', self.expected)

    def test_changed_other_file_rejected(self):
        (self.root / 'one').unlink()
        (self.root / 'two').write_bytes(b'changed')
        with self.assertRaises(ValueError):
            validate_one_file_removal(self.before, capture(self.fd), 'one', self.expected)

    def test_wrong_expected_file_rejected(self):
        (self.root / 'one').unlink()
        with self.assertRaises(ValueError):
            validate_one_file_removal(self.before, capture(self.fd), 'one', self.before['children']['two'])

    def test_symlink_rejected(self):
        (self.root / 'one').unlink()
        os.symlink('two', self.root / 'three')
        with self.assertRaises(ValueError):
            capture(self.fd)

    def test_parent_permissions_changed_rejected(self):
        (self.root / 'one').unlink()
        os.chmod(self.root, 0o755)
        with self.assertRaises(ValueError):
            validate_one_file_removal(self.before, capture(self.fd), 'one', self.expected)


if __name__ == '__main__':
    unittest.main()
