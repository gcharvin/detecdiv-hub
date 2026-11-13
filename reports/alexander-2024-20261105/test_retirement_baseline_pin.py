"""Isolated fixtures for external baseline pin validation."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from retirement_baseline_pin import load_2024_baseline
from verify_retirement_manifest import SOURCE, VERIFIED


class BaselinePinTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.path = self.root / 'baseline.json'
        self.year = '2024_1'
        size = VERIFIED[self.year]
        row = {'path': '.', **{k: 1 for k in ('dev', 'ino', 'mode', 'uid', 'gid', 'nlink', 'size', 'mtime_ns', 'ctime_ns')}}
        rows = [row]
        digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.envelope = {'year': self.year, 'source': str(SOURCE / self.year),
                         'baseline': {'manifest': {'files': size[0], 'logical_bytes': size[1],
                                                   'manifest_sha256': size[2]},
                                      'directories': rows, 'directories_sha256': digest}}
        self.write()

    def tearDown(self):
        self.tmp.cleanup()

    def write(self):
        data = json.dumps(self.envelope).encode()
        self.path.write_bytes(data)
        self.pin = hashlib.sha256(data).hexdigest()

    def test_valid_external_pin(self):
        self.assertEqual(load_2024_baseline(self.path, self.year, self.pin), self.envelope['baseline'])

    def test_wrong_pin_rejected(self):
        with self.assertRaises(ValueError):
            load_2024_baseline(self.path, self.year, '0' * 64)

    def test_wrong_year_rejected(self):
        with self.assertRaises(ValueError):
            load_2024_baseline(self.path, '2024_2', self.pin)

    def test_wrong_source_rejected(self):
        self.envelope['source'] = '/elsewhere'
        self.write()
        with self.assertRaises(ValueError):
            load_2024_baseline(self.path, self.year, self.pin)

    def test_changed_manifest_evidence_rejected(self):
        self.envelope['baseline']['manifest']['files'] += 1
        self.write()
        with self.assertRaises(ValueError):
            load_2024_baseline(self.path, self.year, self.pin)

    def test_changed_directory_digest_rejected(self):
        self.envelope['baseline']['directories'][0]['ino'] += 1
        self.write()
        with self.assertRaises(ValueError):
            load_2024_baseline(self.path, self.year, self.pin)

    def test_symlink_baseline_rejected(self):
        alias = self.root / 'alias.json'
        os.symlink(self.path.name, alias)
        with self.assertRaises(OSError):
            load_2024_baseline(alias, self.year, self.pin)

    def test_other_year_rejected(self):
        with self.assertRaises(ValueError):
            load_2024_baseline(self.path, '2023_1', self.pin)


if __name__ == '__main__':
    unittest.main()
