"""Fixture tests of terminal SHA1 corroboration; no production paths read."""

from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch
import sys

stub = types.ModuleType('audit')
stub.SSH = 'unused'
stub.remote_json = None
with patch.dict(sys.modules, {'audit': stub}):
    import verify_partial_target_sha1_completion as verifier


class CompletionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='alex2024-completion-fixture-')
        self.base = Path(self.tmp.name)
        self.work = self.base / 'partial-target-sha1-20261116'
        self.work.mkdir()
        self.script = b'fixture actor, never executed\n'
        (self.work / 'partial_target_sha1_readonly.py').write_bytes(self.script)
        (self.work / 'controller.exit').write_text('0\n')
        self.roots = [{'year': '2024_1', 'name': f'fixture-{i:02d}', 'files': 1, 'bytes': 4, 'directories': 1}
                      for i in range(17)]
        self.script_sha = hashlib.sha256(self.script).hexdigest()
        self.source_sha = 'a' * 64
        request = {'read_only': True, 'eligible_for_unlink': False, 'script_sha256': self.script_sha,
                   'fresh_source_proof_sha256': self.source_sha, 'roots': self.roots}
        raw = json.dumps(request).encode()
        (self.work / 'request.json').write_bytes(raw)
        self.request_sha = hashlib.sha256(raw).hexdigest()
        results = []
        for i, root in enumerate(self.roots, 1):
            metadata = {k: root[k] for k in ('files', 'bytes', 'directories')}
            results.append({**root, 'sha1_compare_exit': 0, 'log_bytes': 0,
                            'source_metadata': metadata, 'target_metadata': metadata})
            (self.work / f'root-{i:02d}-sha1.log').write_bytes(b'')
        result = {'phase': 'success', 'read_only': True, 'eligible_for_unlink': False,
                  'finished_at': 'fixture', 'roots': results}
        for name in ('result.json', 'state.json'):
            (self.work / name).write_text(json.dumps(result))

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, active=False, source_sha=None):
        code = verifier.CHECK.replace('/home/charvin-admin/storage-transfers/alexander-2024-20261105', str(self.base))
        args = {'script_sha256': self.script_sha, 'request_sha256': self.request_sha,
                'source_proof_sha256': source_sha or self.source_sha, 'roots': self.roots}

        def fake_run(command, **kwargs):
            if command[0] == 'tmux':
                return types.SimpleNamespace(returncode=0 if active else 1)
            return types.SimpleNamespace(returncode=0, stdout='')

        output = io.StringIO()
        with patch('sys.stdin', io.StringIO(json.dumps(args))), patch('subprocess.run', side_effect=fake_run), \
                redirect_stdout(output):
            exec(code, {})
        return json.loads(output.getvalue())

    def test_successful_terminal_receipts(self):
        self.assertEqual(self.check()['roots'], 17)

    def test_active_actor_rejected(self):
        with self.assertRaises(AssertionError):
            self.check(active=True)

    def test_nonempty_terminal_log_rejected(self):
        (self.work / 'root-01-sha1.log').write_text('difference\n')
        with self.assertRaises(AssertionError):
            self.check()

    def test_wrong_prior_source_proof_rejected(self):
        with self.assertRaises(AssertionError):
            self.check(source_sha='b' * 64)


if __name__ == '__main__':
    unittest.main()
