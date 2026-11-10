"""Isolated Linux fixtures for both actual year2 stage suffixes and rename guards."""
import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('year2_publication_under_test', HERE / 'publish_year2_folders.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def run():
    passed = []
    ast.parse(module.TARGET_GUARD)
    names = ['anof_050624_YPDtitr_plate_3k', 'qp_250724_dbud4stest', '281124_Alex']
    with tempfile.TemporaryDirectory(prefix='year2-rename-fixtures-', dir=HERE) as temporary:
        temporary = Path(temporary)
        for stem in names:
            suffix = '20261106' if stem in module.SPECIAL else '20261105'
            for kind in ['success', 'directory', 'file', 'broken_symlink', 'source_symlink', 'source_file', 'wrong_suffix']:
                root = temporary / (stem + '-' + kind)
                root.mkdir()
                source = root / ('.incoming-' + stem + '-' + suffix)
                destination = root / stem
                if kind == 'source_file':
                    source.write_bytes(b'not-a-directory')
                elif kind == 'source_symlink':
                    outside = root / 'outside'
                    outside.mkdir()
                    (outside / 'payload').write_bytes(b'fixture-science')
                    source.symlink_to(outside, target_is_directory=True)
                else:
                    source.mkdir()
                    (source / 'payload').write_bytes(b'fixture-science')
                if kind == 'directory':
                    destination.mkdir()
                    (destination / 'sentinel').write_bytes(b'destination')
                elif kind == 'file':
                    destination.write_bytes(b'destination')
                elif kind == 'broken_symlink':
                    destination.symlink_to(root / 'never-created')
                fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    supplied = source.name
                    if kind == 'wrong_suffix':
                        supplied = '.incoming-' + stem + '-' + ('20261105' if suffix == '20261106' else '20261106')
                    if kind == 'success':
                        before = (source / 'payload').stat().st_ino
                        receipt = module.rename_directory_noreplace(fd, supplied, stem)
                        assert receipt['no_overwrite'] and receipt['directory_no_follow'] and receipt['stage_suffix'] == suffix
                        assert not source.exists() and (destination / 'payload').read_bytes() == b'fixture-science'
                        assert (destination / 'payload').stat().st_ino == before
                    else:
                        try:
                            module.rename_directory_noreplace(fd, supplied, stem)
                        except (OSError, ValueError):
                            pass
                        else:
                            raise AssertionError('Unsafe rename accepted: ' + kind)
                        assert os.path.lexists(source)
                        if kind == 'source_file':
                            assert source.read_bytes() == b'not-a-directory'
                        else:
                            assert (source / 'payload').read_bytes() == b'fixture-science'
                        if kind == 'directory':
                            assert (destination / 'sentinel').read_bytes() == b'destination'
                        elif kind == 'file':
                            assert destination.read_bytes() == b'destination'
                        elif kind == 'broken_symlink':
                            assert destination.is_symlink()
                        else:
                            assert not os.path.lexists(destination)
                    passed.append(stem + ':' + kind)
                    for bad_source, bad_destination in [('../escape', stem), (source.name, '../escape'), (source.name, 'bad/name')]:
                        try:
                            module.rename_directory_noreplace(fd, bad_source, bad_destination)
                        except ValueError:
                            pass
                        else:
                            raise AssertionError('Traversal accepted')
                finally:
                    os.close(fd)
    assert len(passed) == 21
    result = {'success': True, 'tests': passed, 'traversal_rejections': 63,
              'publisher_sha256': hashlib.sha256((HERE / 'publish_year2_folders.py').read_bytes()).hexdigest(),
              'production_paths_used': False, 'publication_started': False, 'source_deleted': False,
              'raw_year2_integrity_and_fresh_own_lease_gates_still_required': True}
    (HERE / 'fixture-result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    run()
