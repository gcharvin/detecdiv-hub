from pathlib import Path
import pytest

from scripts.ops.alexander_2023_transfer import require_mounts, rsync_command, validate_manifest


MOUNTS = """10 1 0:1 / /data rw - cifs //10.20.11.250/DATA rw,username=Fred
11 1 0:2 / /homes2/maliavko rw - cifs //10.20.8.250/home rw,username=maliavko
"""


def test_expected_per_user_mounts():
    require_mounts(MOUNTS)


@pytest.mark.parametrize('text', [MOUNTS.replace('username=maliavko','username=Fred'),
    MOUNTS.replace('/homes2/maliavko rw','/homes2/maliavko ro'),
    MOUNTS.replace('//10.20.8.250/home','//10.20.8.250/homes'),
    MOUNTS+'12 1 0:3 / /homes2/maliavko/DetecdivHub rw - tmpfs tmpfs rw\n',
    MOUNTS+'12 1 0:3 / /data/Alexander/data/2023_1 rw - tmpfs tmpfs rw\n'])
def test_wrong_or_shadowed_mount_is_refused(text):
    with pytest.raises(AssertionError):
        require_mounts(text)


def test_copy_never_deletes_or_moves_source():
    command=rsync_command(Path('/source'),Path('/target'),61440,verify=False)
    assert '--delete' not in command and '--remove-source-files' not in command
    assert '--inplace' not in command and '--links' not in command
    assert '--bwlimit=61440' in command
    assert command[-3:]==['--','/source/','/target/']


def test_verify_compares_all_content_without_writing():
    command=rsync_command(Path('/source'),Path('/target'),61440,verify=True)
    assert '--checksum' in command and '--checksum-choice=sha1' in command
    assert '--dry-run' in command and '--delete' not in command


def test_scope_and_safety_flags_are_pinned():
    manifest={'schema_version':1,'batch_name':'alexander-2023-20261008',
      'years':['2023_1','2023_2','2023_3'],'source_root':'/data/Alexander/data',
      'destination_root':'/homes2/maliavko/DetecdivHub/raw','source_deletion_allowed':False,
      'database_cutover_allowed':False,'bandwidth_kib':61440,'minimum_free_bytes':2_000_000_000_000}
    validate_manifest(manifest)
    for field,value in [('years',['2024_1']),('source_deletion_allowed',True),('database_cutover_allowed',True)]:
        with pytest.raises(AssertionError):
            validate_manifest({**manifest,field:value})
