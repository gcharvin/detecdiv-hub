"""Publish readable Alexander raw paths while preserving existing consumers."""

import argparse
import base64
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "homes2-access-audit-20261101"))
from activate_windows_mapping import remote
from audit import SSH, remote_json

ROOT = "/homes2/maliavko/DetecdivHub/raw"
UNC = r"\\10.20.8.250\homes\maliavko\DetecdivHub\raw"
TARGETS = {
    "2023_1": ".incoming-alexander-2023-20261008/2023_1",
    "2023_2": "2023_2",
    "2023_3": ".incoming-alexander-2023-20261008/2023_3",
    "2024_1": ".incoming-alexander-2024-20261105/2024_1",
    "2024_2": ".incoming-alexander-2024-20261105/2024_2",
}

NATIVE = r'''
import hashlib, json, os, pwd, stat
from pathlib import Path

root = Path('/volume1/homes/maliavko/DetecdivHub/raw')
assert root.resolve(strict=True) == root
owner = pwd.getpwnam('maliavko').pw_uid
fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)

def checked_directory(path):
    assert path.resolve(strict=True) == path
    info = path.lstat()
    assert stat.S_ISDIR(info.st_mode) and info.st_uid == owner
    return info.st_dev, info.st_ino

def sample(target):
    for directory, dirs, names in os.walk(target):
        dirs[:] = sorted(name for name in dirs if not name.startswith(('.', '@')))
        for name in sorted(names):
            path = Path(directory) / name
            info = path.lstat()
            if (name.startswith(('.', '@')) or not stat.S_ISREG(info.st_mode)
                    or not 0 < info.st_size <= 32 * 1024 * 1024):
                continue
            assert path.resolve(strict=True) == path and info.st_uid == owner
            data = path.read_bytes()
            assert len(data) == info.st_size and path.stat().st_mtime_ns == info.st_mtime_ns
            return dict(path=path.relative_to(target).as_posix(), bytes=len(data),
                        sha256=hashlib.sha256(data).hexdigest())
    raise RuntimeError('No suitable payload sample')

before = {}
samples = {}
created = []
try:
    for year, relative in targets.items():
        target = root / relative
        before[year] = checked_directory(target)
        if year != relative:
            assert not os.path.lexists(root / year), 'Readable name already exists'
        samples[year] = sample(target)
    if publish:
        for year, relative in targets.items():
            if year != relative:
                os.symlink(relative, year, dir_fd=fd, target_is_directory=True)
                inode = os.stat(year, dir_fd=fd, follow_symlinks=False).st_ino
                created.append((year, relative, inode))
        for year, relative in targets.items():
            assert checked_directory(root / relative) == before[year]
            assert (root / year).resolve(strict=True) == root / relative
            assert os.path.samefile(root / year / samples[year]['path'],
                                    root / relative / samples[year]['path'])
except BaseException:
    for year, relative, inode in reversed(created):
        current = os.stat(year, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISLNK(current.st_mode) and current.st_ino == inode and os.readlink(year, dir_fd=fd) == relative:
            os.unlink(year, dir_fd=fd)
    raise
finally:
    os.close(fd)
print(json.dumps({'success': True, 'created_aliases': [x[0] for x in created],
                  'original_paths': targets, 'samples': samples}))
'''

VERIFY = r'''
import hashlib, json
from pathlib import Path

def check(root, receipt):
    for year, sample in receipt['samples'].items():
        for folder in {year, receipt['original_paths'][year]}:
            with (root / folder / sample['path']).open('rb') as stream:
                data = stream.read(sample['bytes'] + 1)
            assert len(data) == sample['bytes']
            assert hashlib.sha256(data).hexdigest() == sample['sha256']
'''


def native(publish):
    script = f"targets={TARGETS!r}\npublish={publish!r}\n" + NATIVE
    encoded = base64.b64encode(script.encode()).decode()
    code = f'''
import json, shlex, subprocess
loader = "import base64;exec(base64.b64decode({encoded!r}))"
command = ['ssh', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'ConnectTimeout=10',
           '-i', '/home/charvin-admin/.ssh/detecdiv_backup_ed25519', 'charvin-admin@10.20.8.250',
           shlex.join(['python3', '-u', '-c', loader])]
result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=90)
print(result.stdout.strip().splitlines()[-1])
'''
    return remote("linux", code, timeout=100)


def write(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("preflight", "publish", "verify"))
    operation = parser.parse_args().operation
    private = HERE / "runtime/readable-raw-paths-publication.json"
    public = HERE / "readable-raw-paths-verification.json"
    if operation == "preflight":
        result = native(False)
        print(json.dumps({"success": result["success"], "paths": result["original_paths"],
                          "payload_samples": len(result["samples"])}))
        return
    if operation == "publish":
        assert not private.exists() and not public.exists()
        for number in (1, 2):
            proof = json.loads((HERE / f"raw-year{number}-completion-verification.json").read_bytes())
            assert proof["success"] and proof["full_sha1_passed"] and proof["source_stability_verified"]
        receipt = native(True)
        write(private, receipt)
        print(json.dumps({"created_aliases": receipt["created_aliases"]}))
        return
    receipt = json.loads(private.read_bytes())
    assert receipt["success"] and receipt["original_paths"] == TARGETS
    assert set(receipt["created_aliases"]) == {year for year, path in TARGETS.items() if year != path}
    code = VERIFY + f"\ncheck(Path({ROOT!r}), {receipt!r})\nprint(json.dumps({{'success': True}}))"
    assert remote("linux", code, timeout=90)["success"]
    assert remote_json(SSH, "webserver-labo", code, {}, container=True)["success"]
    exec(VERIFY + "\ncheck(Path(root), receipt)", {"root": UNC, "receipt": receipt})
    result = {"success": True, "readable_paths": sorted(TARGETS),
              "created_aliases": receipt["created_aliases"], "samples_per_host": len(TARGETS),
              "original_and_readable_payloads_match": ["Linux", "API container", "Windows UNC"],
              "original_paths_retained": True, "catalog_changed": False, "source_deleted": False}
    write(public, result)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
