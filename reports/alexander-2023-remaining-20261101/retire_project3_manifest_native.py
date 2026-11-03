"""Native NAS preflight and exact unlink for the pinned 2023_3 manifest."""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys


ROOT = Path("/volume1/DATA/Alexander/data/2023_3")
MANIFEST_SHA = "4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3"
FILES = 708590
BYTES = 5967100516555
SYSTEM_DIRS = {"@eaDir", "#recycle", "#snapshot"}
DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def open_directory(parent_fd, parts):
    fd = os.dup(parent_fd)
    try:
        for part in parts:
            child = os.open(part, DIR_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def check_file(info, row):
    if not (stat.S_ISREG(info.st_mode) and info.st_nlink == 1
            and info.st_size == row["bytes"] and info.st_mtime_ns == row["mtime_ns"]):
        raise ValueError("Source file changed: " + row["path"])


def scan(root_fd, expected):
    def fail(error):
        raise error

    observed = set()
    for parent, dirs, files, fd in os.fwalk(".", dir_fd=root_fd, onerror=fail):
        for name in dirs:
            if not stat.S_ISDIR(os.stat(name, dir_fd=fd, follow_symlinks=False).st_mode):
                raise ValueError("Unexpected directory entry: " + str(Path(parent) / name))
        dirs[:] = [name for name in dirs if name not in SYSTEM_DIRS]
        for name in files:
            relative = str(Path(parent) / name)
            if relative not in expected:
                raise ValueError("Unexpected source file: " + relative)
            check_file(os.stat(name, dir_fd=fd, follow_symlinks=False), expected[relative])
            observed.add(relative)
    if observed != expected.keys():
        raise ValueError("Source inventory differs from manifest")


def retire(root, rows, apply, emit):
    expected = {}
    for row in rows:
        path = row["path"]
        if any(part in {"", ".", ".."} for part in path.split("/")) or path in expected:
            raise ValueError("Invalid or repeated manifest path: " + path)
        expected[path] = row
    slash = os.open("/", DIR_FLAGS)
    try:
        root_fd = open_directory(slash, root.parts[1:])
    finally:
        os.close(slash)
    try:
        scan(root_fd, expected)
        disk = os.fstatvfs(root_fd)
        before = disk.f_bavail * disk.f_frsize
        emit(dict(event="preflight_passed", files=len(expected), free_bytes_before=before))
        if not apply:
            return
        count = size = 0
        for relative, row in expected.items():
            path = Path(relative)
            fd = open_directory(root_fd, path.parent.parts)
            try:
                check_file(os.stat(path.name, dir_fd=fd, follow_symlinks=False), row)
                os.unlink(path.name, dir_fd=fd)
            finally:
                os.close(fd)
            count += 1
            size += row["bytes"]
            emit(dict(event="unlinked", path=relative, bytes=row["bytes"]))
        scan(root_fd, {})
        os.sync()
        disk = os.fstatvfs(root_fd)
        after = disk.f_bavail * disk.f_frsize
        emit(dict(event="complete", files_unlinked=count, logical_bytes_unlinked=size,
                  free_bytes_before=before, free_bytes_after=after,
                  measured_free_bytes_increase=after - before))
    finally:
        os.close(root_fd)


def main():
    request = json.loads(sys.stdin.buffer.readline())
    if request["source_root"] != str(ROOT) or request["mode"] not in {"preflight", "unlink"}:
        raise ValueError("Unexpected retirement scope")
    apply = request["mode"] == "unlink"
    if apply and request.get("verified_cutover") is not True:
        raise ValueError("Verified cutover receipt required")
    manifest = sys.stdin.buffer.read()
    if hashlib.sha256(manifest).hexdigest() != MANIFEST_SHA:
        raise ValueError("Manifest digest mismatch")
    rows = [row for line in manifest.splitlines() if (row := json.loads(line))["kind"] == "file"]
    if len(rows) != FILES or sum(row["bytes"] for row in rows) != BYTES:
        raise ValueError("Manifest totals mismatch")
    retire(ROOT, rows, apply, lambda event: print(json.dumps(event)))


if __name__ == "__main__":
    main()
