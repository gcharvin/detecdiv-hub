"""Interactive storage setup on detecdiv-server; does not control workers.

Run with sudo from an interactive SSH terminal. DSM credentials stay in a
root-only file on the compute host. This script copies no scientific data.
"""
from __future__ import annotations

import getpass
import json
import os
from pathlib import Path
import pwd
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from uuid import uuid4


NAS = "10.20.8.250"
DSM_USER = "maliavko"
WORKER_USER = "charvin-admin"
HOME = Path("/homes2/maliavko")
SOURCE = Path("/mnt/detecdiv-secondary-sauvegarde")
CREDENTIAL = Path("/etc/detecdiv-hub/smb/secondary-maliavko.credentials")


def run(args: list[str], *, capture: bool = False) -> str:
    result = subprocess.run(args, check=True, text=True, capture_output=capture)
    return result.stdout.strip() if capture else ""


def mount_info(path: Path) -> dict:
    payload = json.loads(run(["findmnt", "--json", "-o", "SOURCE,TARGET,FSTYPE,OPTIONS", "-T", str(path)], capture=True))
    return payload["filesystems"][-1]


def require_mount(path: Path, source: str, username: str, read_only: bool) -> None:
    info = mount_info(path)
    options = set(info["options"].split(","))
    if (info["target"] != str(path) or info["source"].rstrip("/") != source
            or info["fstype"] != "cifs" or f"username={username}" not in options
            or ("ro" if read_only else "rw") not in options):
        raise RuntimeError(f"Unexpected mount at {path}: {info}")


def atomic_write(path: Path, content: str, mode: int) -> None:
    if path.is_symlink():
        raise RuntimeError(f"Refusing symlink: {path}")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def as_worker(code: str, *args: str) -> str:
    return run(["runuser", "-u", WORKER_USER, "--", "python3", "-c", code, *args], capture=True)


def main() -> None:
    if os.geteuid() != 0:
        raise SystemExit("Run this helper with sudo in an interactive terminal.")
    if run(["hostname", "-s"], capture=True) != "GC-CALCUL-306":
        raise SystemExit("This helper is for detecdiv-server / GC-CALCUL-306 only.")
    require_mount(Path("/homes2"), f"//{NAS}/homes", "Fred", True)
    if HOME.is_symlink() or SOURCE.is_symlink():
        raise RuntimeError("Refusing a symlink as a storage mountpoint.")
    if not HOME.is_dir():
        raise RuntimeError("The existing maliavko home must be visible under /homes2.")
    worker = pwd.getpwnam(WORKER_USER)
    common = (f"vers=3.0,uid={worker.pw_uid},gid={worker.pw_gid},"
              "file_mode=0600,dir_mode=0700,nobrl,nosuid,nodev,_netdev,nofail,x-systemd.automount")
    home_options = f"rw,credentials={CREDENTIAL},{common}"
    source_options = f"ro,credentials=/home/fred/.smbcredentials_sv,{common}"
    entries = {
        str(HOME): f"//{NAS}/home {HOME} cifs {home_options} 0 0",
        str(SOURCE): f"//{NAS}/Sauvegarde {SOURCE} cifs {source_options} 0 0",
    }
    fstab = Path("/etc/fstab")
    original = fstab.read_text(encoding="utf-8")
    existing = {}
    for line in original.splitlines():
        fields = line.split()
        if fields and not fields[0].startswith("#") and len(fields) >= 2:
            if fields[1] in entries:
                if fields[1] in existing or line.strip() != entries[fields[1]]:
                    raise RuntimeError(f"Existing fstab entry needs review: {fields[1]}")
                existing[fields[1]] = line.strip()
    needs_home_mount = mount_info(HOME)["target"] != str(HOME)
    if not needs_home_mount:
        require_mount(HOME, f"//{NAS}/home", DSM_USER, False)
    if needs_home_mount or not CREDENTIAL.is_file():
        for directory in (CREDENTIAL.parent.parent, CREDENTIAL.parent):
            if directory.is_symlink():
                raise RuntimeError(f"Refusing credential directory symlink: {directory}")
            directory.mkdir(mode=0o700, exist_ok=True)
            directory.chmod(0o700)
        password = getpass.getpass("Mot de passe DSM de maliavko sur 10.20.8.250 : ")
        if not password or "\n" in password or "\r" in password:
            raise RuntimeError("Empty or multiline password; setup cancelled.")
        atomic_write(CREDENTIAL, f"username={DSM_USER}\npassword={password}\n", 0o600)
        del password
    if needs_home_mount:
        run(["mount", "-t", "cifs", f"//{NAS}/home", str(HOME), "-o", home_options])
    require_mount(HOME, f"//{NAS}/home", DSM_USER, False)
    probe = HOME / f".detecdiv-home-probe-{uuid4().hex}"
    as_worker("import os,sys; fd=os.open(sys.argv[1],os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600); os.write(fd,b'detecdiv-probe\\n'); os.close(fd)", str(probe))
    try:
        owner = run(["runuser", "-u", WORKER_USER, "--", "ssh", "-o", "BatchMode=yes",
                     "-o", "ConnectTimeout=10", "-o", "IdentitiesOnly=yes", "-i",
                     "/home/charvin-admin/.ssh/detecdiv_backup_ed25519", f"charvin-admin@{NAS}",
                     f"stat -c %U /volume1/homes/{DSM_USER}/{probe.name}"], capture=True)
        if owner != DSM_USER:
            raise RuntimeError(f"NAS-side probe owner is {owner}, expected {DSM_USER}.")
    finally:
        as_worker("from pathlib import Path; import sys; Path(sys.argv[1]).unlink()", str(probe))
    as_worker("from pathlib import Path; import sys; p=Path(sys.argv[1]); p.mkdir(mode=0o700,exist_ok=True); [(p/n).mkdir(mode=0o700,exist_ok=True) for n in ('projects','raw')]; print(p)", str(HOME / "DetecdivHub"))
    SOURCE.mkdir(mode=0o700, parents=True, exist_ok=True)
    if mount_info(SOURCE)["target"] != str(SOURCE):
        run(["mount", "-t", "cifs", f"//{NAS}/Sauvegarde", str(SOURCE), "-o", source_options])
    require_mount(SOURCE, f"//{NAS}/Sauvegarde", "Fred", True)
    additions = [line for target, line in entries.items() if target not in existing]
    backup = None
    if additions:
        if fstab.read_text(encoding="utf-8") != original:
            raise RuntimeError("fstab changed during setup; refusing to overwrite concurrent changes.")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup = Path(f"/etc/fstab.detecdiv-homes2-{stamp}.bak")
        shutil.copy2(fstab, backup)
        atomic_write(fstab, original.rstrip("\n") + "\n\n# DetecDiv: Alexander home and read-only project inventory source\n" + "\n".join(additions) + "\n", fstab.stat().st_mode & 0o777)
        run(["systemctl", "daemon-reload"])
    print(json.dumps({"home_mount": str(HOME), "smb_identity": DSM_USER,
                      "nas_probe_owner": owner, "source_mount": str(SOURCE),
                      "source_read_only": True, "fstab_backup": str(backup) if backup else None,
                      "data_copied": False, "hub_provider_activated": False}, indent=2))


if __name__ == "__main__":
    main()
