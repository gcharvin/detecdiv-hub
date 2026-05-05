from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from sqlalchemy import func, select

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.db import SessionLocal
from api.models import User


DEFAULT_USER_PASSWORD = "detecdiv"
FIRST_NAME_PATTERN = re.compile(r"^[A-Z][A-Za-z']{1,31}$")
NON_NAME_DIRS = {
    ".trash-1000",
    ".trash-1002",
    "alumni",
    "charvin",
    "common_labo4",
    "detecdiv-archives",
    "groupmeetings",
    "hardware",
    "matlab",
    "micromanager",
    "microscope",
    "old-labo4",
    "softwares",
    "webserver-labo",
    "website",
}

ALUMNI_USER_KEYS = {
    "aizea",
    "andrei",
    "audrey",
    "baptiste",
    "guillaume",
    "pierre",
    "theo",
    "tony",
}


def is_first_name_folder(path: Path) -> bool:
    name = path.name.strip()
    lowered = name.lower()
    if not name or name.startswith("."):
        return False
    if lowered in NON_NAME_DIRS:
        return False
    if any(char.isdigit() for char in name):
        return False
    if any(separator in name for separator in ("_", "-", " ", ".", "/")):
        return False
    return bool(FIRST_NAME_PATTERN.fullmatch(name))


def normalize_user_key(display_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", display_name.lower())


def iter_candidate_dirs(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Root directory does not exist: {root}")
    return sorted((entry for entry in root.iterdir() if entry.is_dir()), key=lambda entry: entry.name.lower())


def main() -> int:
    parser = argparse.ArgumentParser(description="Provision users from top-level /data folders.")
    parser.add_argument("--root", default="/data", help="Root directory to scan (default: /data)")
    parser.add_argument("--apply", action="store_true", help="Persist changes to the database")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    candidates = [path for path in iter_candidate_dirs(root) if is_first_name_folder(path)]

    created: list[str] = []
    updated: list[str] = []
    skipped: list[str] = []

    with SessionLocal() as session:
        for folder in candidates:
            display_name = folder.name
            user_key = normalize_user_key(display_name)
            if not user_key:
                skipped.append(display_name)
                continue

            existing = session.scalars(
                select(User).where(func.lower(User.user_key) == user_key.lower())
            ).first()

            if existing is None:
                existing = session.scalars(
                    select(User).where(func.lower(User.display_name) == display_name.lower())
                ).first()

            if existing is None:
                created.append(f"{user_key} -> {folder}")
                if args.apply:
                    session.add(
                        User(
                            user_key=user_key,
                            display_name=display_name,
                            role="user",
                            is_active=True,
                            admin_portal_access=False,
                            lab_status="alumni" if user_key in ALUMNI_USER_KEYS else "yes",
                            default_path=str(folder),
                            metadata_json={},
                        )
                    )
                    session.flush()
                    user = session.scalars(select(User).where(User.user_key == user_key)).first()
                    if user is not None:
                        from api.services.auth import set_user_password

                        set_user_password(session, user=user, password=DEFAULT_USER_PASSWORD)
                continue

            changes = []
            if not existing.default_path:
                changes.append("default_path")
                if args.apply:
                    existing.default_path = str(folder)
            desired_lab_status = "alumni" if user_key in ALUMNI_USER_KEYS else "yes"
            if existing.lab_status != desired_lab_status:
                changes.append("lab_status")
                if args.apply:
                    existing.lab_status = desired_lab_status
            if existing.display_name != display_name and existing.display_name.lower() == existing.user_key.lower():
                changes.append("display_name")
                if args.apply:
                    existing.display_name = display_name

            if changes:
                updated.append(f"{existing.user_key}: {', '.join(changes)}")
            else:
                skipped.append(existing.user_key)

            if args.apply and existing.password_hash is None:
                from api.services.auth import set_user_password

                set_user_password(session, user=existing, password=DEFAULT_USER_PASSWORD)

        if args.apply:
            session.commit()

    print(f"scan_root={root}")
    print(f"candidates={len(candidates)}")
    print(f"created={len(created)}")
    print(f"updated={len(updated)}")
    print(f"skipped={len(skipped)}")
    if created:
        print("created_items:")
        for item in created:
            print(f"  - {item}")
    if updated:
        print("updated_items:")
        for item in updated:
            print(f"  - {item}")
    if skipped:
        print("skipped_items:")
        for item in skipped:
            print(f"  - {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
