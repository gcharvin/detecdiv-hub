from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.db import SessionLocal
from api.config import get_settings
from api.services.project_indexing import index_project_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index a DetecDiv project root into detecdiv-hub.")
    parser.add_argument("source_path", help="Project root to scan")
    parser.add_argument("--storage-root-name", default=None)
    parser.add_argument("--host-scope", default="server", choices=["server", "client", "all"])
    parser.add_argument("--root-type", default="project_root")
    parser.add_argument("--owner-user-key", default=None)
    parser.add_argument("--visibility", default="private", choices=["private", "shared", "public"])
    parser.add_argument("--clear-existing-for-root", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    with SessionLocal() as session:
        result = index_project_root(
            session,
            root_path=args.source_path,
            storage_root_name=args.storage_root_name,
            host_scope=args.host_scope,
            root_type=args.root_type,
            owner_user_key=args.owner_user_key or settings.default_user_key,
            visibility=args.visibility,
            clear_existing_for_root=args.clear_existing_for_root,
        )
        session.commit()

    print(
        f"Indexed {result.indexed_projects} project(s) from {result.root_path} "
        f"into storage root {result.storage_root_name} for owner {result.owner_user_key}."
    )


if __name__ == "__main__":
    main()
