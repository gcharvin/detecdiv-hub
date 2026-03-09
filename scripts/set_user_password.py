from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.db import SessionLocal
from api.models import User
from api.services.auth import set_user_password


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update a hub user password.")
    parser.add_argument("user_key")
    parser.add_argument("password")
    parser.add_argument("--display-name", default=None)
    parser.add_argument("--role", default="user")
    parser.add_argument("--email", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with SessionLocal() as session:
        user = session.query(User).filter(User.user_key == args.user_key).first()
        if user is None:
            user = User(
                user_key=args.user_key,
                display_name=args.display_name or args.user_key,
                email=args.email,
                role=args.role,
                is_active=True,
                metadata_json={},
            )
            session.add(user)
            session.flush()
        set_user_password(session, user=user, password=args.password)
        session.commit()
    print(f"Password updated for {args.user_key}.")


if __name__ == "__main__":
    main()
