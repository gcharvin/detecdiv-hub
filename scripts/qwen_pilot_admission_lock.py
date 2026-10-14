"""Bounded transaction lock for a short Qwen coexistence pilot, in the API container.

No catalog rows are changed. Closing the transaction always releases admission.
New queued jobs request preemption; the controller stops Qwen before releasing.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from sqlalchemy import text

from api.db import SessionLocal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("owner")
    args = parser.parse_args()
    state = {"owner": args.owner, "pid": os.getpid(), "status": "starting"}
    output = args.root / "state.json"
    started = time.time()

    def save():
        state["checked_at"] = time.time()
        temporary = output.with_suffix(".tmp")
        temporary.write_text(json.dumps(state), encoding="utf-8")
        temporary.replace(output)

    with SessionLocal() as db:
        try:
            db.execute(text("SET LOCAL lock_timeout='5s'"))
            db.execute(text("SELECT id FROM execution_targets WHERE target_key='detecdiv-server' FOR UPDATE")).one()
            state["status"] = "locked"
            while time.time() - started < 720 and not (args.root / "stop").exists():
                queued = db.execute(text("""SELECT j.id::text FROM jobs j
                    LEFT JOIN execution_targets t ON j.execution_target_id=t.id
                    WHERE j.status='queued' AND (t.target_key='detecdiv-server' OR t.id IS NULL)
                    AND j.params_json->>'job_kind'='pipeline_run'""")).scalars().all()
                state["queued_jobs"] = queued
                state["cancel_requested"] = bool(queued)
                save()
                time.sleep(1)
        finally:
            db.rollback()
            state["status"] = "released"
            save()


if __name__ == "__main__":
    main()
