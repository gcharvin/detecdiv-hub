"""Compare real year2 source API responses with verified homes2 files."""

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import time
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.orm import load_only

from api.db import SessionLocal
from api.models import Artifact, Job, RawDataset, User
import api.routes_raw_datasets as routes

WORK = Path(__file__).resolve().parent
SOURCE = Path("/data/Alexander/data/2024_2")
TARGET = Path("/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_2")
MANIFEST_SHA256 = "004fb3404b4af8fe09b6063a09e3a162488f689499ddb51087513daf3d9f4fad"
PLAN_SHA256 = "36329bad1371cf9d95251eab52033ccc8e78eb79ca56a95468cf53bdae53125d"
EXPECTED_COUNT = 877
EXPECTED_BYTES = 4982573526


def check_deadline(deadline):
    if time.time() >= deadline:
        raise TimeoutError("Media verification deadline reached")


def digest_file(path, deadline):
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            check_deadline(deadline)
            size += len(block)
            digest.update(block)
    return digest.hexdigest(), size


async def digest_response(response, deadline):
    digest = hashlib.sha256()
    size = 0
    started = complete = False

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        nonlocal size, started, complete
        check_deadline(deadline)
        if message["type"] == "http.response.start" and not started:
            assert message["status"] == 200
            started = True
        elif message["type"] == "http.response.body" and started and not complete:
            body = message.get("body", b"")
            size += len(body)
            digest.update(body)
            complete = not message.get("more_body", False)
        else:
            raise ValueError("Unexpected ASGI response sequence")

    scope = {
        "type": "http", "method": "GET", "headers": [], "extensions": {},
        "path": "/readonly-year2-media-verification", "asgi": {"version": "3.0"},
    }
    await response(scope, receive, send)
    assert started and complete
    return digest.hexdigest(), size


def catalog_references(db):
    prefix = str(SOURCE) + "/"
    artifacts = select(Artifact.id, Artifact.uri, Job.raw_dataset_id).outerjoin(
        Job, Job.id == Artifact.job_id
    ).where(func.regexp_replace(Artifact.uri, r"^[[:space:]]+", "").startswith(prefix, autoescape=True))
    settings = select(RawDataset.id, RawDataset.display_settings_uri, RawDataset.id).where(
        func.regexp_replace(RawDataset.display_settings_uri, r"^[[:space:]]+", "").startswith(prefix, autoescape=True)
    )
    return {
        (kind, str(identifier), uri, str(raw_id) if raw_id else None)
        for kind, query in (("artifact", artifacts), ("display_settings", settings))
        for identifier, uri, raw_id in db.execute(query)
    }


def fingerprint(path):
    metadata = path.lstat()
    assert stat.S_ISREG(metadata.st_mode) and path.resolve(strict=True) == path
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns


def verify_row(db, operator, row, deadline):
    check_deadline(deadline)
    relative = row["relative_path"]
    assert all(part not in ("", ".", "..") for part in relative.split("/"))
    assert "\\" not in relative and "\x00" not in relative
    source, target = SOURCE / relative, TARGET / relative
    assert row["uri"] == str(source) and row["raw_location_in_scope"] is True
    source_before, target_before = fingerprint(source), fingerprint(target)
    assert source_before[2] == target_before[2] == row["initial_native_bytes"]
    identifier = UUID(row["id"])
    db.expire_all()
    if row["kind"] == "artifact":
        artifact = db.get(Artifact, identifier)
        assert artifact.uri == str(source) and str(artifact.job.raw_dataset_id) == row["raw_dataset_id"]
        response = routes.get_raw_preview_artifact_content(identifier, db, operator)
    else:
        assert row["kind"] == "display_settings" and row["id"] == row["raw_dataset_id"]
        response = routes.get_raw_dataset_display_settings(identifier, db, operator)
    assert Path(response.path) == source
    digest, size = asyncio.run(digest_response(response, deadline))
    assert digest_file(target, deadline) == (digest, size) and size == source_before[2]
    assert fingerprint(source) == source_before and fingerprint(target) == target_before
    return {"kind": row["kind"], "id": row["id"], "raw_dataset_id": row["raw_dataset_id"],
            "relative_path": relative, "bytes": size, "sha256": digest}


def verify(control, state, receipt):
    deadline = control["deadline_unix"]
    check_deadline(deadline)
    assert control["read_only"] is True and control["uri_changes_allowed"] is False
    assert control["source_deletion_allowed"] is False
    assert digest_file(Path(__file__), deadline)[0] == control["runner_sha256"]
    assert digest_file(WORK / "plan.json", deadline)[0] == PLAN_SHA256
    proof = WORK / "raw-year2-completion-verification.json"
    assert digest_file(proof, deadline)[0] == control["raw_completion_sha256"]
    completion = json.loads(proof.read_text())
    assert completion["year"] == "2024_2" and completion["success"] is True
    assert completion["full_sha1_passed"] is True and completion["source_stability_verified"] is True
    assert completion["controller_exit"] == 0 and completion["verification_log_bytes"] == 0
    assert completion["native_before_after_manifest_sha256"] == MANIFEST_SHA256
    assert (completion["files"], completion["logical_bytes"]) == (974112, 5856516249162)
    plan = json.loads((WORK / "plan.json").read_text())
    rows = plan["references"]
    assert plan["initial_manifest_sha256"] == MANIFEST_SHA256
    assert len(rows) == len({row["relative_path"] for row in rows}) == EXPECTED_COUNT
    assert sum(row["initial_native_bytes"] for row in rows) == EXPECTED_BYTES
    route = Path(routes.__file__)
    assert digest_file(route, deadline)[0] == plan["route_sha256"]
    expected = {(row["kind"], row["id"], row["uri"], row["raw_dataset_id"]) for row in rows}
    assert len(expected) == EXPECTED_COUNT
    with SessionLocal() as db:
        db.execute(text("SET TRANSACTION READ ONLY"))
        assert catalog_references(db) == expected
        operator = db.scalars(select(User).options(load_only(User.id, User.role)).where(
            User.user_key == "gilles", User.role == "admin", User.is_active.is_(True)
        )).one()
        state["phase"] = "streaming_source_handlers"
        for row in rows:
            evidence = verify_row(db, operator, row, deadline)
            line = json.dumps(evidence)
            receipt.write(line + "\n")
            receipt.flush()
            print(line, flush=True)
            state["handlers_verified"] += 1
            state["bytes_verified"] += evidence["bytes"]
        assert catalog_references(db) == expected
        assert digest_file(route, deadline)[0] == plan["route_sha256"]
    assert state["handlers_verified"] == EXPECTED_COUNT and state["bytes_verified"] == EXPECTED_BYTES
    state.update(success=True, phase="verified_before_uri_cutover", route_sha256=plan["route_sha256"],
                 receipt_sha256=digest_file(WORK / "receipt.jsonl", deadline)[0])


def main():
    os.umask(0o077)
    os.nice(19)
    assert not (WORK / "result.json").exists()
    control = json.loads((WORK / "controller-request.json").read_text())
    state = {"phase": "preflight", "success": False, "handlers_verified": 0, "bytes_verified": 0,
             "read_only": True, "source_deleted": False, "uri_changed": False}
    with (WORK / "receipt.jsonl").open("x") as receipt:
        try:
            verify(control, state, receipt)
        except Exception as error:
            state.update(success=False, phase="failed_readonly", error_type=type(error).__name__)
            raise
        finally:
            state["finished_at"] = datetime.now(timezone.utc).isoformat()
            with (WORK / "result.json").open("x") as stream:
                json.dump(state, stream, indent=2)
                stream.write("\n")
            print(json.dumps({"kind": "terminal_result", **state}), flush=True)


if __name__ == "__main__":
    main()
