"""Finish the already-authorized catalog cutover after both readers pass."""

from datetime import datetime, timezone
import json
import time

from year2_final_host_operations import HERE, PRIVATE, read_json, status, verify
from cleanup_year2_final_windows import main as cleanup
from cutover_year2_project_catalog import main as cutover
from verify_year2_project_catalog_completion import main as postflight

STATE = PRIVATE / "year2-cutover-watch-state.json"
LOCK = PRIVATE / "year2-cutover-watch-started.json"
HOST_PROOF = HERE / "year2-final-hosts-completion-verification.json"
CUTOVER = HERE / "year2-project-catalog-cutover-result.json"
POSTFLIGHT = HERE / "year2-project-catalog-preservation-postflight.json"
CLEANUP = HERE / "year2-windows-cleanup.json"


def record(phase, **fields):
    payload = {"phase": phase, "checked_at": datetime.now(timezone.utc).isoformat(), **fields}
    temporary = STATE.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(STATE)


def wait_for_windows(deadline):
    failures = 0
    while time.time() < deadline:
        try:
            reports = status()
        except (OSError, RuntimeError, ValueError) as error:
            failures += 1
            record("waiting_for_status", transient_errors=failures, error_type=type(error).__name__)
            if failures >= 5:
                raise
        else:
            failures = 0
            windows = reports["windows"]
            task = windows["task"]
            state = windows.get("state", {})
            if task["state"] == "Ready" and task["result"] == 0:
                return reports
            if state.get("phase") == "failed_safe_readonly" or task["state"] != "Running":
                record("windows_reader_failed", task=task, reader_state=state)
                raise RuntimeError("Windows reader did not finish successfully")
            progress = windows.get("progress", {})
            record("waiting_for_windows", project_index=progress.get("project_index"), fov=progress.get("fov"))
        time.sleep(45)
    raise TimeoutError("The publication lease window expired before the Windows reader finished")


def main():
    with LOCK.open("x", encoding="utf-8") as output:
        json.dump({"started_at": datetime.now(timezone.utc).isoformat()}, output)
    try:
        control = read_json(PRIVATE / "year2-final-host-control.json")
        worker = read_json(PRIVATE / "year2-windows-worker-path-verification.json")
        assert worker["success"] and worker["projects"] == 17
        if not HOST_PROOF.exists():
            verify(wait_for_windows(control["own_lease_deadline_unix"]))
        assert read_json(HOST_PROOF)["success"] is True
        record("both_hosts_verified")
        if not POSTFLIGHT.exists():
            if not CUTOVER.exists():
                try:
                    cutover()
                except Exception:
                    # A lost response can follow a committed transaction.
                    # Verify the database outcome before considering any retry.
                    postflight()
            if not POSTFLIGHT.exists():
                postflight()
        proof = read_json(POSTFLIGHT)
        assert proof["success"] and proof["projects"] == proof["project_api_routes_verified"] == 17
        record("catalog_verified", projects=17)
        if not CLEANUP.exists():
            cleanup()
        record("complete", projects=17, catalog_verified=True)
    except Exception as error:
        previous = read_json(STATE) if STATE.exists() else {}
        record("requires_review", error_type=type(error).__name__, error=str(error),
               previous_phase=previous.get("phase"), catalog_verified=POSTFLIGHT.exists())
        raise


if __name__ == "__main__":
    main()
