import importlib.util
from pathlib import Path
import sys

import pytest


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location("alex_rollout", SCRIPTS / "finish_alex_worker_fix_rollout.py")
rollout = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rollout)


def fleet():
    return [{"target": target, "instance": instance, "fingerprint": settings["fingerprint"],
             "job": None, "pid": 123}
            for target, settings in rollout.TARGETS.items() for instance in settings["instances"]]


def test_missing_windows_worker_cannot_be_reported_complete():
    with pytest.raises(RuntimeError, match="Missing worker"):
        rollout.pending_workers([w for w in fleet() if w["target"] == "detecdiv-server"],
                                "windows-10-20-11-56")


def test_additional_autoscaled_old_worker_requires_activation():
    rows = fleet() + [{"target": "detecdiv-server", "instance": "@7", "fingerprint": "old"}]
    assert [w["instance"] for w in rollout.pending_workers(rows, "detecdiv-server")] == ["@7"]


def test_busy_fleet_never_reloads_or_stops_workers(monkeypatch, capsys):
    rows = fleet()
    for row in rows:
        row.update(fingerprint="old", job="active-analysis")
    monkeypatch.setattr(rollout, "snapshot", lambda config: rows)
    monkeypatch.setattr(rollout, "reserve", lambda *args: {"reserved": False, "idle": []})
    def forbidden(*args):
        pytest.fail("An occupied worker must never be reloaded")
    monkeypatch.setattr(rollout, "reload_linux", forbidden)
    monkeypatch.setattr(rollout, "reload_windows", forbidden)
    rollout.main()
    assert '"complete": false' in capsys.readouterr().out


def test_reload_failure_restores_admission(monkeypatch):
    rows = fleet()
    rows[0]["fingerprint"] = "old"
    monkeypatch.setattr(rollout, "snapshot", lambda config: rows)
    monkeypatch.setattr(rollout, "reserve", lambda *args: {"reserved": True, "idle": [rows[0]]})
    def fail(*args):
        raise RuntimeError("Source changed since staging")
    restored = []
    monkeypatch.setattr(rollout, "reload_linux", fail)
    monkeypatch.setattr(rollout, "restore", lambda config, target: restored.append(target))
    with pytest.raises(RuntimeError, match="Source changed"):
        rollout.main()
    assert restored == ["detecdiv-server"]
