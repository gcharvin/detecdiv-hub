from types import SimpleNamespace

import pytest

import api.services.job_priority_settings as priorities


class DummySession:
    def __init__(self, entry=None):
        self.entry = entry
        self.added = []

    def get(self, _model, key):
        if key == priorities.JOB_PRIORITY_SETTING_KEY:
            return self.entry
        return None

    def add(self, item):
        self.entry = item
        self.added.append(item)

    def flush(self):
        return None


@pytest.fixture(autouse=True)
def skip_table_bootstrap(monkeypatch):
    monkeypatch.setattr(priorities, "ensure_system_settings_table", lambda _session: None)


def test_default_priorities_put_archives_behind_normal_jobs_and_previews():
    config = priorities.resolve_job_priority_runtime_config(DummySession())

    assert config.priority_for("pipeline_run", requested_priority=999) == 10
    assert config.priority_for("raw_preview_video", requested_priority=999) == 100
    assert config.priority_for("archive_raw_dataset", requested_priority=40) == 150
    assert config.priority_for("unregistered_kind", requested_priority=77) == 77


def test_stored_priorities_override_defaults_and_validate_updates():
    session = DummySession(
        SimpleNamespace(value_json={"priorities": {"archive_raw_dataset": 175}})
    )

    config = priorities.resolve_job_priority_runtime_config(session)
    assert config.priorities["archive_raw_dataset"] == 175

    updated = priorities.update_job_priority_runtime_config(
        session,
        updates={"archive_raw_dataset": 160, "pipeline_run": 5},
    )
    assert updated.priorities["archive_raw_dataset"] == 160
    assert updated.priorities["pipeline_run"] == 5

    with pytest.raises(ValueError, match="Unknown job priority"):
        priorities.update_job_priority_runtime_config(session, updates={"not_a_job": 1})
