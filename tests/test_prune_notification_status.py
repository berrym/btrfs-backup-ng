"""A prune notification must not claim success the command itself denies.

`volumes_failed` counts only volumes that failed while being PLANNED. A deletion
that fails during execution is recorded in `errors` and makes prune exit 1, but
it never touched that counter, so a prune in which every deletion failed
notified "success" while exiting non-zero. The notification is what an
unattended operator sees.
"""

import pytest

from btrfs_backup_ng.cli import prune as prune_cli
from btrfs_backup_ng.config.schema import GlobalConfig, NotificationConfig


@pytest.fixture
def captured(monkeypatch):
    """Capture the event a prune run would emit."""
    events = []

    def fake_create(**kwargs):
        events.append(kwargs)
        return object()

    monkeypatch.setattr(prune_cli, "create_prune_event", fake_create)
    monkeypatch.setattr(prune_cli, "send_notifications", lambda *a, **k: {})
    return events


def _config():
    """The real config objects, so the test cannot drift from their shape."""
    notifications = NotificationConfig()
    notifications.email.enabled = True
    global_config = GlobalConfig()
    global_config.notifications = notifications
    return type("_Config", (), {"global_config": global_config})()


def _notify(captured, *, processed, failed, pruned, errors):
    prune_cli._send_prune_notifications(
        _config(),
        volumes_processed=processed,
        volumes_failed=failed,
        snapshots_pruned=pruned,
        duration_seconds=1.0,
        errors=errors,
    )
    return captured[-1]["status"]


def test_failed_deletions_are_not_reported_as_success(captured):
    """No volume failed to PLAN, but every deletion failed."""
    status = _notify(
        captured, processed=1, failed=0, pruned=0, errors=["Target /backup: busy"]
    )

    assert status != "success", (
        "prune notified success while its own exit code reported failure"
    )
    assert status == "partial"


def test_a_clean_prune_is_still_success(captured):
    assert _notify(captured, processed=2, failed=0, pruned=5, errors=[]) == "success"


def test_a_prune_with_nothing_to_delete_is_success(captured):
    """Retention already satisfied is a success, not a failure."""
    assert _notify(captured, processed=2, failed=0, pruned=0, errors=[]) == "success"


def test_every_volume_failing_is_a_failure(captured):
    assert (
        _notify(captured, processed=2, failed=2, pruned=0, errors=["a", "b"])
        == "failure"
    )


def test_some_volumes_failing_is_partial(captured):
    assert _notify(captured, processed=3, failed=1, pruned=2, errors=["a"]) == "partial"
