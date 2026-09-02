"""The completion notification must describe what actually happened.

`snapshots_created` was the number of fully-successful VOLUMES, so a volume
that created a snapshot and delivered it to one target while another failed
reported zero snapshots created. And any single-volume run with a failure
reported status "failure" even when a backup had been delivered.
"""

from __future__ import annotations


class TestTheNotificationDescribesWhatHappened:
    """Two counts in the completion notification were not what they claimed.

    `snapshots_created` was the number of fully-successful VOLUMES, so a volume
    that created a snapshot and delivered it to one target while another target
    failed reported zero snapshots created. And any single-volume run with a
    failure reported status "failure" even when a backup had been delivered,
    because volumes_failed == volumes_processed was treated as total failure.
    """

    def _status(self, monkeypatch, volumes_processed, volumes_failed, completed):
        from types import SimpleNamespace

        from btrfs_backup_ng.cli import run as mod

        seen = {}

        class _Stop(Exception):
            pass

        def _capture(**kw):
            seen.update(kw)
            # The status is decided before any channel config is read; stop here
            # rather than building a full email/webhook config the test does not
            # care about.
            raise _Stop

        monkeypatch.setattr(mod, "create_backup_event", _capture)
        config = SimpleNamespace(
            global_config=SimpleNamespace(
                notifications=SimpleNamespace(is_enabled=lambda: True)
            )
        )
        try:
            mod._send_backup_notifications(
                config,
                volumes_processed=volumes_processed,
                volumes_failed=volumes_failed,
                snapshots_created=1,
                transfers_completed=completed,
                transfers_failed=1,
                duration_seconds=1.0,
                errors=[],
            )
        except _Stop:
            pass
        return seen.get("status")

    def test_a_run_that_delivered_something_is_partial_not_failure(self, monkeypatch):
        assert (
            self._status(
                monkeypatch, volumes_processed=1, volumes_failed=1, completed=1
            )
            == "partial"
        )

    def test_a_run_that_delivered_nothing_is_still_a_failure(self, monkeypatch):
        """Mutation guard: always reporting 'partial' hides a total failure."""
        assert (
            self._status(
                monkeypatch, volumes_processed=1, volumes_failed=1, completed=0
            )
            == "failure"
        )

    def test_snapshots_created_is_counted_where_snapshots_are_created(self):
        """It must come from the creation site, not from a volume success tally."""
        import inspect

        from btrfs_backup_ng.cli import run as mod

        source = inspect.getsource(mod)
        assert 'stats["snapshots_created"] += 1' in source
        assert "snapshots_created=success_count" not in source
