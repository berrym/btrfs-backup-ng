"""A legacy run must not report success when its retention failed.

`cleanup_snapshots` called `delete_old_snapshots` and discarded the
DeletionResult, and caught AbortError at DEBUG. A prune that deleted nothing --
unreadable lock file, busy target, every btrfs delete failing -- was
indistinguishable from a clean one, and the run still exited 0 while the
target filled up.
"""

from btrfs_backup_ng import __util__, _legacy_main
from btrfs_backup_ng.endpoint.common import DeletionResult


class _Endpoint:
    def __init__(self, result=None, raises=None):
        self._result = result if result is not None else DeletionResult()
        self._raises = raises

    def delete_old_snapshots(self, keep):
        if self._raises:
            raise self._raises
        return self._result

    def __str__(self):
        return "test-endpoint"


OPTIONS = {"num_snapshots": 5, "num_backups": 5}


def test_a_clean_prune_reports_clean():
    assert _legacy_main.cleanup_snapshots(_Endpoint(), [_Endpoint()], OPTIONS)


def test_a_failed_deletion_is_not_clean():
    failed = DeletionResult(failed=[("home.20260101", "Device or resource busy")])

    assert not _legacy_main.cleanup_snapshots(_Endpoint(failed), [_Endpoint()], OPTIONS)


def test_a_failed_deletion_on_a_destination_is_not_clean():
    failed = DeletionResult(failed=[("home.20260101", "Device or resource busy")])

    assert not _legacy_main.cleanup_snapshots(_Endpoint(), [_Endpoint(failed)], OPTIONS)


def test_a_skip_is_not_a_failure():
    """Refusing to delete a locked snapshot is the guard working, not an error."""
    skipped = DeletionResult(skipped=[("home.20260101", "held by a retention lock")])

    assert _legacy_main.cleanup_snapshots(_Endpoint(skipped), [_Endpoint()], OPTIONS)


def test_an_abort_is_reported_not_swallowed(caplog):
    endpoint = _Endpoint(raises=__util__.AbortError("lock file is unreadable"))

    assert not _legacy_main.cleanup_snapshots(endpoint, [_Endpoint()], OPTIONS)


def test_a_failed_deletion_is_visible_at_default_verbosity(monkeypatch):
    """It used to be logged at DEBUG, so nobody running normally ever saw it."""
    recorded = []
    monkeypatch.setattr(
        _legacy_main.logger, "error", lambda msg, *a, **k: recorded.append(msg % a)
    )
    failed = DeletionResult(failed=[("home.20260101", "Device or resource busy")])

    _legacy_main.cleanup_snapshots(_Endpoint(failed), [_Endpoint()], OPTIONS)

    assert any("FAILED to delete" in line for line in recorded), recorded


def test_run_task_treats_a_retention_failure_as_a_failed_run():
    """The modern run path already exits non-zero rather than skip retention."""
    import inspect

    source = inspect.getsource(_legacy_main.run_task)
    assert "if not cleanup_snapshots(" in source, (
        "run_task ignores the cleanup verdict, so a run whose retention failed "
        "still exits 0"
    )
