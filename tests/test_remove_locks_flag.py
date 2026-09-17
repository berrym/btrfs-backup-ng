"""`--remove-locks` must actually remove locks.

The flag was declared in the legacy parser and parsed into the options dict,
but nothing ever read it: the command accepted it, did a normal backup run or
nothing at all, and exited 0. Its help text promises "Remove locks for all
given destinations from all snapshots."
"""

import inspect
import tempfile
import time
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__, _legacy_main
from btrfs_backup_ng.endpoint.local import LocalEndpoint


class _Destination:
    def __init__(self, ident):
        self._ident = ident

    def get_id(self):
        return self._ident


@pytest.fixture
def source(monkeypatch):
    """A real endpoint whose listing is stood in for; locks stay real."""
    path = Path(tempfile.mkdtemp())
    endpoint = LocalEndpoint(config={"path": str(path), "snap_prefix": "home."})
    snapshots = [
        __util__.Snapshot(path, "home.", endpoint, time_obj=time.localtime(t))
        for t in (1704067200, 1704153600)
    ]
    monkeypatch.setattr(endpoint, "list_snapshots", lambda *a, **k: list(snapshots))
    return endpoint, snapshots


def test_it_removes_the_locks_of_the_named_destinations(source):
    endpoint, snapshots = source
    for snapshot in snapshots:
        endpoint.set_lock(snapshot, "dest-A", True)

    _legacy_main.remove_locks(endpoint, [_Destination("dest-A")])

    assert all(not s.locks for s in snapshots), (
        f"locks survived --remove-locks: {[s.locks for s in snapshots]}"
    )


def test_it_leaves_another_destinations_lock_alone(source):
    """Locks are keyed by destination; only the ones named are cleared."""
    endpoint, snapshots = source
    endpoint.set_lock(snapshots[0], "dest-A", True)
    endpoint.set_lock(snapshots[0], "dest-B", True)

    _legacy_main.remove_locks(endpoint, [_Destination("dest-A")])

    assert snapshots[0].locks == {"dest-B"}


def test_it_removes_parent_locks_too(source):
    """A parent lock pins a snapshot just as firmly as a direct one."""
    endpoint, snapshots = source
    endpoint.set_lock(snapshots[0], "dest-A", True, parent=True)
    assert snapshots[0].parent_locks == {"dest-A"}

    _legacy_main.remove_locks(endpoint, [_Destination("dest-A")])

    assert not snapshots[0].parent_locks


def test_the_count_it_reports_is_the_number_it_removed(source, monkeypatch):
    """The summary line is the only feedback the operator gets, so it must not
    count locks that were not there to remove."""
    endpoint, snapshots = source
    endpoint.set_lock(snapshots[0], "dest-A", True)  # one lock, two snapshots

    recorded = []
    monkeypatch.setattr(
        _legacy_main.logger,
        "info",
        lambda msg, *a, **k: recorded.append(msg % a if a else msg),
    )

    _legacy_main.remove_locks(endpoint, [_Destination("dest-A")])

    assert any("Removed 1 lock(s)" in line for line in recorded), recorded


def test_run_task_actually_consults_the_flag():
    """Guard the wiring, not just the helper: the flag was inert for its whole
    existence precisely because nothing read it."""
    source_text = inspect.getsource(_legacy_main.run_task)
    assert "remove_locks" in source_text, (
        "run_task never reads remove_locks, so --remove-locks does nothing"
    )
