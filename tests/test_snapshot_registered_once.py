"""One snapshot, one cache entry -- and -N must keep what it was asked to keep.

``Endpoint.snapshot`` registered the new snapshot from INSIDE the loop over its
commands. ``sync`` defaults to True, so there are normally two commands -- the
create and the ``btrfs subvolume sync`` -- and the snapshot landed in the listing
cache twice.

Count-based retention then budgeted for a snapshot that does not exist. The
damage is not proportional to the error: the duplicate sits at the END of the
list, so ``unlocked[:-keep]`` shifts by one and the extra casualty is an old
snapshot -- except at ``-N 1``, where the duplicate of the newest pushes the REAL
newest into the delete slice and the source is left with nothing at all,
including the snapshot just taken.

Measured before the fix, with the cache populated:

    4 existing + 1 new, -N 2  ->  left 1
    3 existing + 1 new, -N 1  ->  left 0

NOT REACHABLE FROM ANY SHIPPED FLOW, and that is exactly why these tests exist.
``add_snapshot`` returns early while the cache is None, and no entry point lists
the source before snapshotting it: the legacy path snapshots then prunes, `run`
and `snapshot` only prepare(), and sync_snapshots lists with flush_cache=True.
The safety is a coincidence of call order that nobody is maintaining. One
pre-flight listing added to `run` for an unrelated reason turns `-N 1` into total
loss, so every test here deliberately creates the arrangement no shipped flow
currently produces.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from btrfs_backup_ng.endpoint.local import LocalEndpoint

logging.disable(logging.NOTSET)


def _endpoint(tmp_path, existing=0):
    source = tmp_path / "src"
    source.mkdir(exist_ok=True)
    dest = tmp_path / "snaps"
    dest.mkdir(exist_ok=True)
    for i in range(existing):
        (dest / f"s-2024010{i + 1}-120000").mkdir()
    return LocalEndpoint(
        config={"path": str(dest), "source": str(source), "snap_prefix": "s-"}
    )


def _take_and_prune(endpoint, *, keep, sync=True):
    """List FIRST -- the arrangement that populates the cache -- then snapshot
    and prune, recording the paths the prune actually deleted."""
    deleted: list[str] = []

    def record(options, **_kw):
        cmd = options["command"]
        if isinstance(cmd[-1], tuple):
            deleted.append(str(cmd[-1][0]).rsplit("/", 1)[-1])

    with (
        patch.object(LocalEndpoint, "_remount", return_value=None),
        patch.object(LocalEndpoint, "_exec_command", side_effect=record),
    ):
        endpoint.list_snapshots()
        endpoint.snapshot(sync=sync)
        deleted.clear()  # count only the prune
        endpoint.delete_old_snapshots(keep=keep)
    return [d for d in deleted if d]


class TestOneSnapshotIsRegisteredOnce:
    @pytest.mark.parametrize("sync", [True, False])
    def test_the_cache_gains_exactly_one_entry(self, tmp_path, sync):
        """sync=True issues two commands. The registration must not follow them."""
        endpoint = _endpoint(tmp_path)
        with (
            patch.object(LocalEndpoint, "_remount", return_value=None),
            patch.object(LocalEndpoint, "_exec_command", return_value=None),
            patch.object(LocalEndpoint, "_listdir", return_value=[]),
        ):
            endpoint.list_snapshots()
            before = len(endpoint.list_snapshots())
            endpoint.snapshot(sync=sync)
            after = len(endpoint.list_snapshots())

        assert after - before == 1, (
            f"one snapshot with sync={sync} added {after - before} cache entries"
        )

    def test_add_snapshot_is_idempotent_by_path(self, tmp_path):
        """Belt as well as braces: a cache that CAN hold a snapshot twice is a
        retention budget that can be wrong by that many."""
        endpoint = _endpoint(tmp_path)
        with (
            patch.object(LocalEndpoint, "_remount", return_value=None),
            patch.object(LocalEndpoint, "_exec_command", return_value=None),
            patch.object(LocalEndpoint, "_listdir", return_value=[]),
        ):
            endpoint.list_snapshots()
            endpoint.snapshot(sync=False)
            (snap,) = endpoint.list_snapshots()
            endpoint.add_snapshot(snap)
            endpoint.add_snapshot(snap)

            assert len(endpoint.list_snapshots()) == 1


class TestCountBasedRetentionKeepsWhatItWasAsked:
    @pytest.mark.parametrize(
        ("existing", "keep"), [(1, 1), (3, 1), (4, 2), (4, 3), (5, 4)]
    )
    def test_exactly_keep_snapshots_survive(self, tmp_path, existing, keep):
        endpoint = _endpoint(tmp_path, existing=existing)

        deleted = _take_and_prune(endpoint, keep=keep)

        real = existing + 1
        assert real - len(deleted) == keep, (
            f"{real} real snapshots under -N {keep} left {real - len(deleted)}"
        )

    def test_minus_n_one_does_not_destroy_the_snapshot_just_taken(self, tmp_path):
        """The sharpest edge. Before the fix this left NOTHING: the duplicate of
        the newest pushed the real newest into the delete slice."""
        endpoint = _endpoint(tmp_path, existing=3)

        deleted = _take_and_prune(endpoint, keep=1)

        assert len(deleted) == 3, f"expected 3 deletions, got {deleted}"
        newest = sorted(endpoint.list_snapshots(), key=lambda s: s.time_obj)[-1]
        assert str(newest.get_path()).rsplit("/", 1)[-1] not in deleted, (
            "the snapshot just taken was deleted by its own run's retention"
        )

    def test_the_extra_casualty_was_an_old_snapshot(self, tmp_path):
        """Pins WHICH one the duplicate cost, so a regression is recognisable
        rather than just a wrong count."""
        endpoint = _endpoint(tmp_path, existing=4)

        deleted = _take_and_prune(endpoint, keep=2)

        assert deleted == [
            "s-20240101-120000",
            "s-20240102-120000",
            "s-20240103-120000",
        ], deleted


def test_the_registration_is_outside_the_command_loop(tmp_path):
    """Pinned structurally too: the count assertions above only fail while the
    cache happens to be populated, and no shipped flow populates it."""
    import ast
    import inspect

    from btrfs_backup_ng.endpoint import common

    # Parsed from the MODULE, not from the attribute: Endpoint.snapshot is
    # wrapped by @require_source, which uses no functools.wraps, so
    # inspect.getsource on it returns the decorator's body instead.
    module = ast.parse(inspect.getsource(common))
    cls = next(
        n for n in module.body if isinstance(n, ast.ClassDef) and n.name == "Endpoint"
    )
    fn = next(
        n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "snapshot"
    )
    for node in ast.walk(fn):
        if not isinstance(node, ast.For):
            continue
        calls = [
            n
            for n in ast.walk(node)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "add_snapshot"
        ]
        assert not calls, (
            "add_snapshot is inside the command loop again; sync=True issues two "
            "commands, so the snapshot registers twice"
        )
