"""A restore must not leave a pin behind on the backup.

`restore_snapshot` pins the snapshot it is reading, and its parent, so a prune
running elsewhere cannot delete the bytes mid-restore. Since 0.9.5 those pins
PERSIST on a remote target, so a pin that is taken and never released does not
die with the process -- it blocks every later prune of that snapshot until it
is swept as stale.

The parent pin was taken before the try/finally that releases it, so a failure
there leaked the snapshot pin that had already been taken.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from btrfs_backup_ng.core import restore as core_restore


class _Snap:
    def __init__(self, name):
        self.name = name
        self.locks = set()
        self.parent_locks = set()

    def get_name(self):
        return self.name

    def get_path(self):
        return f"/backup/{self.name}"

    def __str__(self):
        return self.name


class _Backup:
    """Records the pin/unpin calls so the balance can be asserted."""

    def __init__(self, fail_on_parent=False, fail_release=False):
        self.config = {"path": "/backup", "snap_prefix": ""}
        self.held: set[tuple[str, bool]] = set()
        self.fail_on_parent = fail_on_parent
        self.fail_release = fail_release

    def set_lock(self, snapshot, lock_id, lock_state, parent=False):
        key = (snapshot.get_name(), parent)
        if lock_state:
            if parent and self.fail_on_parent:
                raise OSError("could not write the parent lock")
            self.held.add(key)
        else:
            if self.fail_release:
                raise OSError("lock file is gone")
            self.held.discard(key)

    def get_id(self):
        return "backup"


class _Local:
    def __init__(self):
        self.config = {"path": "/mnt/restore", "snap_prefix": ""}

    def list_snapshots(self, flush_cache=False):
        # The collision check reads the destination and RAISES if it cannot --
        # an endpoint that cannot answer must not be read as "not there".
        return []

    def get_id(self):
        return "local"


@pytest.fixture
def _quiet():
    """Neutralise everything past the pins; the pins are what is under test."""
    with (
        patch.object(core_restore, "send_snapshot"),
        patch.object(core_restore, "verify_restored_snapshot"),
        patch.object(core_restore, "log_transaction"),
    ):
        yield


def _run(backup, **kw):
    return core_restore.restore_snapshot(
        backup,
        _Local(),
        _Snap("home-20240102-120000"),
        parent=_Snap("home-20240101-120000"),
        options={"skip_verify": True},
        session_id="test",
        **kw,
    )


class TestThePinsAreBalanced:
    def test_a_clean_restore_releases_both(self, _quiet):
        backup = _Backup()
        _run(backup)
        assert backup.held == set(), f"pins left behind: {backup.held}"

    def test_a_failed_transfer_releases_both(self, _quiet):
        backup = _Backup()
        with patch.object(
            core_restore, "send_snapshot", side_effect=OSError("link died")
        ):
            with pytest.raises(core_restore.RestoreError):
                _run(backup)
        assert backup.held == set(), f"pins left behind after a failure: {backup.held}"

    def test_a_failed_parent_pin_does_not_leak_the_snapshot_pin(self, _quiet):
        """THE regression: the first pin was taken outside the try/finally."""
        backup = _Backup(fail_on_parent=True)
        with pytest.raises(Exception):  # noqa: B017 - any failure, but it must not leak
            _run(backup)
        assert backup.held == set(), (
            f"the snapshot stayed pinned because the parent pin failed: "
            f"{backup.held}. On a remote target that pin persists and blocks "
            f"every later prune of that snapshot."
        )

    def test_a_release_that_fails_does_not_mask_the_real_error(self, _quiet):
        """The operator must see 'link died', not the lock-file error."""
        backup = _Backup(fail_release=True)
        with patch.object(
            core_restore, "send_snapshot", side_effect=OSError("link died")
        ):
            with pytest.raises(core_restore.RestoreError, match="link died"):
                _run(backup)
