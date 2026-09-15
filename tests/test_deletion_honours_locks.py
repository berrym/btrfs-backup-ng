"""A retention lock must stop a deletion on every scheme, raw included.

A retention lock is what a restore holds while it reads a snapshot. The base
``Endpoint.delete_snapshots`` checks it, but ``RawEndpoint`` replaces that method
wholesale and never did, and ``SSHRawEndpoint``'s override consults only the
remote lock. So ``set_lock`` on a raw target pinned nothing in this process: with
the lock set, a prune deleted the stream a restore was reading. Measured on this
tree before the fix -- the verdict was deleted=1, skipped=0, and the file was
gone.

``RawEndpoint.set_lock``'s own docstring says it "mutates only the in-memory lock
set so the restore/transfer lock-guard logic works". There was no guard reading
it.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


class TestARetentionLockStopsARawDeletion:
    def test_a_locked_raw_stream_survives_the_prune(self, tmp_path):
        ep = RawEndpoint(config={"path": str(tmp_path)})
        snap = RawSnapshot(
            name="root.20240101T120000",
            stream_path=tmp_path / "root.20240101T120000.btrfs",
        )
        snap.stream_path.write_bytes(b"a backup a restore is reading right now")
        ep.set_lock(snap, "restore-in-progress", True)

        result = ep.delete_snapshots([snap])

        assert snap.stream_path.exists(), (
            "the stream a restore holds was deleted out from under it"
        )
        assert result.deleted_count == 0
        assert result.skipped_count == 1

    def test_a_parent_lock_also_stops_it(self, tmp_path):
        """A parent pin protects the stream an in-flight restore needs to replay
        its incremental chain, and is keyed apart from a direct one."""
        ep = RawEndpoint(config={"path": str(tmp_path)})
        snap = RawSnapshot(
            name="root.20240101T120000",
            stream_path=tmp_path / "root.20240101T120000.btrfs",
        )
        snap.stream_path.write_bytes(b"parent stream")
        ep.set_lock(snap, "restore-in-progress", True, parent=True)

        result = ep.delete_snapshots([snap])

        assert snap.stream_path.exists()
        assert result.skipped_count == 1

    def test_an_unlocked_stream_beside_a_locked_one_is_still_deleted(self, tmp_path):
        """The guard must not become a whole-batch refusal."""
        ep = RawEndpoint(config={"path": str(tmp_path)})
        snaps = []
        for name in ("root.20240101T120000", "root.20240102T120000"):
            snap = RawSnapshot(name=name, stream_path=tmp_path / f"{name}.btrfs")
            snap.stream_path.write_bytes(b"stream")
            snaps.append(snap)
        ep.set_lock(snaps[0], "restore", True)

        result = ep.delete_snapshots(snaps)

        assert snaps[0].stream_path.exists()
        assert not snaps[1].stream_path.exists()
        assert result.deleted_count == 1 and result.skipped_count == 1

    def test_a_locked_raw_ssh_stream_survives_too(self):
        """The remote lock is the cross-process guard; this is the in-process one,
        and it is what still holds when --skip-remote-lock was passed."""
        ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
        ep._lock_target_path = MagicMock(return_value=None)
        snap = RawSnapshot(name="root.1", stream_path=Path("/backup/root.1.btrfs"))
        # The in-memory set directly: set_lock would also write the REMOTE lock,
        # which is the other guard and needs a reachable target. This test is
        # about the delete path reading the in-process pin.
        snap.locks.add("restore-in-progress")

        with patch.object(raw_mod.subprocess, "run") as run:
            result = ep._delete_snapshots_locked([snap])

        assert result.skipped_count == 1 and result.deleted_count == 0
        assert not any("rm -f" in str(c) for c in run.call_args_list), (
            "a remote rm was issued for a locked stream"
        )
