"""Atomic raw stream write (0.8.5 PR1).

A raw receive writes to a ``.part`` file and is published to its final name only
by ``commit_receive()``, which the transfer engine calls after confirming the
pipeline succeeded. A crash therefore leaves at most a ``.part`` file, which
discovery must ignore -- so a partial transfer can never be listed as a complete
backup (the raw phantom-backup bug).

These tests are written to FAIL if the atomic-write behavior is reverted:
  * remove the ``.part`` exclusion in discover_raw_snapshots -> the phantom
    tests fail (a partial is listed as a backup);
  * make receive() write straight to the final name -> the "final absent before
    commit" assertions fail.
"""

from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import discover_raw_snapshots


def test_part_file_is_never_discovered(tmp_path):
    """A leftover ``.part`` file (a crashed/uncommitted transfer) is not a backup."""
    (tmp_path / "good-20260101.btrfs").write_bytes(b"complete")
    (tmp_path / "crashed-20260102.btrfs.part").write_bytes(b"partial")

    found = discover_raw_snapshots(tmp_path)
    names = {s.name for s in found}
    assert "good-20260101" in names
    # The partial must not appear under any name...
    assert not any("crashed" in n for n in names)
    # ...and no discovered stream is ever a .part file.
    assert all(not s.stream_path.name.endswith(".part") for s in found)


def test_receive_then_commit_publishes_atomically(tmp_path):
    """receive() writes to .part; the final name appears only after commit."""
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload-bytes")

    with open(src, "rb") as stdin:
        proc = endpoint.receive(stdin, snapshot_name="snap")
        proc.communicate()
    assert proc.returncode == 0

    final = tmp_path / "snap.btrfs"
    part = tmp_path / "snap.btrfs.part"
    # Uncommitted: only the .part exists, and discovery ignores it -- so a crash
    # here leaves nothing that looks like a complete backup.
    assert part.exists()
    assert not final.exists()
    assert discover_raw_snapshots(tmp_path) == []

    endpoint.commit_receive()
    # Committed: the final name exists, the .part is gone, and it is now the only
    # discoverable snapshot.
    assert final.exists()
    assert not part.exists()
    assert final.read_bytes() == b"payload-bytes"
    assert [s.name for s in discover_raw_snapshots(tmp_path)] == ["snap"]


def test_commit_missing_part_fails_loud(tmp_path):
    """If a receive ran but its .part is gone at commit time, fail loud rather
    than report a success with no file on disk (never fabricate a final file)."""
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    endpoint._pending_metadata = {
        "name": "x",
        "stream_path": tmp_path / "x.btrfs",
        "part_path": tmp_path / "x.btrfs.part",  # does not exist
        "parent_name": None,
        "compress": None,
        "encrypt": None,
        "gpg_recipient": None,
    }
    with pytest.raises(RuntimeError, match="is missing"):
        endpoint.commit_receive()
    assert not (tmp_path / "x.btrfs").exists()


def test_commit_without_receive_is_noop(tmp_path):
    """A fresh endpoint that never received anything must commit as a safe no-op
    (its dummy metadata has no name), not touch the filesystem, and not raise."""
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    # Do NOT call receive(); _pending_metadata is the dummy init.
    endpoint.commit_receive()
    assert list(tmp_path.iterdir()) == []


def test_ssh_commit_builds_sync_mv_sync_script_and_publishes():
    """SSH commit runs exactly 'sync && mv -f <part> <final> && sync' remotely
    (leading sync flushes bytes before rename; trailing sync makes the rename
    durable). Mocked -- runs without any real SSH."""
    ep = SSHRawEndpoint.__new__(SSHRawEndpoint)
    ep._pending_metadata = {
        "name": "snap",
        "stream_path": "/remote/snap.btrfs",
        "part_path": "/remote/snap.btrfs.part",
    }
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stderr=b"")
    )
    ep.commit_receive()

    ep._exec_remote_command.assert_called_once()
    argv = ep._exec_remote_command.call_args[0][0]
    assert argv[0] == "sh"
    assert argv[1] == "-c"
    assert argv[2] == (
        "sync && mv -f /remote/snap.btrfs.part /remote/snap.btrfs && sync"
    )


def test_ssh_commit_raises_on_remote_failure():
    """A nonzero remote return must raise, so the engine treats an unpublished
    remote stream as a failed transfer."""
    ep = SSHRawEndpoint.__new__(SSHRawEndpoint)
    ep._pending_metadata = {
        "name": "snap",
        "stream_path": "/remote/snap.btrfs",
        "part_path": "/remote/snap.btrfs.part",
    }
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=1, stderr=b"mv: cannot stat")
    )
    with pytest.raises(RuntimeError, match="Failed to publish remote raw stream"):
        ep.commit_receive()


def test_ssh_commit_wraps_in_sudo_when_configured():
    """With ssh_sudo, the remote commit is wrapped in sudo. Exercises the real
    command construction (only subprocess.run is mocked) -- no live SSH -- so the
    sudo path is proven without setting up passwordless root access."""
    from unittest.mock import patch

    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas", "ssh_sudo": True})
    ep._pending_metadata = {
        "name": "snap",
        "stream_path": "/backup/snap.btrfs",
        "part_path": "/backup/snap.btrfs.part",
    }
    with patch("btrfs_backup_ng.endpoint.raw.subprocess.run") as mrun:
        mrun.return_value = MagicMock(returncode=0, stderr=b"")
        ep.commit_receive()

    mrun.assert_called_once()
    remote = mrun.call_args[0][0][-1]  # last element of the ssh argv
    assert remote.startswith("sudo ")
    assert "sync && mv -f" in remote
    assert remote.rstrip().endswith("&& sync") or "&& sync" in remote


def test_commit_never_overwrites_a_committed_final(tmp_path):
    """commit_receive publishes the current .part; it must not touch an unrelated
    already-committed backup that shares the directory."""
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    prior = tmp_path / "prior.btrfs"
    prior.write_bytes(b"prior-good-backup")

    src = tmp_path / "src.bin"
    src.write_bytes(b"new-stream")
    with open(src, "rb") as stdin:
        proc = endpoint.receive(stdin, snapshot_name="new")
        proc.communicate()
    endpoint.commit_receive()

    # The prior backup is untouched; the new one is published alongside it.
    assert prior.read_bytes() == b"prior-good-backup"
    assert (tmp_path / "new.btrfs").read_bytes() == b"new-stream"
