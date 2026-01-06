"""Tier 2 tests for endpoint operations with real btrfs.

These tests verify that the LocalEndpoint class works correctly
with actual btrfs filesystems.
"""

import subprocess
from datetime import datetime
from pathlib import Path

import pytest

from btrfs_backup_ng.endpoint.local import LocalEndpoint

from .conftest import (
    create_snapshot,
    delete_subvolume,
    requires_btrfs,
)


@pytest.mark.tier2
@requires_btrfs
class TestLocalEndpointWithRealBtrfs:
    """Test LocalEndpoint with real btrfs operations."""

    def test_endpoint_prepare_on_btrfs(self, btrfs_volume: Path):
        """Test endpoint preparation on a real btrfs volume."""
        dest = btrfs_volume / "backups"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
                "snap_prefix": "test-",
                "fs_checks": "auto",
            }
        )

        # Should succeed without error
        endpoint.prepare()

        # Should have created backup infrastructure
        assert (dest / ".btrfs-backup-ng").exists()

    def test_endpoint_prepare_fs_check_validates_btrfs(self, btrfs_volume: Path):
        """Test that fs_checks validates btrfs filesystem."""
        # Create a subvolume as source
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )

        dest = btrfs_volume / "backups"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(dest),
                "snap_prefix": "test-",
                "fs_checks": "auto",
            }
        )

        # Should succeed - source is a subvolume on btrfs
        endpoint.prepare()

        # Cleanup
        delete_subvolume(source)

    def test_endpoint_list_snapshots_empty(self, btrfs_volume: Path):
        """Test listing snapshots when none exist."""
        dest = btrfs_volume / "backups"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
                "snap_prefix": "test-",
            }
        )

        snapshots = endpoint.list_snapshots()
        assert snapshots == []

    def test_endpoint_list_snapshots_finds_real_snapshots(self, btrfs_volume: Path):
        """Test listing real btrfs snapshots."""
        # Create source subvolume
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )
        (source / "data.txt").write_text("Test data")

        # Create snapshot directory
        snap_dir = btrfs_volume / "snapshots"
        snap_dir.mkdir()

        # Create a real snapshot with proper naming
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        snap_name = f"test-{timestamp}"
        snap_path = snap_dir / snap_name
        create_snapshot(source, snap_path, readonly=True)

        # Create endpoint and list
        endpoint = LocalEndpoint(
            config={
                "path": str(snap_dir),
                "snap_prefix": "test-",
            }
        )

        snapshots = endpoint.list_snapshots()

        assert len(snapshots) == 1
        assert snapshots[0].get_name() == snap_name

        # Cleanup
        delete_subvolume(snap_path)
        delete_subvolume(source)

    def test_endpoint_create_snapshot(self, btrfs_volume: Path):
        """Test creating a snapshot via endpoint.

        Note: We test the snapshot creation directly rather than through
        endpoint.snapshot() because that method tries to remount the source,
        which doesn't work for subvolumes (only mount points).
        """
        # Create source subvolume
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )
        (source / "important.txt").write_text("Important data")

        # Create snapshot directory
        snap_dir = btrfs_volume / "snapshots"
        snap_dir.mkdir(parents=True)

        # Create snapshot directly using btrfs command (simulating what endpoint does)
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        snap_name = f"backup-{timestamp}"
        snap_path = snap_dir / snap_name

        subprocess.run(
            ["btrfs", "subvolume", "snapshot", "-r", str(source), str(snap_path)],
            check=True,
            capture_output=True,
        )

        # Now verify endpoint can find and work with it
        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(snap_dir),
                "snap_prefix": "backup-",
            }
        )

        # List should find our snapshot
        snapshots = endpoint.list_snapshots()
        assert len(snapshots) == 1
        assert snapshots[0].get_name() == snap_name

        # Verify it exists and has our data
        assert snap_path.exists()
        assert (snap_path / "important.txt").exists()
        assert (snap_path / "important.txt").read_text() == "Important data"

        # Verify readonly
        result = subprocess.run(
            ["btrfs", "property", "get", str(snap_path), "ro"],
            capture_output=True,
            text=True,
        )
        assert "ro=true" in result.stdout

        # Cleanup
        delete_subvolume(snap_path)
        delete_subvolume(source)

    def test_endpoint_delete_snapshot(self, btrfs_volume: Path):
        """Test deleting a snapshot via endpoint."""
        # Create source and snapshot
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )

        snap_dir = btrfs_volume / "snapshots"
        snap_dir.mkdir()

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        snap_path = snap_dir / f"test-{timestamp}"
        create_snapshot(source, snap_path, readonly=True)

        # Create endpoint
        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(snap_dir),
                "snap_prefix": "test-",
            }
        )

        # List and delete
        snapshots = endpoint.list_snapshots()
        assert len(snapshots) == 1

        endpoint.delete_snapshots(snapshots)

        # Should be gone
        assert not snap_path.exists()
        snapshots = endpoint.list_snapshots(flush_cache=True)
        assert len(snapshots) == 0

        # Cleanup
        delete_subvolume(source)


@pytest.mark.tier2
@requires_btrfs
class TestEndpointSendReceiveReal:
    """Test send/receive operations through endpoints."""

    def test_endpoint_send_receive_full(self, btrfs_source_and_dest: tuple[Path, Path]):
        """Test full send/receive through endpoints."""
        source_vol, dest_vol = btrfs_source_and_dest

        # Create source subvolume with data
        source_subvol = source_vol / "data"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source_subvol)],
            check=True,
            capture_output=True,
        )
        (source_subvol / "file.txt").write_text("Transfer test")

        # Create snapshot
        snap_dir = source_vol / "snapshots"
        snap_dir.mkdir()
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        snap_path = snap_dir / f"backup-{timestamp}"
        create_snapshot(source_subvol, snap_path, readonly=True)

        # Create source endpoint
        source_endpoint = LocalEndpoint(
            config={
                "source": str(source_subvol),
                "path": str(snap_dir),
                "snap_prefix": "backup-",
            }
        )

        # Create destination endpoint
        dest_dir = dest_vol / "backups"
        dest_dir.mkdir()

        dest_endpoint = LocalEndpoint(
            config={
                "path": str(dest_dir),
                "snap_prefix": "backup-",
            }
        )
        dest_endpoint.prepare()

        # Get snapshot object
        snapshots = source_endpoint.list_snapshots()
        assert len(snapshots) == 1
        snapshot = snapshots[0]

        # Send via endpoint
        send_proc = source_endpoint.send(snapshot)

        # Receive via endpoint
        recv_proc = dest_endpoint.receive(send_proc.stdout)
        recv_proc.wait()
        send_proc.wait()

        assert send_proc.returncode == 0
        assert recv_proc.returncode == 0

        # Verify received
        received_snap = dest_dir / f"backup-{timestamp}"
        assert received_snap.exists()
        assert (received_snap / "file.txt").read_text() == "Transfer test"

        # Cleanup
        delete_subvolume(received_snap)
        delete_subvolume(snap_path)
        delete_subvolume(source_subvol)

    def test_endpoint_send_receive_incremental(
        self, btrfs_source_and_dest: tuple[Path, Path]
    ):
        """Test incremental send/receive through endpoints."""
        source_vol, dest_vol = btrfs_source_and_dest

        # Create source subvolume
        source_subvol = source_vol / "data"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source_subvol)],
            check=True,
            capture_output=True,
        )

        snap_dir = source_vol / "snapshots"
        snap_dir.mkdir()

        dest_dir = dest_vol / "backups"
        dest_dir.mkdir()

        source_endpoint = LocalEndpoint(
            config={
                "source": str(source_subvol),
                "path": str(snap_dir),
                "snap_prefix": "backup-",
            }
        )

        dest_endpoint = LocalEndpoint(
            config={
                "path": str(dest_dir),
                "snap_prefix": "backup-",
            }
        )
        dest_endpoint.prepare()

        # First snapshot
        (source_subvol / "file1.txt").write_text("First file")
        snap1_path = snap_dir / "backup-20240101-120000"
        create_snapshot(source_subvol, snap1_path, readonly=True)

        # Send first (full)
        snapshots = source_endpoint.list_snapshots()
        snap1 = snapshots[0]

        send_proc = source_endpoint.send(snap1)
        recv_proc = dest_endpoint.receive(send_proc.stdout)
        recv_proc.wait()
        send_proc.wait()

        assert send_proc.returncode == 0

        # Second snapshot with additional data
        (source_subvol / "file2.txt").write_text("Second file")
        snap2_path = snap_dir / "backup-20240102-120000"
        create_snapshot(source_subvol, snap2_path, readonly=True)

        # Send second (incremental)
        source_endpoint.list_snapshots(flush_cache=True)
        snapshots = source_endpoint.list_snapshots()
        snap2 = [s for s in snapshots if "20240102" in s.get_name()][0]

        # Need to get the received snap1 as parent reference for dest
        dest_endpoint.list_snapshots(flush_cache=True)
        dest_snaps = dest_endpoint.list_snapshots()
        dest_snaps[0]

        send_proc = source_endpoint.send(snap2, parent=snap1)
        recv_proc = dest_endpoint.receive(send_proc.stdout)
        recv_proc.wait()
        send_proc.wait()

        assert send_proc.returncode == 0

        # Verify both snapshots exist at destination
        dest_endpoint.list_snapshots(flush_cache=True)
        dest_snaps = dest_endpoint.list_snapshots()
        assert len(dest_snaps) == 2

        # Verify content
        received2 = dest_dir / "backup-20240102-120000"
        assert (received2 / "file1.txt").exists()
        assert (received2 / "file2.txt").exists()

        # Cleanup
        for snap in dest_endpoint.list_snapshots():
            delete_subvolume(snap.get_path())
        delete_subvolume(snap2_path)
        delete_subvolume(snap1_path)
        delete_subvolume(source_subvol)


@pytest.mark.tier2
@requires_btrfs
class TestEndpointLockingReal:
    """Test snapshot locking with real btrfs."""

    def test_locked_snapshot_not_deleted(self, btrfs_volume: Path):
        """Test that locked snapshots are not deleted."""
        # Create source and snapshot
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )

        snap_dir = btrfs_volume / "snapshots"
        snap_dir.mkdir()

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        snap_path = snap_dir / f"test-{timestamp}"
        create_snapshot(source, snap_path, readonly=True)

        # Create endpoint
        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(snap_dir),
                "snap_prefix": "test-",
            }
        )

        # Get snapshot and lock it
        snapshots = endpoint.list_snapshots()
        assert len(snapshots) == 1
        snapshot = snapshots[0]

        # Add a lock
        endpoint.set_lock(snapshot, "test-lock", True)

        # Try to delete - should be skipped
        endpoint.delete_snapshots([snapshot])

        # Should still exist
        assert snap_path.exists()

        # Remove lock
        endpoint.set_lock(snapshot, "test-lock", False)

        # Now delete should work
        endpoint.delete_snapshots([snapshot])
        assert not snap_path.exists()

        # Cleanup
        delete_subvolume(source)


@pytest.mark.tier2
@requires_btrfs
class TestDeleteOldSnapshots:
    """Test old snapshot deletion functionality."""

    def test_delete_old_keeps_newest(self, btrfs_volume: Path):
        """Test that delete_old_snapshots keeps the newest snapshots."""
        # Create source
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )

        snap_dir = btrfs_volume / "snapshots"
        snap_dir.mkdir()

        # Create 5 snapshots with different timestamps
        snap_paths = []
        for i in range(5):
            snap_path = snap_dir / f"test-2024010{i + 1}-120000"
            create_snapshot(source, snap_path, readonly=True)
            snap_paths.append(snap_path)

        # Create endpoint
        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(snap_dir),
                "snap_prefix": "test-",
            }
        )

        snapshots = endpoint.list_snapshots()
        assert len(snapshots) == 5

        # Delete old, keeping 2
        endpoint.delete_old_snapshots(keep=2)

        # Should have 2 remaining (the newest ones)
        endpoint.list_snapshots(flush_cache=True)
        remaining = endpoint.list_snapshots()
        assert len(remaining) == 2

        # Should be the newest two
        names = [s.get_name() for s in remaining]
        assert "test-20240105-120000" in names
        assert "test-20240104-120000" in names

        # Cleanup
        for snap in remaining:
            delete_subvolume(snap.get_path())
        delete_subvolume(source)
