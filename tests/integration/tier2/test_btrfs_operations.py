"""Tier 2 tests for real btrfs snapshot, send, and receive operations.

These tests verify that the core btrfs operations work correctly
using loopback filesystems.
"""

import subprocess
from pathlib import Path

import pytest

from .conftest import (
    create_snapshot,
    delete_subvolume,
    requires_btrfs,
    send_snapshot,
)


@pytest.mark.tier2
@requires_btrfs
class TestBtrfsSubvolumeOperations:
    """Test basic btrfs subvolume operations."""

    def test_create_subvolume(self, btrfs_volume: Path):
        """Test creating a btrfs subvolume."""
        subvol = btrfs_volume / "test_subvol"

        result = subprocess.run(
            ["btrfs", "subvolume", "create", str(subvol)],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert subvol.exists()
        assert subvol.is_dir()

        # Verify it's a subvolume
        result = subprocess.run(
            ["btrfs", "subvolume", "show", str(subvol)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

        # Cleanup
        delete_subvolume(subvol)

    def test_list_subvolumes(self, btrfs_subvolume: Path):
        """Test listing btrfs subvolumes."""
        result = subprocess.run(
            ["btrfs", "subvolume", "list", str(btrfs_subvolume.parent)],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "data" in result.stdout  # Our fixture creates "data" subvolume

    def test_delete_subvolume(self, btrfs_volume: Path):
        """Test deleting a btrfs subvolume."""
        subvol = btrfs_volume / "to_delete"

        # Create
        subprocess.run(
            ["btrfs", "subvolume", "create", str(subvol)],
            check=True,
            capture_output=True,
        )
        assert subvol.exists()

        # Delete
        result = subprocess.run(
            ["btrfs", "subvolume", "delete", str(subvol)],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert not subvol.exists()


@pytest.mark.tier2
@requires_btrfs
class TestBtrfsSnapshotOperations:
    """Test btrfs snapshot operations."""

    def test_create_readonly_snapshot(self, btrfs_subvolume: Path):
        """Test creating a readonly snapshot."""
        snapshot_path = btrfs_subvolume.parent / "snapshot_ro"

        create_snapshot(btrfs_subvolume, snapshot_path, readonly=True)

        assert snapshot_path.exists()

        # Verify it's readonly
        result = subprocess.run(
            ["btrfs", "property", "get", str(snapshot_path), "ro"],
            capture_output=True,
            text=True,
        )
        assert "ro=true" in result.stdout

        # Cleanup
        delete_subvolume(snapshot_path)

    def test_create_readwrite_snapshot(self, btrfs_subvolume: Path):
        """Test creating a read-write snapshot."""
        snapshot_path = btrfs_subvolume.parent / "snapshot_rw"

        create_snapshot(btrfs_subvolume, snapshot_path, readonly=False)

        assert snapshot_path.exists()

        # Verify it's read-write
        result = subprocess.run(
            ["btrfs", "property", "get", str(snapshot_path), "ro"],
            capture_output=True,
            text=True,
        )
        assert "ro=false" in result.stdout

        # Cleanup
        delete_subvolume(snapshot_path)

    def test_snapshot_preserves_data(self, btrfs_with_data: Path):
        """Test that snapshots preserve file contents."""
        # Write unique content
        test_file = btrfs_with_data / "unique_content.txt"
        test_file.write_text("Unique test content 12345")

        # Create snapshot
        snapshot_path = btrfs_with_data.parent / "data_snapshot"
        create_snapshot(btrfs_with_data, snapshot_path, readonly=True)

        # Verify content in snapshot
        snapshot_file = snapshot_path / "unique_content.txt"
        assert snapshot_file.exists()
        assert snapshot_file.read_text() == "Unique test content 12345"

        # Modify original
        test_file.write_text("Modified content")

        # Snapshot should still have original content
        assert snapshot_file.read_text() == "Unique test content 12345"

        # Cleanup
        delete_subvolume(snapshot_path)

    def test_multiple_snapshots(self, btrfs_with_data: Path):
        """Test creating multiple snapshots."""
        snapshots = []

        for i in range(3):
            # Modify data between snapshots
            (btrfs_with_data / f"file_{i}.txt").write_text(f"Content {i}")

            snapshot_path = btrfs_with_data.parent / f"snapshot_{i}"
            create_snapshot(btrfs_with_data, snapshot_path, readonly=True)
            snapshots.append(snapshot_path)

        # Verify each snapshot has correct files
        assert not (snapshots[0] / "file_1.txt").exists()
        assert (snapshots[1] / "file_1.txt").exists()
        assert (snapshots[2] / "file_2.txt").exists()

        # Cleanup
        for snap in snapshots:
            delete_subvolume(snap)


@pytest.mark.tier2
@requires_btrfs
class TestBtrfsSendReceive:
    """Test btrfs send/receive operations."""

    def test_full_send_receive(self, btrfs_source_and_dest: tuple[Path, Path]):
        """Test sending a full snapshot to another volume."""
        source, dest = btrfs_source_and_dest

        # Create source subvolume with data
        source_subvol = source / "data"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source_subvol)],
            check=True,
            capture_output=True,
        )
        (source_subvol / "test.txt").write_text("Test data for transfer")

        # Create snapshot
        snapshot = source / "snapshot_1"
        create_snapshot(source_subvol, snapshot, readonly=True)

        # Send to destination
        send_snapshot(snapshot, dest)

        # Verify received snapshot
        received = dest / "snapshot_1"
        assert received.exists()
        assert (received / "test.txt").read_text() == "Test data for transfer"

        # Cleanup
        delete_subvolume(received)
        delete_subvolume(snapshot)
        delete_subvolume(source_subvol)

    def test_incremental_send_receive(self, btrfs_source_and_dest: tuple[Path, Path]):
        """Test incremental send/receive between snapshots."""
        source, dest = btrfs_source_and_dest

        # Create source subvolume
        source_subvol = source / "data"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source_subvol)],
            check=True,
            capture_output=True,
        )

        # Initial data and snapshot
        (source_subvol / "file1.txt").write_text("Initial content")
        snapshot1 = source / "snapshot_1"
        create_snapshot(source_subvol, snapshot1, readonly=True)

        # Send first snapshot (full)
        send_snapshot(snapshot1, dest)

        # Add more data and create second snapshot
        (source_subvol / "file2.txt").write_text("Additional content")
        snapshot2 = source / "snapshot_2"
        create_snapshot(source_subvol, snapshot2, readonly=True)

        # Send second snapshot incrementally
        received1 = dest / "snapshot_1"
        send_snapshot(snapshot2, dest, parent=snapshot1)

        # Verify both snapshots on destination
        received2 = dest / "snapshot_2"
        assert received1.exists()
        assert received2.exists()

        # First snapshot should have only file1
        assert (received1 / "file1.txt").exists()
        assert not (received1 / "file2.txt").exists()

        # Second snapshot should have both
        assert (received2 / "file1.txt").exists()
        assert (received2 / "file2.txt").exists()

        # Cleanup
        delete_subvolume(received2)
        delete_subvolume(received1)
        delete_subvolume(snapshot2)
        delete_subvolume(snapshot1)
        delete_subvolume(source_subvol)

    def test_send_receive_with_nested_subvolumes(
        self, btrfs_source_and_dest: tuple[Path, Path]
    ):
        """Test send/receive with nested directory structure."""
        source, dest = btrfs_source_and_dest

        # Create source with nested structure
        source_subvol = source / "data"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source_subvol)],
            check=True,
            capture_output=True,
        )

        # Create nested directories
        nested = source_subvol / "level1" / "level2" / "level3"
        nested.mkdir(parents=True)
        (nested / "deep_file.txt").write_text("Deep nested content")

        # Create snapshot and send
        snapshot = source / "nested_snapshot"
        create_snapshot(source_subvol, snapshot, readonly=True)
        send_snapshot(snapshot, dest)

        # Verify structure preserved
        received = dest / "nested_snapshot"
        deep_file = received / "level1" / "level2" / "level3" / "deep_file.txt"
        assert deep_file.exists()
        assert deep_file.read_text() == "Deep nested content"

        # Cleanup
        delete_subvolume(received)
        delete_subvolume(snapshot)
        delete_subvolume(source_subvol)

    def test_send_receive_large_file(self, btrfs_source_and_dest: tuple[Path, Path]):
        """Test send/receive with larger files."""
        import os

        source, dest = btrfs_source_and_dest

        # Create source with large file
        source_subvol = source / "data"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source_subvol)],
            check=True,
            capture_output=True,
        )

        # Create 1MB file
        large_file = source_subvol / "large.bin"
        data = os.urandom(1024 * 1024)
        large_file.write_bytes(data)

        # Snapshot and send
        snapshot = source / "large_snapshot"
        create_snapshot(source_subvol, snapshot, readonly=True)
        send_snapshot(snapshot, dest)

        # Verify
        received = dest / "large_snapshot"
        received_file = received / "large.bin"
        assert received_file.exists()
        assert received_file.read_bytes() == data

        # Cleanup
        delete_subvolume(received)
        delete_subvolume(snapshot)
        delete_subvolume(source_subvol)


@pytest.mark.tier2
@requires_btrfs
class TestBtrfsPropertyOperations:
    """Test btrfs property operations."""

    def test_set_readonly_property(self, btrfs_subvolume: Path):
        """Test setting readonly property on a subvolume."""
        # Initially read-write
        result = subprocess.run(
            ["btrfs", "property", "get", str(btrfs_subvolume), "ro"],
            capture_output=True,
            text=True,
        )
        assert "ro=false" in result.stdout

        # Set to readonly
        subprocess.run(
            ["btrfs", "property", "set", str(btrfs_subvolume), "ro", "true"],
            check=True,
            capture_output=True,
        )

        result = subprocess.run(
            ["btrfs", "property", "get", str(btrfs_subvolume), "ro"],
            capture_output=True,
            text=True,
        )
        assert "ro=true" in result.stdout

        # Set back to read-write for cleanup
        subprocess.run(
            ["btrfs", "property", "set", str(btrfs_subvolume), "ro", "false"],
            check=True,
            capture_output=True,
        )

    def test_convert_readonly_to_readwrite(self, btrfs_with_data: Path):
        """Test converting a readonly snapshot to read-write."""
        # Create readonly snapshot
        snapshot = btrfs_with_data.parent / "ro_snapshot"
        create_snapshot(btrfs_with_data, snapshot, readonly=True)

        # Verify readonly
        result = subprocess.run(
            ["btrfs", "property", "get", str(snapshot), "ro"],
            capture_output=True,
            text=True,
        )
        assert "ro=true" in result.stdout

        # Convert to read-write
        subprocess.run(
            ["btrfs", "property", "set", "-ts", str(snapshot), "ro", "false"],
            check=True,
            capture_output=True,
        )

        # Should be writable now
        test_file = snapshot / "new_file.txt"
        test_file.write_text("Written after conversion")
        assert test_file.read_text() == "Written after conversion"

        # Cleanup
        delete_subvolume(snapshot)
