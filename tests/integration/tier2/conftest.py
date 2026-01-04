"""Fixtures for Tier 2 btrfs integration tests.

Provides loopback btrfs filesystems for testing real btrfs operations.
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Generator

import pytest


def has_btrfs_support() -> bool:
    """Check if system has btrfs support."""
    # Check for btrfs command
    if not shutil.which("btrfs"):
        return False

    # Check for mkfs.btrfs
    if not shutil.which("mkfs.btrfs"):
        return False

    # Check if we're root (needed for mount)
    if os.geteuid() != 0:
        return False

    # Check if btrfs module is available
    try:
        result = subprocess.run(
            ["modprobe", "btrfs"],
            capture_output=True,
            timeout=10,
        )
        # Module load might fail but btrfs could still be built-in
    except Exception:
        pass

    return True


# Skip marker for all tier2 tests
requires_btrfs = pytest.mark.skipif(
    not has_btrfs_support(),
    reason="Requires btrfs-progs and root privileges",
)


class LoopbackBtrfs:
    """Context manager for a loopback-mounted btrfs filesystem."""

    def __init__(
        self,
        size_mb: int = 256,
        label: str = "test",
        base_dir: Path | None = None,
    ):
        self.size_mb = size_mb
        self.label = label
        self.base_dir = base_dir
        self.temp_dir: Path | None = None
        self.image_path: Path | None = None
        self.mount_point: Path | None = None
        self.loop_device: str | None = None

    def __enter__(self) -> Path:
        """Create and mount the loopback btrfs filesystem."""
        # Create temp directory
        if self.base_dir:
            self.temp_dir = Path(tempfile.mkdtemp(dir=self.base_dir))
        else:
            self.temp_dir = Path(tempfile.mkdtemp(prefix="btrfs_test_"))

        self.image_path = self.temp_dir / f"{self.label}.img"
        self.mount_point = self.temp_dir / "mnt"
        self.mount_point.mkdir()

        try:
            # Create sparse file
            subprocess.run(
                ["truncate", "-s", f"{self.size_mb}M", str(self.image_path)],
                check=True,
                capture_output=True,
            )

            # Format as btrfs
            subprocess.run(
                ["mkfs.btrfs", "-L", self.label, str(self.image_path)],
                check=True,
                capture_output=True,
            )

            # Set up loop device
            result = subprocess.run(
                ["losetup", "--find", "--show", str(self.image_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            self.loop_device = result.stdout.strip()

            # Mount the filesystem
            subprocess.run(
                ["mount", self.loop_device, str(self.mount_point)],
                check=True,
                capture_output=True,
            )

            return self.mount_point

        except subprocess.CalledProcessError as e:
            self._cleanup()
            raise RuntimeError(f"Failed to create loopback btrfs: {e.stderr}") from e

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Unmount and clean up."""
        self._cleanup()
        return False

    def _cleanup(self):
        """Clean up mount, loop device, and temp files."""
        # Unmount
        if self.mount_point and self.mount_point.exists():
            try:
                # Sync first to ensure all data is written
                subprocess.run(["sync"], check=False)
                subprocess.run(
                    ["umount", str(self.mount_point)],
                    check=False,
                    capture_output=True,
                )
            except Exception:
                pass

        # Detach loop device
        if self.loop_device:
            try:
                subprocess.run(
                    ["losetup", "-d", self.loop_device],
                    check=False,
                    capture_output=True,
                )
            except Exception:
                pass

        # Remove temp directory
        if self.temp_dir and self.temp_dir.exists():
            try:
                shutil.rmtree(self.temp_dir)
            except Exception:
                pass


@pytest.fixture
def btrfs_volume() -> Generator[Path, None, None]:
    """Create a single loopback btrfs volume for testing.

    Yields the mount point path.
    """
    with LoopbackBtrfs(size_mb=256, label="source") as mount_point:
        yield mount_point


@pytest.fixture
def btrfs_source_and_dest() -> Generator[tuple[Path, Path], None, None]:
    """Create source and destination btrfs volumes for transfer testing.

    Yields a tuple of (source_mount, dest_mount).
    """
    with LoopbackBtrfs(size_mb=256, label="source") as source:
        with LoopbackBtrfs(size_mb=256, label="dest") as dest:
            yield source, dest


@pytest.fixture
def btrfs_subvolume(btrfs_volume: Path) -> Generator[Path, None, None]:
    """Create a btrfs subvolume within the test volume.

    Yields the subvolume path.
    """
    subvol_path = btrfs_volume / "data"

    subprocess.run(
        ["btrfs", "subvolume", "create", str(subvol_path)],
        check=True,
        capture_output=True,
    )

    yield subvol_path

    # Cleanup - delete the subvolume
    try:
        subprocess.run(
            ["btrfs", "subvolume", "delete", str(subvol_path)],
            check=False,
            capture_output=True,
        )
    except Exception:
        pass


@pytest.fixture
def btrfs_with_data(btrfs_subvolume: Path) -> Generator[Path, None, None]:
    """Create a btrfs subvolume with some test data.

    Yields the subvolume path containing test files.
    """
    # Create some test files
    (btrfs_subvolume / "file1.txt").write_text("Hello, World!")
    (btrfs_subvolume / "file2.txt").write_text("Test data for btrfs backup")

    subdir = btrfs_subvolume / "subdir"
    subdir.mkdir()
    (subdir / "nested.txt").write_text("Nested file content")

    # Create a larger file for transfer testing
    large_file = btrfs_subvolume / "large.bin"
    large_file.write_bytes(os.urandom(1024 * 100))  # 100KB

    yield btrfs_subvolume


def create_snapshot(source: Path, dest: Path, readonly: bool = True) -> Path:
    """Helper to create a btrfs snapshot.

    Args:
        source: Source subvolume path
        dest: Destination snapshot path
        readonly: Whether to create a readonly snapshot

    Returns:
        Path to the created snapshot
    """
    cmd = ["btrfs", "subvolume", "snapshot"]
    if readonly:
        cmd.append("-r")
    cmd.extend([str(source), str(dest)])

    subprocess.run(cmd, check=True, capture_output=True)
    return dest


def delete_subvolume(path: Path) -> None:
    """Helper to delete a btrfs subvolume."""
    subprocess.run(
        ["btrfs", "subvolume", "delete", str(path)],
        check=True,
        capture_output=True,
    )


def send_snapshot(snapshot: Path, dest_path: Path, parent: Path | None = None) -> None:
    """Helper to send a snapshot to a destination.

    Args:
        snapshot: Snapshot to send
        dest_path: Destination directory for receive
        parent: Optional parent snapshot for incremental send
    """
    send_cmd = ["btrfs", "send"]
    if parent:
        send_cmd.extend(["-p", str(parent)])
    send_cmd.append(str(snapshot))

    recv_cmd = ["btrfs", "receive", str(dest_path)]

    # Pipe send to receive
    send_proc = subprocess.Popen(send_cmd, stdout=subprocess.PIPE)
    recv_proc = subprocess.Popen(recv_cmd, stdin=send_proc.stdout)

    send_proc.stdout.close()
    recv_proc.communicate()

    if send_proc.wait() != 0:
        raise RuntimeError("btrfs send failed")
    if recv_proc.returncode != 0:
        raise RuntimeError("btrfs receive failed")
