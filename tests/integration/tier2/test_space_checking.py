"""Tier 2 tests for space checking with real btrfs filesystems.

These tests verify that space checking works correctly with actual
btrfs filesystems using loopback devices, including quota support.
"""

import subprocess
from pathlib import Path

import pytest

from btrfs_backup_ng.core.space import (
    SpaceInfo,
    check_space_availability,
    format_space_check,
    get_btrfs_quota_info,
    get_filesystem_space,
    get_space_info,
)

from .conftest import (
    delete_subvolume,
    requires_btrfs,
)


@pytest.mark.tier2
@requires_btrfs
class TestGetFilesystemSpaceReal:
    """Test get_filesystem_space with real filesystems."""

    def test_gets_space_info(self, btrfs_volume: Path):
        """Test getting space info from a real btrfs volume."""
        total, used, available = get_filesystem_space(str(btrfs_volume))

        assert total > 0
        assert used >= 0
        assert available > 0
        assert total >= used + available  # May not be exact due to reserved space

    def test_space_changes_after_write(self, btrfs_volume: Path):
        """Test that space info reflects written data."""
        _, _, available_before = get_filesystem_space(str(btrfs_volume))

        # Write 1MB of data
        test_file = btrfs_volume / "test_file.bin"
        test_file.write_bytes(b"\x00" * (1024 * 1024))

        # Sync to ensure data is written
        subprocess.run(["sync"], check=True)

        _, _, available_after = get_filesystem_space(str(btrfs_volume))

        # Available space should decrease
        assert available_after < available_before

        # Cleanup
        test_file.unlink()


@pytest.mark.tier2
@requires_btrfs
class TestBtrfsQuotaReal:
    """Test btrfs quota operations with real filesystems."""

    def test_quotas_disabled_by_default(self, btrfs_volume: Path):
        """Test that quotas are disabled by default."""
        result = get_btrfs_quota_info(str(btrfs_volume))
        # Should return None when quotas are not enabled
        assert result is None

    def test_enable_and_query_quotas(self, btrfs_volume: Path):
        """Test enabling quotas and querying them."""
        # Enable quotas
        subprocess.run(
            ["btrfs", "quota", "enable", str(btrfs_volume)],
            check=True,
            capture_output=True,
        )

        try:
            # Create a subvolume to get qgroup for
            subvol = btrfs_volume / "quota_test"
            subprocess.run(
                ["btrfs", "subvolume", "create", str(subvol)],
                check=True,
                capture_output=True,
            )

            # Rescan quotas
            subprocess.run(
                ["btrfs", "quota", "rescan", "-w", str(btrfs_volume)],
                check=True,
                capture_output=True,
            )

            # Query quota info
            result = get_btrfs_quota_info(str(subvol))

            # Should now return quota info (limit=None means unlimited)
            assert result is not None
            limit, used = result
            # No limit set, so limit should be None
            assert limit is None
            assert used >= 0

            # Cleanup subvolume
            delete_subvolume(subvol)

        finally:
            # Disable quotas
            subprocess.run(
                ["btrfs", "quota", "disable", str(btrfs_volume)],
                check=False,
                capture_output=True,
            )

    def test_quota_limit_enforcement(self, btrfs_volume: Path):
        """Test setting and querying a quota limit."""
        # Enable quotas
        subprocess.run(
            ["btrfs", "quota", "enable", str(btrfs_volume)],
            check=True,
            capture_output=True,
        )

        try:
            # Create a subvolume
            subvol = btrfs_volume / "limited_subvol"
            subprocess.run(
                ["btrfs", "subvolume", "create", str(subvol)],
                check=True,
                capture_output=True,
            )

            # Get the subvolume ID for quota assignment
            result = subprocess.run(
                ["btrfs", "subvolume", "show", str(subvol)],
                capture_output=True,
                text=True,
            )
            # Parse subvolume ID from output
            for line in result.stdout.splitlines():
                if "Subvolume ID:" in line:
                    subvol_id = line.split()[-1]
                    break
            else:
                pytest.skip("Could not determine subvolume ID")

            # Set a 100MB quota limit
            qgroup = f"0/{subvol_id}"
            subprocess.run(
                ["btrfs", "qgroup", "limit", "100M", qgroup, str(btrfs_volume)],
                check=True,
                capture_output=True,
            )

            # Rescan quotas
            subprocess.run(
                ["btrfs", "quota", "rescan", "-w", str(btrfs_volume)],
                check=True,
                capture_output=True,
            )

            # Query quota info
            result = get_btrfs_quota_info(str(subvol))

            assert result is not None
            limit, used = result
            # Should have 100MB limit (104857600 bytes)
            assert limit == 100 * 1024 * 1024
            assert used >= 0

            # Cleanup
            delete_subvolume(subvol)

        finally:
            subprocess.run(
                ["btrfs", "quota", "disable", str(btrfs_volume)],
                check=False,
                capture_output=True,
            )


@pytest.mark.tier2
@requires_btrfs
class TestGetSpaceInfoReal:
    """Test get_space_info with real filesystems."""

    def test_space_info_without_quotas(self, btrfs_volume: Path):
        """Test getting complete space info without quotas."""
        info = get_space_info(str(btrfs_volume))

        assert isinstance(info, SpaceInfo)
        assert info.path == str(btrfs_volume)
        assert info.total_bytes > 0
        assert info.available_bytes > 0
        assert info.quota_enabled is False
        assert info.source == "statvfs"

    def test_space_info_with_quotas(self, btrfs_volume: Path):
        """Test getting complete space info with quotas enabled."""
        # Enable quotas
        subprocess.run(
            ["btrfs", "quota", "enable", str(btrfs_volume)],
            check=True,
            capture_output=True,
        )

        try:
            # Create a subvolume
            subvol = btrfs_volume / "info_test"
            subprocess.run(
                ["btrfs", "subvolume", "create", str(subvol)],
                check=True,
                capture_output=True,
            )

            # Rescan
            subprocess.run(
                ["btrfs", "quota", "rescan", "-w", str(btrfs_volume)],
                check=True,
                capture_output=True,
            )

            info = get_space_info(str(subvol))

            assert isinstance(info, SpaceInfo)
            assert info.quota_enabled is True
            assert info.source == "statvfs+btrfs_qgroup"
            # No limit set, so effective_available should be filesystem available
            assert info.effective_available == info.available_bytes

            # Cleanup
            delete_subvolume(subvol)

        finally:
            subprocess.run(
                ["btrfs", "quota", "disable", str(btrfs_volume)],
                check=False,
                capture_output=True,
            )


@pytest.mark.tier2
@requires_btrfs
class TestCheckSpaceAvailabilityReal:
    """Test space availability checking with real filesystems."""

    def test_sufficient_space(self, btrfs_volume: Path):
        """Test check passes when there is sufficient space."""
        info = get_space_info(str(btrfs_volume))

        # Request 1MB, should have plenty of space in 256MB volume
        check = check_space_availability(info, 1024 * 1024)

        assert check.sufficient is True
        assert check.warning_message is None

    def test_insufficient_space(self, btrfs_volume: Path):
        """Test check fails when requesting more than available."""
        info = get_space_info(str(btrfs_volume))

        # Request more than the volume size
        check = check_space_availability(info, info.total_bytes * 2)

        assert check.sufficient is False
        assert check.warning_message is not None
        assert "Insufficient" in check.warning_message

    def test_quota_restricts_space(self, btrfs_volume: Path):
        """Test that quota limits restrict available space."""
        # Enable quotas
        subprocess.run(
            ["btrfs", "quota", "enable", str(btrfs_volume)],
            check=True,
            capture_output=True,
        )

        try:
            # Create a subvolume
            subvol = btrfs_volume / "quota_check"
            subprocess.run(
                ["btrfs", "subvolume", "create", str(subvol)],
                check=True,
                capture_output=True,
            )

            # Get subvolume ID
            result = subprocess.run(
                ["btrfs", "subvolume", "show", str(subvol)],
                capture_output=True,
                text=True,
            )
            for line in result.stdout.splitlines():
                if "Subvolume ID:" in line:
                    subvol_id = line.split()[-1]
                    break
            else:
                pytest.skip("Could not determine subvolume ID")

            # Set a 10MB quota limit
            qgroup = f"0/{subvol_id}"
            subprocess.run(
                ["btrfs", "qgroup", "limit", "10M", qgroup, str(btrfs_volume)],
                check=True,
                capture_output=True,
            )

            # Rescan
            subprocess.run(
                ["btrfs", "quota", "rescan", "-w", str(btrfs_volume)],
                check=True,
                capture_output=True,
            )

            info = get_space_info(str(subvol))

            # Quota should restrict effective available
            assert info.quota_enabled is True
            assert info.quota_limit == 10 * 1024 * 1024
            assert info.effective_available <= 10 * 1024 * 1024

            # Request 20MB - should fail due to 10MB quota
            check = check_space_availability(info, 20 * 1024 * 1024)
            assert check.sufficient is False

            # Request 1MB - should succeed
            check = check_space_availability(info, 1024 * 1024, min_safety_bytes=0)
            assert check.sufficient is True

            # Cleanup
            delete_subvolume(subvol)

        finally:
            subprocess.run(
                ["btrfs", "quota", "disable", str(btrfs_volume)],
                check=False,
                capture_output=True,
            )


@pytest.mark.tier2
@requires_btrfs
class TestFormatSpaceCheckReal:
    """Test formatting space check results with real data."""

    def test_format_real_check(self, btrfs_volume: Path):
        """Test formatting a real space check result."""
        info = get_space_info(str(btrfs_volume))
        check = check_space_availability(info, 10 * 1024 * 1024)  # 10MB

        formatted = format_space_check(check)

        assert "Destination Space Check" in formatted
        assert "Filesystem space:" in formatted
        assert "Required:" in formatted
        assert "Status:" in formatted
        # Should show OK for 10MB in 256MB volume
        assert "OK" in formatted or "Sufficient" in formatted
