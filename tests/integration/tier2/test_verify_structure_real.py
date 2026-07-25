"""Tier 2 tests for R8b metadata structural validation with real btrfs.

The unit tests (tests/test_verify.py::TestVerifyMetadataStructural) cover the cases
reachable without root -- a plain directory is 'invalid', raw sidecar/parent logic. These
cover the case that needs a REAL received subvolume: a genuinely received read-only
subvolume must verify 'ok', and a locally-created (non-received) subvolume must be
'unverifiable' (a subvolume, but not a backup this tool produced) -- never a false pass.
"""

import subprocess

import pytest

from btrfs_backup_ng.core.verify import verify_metadata
from btrfs_backup_ng.endpoint.local import LocalEndpoint

from .conftest import create_snapshot, requires_btrfs, send_snapshot


@pytest.mark.tier2
@requires_btrfs
class TestVerifyStructureRealBtrfs:
    """endpoint.verify_structure / verify_metadata against real btrfs subvolumes."""

    def test_received_subvolume_verifies_ok(self, btrfs_source_and_dest):
        """A real RECEIVED read-only subvolume (non-empty received_uuid) -> structure ok,
        verify_metadata passes. Proves the received-backup path is not false-failed."""
        source, dest = btrfs_source_and_dest
        subvol = source / "data"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(subvol)],
            check=True,
            capture_output=True,
        )
        (subvol / "file.txt").write_text("payload")
        # A read-only snapshot named like a backup, then send/receive it into dest so the
        # received copy carries a received_uuid (only btrfs receive sets that).
        snap = source / "home-20260101-120000"
        create_snapshot(subvol, snap, readonly=True)
        send_snapshot(snap, dest)

        ep = LocalEndpoint(config={"path": str(dest), "snap_prefix": "home-"})
        (received,) = ep.list_snapshots(flush_cache=True)
        verdict = ep.verify_structure(received)
        assert verdict.status == "ok", verdict.message

        report = verify_metadata(ep)
        assert report.passed == 1 and report.failed == 0
        assert report.results[0].details["structure"] == "ok"

    def test_non_received_subvolume_is_unverifiable(self, btrfs_volume):
        """A locally-CREATED subvolume (never received, empty received_uuid) named like a
        backup -> 'unverifiable' (a subvolume, but not a received backup) -- NOT a false
        'ok', and NOT a failure. This is the masquerade the received_uuid check catches."""
        dest = btrfs_volume / "backups"
        dest.mkdir()
        local_subvol = dest / "home-20260101-120000"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(local_subvol)],
            check=True,
            capture_output=True,
        )

        ep = LocalEndpoint(config={"path": str(dest), "snap_prefix": "home-"})
        (snap,) = ep.list_snapshots(flush_cache=True)
        verdict = ep.verify_structure(snap)

        assert verdict.status == "unverifiable", verdict.message
        # Not a failure (never fail on inability to prove), but not a clean 'ok' either.
        report = verify_metadata(ep)
        assert report.failed == 0
        assert report.results[0].details["structure"] == "unverifiable"

    def test_plain_directory_on_btrfs_is_invalid(self, btrfs_volume):
        """A plain directory (not a subvolume) on a real btrfs filesystem -> 'invalid'
        (inode != 256), failing verify_metadata -- the interrupted-receive leftover case
        on the real filesystem type it occurs on."""
        dest = btrfs_volume / "backups"
        dest.mkdir()
        (dest / "home-20260101-120000").mkdir()  # a plain directory, not a subvolume

        ep = LocalEndpoint(config={"path": str(dest), "snap_prefix": "home-"})
        report = verify_metadata(ep)

        assert report.failed == 1 and report.passed == 0
        assert report.results[0].details["structure"] == "invalid"
