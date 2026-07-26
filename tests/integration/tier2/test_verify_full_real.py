"""Tier 2: verify_full does a REAL incremental-backup restore and passes (R8d).

The unit tests mock _test_restore (test theater). These do a genuine send/receive of a
base+incremental chain into a backup location, then run verify_full on the LATEST
(incremental) snapshot and assert it PASSES. Before R8d, verify_full sent `-p parent` into
the temp -> `btrfs receive` failed with "cannot find parent subvolume" -> a perfectly good
backup was reported FAIL (the F5 false-negative). R8d does a FULL send of the target, which
needs no parent present.

EMPIRICAL NOTE (verified on real btrfs, matching the R10b lesson): F5 only manifests when
the temp dir is on a DIFFERENT filesystem than the backup -- `btrfs receive` searches the
WHOLE destination fs for the parent's received_uuid, so an incremental into a temp on the
SAME fs as the backup finds the parent and (mis-)succeeds. It ALWAYS manifests for remote
backups (temp is local, backup is remote = different fs). So these tests deliberately put
the backup on the SOURCE loop fs and temp on the DEST loop fs (a separate filesystem), so
the old `-p` behavior genuinely fails and Option B (full send) genuinely fixes it.
"""

import subprocess
from unittest.mock import patch

import pytest

from btrfs_backup_ng.core.verify import verify_full
from btrfs_backup_ng.endpoint.local import LocalEndpoint

from .conftest import LoopbackBtrfs, create_snapshot, requires_btrfs, send_snapshot


def _run(*args):
    subprocess.run(list(args), check=True, capture_output=True)


@pytest.mark.tier2
@requires_btrfs
class TestVerifyFullRealBtrfs:
    """verify_full against a real received base+incremental backup chain."""

    def _build_backup(self, source):
        """Materialize a base + incremental RECEIVED backup chain under source/backup (on
        the SOURCE loop fs). The incremental SHARES a large extent with its base (the
        common case). Returns (backup_dir, latest_name)."""
        subvol = source / "data"
        _run("btrfs", "subvolume", "create", str(subvol))
        # A large shared file (unchanged between snapshots -> the incremental references
        # the parent's extents; this is what makes an -p receive need the parent present).
        _run(
            "dd",
            "if=/dev/urandom",
            f"of={subvol}/big",
            "bs=1M",
            "count=32",
            "status=none",
        )

        backup = source / "backup"
        backup.mkdir()

        base = source / "home-20260101-120000"
        create_snapshot(subvol, base, readonly=True)
        send_snapshot(base, backup)  # -> backup/home-20260101-120000 (received base)

        (subvol / "small").write_text("incremental change")  # big UNCHANGED (shared)
        incr = source / "home-20260102-120000"
        create_snapshot(subvol, incr, readonly=True)
        send_snapshot(incr, backup, parent=base)  # -> received incremental

        return backup, "home-20260102-120000"

    def test_verify_full_latest_incremental_passes(self, btrfs_source_and_dest):
        """The latest (incremental) backup verifies via a FULL send -> PASS, with temp on a
        SEPARATE fs from the backup. Mutation guard: re-introducing `-p parent` makes the
        incremental receive fail ('cannot find parent subvolume' on the temp fs) -> this
        asserts PASS, so the F5 regression is caught."""
        source, dest = btrfs_source_and_dest
        backup, latest = self._build_backup(source)
        temp = dest / "verify-temp"  # DEST fs -- a DIFFERENT filesystem than the backup
        temp.mkdir()

        ep = LocalEndpoint(config={"path": str(backup), "snap_prefix": "home-"})
        report = verify_full(ep, temp_dir=temp, cleanup=True)

        assert report.total == 1
        assert report.passed == 1 and report.failed == 0, report.results[0].message
        r = report.results[0]
        assert r.snapshot_name == latest
        assert r.details["status"] == "ok"
        assert r.details["full_send"] is True

    def test_verify_full_cleanup_no_leak_in_user_temp(self, btrfs_source_and_dest):
        """With a user-supplied --temp-dir, the restored subvolume is DELETED (no leak)
        while the user's temp DIRECTORY itself remains. Mutation guard: the old
        own_temp-gated deletion would leave the received subvolume behind."""
        source, dest = btrfs_source_and_dest
        backup, latest = self._build_backup(source)
        temp = dest / "user-temp"  # a DIFFERENT fs than the backup (source fs)
        temp.mkdir()

        ep = LocalEndpoint(config={"path": str(backup), "snap_prefix": "home-"})
        report = verify_full(ep, temp_dir=temp, cleanup=True)
        assert report.passed == 1, report.results[0].message

        restored = temp / latest
        assert not restored.exists(), "restored subvolume leaked into user --temp-dir"
        assert temp.exists(), "user-supplied temp dir must not be removed"

    def test_verify_full_non_subvolume_target_fails(self, btrfs_source_and_dest):
        """A backup entry that is NOT a real subvolume (e.g. an interrupted receive left a
        plain directory) -> `btrfs send` fails -> FAIL, not a false pass and not swallowed
        as unverifiable. Proves a genuine restore failure is caught."""
        _source, dest = btrfs_source_and_dest
        backup = dest / "backup"
        backup.mkdir()
        # A plain directory named like a snapshot (the interrupted-receive leftover).
        (backup / "home-20260101-120000").mkdir()

        ep = LocalEndpoint(config={"path": str(backup), "snap_prefix": "home-"})
        temp = dest / "verify-temp"
        temp.mkdir()
        report = verify_full(ep, temp_dir=temp, cleanup=True)

        assert report.failed == 1
        assert report.results[0].passed is False
        assert report.results[0].details["status"] == "failed"

    def test_verify_full_insufficient_temp_space_is_unverifiable(self):
        """A restore whose target does not fit in the temp filesystem -> UNVERIFIABLE, not
        FAIL. On real btrfs the transfer's own space preflight raises
        __util__.InsufficientSpaceError before the receive; verify_full must classify that
        (by TYPE -- its message is not 'no space left') as environmental, never as a backup
        failure. Guards the review's exact finding: a good backup that couldn't be restored
        for lack of space must not be reported corrupt."""
        # A 200 MiB target restored into a 150 MiB temp fs -> guaranteed ENOSPC. The
        # source fs must hold data (200) + its received backup copy (200), so 600 MiB.
        with (
            LoopbackBtrfs(size_mb=600, label="src") as src,
            LoopbackBtrfs(size_mb=150, label="tinytmp") as tmp,
        ):
            subvol = src / "data"
            _run("btrfs", "subvolume", "create", str(subvol))
            _run(
                "dd",
                "if=/dev/urandom",
                f"of={subvol}/big",
                "bs=1M",
                "count=200",
                "status=none",
            )
            backup = src / "backup"
            backup.mkdir()
            snap = src / "home-20260101-120000"
            create_snapshot(subvol, snap, readonly=True)
            send_snapshot(snap, backup)

            ep = LocalEndpoint(config={"path": str(backup), "snap_prefix": "home-"})
            # Force past the space preflight so the REAL receive hits ENOSPC.
            with patch(
                "btrfs_backup_ng.core.verify._estimate_temp_shortfall",
                return_value=None,
            ):
                report = verify_full(ep, temp_dir=tmp / "t", cleanup=True)

        assert report.failed == 0, report.results[0].message
        assert report.results[0].details["status"] == "unverifiable"
