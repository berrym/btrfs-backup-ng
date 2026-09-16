"""doctor is the command that must be believed before trusting the tool.

Three of its verdicts were reported OK for states it had not checked, or could
not check, or that were not OK:

* ``_is_lock_stale`` decides staleness by looking for a pid in the lock id. No
  lock id this project writes contains one -- a restore session is
  ``restore:{session_id}`` and a destination id is a path or a URL -- so the
  check was inert for every real input and, returning False for all of them,
  reported "No stale locks found" for a lock file full of locks. Its advertised
  fix was unreachable for the same reason.
* An unreadable lock file produced a bare log warning and no finding, so it
  reached the same "No stale locks found". A lock is what stops retention
  pruning a snapshot; a lock file nobody can read is the case the operator ran
  this command to find.
* ``_check_recent_failures`` partitions the log into "failed" and "completed"
  and reports OK when the first is empty. The logger writes a third status,
  "started", so a run killed part-way sat in neither list; and an empty window
  printed a green "All 0 operation(s) successful in last 24h".
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.doctor import (
    DiagnosticSeverity,
    Doctor,
)


def _doctor_with_lock_file(tmp_path, monkeypatch, locks):
    """A volume whose snapshot dir really contains a lock file on disk.

    _check_stale_locks builds the path as
    Path(volume.path) / volume.snapshot_dir / ".btrfs-backup-ng.locks" and skips
    the volume entirely unless it exists, so the file has to be real -- a
    MagicMock volume alone made every one of these tests pass by never running
    the loop under test.
    """
    snapshot_dir = tmp_path / ".snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / ".btrfs-backup-ng.locks").write_text(json.dumps(locks))

    volume = MagicMock()
    volume.path = str(tmp_path)
    volume.snapshot_dir = ".snapshots"
    config = MagicMock()
    config.get_enabled_volumes.return_value = [volume]
    doctor = Doctor(config=config)
    doctor._volume_filter = None
    monkeypatch.setattr("btrfs_backup_ng.__util__.read_locks", lambda *a, **k: locks)
    return doctor


class TestTheLockIdsThisProjectActuallyWrites:
    """Every one of them, measured. If any becomes determinable the check can
    start answering for it -- but none may quietly go back to reading False."""

    REAL_IDS = [
        "restore:3f8a1c2e",
        "/mnt/backup",
        "ssh://user@host:/mnt/backup",
        "raw:///mnt/backup",
        "unknown://mnt/backup",
    ]

    @pytest.mark.parametrize("lock_id", REAL_IDS)
    def test_staleness_is_undeterminable_not_false(self, lock_id):
        assert Doctor()._is_lock_stale(lock_id) is None, (
            f"{lock_id!r} reported a definite staleness verdict it cannot have"
        )

    def test_a_pid_bearing_id_is_still_answered(self):
        doctor = Doctor()
        with patch("os.kill", return_value=None):
            assert doctor._is_lock_stale("transfer:12345") is False
        with patch("os.kill", side_effect=OSError):
            assert doctor._is_lock_stale("transfer:12345") is True


class TestTheLockCheckSaysWhichAllClearItIs:
    def test_no_locks_at_all(self, tmp_path, monkeypatch):
        doctor = _doctor_with_lock_file(tmp_path, monkeypatch, {})

        findings = doctor._check_stale_locks()

        assert [f.severity for f in findings] == [DiagnosticSeverity.OK]
        assert "No locks held" in findings[0].message

    def test_a_held_lock_nobody_can_identify_is_reported(self, tmp_path, monkeypatch):
        doctor = _doctor_with_lock_file(
            tmp_path, monkeypatch, {"snap-1": {"locks": ["restore:abc123"]}}
        )
        findings = doctor._check_stale_locks()

        assert any(f.severity == DiagnosticSeverity.INFO for f in findings), (
            f"a held lock was passed over silently: {[f.message for f in findings]}"
        )
        assert not any(
            f.severity == DiagnosticSeverity.OK and "No locks held" in f.message
            for f in findings
        )

    def test_an_unreadable_lock_file_is_not_an_all_clear(self, tmp_path, monkeypatch):
        doctor = _doctor_with_lock_file(tmp_path, monkeypatch, {"snap-1": {}})
        monkeypatch.setattr(
            "btrfs_backup_ng.__util__.read_locks",
            MagicMock(side_effect=PermissionError("Permission denied")),
        )

        findings = doctor._check_stale_locks()

        assert any(f.severity == DiagnosticSeverity.WARN for f in findings), (
            f"an unreadable lock file reported clean: {[f.message for f in findings]}"
        )
        assert not any(f.severity == DiagnosticSeverity.OK for f in findings)


class TestTheRecentFailuresCheck:
    @staticmethod
    def _doctor_with_log(tmp_path, monkeypatch, transactions):
        log = tmp_path / "transactions.jsonl"
        log.write_text("")
        config = MagicMock()
        config.global_config.transaction_log = str(log)
        doctor = Doctor(config=config)
        monkeypatch.setattr(
            "btrfs_backup_ng.transaction.read_transaction_log",
            lambda *a, **k: transactions,
        )
        return doctor

    @staticmethod
    def _txn(status, snapshot="s1", action="backup"):
        return {
            "status": status,
            "snapshot": snapshot,
            "action": action,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }

    def test_a_run_that_started_and_never_finished_is_reported(
        self, tmp_path, monkeypatch
    ):
        """A killed run leaves a `started` record with no partner. It was in
        neither the failed nor the completed list, so it reported OK."""
        doctor = self._doctor_with_log(
            tmp_path, monkeypatch, [self._txn("started", "s1")]
        )

        findings = doctor._check_recent_failures()

        assert any(f.severity == DiagnosticSeverity.WARN for f in findings), (
            f"a half-finished run reported clean: {[f.message for f in findings]}"
        )

    def test_a_started_record_with_a_partner_is_not_reported(
        self, tmp_path, monkeypatch
    ):
        doctor = self._doctor_with_log(
            tmp_path,
            monkeypatch,
            [self._txn("started", "s1"), self._txn("completed", "s1")],
        )

        findings = doctor._check_recent_failures()

        assert all(f.severity == DiagnosticSeverity.OK for f in findings), [
            f.message for f in findings
        ]

    def test_an_empty_window_is_not_all_zero_operations_successful(
        self, tmp_path, monkeypatch
    ):
        """For a tool whose job is to run on a timer, nothing having happened in
        24 hours is the finding, not a green line reading 'All 0 successful'."""
        doctor = self._doctor_with_log(tmp_path, monkeypatch, [])

        findings = doctor._check_recent_failures()

        assert any(f.severity == DiagnosticSeverity.WARN for f in findings)
        assert not any("All 0 operation" in f.message for f in findings)

    def test_a_genuinely_clean_window_is_still_ok(self, tmp_path, monkeypatch):
        doctor = self._doctor_with_log(
            tmp_path, monkeypatch, [self._txn("completed", "s1")]
        )

        findings = doctor._check_recent_failures()

        assert [f.severity for f in findings] == [DiagnosticSeverity.OK]
        assert "All 1 operation(s) successful" in findings[0].message

    def test_failures_are_still_reported(self, tmp_path, monkeypatch):
        doctor = self._doctor_with_log(
            tmp_path, monkeypatch, [self._txn("failed", "s1")]
        )

        findings = doctor._check_recent_failures()

        assert any(f.severity == DiagnosticSeverity.WARN for f in findings)
