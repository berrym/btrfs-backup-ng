"""doctor is the command that must be believed before trusting the tool.

Three of its verdicts were reported OK for states it had not checked, or could
not check, or that were not OK:

* The lock check decided staleness by looking for a pid in the lock id. No
  lock id this project writes contains one -- a restore session is
  ``restore:{session_id}`` and a destination id is a path or a URL -- so the
  check was inert for every real input and, returning False for all of them,
  reported "No stale locks found" for a lock file full of locks. Its advertised
  fix was unreachable for the same reason, and both are gone: every lock is
  reported as held, and none is judged. The check also read the lock file at
  the configured base for an absolute ``snapshot_dir``, where ``run`` keeps it
  at ``<base>/<source name>``, so it reported "No locks held" for every such
  volume.
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
import os
import time
from pathlib import Path
from unittest.mock import MagicMock


from btrfs_backup_ng import __util__
from btrfs_backup_ng.core.doctor import (
    DiagnosticReport,
    DiagnosticSeverity,
    Doctor,
)
from btrfs_backup_ng.sshutil.lock import LOCK_DIR_NAME, holders_beside, local_manager

LOCK_FILE = ".btrfs-backup-ng.locks"
HOLDERS = LOCK_FILE + ".holders"
DEAD = 1000  # seconds: past the stale threshold, so the holder is gone


def _doctor_with_lock_file(
    tmp_path, monkeypatch, locks, snapshot_dir=".snapshots", targets=()
):
    """A volume whose snapshot dir really contains a lock file on disk, and
    the snapshots the file names.

    _check_stale_locks reads the lock file where ``run`` writes it, through
    ``resolve_snapshot_dir``, and skips the volume entirely unless it exists,
    so the file has to be real -- a MagicMock volume alone made every one of
    these tests pass by never running the loop under test. ``snapshot_dir`` is
    the volume's setting, relative or absolute; the file is written where the
    resolver will look.
    """
    source = tmp_path / "home"
    source.mkdir(exist_ok=True)
    if snapshot_dir.startswith("/"):
        base = tmp_path / snapshot_dir.lstrip("/")
        where = base / source.name
        setting = str(base)
    else:
        where = source / snapshot_dir
        setting = snapshot_dir
    where.mkdir(parents=True, exist_ok=True)
    (where / LOCK_FILE).write_text(json.dumps(locks))
    for name in locks:
        (where / name).mkdir(exist_ok=True)

    volume = MagicMock()
    volume.path = str(source)
    volume.snapshot_dir = setting
    volume.targets = list(targets)
    config = MagicMock()
    config.get_enabled_volumes.return_value = [volume]
    doctor = Doctor(config=config)
    doctor._volume_filter = None
    doctor._where = where
    return doctor


def _record(where: Path, name: str, holder_id: str, age: int = 0) -> Path:
    """A holder record beside the lock file, ``age`` seconds old."""
    manager = holders_beside(where / LOCK_FILE)
    manager.acquire_shared(f"snap-{name}", holder_id)
    (path,) = [
        p
        for p in (where / HOLDERS).rglob("holders/*")
        if json.loads(p.read_text()).get("lock_id") == holder_id
        and json.loads(p.read_text()).get("name") == f"snap-{name}"
    ]
    old = time.time() - age
    os.utime(path, (old, old))
    return path


def _ledger(where: Path) -> dict:
    raw = __util__.read_locks((where / LOCK_FILE).read_text())
    return {n: {k: set(v) for k, v in e.items()} for n, e in raw.items()}


def _records(where: Path) -> set:
    return (
        {p.name for p in (where / HOLDERS).rglob("holders/*")}
        if (where / HOLDERS).exists()
        else set()
    )


def _apply(doctor: Doctor) -> list:
    report = DiagnosticReport()
    report.findings.extend(doctor._check_stale_locks())
    return doctor.apply_fixes(report)


class TestTheLockFileIsReadWhereRunWritesIt:
    """An absolute ``snapshot_dir`` keeps each volume's snapshots, and the lock
    file, at ``<base>/<source name>``. The check looked at the base alone,
    found nothing, and reported "No locks held" for every such volume."""

    def test_an_absolute_snapshot_dir(self, tmp_path, monkeypatch):
        doctor = _doctor_with_lock_file(
            tmp_path,
            monkeypatch,
            {"snap-1": {"locks": ["restore:abc123"]}},
            snapshot_dir="/snapshots",
        )
        findings = doctor._check_stale_locks()

        assert [f.severity for f in findings] == [DiagnosticSeverity.INFO]
        assert findings[0].details["lock_file"] == str(
            tmp_path / "snapshots" / "home" / LOCK_FILE
        )

    def test_a_missing_base_is_not_an_all_clear(self, tmp_path, monkeypatch):
        doctor = _doctor_with_lock_file(
            tmp_path, monkeypatch, {"snap-1": {"locks": ["restore:abc123"]}}
        )
        (volume,) = doctor.config.get_enabled_volumes()
        volume.snapshot_dir = str(tmp_path / "not-mounted")

        findings = doctor._check_stale_locks()

        assert [f.severity for f in findings] == [DiagnosticSeverity.WARN]
        assert "Could not examine the locks" in findings[0].message
        assert "not-mounted" in findings[0].message


class TestEveryPinIsReportedWithItsHolder:
    def test_a_live_holder_is_named(self, tmp_path, monkeypatch):
        doctor = _doctor_with_lock_file(
            tmp_path, monkeypatch, {"snap-1": {"locks": ["restore:abc"]}}
        )
        _record(doctor._where, "snap-1", "restore:abc")

        (finding,) = doctor._check_stale_locks()

        assert finding.severity == DiagnosticSeverity.INFO
        assert f"pid {os.getpid()} on {os.uname().nodename}" in finding.message
        assert not finding.fixable
        assert finding.details["pid"] == os.getpid()

    def test_a_pin_without_a_record_is_reported_and_not_judged(
        self, tmp_path, monkeypatch
    ):
        doctor = _doctor_with_lock_file(
            tmp_path,
            monkeypatch,
            {"snap-1": {"locks": ["restore:abc", "/dest"], "parent_locks": ["/dest"]}},
        )
        findings = doctor._check_stale_locks()

        assert [f.severity for f in findings] == [DiagnosticSeverity.INFO] * 3
        assert all("no holder record" in f.message for f in findings)
        assert not any(f.fixable for f in findings)
        (restore,) = [f for f in findings if f.details["lock_id"] == "restore:abc"]
        assert "restore --unlock" in restore.message

    def test_fixes_touch_neither_a_live_nor_an_unrecorded_pin(
        self, tmp_path, monkeypatch
    ):
        doctor = _doctor_with_lock_file(
            tmp_path,
            monkeypatch,
            {
                "snap-1": {"locks": ["restore:live"], "parent_locks": ["/dest"]},
                "snap-2": {"locks": ["restore:unrecorded"]},
            },
        )
        _record(doctor._where, "snap-1", "restore:live")
        _record(doctor._where, "snap-1", "p:/dest")
        before = _ledger(doctor._where)

        assert _apply(doctor) == []
        assert _ledger(doctor._where) == before
        assert len(_records(doctor._where)) == 2


class TestTheFixesDoctorCanProve:
    def test_an_entry_for_a_snapshot_that_no_longer_exists_is_removed(
        self, tmp_path, monkeypatch
    ):
        doctor = _doctor_with_lock_file(
            tmp_path,
            monkeypatch,
            {
                "gone": {"locks": ["/dest"], "parent_locks": ["/dest"]},
                "kept": {"locks": ["/dest"]},
            },
        )
        (doctor._where / "gone").rmdir()
        _record(doctor._where, "gone", "/dest")
        _record(doctor._where, "kept", "/dest")

        findings = doctor._check_stale_locks()
        gone = [f for f in findings if f.details.get("snapshot") == "gone"]
        assert [f.severity for f in gone] == [DiagnosticSeverity.WARN]
        assert gone[0].fixable and "no longer exists" in gone[0].message

        results = _apply(doctor)
        assert [r.success for r in results] == [True]
        assert _ledger(doctor._where) == {"kept": {"locks": {"/dest"}}}
        assert len(_records(doctor._where)) == 1

    def test_an_entry_that_holds_nothing_is_removed(self, tmp_path, monkeypatch):
        doctor = _doctor_with_lock_file(
            tmp_path,
            monkeypatch,
            {"empty": {"locks": []}, "kept": {"locks": ["/dest"]}},
        )
        (finding,) = [f for f in doctor._check_stale_locks() if f.fixable]
        assert "holds nothing" in finding.message

        assert [r.success for r in _apply(doctor)] == [True]
        assert _ledger(doctor._where) == {"kept": {"locks": {"/dest"}}}

    def test_a_restore_pin_whose_holder_is_gone_is_removed_with_its_record(
        self, tmp_path, monkeypatch
    ):
        doctor = _doctor_with_lock_file(
            tmp_path,
            monkeypatch,
            {"snap-1": {"locks": ["restore:dead", "restore:live"]}},
        )
        dead = _record(doctor._where, "snap-1", "restore:dead", age=DEAD)
        _record(doctor._where, "snap-1", "restore:live")

        fixable = [f for f in doctor._check_stale_locks() if f.fixable]
        assert [f.details["lock_id"] for f in fixable] == ["restore:dead"]
        assert "holder is gone" in fixable[0].message

        assert [r.success for r in _apply(doctor)] == [True]
        assert _ledger(doctor._where) == {"snap-1": {"locks": {"restore:live"}}}
        assert not dead.exists()
        assert len(_records(doctor._where)) == 1

    def test_a_parent_pin_whose_holder_is_gone_is_removed(self, tmp_path, monkeypatch):
        doctor = _doctor_with_lock_file(
            tmp_path,
            monkeypatch,
            {"snap-1": {"locks": ["/dest"], "parent_locks": ["/dest"]}},
        )
        _record(doctor._where, "snap-1", "p:/dest", age=DEAD)
        _record(doctor._where, "snap-1", "/dest", age=DEAD)

        findings = doctor._check_stale_locks()
        parent = [f for f in findings if f.details["kind"] == "parent pin"]
        own = [f for f in findings if f.details["kind"] == "pin"]
        assert parent[0].fixable and parent[0].severity == DiagnosticSeverity.WARN
        assert not own[0].fixable and "kept until the next run" in own[0].message

        assert [r.success for r in _apply(doctor)] == [True]
        assert _ledger(doctor._where) == {"snap-1": {"locks": {"/dest"}}}
        assert _records(doctor._where) and len(_records(doctor._where)) == 1

    def test_a_record_that_outlived_its_pin_is_removed_when_dead(
        self, tmp_path, monkeypatch
    ):
        doctor = _doctor_with_lock_file(
            tmp_path, monkeypatch, {"snap-1": {"locks": ["/dest"]}}
        )
        _record(doctor._where, "snap-1", "/dest")
        orphan_dead = _record(doctor._where, "snap-1", "restore:old", age=DEAD)
        orphan_live = _record(doctor._where, "snap-1", "restore:new")

        findings = doctor._check_stale_locks()
        fixable = [f for f in findings if f.fixable]
        assert [f.details["lock_id"] for f in fixable] == ["restore:old"]
        assert any("has no pin in the lock file" in f.message for f in findings)

        assert [r.success for r in _apply(doctor)] == [True]
        assert not orphan_dead.exists() and orphan_live.exists()
        assert _ledger(doctor._where) == {"snap-1": {"locks": {"/dest"}}}

    def test_a_dead_record_in_a_directory_store_is_swept(self, tmp_path, monkeypatch):
        doctor = _doctor_with_lock_file(tmp_path, monkeypatch, {})
        where = doctor._where
        (where / LOCK_FILE).unlink()
        (where / LOCK_DIR_NAME).mkdir()
        store = local_manager(where)
        store.acquire_shared("snap-s1", "restore:dead")
        store.acquire_shared("snap-s1", "restore:live")
        (dead,) = [
            p
            for p in (where / LOCK_DIR_NAME).rglob("holders/*")
            if json.loads(p.read_text())["lock_id"] == "restore:dead"
        ]
        old = time.time() - DEAD
        os.utime(dead, (old, old))

        findings = doctor._check_stale_locks()
        assert [
            f.fixable for f in sorted(findings, key=lambda f: f.details["lock_id"])
        ] == [True, False]

        assert [r.success for r in _apply(doctor)] == [True]
        assert not dead.exists()
        assert local_manager(where).live_lock_names() == {"snap-s1"}

    def test_a_local_targets_store_is_examined_too(self, tmp_path, monkeypatch):
        target_dir = tmp_path / "mirror"
        target_dir.mkdir()
        (target_dir / LOCK_DIR_NAME).mkdir()
        store = local_manager(target_dir)
        store.acquire_shared("snap-s1", "restore:dead")
        (dead,) = list((target_dir / LOCK_DIR_NAME).rglob("holders/*"))
        old = time.time() - DEAD
        os.utime(dead, (old, old))
        target = MagicMock()
        target.path = str(target_dir)
        doctor = _doctor_with_lock_file(tmp_path, monkeypatch, {}, targets=[target])

        (finding,) = doctor._check_stale_locks()
        assert finding.fixable and str(target_dir) in finding.message

        assert [r.success for r in _apply(doctor)] == [True]
        assert not dead.exists()


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
