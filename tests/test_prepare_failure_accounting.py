"""A target that never got as far as a transfer must not be reported as backed up.

`_backup_volume` prepared each destination in a loop, and a failure there was
logged and appended to `errors` -- but `all_success` was declared BELOW that loop
and never saw it, and `stats["failed"]` was untouched. So a volume with more than
one target, where one failed to prepare and another transferred, returned
success: exit 0, a "success" notification, and the failure visible only as a log
line.

Single-target volumes were saved by accident, by the `if not
destination_endpoints: return False` guard, which is why this survived.

That covers every reason preparation can fail. `require_mount` is one of them, so
the mount check could correctly detect that an external drive was absent, refuse
the target, and the run would still report that the backup worked -- the project's
signature defect sitting directly underneath a safety feature.

The snapper path (`_backup_snapper_volume`) already accounted for this correctly;
only the native path did not.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.cli.run as run_mod
from btrfs_backup_ng import __util__
from btrfs_backup_ng.config.schema import (
    Config,
    GlobalConfig,
    TargetConfig,
    VolumeConfig,
)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    (tmp_path / "src").mkdir()

    def _endpoint(spec, common_config=None, source=False, **kw):
        m = MagicMock()
        m.snapshot.return_value = MagicMock()
        m.list_snapshots.return_value = [MagicMock()]
        return m

    monkeypatch.setattr(run_mod.endpoint, "choose_endpoint", _endpoint)
    monkeypatch.setattr(run_mod, "_transfer_to_target", lambda *a, **k: True)
    monkeypatch.setattr(run_mod, "_prune_after_transfer", lambda *a, **k: True)
    return tmp_path, monkeypatch


def _run(rig, targets, mounted=()):
    tmp_path, monkeypatch = rig
    wanted = {str(m) for m in mounted}
    monkeypatch.setattr(__util__, "is_mounted", lambda p: str(p) in wanted)
    volume = VolumeConfig(
        path=str(tmp_path / "src"),
        snapshot_prefix="t-",
        snapshot_dir=str(tmp_path / "snaps"),
        targets=targets,
    )
    config = Config(global_config=GlobalConfig(), volumes=[volume])
    return run_mod._backup_volume(volume, config, parallel_targets=1)


class TestAPreparationFailureIsReportedAsAFailure:
    def test_one_failed_target_alongside_a_good_one_fails_the_volume(
        self, rig, tmp_path
    ):
        """THE regression. Previously ok=True, failed=0."""
        good = tmp_path / "good"
        good.mkdir()
        usb = tmp_path / "usb"
        usb.mkdir()

        ok, stats, errors = _run(
            rig,
            [
                TargetConfig(path=str(usb), require_mount=True),  # drive absent
                TargetConfig(path=str(good)),
            ],
            mounted=[],
        )

        assert ok is False, (
            "a target was refused and the volume still reported success -- exit 0 "
            "and a success notification for a backup that did not happen"
        )
        assert stats["failed"] == 1, (
            f"the refused target was not counted: {stats}. The summary and the "
            f"notification both read from this."
        )
        assert any("usb" in e for e in errors), errors

    def test_the_error_names_the_target(self, rig, tmp_path):
        good = tmp_path / "good"
        good.mkdir()
        usb = tmp_path / "usb"
        usb.mkdir()
        _ok, _stats, errors = _run(
            rig,
            [
                TargetConfig(path=str(usb), require_mount=True),
                TargetConfig(path=str(good)),
            ],
            mounted=[],
        )
        assert any(str(usb) in e for e in errors), (
            f"the operator cannot tell WHICH target failed: {errors}"
        )

    def test_several_failures_are_all_counted(self, rig, tmp_path):
        good = tmp_path / "good"
        good.mkdir()
        a, b = tmp_path / "a", tmp_path / "b"
        a.mkdir()
        b.mkdir()
        ok, stats, _errors = _run(
            rig,
            [
                TargetConfig(path=str(a), require_mount=True),
                TargetConfig(path=str(b), require_mount=True),
                TargetConfig(path=str(good)),
            ],
            mounted=[],
        )
        assert ok is False
        assert stats["failed"] == 2, stats

    def test_a_single_failing_target_still_fails(self, rig, tmp_path):
        """Was already correct, via the no-endpoints guard. Must stay correct."""
        usb = tmp_path / "usb"
        usb.mkdir()
        ok, _stats, _errors = _run(
            rig, [TargetConfig(path=str(usb), require_mount=True)], mounted=[]
        )
        assert ok is False


class TestASuccessfulRunIsStillASuccess:
    """The fix must not make working backups report failure."""

    def test_all_targets_succeeding_reports_success(self, rig, tmp_path):
        a, b = tmp_path / "a", tmp_path / "b"
        a.mkdir()
        b.mkdir()
        ok, stats, errors = _run(
            rig, [TargetConfig(path=str(a)), TargetConfig(path=str(b))]
        )
        assert ok is True, f"a clean run was reported as failed: {errors}"
        assert stats["failed"] == 0

    def test_a_satisfied_mount_check_does_not_count_as_a_failure(self, rig, tmp_path):
        usb = tmp_path / "usb"
        usb.mkdir()
        ok, stats, errors = _run(
            rig, [TargetConfig(path=str(usb), require_mount=True)], mounted=[usb]
        )
        assert ok is True, f"a mounted drive was treated as a failure: {errors}"
        assert stats["failed"] == 0


class TestTheEarlyReturnCountsToo:
    """When EVERY target fails, `if not destination_endpoints: return False`
    already reported the volume as failed -- but with `stats["failed"] == 0`, so
    the notification read "1 volume failed, 0 transfers failed". The verdict and
    the counts disagreed about the same run."""

    def test_every_target_failing_is_counted(self, rig, tmp_path):
        a, b = tmp_path / "a", tmp_path / "b"
        a.mkdir()
        b.mkdir()
        ok, stats, errors = _run(
            rig,
            [
                TargetConfig(path=str(a), require_mount=True),
                TargetConfig(path=str(b), require_mount=True),
            ],
            mounted=[],
        )
        assert ok is False
        assert stats["failed"] == 2, (
            f"the volume was reported failed but the counts say nothing failed: {stats}"
        )
        assert len(errors) == 2


class TestTheFixDoesNotSabotageWorkingTargets:
    """A fix that makes a failing target poison its siblings would be worse than
    the bug."""

    def test_a_good_target_alongside_a_failing_one_still_transfers(
        self, rig, tmp_path, monkeypatch
    ):
        transferred: list = []

        def _spy(*a, **k):
            # MUST return True. `list.append` returns None, and a falsy return
            # here is read as a failed transfer -- which made this test report
            # the code broken when only the spy was.
            transferred.append(True)
            return True

        monkeypatch.setattr(run_mod, "_transfer_to_target", _spy)
        good = tmp_path / "good"
        good.mkdir()
        usb = tmp_path / "usb"
        usb.mkdir()
        _run(
            rig,
            [
                TargetConfig(path=str(usb), require_mount=True),
                TargetConfig(path=str(good)),
            ],
            mounted=[],
        )
        assert len(transferred) == 1, (
            "the working target did not receive its backup because a sibling "
            "was refused"
        )

    def test_a_failing_volume_returns_rather_than_raising(self, rig, tmp_path):
        """The caller loops over volumes and appends each result, so a failure
        must come back as a return value. Raising would abort the loop and stop
        every LATER volume being backed up -- a far worse defect than the one
        being fixed.

        Asserted behaviourally. An earlier version of this test also checked for
        "results.append" in the caller's source text, which pins a phrasing
        rather than a property and is the exact weakness removed elsewhere in
        this suite.
        """
        usb = tmp_path / "usb"
        usb.mkdir()
        ok, stats, errors = _run(
            rig, [TargetConfig(path=str(usb), require_mount=True)], mounted=[]
        )
        assert ok is False
        assert stats["failed"] == 1
        assert len(errors) == 1

    def test_a_second_volume_is_still_backed_up(self, rig, tmp_path, monkeypatch):
        """Drive two volumes in sequence, the first failing."""
        transferred: list = []

        def _spy(*a, **k):
            # MUST return True. `list.append` returns None, and a falsy return
            # here is read as a failed transfer -- which made this test report
            # the code broken when only the spy was.
            transferred.append(True)
            return True

        monkeypatch.setattr(run_mod, "_transfer_to_target", _spy)
        usb = tmp_path / "usb"
        usb.mkdir()
        good = tmp_path / "good"
        good.mkdir()

        first, _s, _e = _run(
            rig, [TargetConfig(path=str(usb), require_mount=True)], mounted=[]
        )
        assert first is False
        transferred.clear()

        second, _s2, _e2 = _run(rig, [TargetConfig(path=str(good))])
        assert second is True, "a later volume was affected by an earlier failure"
        assert len(transferred) == 1
