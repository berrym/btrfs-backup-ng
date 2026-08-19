"""Snapper destination retention -- STAGE 1: planning only, nothing is deleted.

`run` on a native volume ends with _prune_after_transfer. The snapper path ended
with no retention phase at all (cli/run.py, and cli/prune.py had zero snapper
references), so snapper destinations grew without bound.

Wiring the existing engine in naively would have been worse than nothing.
apply_retention takes each snapshot's time from its NAME, and snapper
destination backups are numbered slots -- .snapshots/558 -- whose date lives in
info.xml. Every one of them would fail name parsing, hit the quarantine branch,
and be kept: retention would report success having deleted nothing. That is the
same "a check that did not happen, reported as one that did" defect this whole
series has been closing.

So the engine learned where else a timestamp can come from, and these tests pin
that the plan is a real decision rather than a quarantine.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.cli.prune import (
    format_snapper_retention_plan,
    plan_snapper_retention,
    snapper_backup_timestamp,
)
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.retention import apply_retention

NOW = datetime(2026, 8, 18, 12, 0, 0)


def _backup(number, days_ago=None, description="", date=None):
    if date is None and days_ago is not None:
        date = NOW - timedelta(days=days_ago)
    metadata = SimpleNamespace(date=date, description=description, num=number)
    return {
        "number": number,
        "snapshot_path": f"/backups/.snapshots/{number}/snapshot",
        "info_xml_path": f"/backups/.snapshots/{number}/info.xml",
        "metadata": metadata,
    }


def _retention(**kw):
    base = {
        "hourly": 0,
        "daily": 7,
        "weekly": 0,
        "monthly": 0,
        "yearly": 0,
        "min": "1d",
    }
    base.update(kw)
    return RetentionConfig(**base)


class TestTheEngineCanTimeANumberedSlot:
    def test_a_slot_number_alone_would_be_quarantined(self):
        """The reason a callback was needed at all: without it the name is '558'
        and no format parses it, so the backup is kept and never counted."""
        backups = [_backup(n, days_ago=n * 10) for n in range(1, 6)]
        keep, delete = apply_retention(
            backups, _retention(), get_name=lambda b: str(b["number"]), now=NOW
        )
        assert delete == [], "without a timestamp source everything must be kept"
        assert len(keep) == 5

    def test_with_the_callback_it_makes_a_real_decision(self):
        # 12 backups on 12 distinct days against daily=7: five fall outside
        # the buckets. Five on five days ALL fit and correctly delete nothing.
        backups = [_backup(n, days_ago=n) for n in range(1, 13)]
        keep, delete = apply_retention(
            backups,
            _retention(),
            get_name=lambda b: str(b["number"]),
            now=NOW,
            get_timestamp=snapper_backup_timestamp,
        )
        assert delete, "the whole point: old backups are now selectable"
        assert keep

    def test_the_newest_is_always_kept(self):
        backups = [_backup(n, days_ago=n * 30) for n in range(1, 13)]
        keep, delete = apply_retention(
            backups, _retention(), now=NOW, get_timestamp=snapper_backup_timestamp
        )
        assert _backup(1, days_ago=30)["number"] in [b["number"] for b in keep]
        assert 1 not in [b["number"] for b in delete]

    def test_a_backup_with_no_date_is_never_deleted(self):
        """An undated backup is one we could not time. Deleting it would be
        acting on a fact we do not have."""
        backups = [
            _backup(1, days_ago=1),
            _backup(2, date=None),
            _backup(3, days_ago=400),
        ]
        _keep, delete = apply_retention(
            backups, _retention(), now=NOW, get_timestamp=snapper_backup_timestamp
        )
        assert 2 not in [b["number"] for b in delete]

    def test_a_non_datetime_date_is_treated_as_unknown(self):
        backup = _backup(1)
        backup["metadata"] = SimpleNamespace(date="2026-08-18", description="")
        assert snapper_backup_timestamp(backup) is None

    def test_a_backup_with_no_metadata_is_treated_as_unknown(self):
        assert snapper_backup_timestamp({"number": 5, "metadata": None}) is None


class TestThePlanUsesTheSharedEnumeration:
    def _plan(self, backups, retention=None, **kw):
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups", return_value=backups
        ):
            return plan_snapper_retention(
                "/backups", retention or _retention(), now=NOW, **kw
            )

    def test_an_empty_destination_plans_nothing(self):
        assert self._plan([]) == ([], [])

    def test_old_backups_are_selected_for_deletion(self):
        keep, delete = self._plan([_backup(n, days_ago=n) for n in range(1, 13)])
        assert delete
        assert len(keep) + len(delete) == 12

    def test_everything_within_min_is_kept(self):
        keep, delete = self._plan(
            [_backup(n, days_ago=n) for n in range(1, 5)],
            retention=_retention(min="30d"),
        )
        assert delete == []
        assert len(keep) == 4

    def test_an_enumeration_failure_propagates(self):
        """A location that could not be read must never be planned as empty --
        an empty plan on a full destination is how retention deletes nothing
        while claiming to have run, or worse."""
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups",
            side_effect=RuntimeError("could not enumerate"),
        ):
            with pytest.raises(RuntimeError, match="could not enumerate"):
                plan_snapper_retention("/backups", _retention())

    def test_the_endpoint_options_are_threaded_through(self):
        """A destination written with --ssh-sudo is root-owned; reading it back
        without the same options enumerates as empty."""
        seen = {}

        def fake(path, options):
            seen["options"] = options
            return []

        with patch("btrfs_backup_ng.core.restore.list_snapper_backups", fake):
            plan_snapper_retention("/backups", _retention(), {"ssh_sudo": True})
        assert seen["options"] == {"ssh_sudo": True}


class TestThePlanIsReadable:
    def test_it_reports_both_sides(self):
        keep = [_backup(9, days_ago=1, description="recent")]
        delete = [_backup(1, days_ago=400, description="ancient")]
        text = format_snapper_retention_plan("/backups", keep, delete)
        assert "Keep: 1" in text
        assert "Would delete: 1" in text
        assert "slot 9" in text and "slot 1" in text
        assert "recent" in text and "ancient" in text

    def test_it_is_ordered_newest_first(self):
        backups = [
            _backup(1, days_ago=30),
            _backup(2, days_ago=1),
            _backup(3, days_ago=10),
        ]
        text = format_snapper_retention_plan("/backups", backups, [])
        order = [line for line in text.splitlines() if "slot" in line]
        assert "slot 2" in order[0] and "slot 1" in order[-1]

    def test_an_undated_backup_still_renders(self):
        text = format_snapper_retention_plan("/backups", [_backup(4, date=None)], [])
        assert "date unknown" in text


class TestTheRunPhaseContract:
    """The retention phase now DELETES, so it takes the native phase's strict
    contract: anything that stops retention happening fails the run rather than
    passing quietly."""

    def _phase(self, backups=None, targets=None, retention=None, side_effect=None):
        from btrfs_backup_ng.cli import run as run_cli

        volume = SimpleNamespace(path="/home", targets=[])
        config = SimpleNamespace(
            get_effective_retention=lambda v: retention or _retention()
        )
        if targets is None:
            targets = [(SimpleNamespace(path="/backups"), {})]
        errors: list = []
        patcher = patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups",
            side_effect=side_effect,
            **({} if side_effect else {"return_value": backups or []}),
        )
        with patcher:
            with patch(
                "btrfs_backup_ng.cli.prune.delete_snapper_backups",
                return_value=(len(backups or []), []),
            ) as deletes:
                ok = run_cli._prune_snapper_after_transfer(
                    volume, config, targets, errors
                )
        return ok, errors, deletes

    def test_old_backups_are_actually_deleted(self):
        backups = [_backup(n, days_ago=n * 20) for n in range(1, 13)]
        ok, errors, deletes = self._phase(backups)
        assert ok and not errors
        assert deletes.called, "the phase must delete, not just plan"
        _path, to_delete, _opts = deletes.call_args[0]
        assert to_delete, "something should have been selected"

    def test_a_degenerate_policy_fails_the_run(self):
        """The report-only stage warned; a stage that deletes must refuse and
        say so, exactly as the native phase does."""
        ok, errors, deletes = self._phase(
            [_backup(1, days_ago=1)], retention=_retention(daily=0, min="1h")
        )
        assert ok is False
        assert any("Degenerate" in e for e in errors)
        assert not deletes.called, "a refused policy must not delete"

    def test_an_enumeration_failure_fails_the_run(self):
        """A destination that could not be read is not an empty one. Pruning
        nothing is safe; reporting success is not."""
        ok, errors, deletes = self._phase(side_effect=RuntimeError("unreachable"))
        assert ok is False
        assert any("unreachable" in e for e in errors)
        assert not deletes.called

    def test_nothing_to_delete_is_a_success(self):
        ok, errors, deletes = self._phase([_backup(1, days_ago=1)])
        assert ok is True and not errors
        assert not deletes.called

    def test_no_successful_targets_prunes_nothing(self):
        from btrfs_backup_ng.cli import run as run_cli

        with patch("btrfs_backup_ng.core.restore.list_snapper_backups") as listing:
            ok = run_cli._prune_snapper_after_transfer(
                SimpleNamespace(path="/home"), SimpleNamespace(), [], []
            )
        assert ok is True
        assert not listing.called, "a failed transfer's target must not be pruned"

    def test_a_delete_failure_fails_the_run(self):
        from btrfs_backup_ng.cli import run as run_cli

        volume = SimpleNamespace(path="/home", targets=[])
        config = SimpleNamespace(get_effective_retention=lambda v: _retention())
        errors: list = []
        backups = [_backup(n, days_ago=n * 20) for n in range(1, 13)]
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups", return_value=backups
        ):
            with patch(
                "btrfs_backup_ng.cli.prune.delete_snapper_backups",
                return_value=(0, ["Delete slot 12: permission denied"]),
            ):
                ok = run_cli._prune_snapper_after_transfer(
                    volume, config, [(SimpleNamespace(path="/backups"), {})], errors
                )
        assert ok is False
        assert any("permission denied" in e for e in errors)

    def test_the_source_is_never_pruned(self):
        """snapper owns its own timeline. Only destination paths are planned."""
        from btrfs_backup_ng.cli import run as run_cli

        seen = []
        volume = SimpleNamespace(path="/home", targets=[])
        config = SimpleNamespace(get_effective_retention=lambda v: _retention())

        def record(path, options):
            seen.append(path)
            return []

        with patch("btrfs_backup_ng.core.restore.list_snapper_backups", record):
            run_cli._prune_snapper_after_transfer(
                volume, config, [(SimpleNamespace(path="/backups"), {})], []
            )
        assert seen == ["/backups"]
        assert "/home" not in seen, "the snapper source must never be planned"


class TestTheDeletePrimitive:
    """Two artifacts per slot, two privilege regimes. Measured on a real host:
    `btrfs subvolume delete` is covered by the documented NOPASSWD rule, and
    `sudo rm` is refused because rm is not btrfs."""

    def _endpoint(self, path="/backups"):
        return SimpleNamespace(config={"path": path})

    def test_the_subvolume_is_deleted_before_the_slot_directory(self):
        """If the subvolume delete fails there is still data in the slot;
        removing info.xml first would strand it."""
        from btrfs_backup_ng.cli import prune

        order = []
        endpoint = self._endpoint()

        def exec_remote(cmd, **kw):
            order.append(cmd[0] if cmd[0] != "btrfs" else " ".join(cmd[:3]))
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

        endpoint._exec_remote_command = exec_remote
        prune._delete_snapper_slot_btrfs(endpoint, "/backups/.snapshots/9", True)
        assert order == ["btrfs subvolume delete", "rm"], order

    def test_a_failed_subvolume_delete_leaves_the_slot_alone(self):
        from btrfs_backup_ng.cli import prune

        calls = []
        endpoint = self._endpoint()

        def exec_remote(cmd, **kw):
            calls.append(cmd)
            rc = 1 if cmd[0] == "btrfs" else 0
            return SimpleNamespace(returncode=rc, stdout=b"", stderr=b"nope")

        endpoint._exec_remote_command = exec_remote
        with pytest.raises(Exception, match="could not delete"):
            prune._delete_snapper_slot_btrfs(endpoint, "/backups/.snapshots/9", True)
        assert not any(c[0] == "rm" for c in calls), (
            "info.xml removed despite live data"
        )

    def test_a_failed_directory_removal_is_not_fatal(self):
        """The space is already reclaimed; a leftover empty directory is litter,
        and enumeration ignores a slot with no published snapshot."""
        from btrfs_backup_ng.cli import prune

        endpoint = self._endpoint()
        endpoint._exec_remote_command = lambda cmd, **kw: SimpleNamespace(
            returncode=0 if cmd[0] == "btrfs" else 1, stdout=b"", stderr=b"denied"
        )
        prune._delete_snapper_slot_btrfs(endpoint, "/backups/.snapshots/9", True)

    def test_it_targets_the_exact_slot_path(self):
        from btrfs_backup_ng.cli import prune

        seen = []
        endpoint = self._endpoint()
        endpoint._exec_remote_command = lambda cmd, **kw: (
            seen.append(cmd) or SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
        )
        prune._delete_snapper_slot_btrfs(endpoint, "/backups/.snapshots/9", True)
        assert seen[0][-1] == "/backups/.snapshots/9/snapshot"
        assert seen[1][-1] == "/backups/.snapshots/9"

    def test_a_raw_target_reuses_the_endpoint_delete(self):
        """Not reimplemented here: RawEndpoint.delete_snapshots already holds the
        per-target lock and enforces the chain guard that refuses to orphan an
        incremental child."""
        from btrfs_backup_ng.cli import prune

        snap = SimpleNamespace(get_name=lambda: "home-1")
        endpoint = SimpleNamespace(
            config={"path": "raw:///backups"},
            list_snapshots=lambda: [snap],
            delete_snapshots=MagicMock(),
        )
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=endpoint):
            deleted, errors = prune.delete_snapper_backups(
                "raw:///backups", [{"number": 1, "backup_name": "home-1"}]
            )
        assert deleted == 1 and not errors
        endpoint.delete_snapshots.assert_called_once()

    def test_a_raw_backup_with_no_matching_stream_is_an_error_not_a_silent_skip(self):
        from btrfs_backup_ng.cli import prune

        endpoint = SimpleNamespace(
            config={"path": "raw:///backups"},
            list_snapshots=lambda: [],
            delete_snapshots=MagicMock(),
        )
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=endpoint):
            deleted, errors = prune.delete_snapper_backups(
                "raw:///backups", [{"number": 1, "backup_name": "gone"}]
            )
        assert deleted == 0
        assert errors and "gone" in errors[0]
        assert not endpoint.delete_snapshots.called

    def test_one_bad_delete_does_not_abort_the_rest(self):
        from btrfs_backup_ng.cli import prune

        endpoint = self._endpoint()
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=endpoint):
            with patch.object(
                prune,
                "_delete_snapper_slot_btrfs",
                side_effect=[RuntimeError("boom"), None, None],
            ):
                deleted, errors = prune.delete_snapper_backups(
                    "/backups", [{"number": n} for n in (1, 2, 3)]
                )
        assert deleted == 2
        assert len(errors) == 1 and "slot 1" in errors[0]

    def test_deleting_nothing_is_a_no_op(self):
        from btrfs_backup_ng.cli import prune

        with patch("btrfs_backup_ng.endpoint.choose_endpoint") as ce:
            assert prune.delete_snapper_backups("/backups", []) == (0, [])
        assert not ce.called


class TestSlotNumbersDoNotDecideAnything:
    """Real snapper numbers slots ASCENDING with time -- slot 12 is newer than
    slot 1. An early fixture here had it backwards (slot 1 newest), and while the
    result was correct by date, it meant the realistic ordering was never
    exercised. If anything in this path ever starts reasoning about the number
    instead of the date, these fail.
    """

    AGES = [0, 1, 2, 3, 5, 9, 16, 30, 60, 90, 120, 150]

    def _decide(self, backups):
        keep, delete = apply_retention(
            backups,
            _retention(daily=7, weekly=4, monthly=3, min="2d"),
            now=NOW,
            get_timestamp=snapper_backup_timestamp,
        )
        ages = sorted((NOW - b["metadata"].date).days for b in delete)
        return sorted(b["number"] for b in delete), ages

    def _ascending_with_time(self):
        """Real snapper: the highest number is the newest."""
        return [
            _backup(len(self.AGES) - i, days_ago=d) for i, d in enumerate(self.AGES)
        ]

    def _descending_with_time(self):
        """The inverted layout, kept as a control."""
        return [_backup(i + 1, days_ago=d) for i, d in enumerate(self.AGES)]

    def test_the_same_snapshots_are_chosen_under_either_numbering(self):
        _nums_a, ages_a = self._decide(self._ascending_with_time())
        _nums_b, ages_b = self._decide(self._descending_with_time())
        assert ages_a == ages_b, "the decision moved when only the numbering did"

    def test_with_real_snapper_numbering_the_low_numbers_go(self):
        numbers, ages = self._decide(self._ascending_with_time())
        assert numbers and max(numbers) < 6, (
            f"expected the OLDEST (low-numbered) slots, got {numbers}"
        )
        assert min(ages) >= 90, f"only genuinely old backups may go, got {ages}"

    def test_the_newest_slot_is_never_deleted(self):
        numbers, _ages = self._decide(self._ascending_with_time())
        assert 12 not in numbers, "the newest backup was selected for deletion"

    def test_it_is_the_date_not_the_number_that_orders_them(self):
        """Numbers deliberately scrambled against dates: snapper reuses numbers
        after its own pruning, so a low number can be newer than a high one.
        Enough backups to overflow the buckets, otherwise even a very old lone
        snapshot is legitimately kept as the sole occupant of its bucket."""
        # number 99 is the OLDEST, number 1 is the NEWEST -- the opposite of what
        # the numbers suggest.
        scrambled = [99, 3, 77, 12, 5, 41, 8, 60, 2, 33, 17, 1]
        ages = sorted(self.AGES, reverse=True)  # oldest first, matching the numbers
        backups = [_backup(n, days_ago=a) for n, a in zip(scrambled, ages)]

        _keep, delete = apply_retention(
            backups,
            _retention(daily=7, weekly=4, monthly=3, min="2d"),
            now=NOW,
            get_timestamp=snapper_backup_timestamp,
        )
        deleted_ages = sorted((NOW - b["metadata"].date).days for b in delete)
        assert deleted_ages, "nothing was selected"
        # Whatever went, it must be the OLDEST backups -- never the newest.
        assert min(deleted_ages) >= 90, deleted_ages
        assert 99 in [b["number"] for b in delete], "the oldest backup survived"
        assert 1 not in [b["number"] for b in delete], "the newest was deleted"


class TestTheRunPathActuallyInvokesRetention:
    """Pinning the two lines that connect the phase to the pipeline.

    Everything else here drives _prune_snapper_after_transfer directly, so
    deleting its call from _backup_snapper_volume -- and the `and prune_ok` on
    the return -- left the whole suite green while retention silently stopped
    running. That is the same defect shape this series exists to remove, so it
    does not get to live in the code that fixes it.
    """

    def _run_snapper_backup(self, monkeypatch, prune_result=True):
        from pathlib import Path

        from btrfs_backup_ng.cli import run as run_mod
        from btrfs_backup_ng.config.schema import (
            Config,
            GlobalConfig,
            TargetConfig,
            VolumeConfig,
        )
        from btrfs_backup_ng.core import operations as ops
        from btrfs_backup_ng.snapper.scanner import SnapperConfig

        monkeypatch.setattr(ops, "sync_snapper_snapshots", lambda *a, **k: 1)
        monkeypatch.setattr(
            run_mod.endpoint,
            "choose_endpoint",
            lambda *a, **k: MagicMock(_is_remote=False),
        )

        volume = VolumeConfig(path="/", snapshot_prefix="root-")
        volume.source = "snapper"
        volume.snapper = MagicMock(config_name="root")
        volume.targets = [TargetConfig(path="/mnt/backup")]
        config = Config(global_config=GlobalConfig(), volumes=[volume])

        calls = []

        def fake_phase(vol, cfg, succeeded, errors):
            calls.append((vol, succeeded))
            if not prune_result:
                errors.append("Prune /mnt/backup: boom")
            return prune_result

        monkeypatch.setattr(run_mod, "_prune_snapper_after_transfer", fake_phase)

        with patch("btrfs_backup_ng.snapper.SnapperScanner") as scanner_cls:
            scanner = MagicMock()
            scanner.find_config_for_path.return_value = SnapperConfig(
                name="root", subvolume=Path("/")
            )
            scanner_cls.return_value = scanner
            ok, stats, errors = run_mod._backup_snapper_volume(volume, config)
        return ok, stats, errors, calls

    def test_the_retention_phase_is_called(self, monkeypatch):
        _ok, _stats, _errors, calls = self._run_snapper_backup(monkeypatch)
        assert calls, "the snapper run path never invoked retention"

    def test_it_is_given_the_targets_that_succeeded(self, monkeypatch):
        _ok, _stats, _errors, calls = self._run_snapper_backup(monkeypatch)
        _volume, succeeded = calls[0]
        assert succeeded, "retention was called with no successful targets"
        target, endpoint_config = succeeded[0]
        assert target.path == "/mnt/backup"
        assert "path" in endpoint_config, endpoint_config

    def test_a_successful_prune_leaves_the_run_successful(self, monkeypatch):
        ok, _stats, errors, _calls = self._run_snapper_backup(monkeypatch)
        assert ok is True and not errors

    def test_a_failed_prune_fails_the_run(self, monkeypatch):
        """Mutation guard for `and prune_ok`: drop it and a retention failure is
        reported as a clean backup."""
        ok, _stats, errors, _calls = self._run_snapper_backup(
            monkeypatch, prune_result=False
        )
        assert ok is False, "a failed retention was reported as a successful run"
        assert any("boom" in e for e in errors)
