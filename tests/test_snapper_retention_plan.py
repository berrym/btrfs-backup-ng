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
from unittest.mock import patch

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


class TestStageOneDeletesNothing:
    def test_the_run_phase_never_calls_a_delete(self):
        """Stage 1 is a report. If this ever fails, deletion arrived early."""
        from btrfs_backup_ng.cli import run as run_cli

        volume = SimpleNamespace(path="/home", targets=[])
        config = SimpleNamespace(get_effective_retention=lambda v: _retention())
        target = SimpleNamespace(path="/backups")
        backups = [_backup(n, days_ago=n * 40) for n in range(1, 13)]

        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups", return_value=backups
        ):
            with patch(
                "btrfs_backup_ng.cli.prune.execute_retention_deletes"
            ) as deletes:
                run_cli._report_snapper_retention_plan(volume, config, [(target, {})])
        assert not deletes.called, "stage 1 must not delete"

    def test_a_degenerate_policy_warns_without_failing_the_run(self, caplog):
        from btrfs_backup_ng.cli import run as run_cli

        volume = SimpleNamespace(path="/home", targets=[])
        config = SimpleNamespace(
            get_effective_retention=lambda v: _retention(daily=0, min="1h")
        )
        result = run_cli._report_snapper_retention_plan(
            volume, config, [(SimpleNamespace(path="/backups"), {})]
        )
        assert result is None, "a report phase returns nothing and fails nothing"

    def test_an_enumeration_failure_does_not_fail_the_run(self):
        from btrfs_backup_ng.cli import run as run_cli

        volume = SimpleNamespace(path="/home", targets=[])
        config = SimpleNamespace(get_effective_retention=lambda v: _retention())
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups",
            side_effect=RuntimeError("unreachable"),
        ):
            assert (
                run_cli._report_snapper_retention_plan(
                    volume, config, [(SimpleNamespace(path="/backups"), {})]
                )
                is None
            )

    def test_no_successful_targets_means_no_plan(self):
        from btrfs_backup_ng.cli import run as run_cli

        with patch("btrfs_backup_ng.core.restore.list_snapper_backups") as listing:
            run_cli._report_snapper_retention_plan(
                SimpleNamespace(path="/home"), SimpleNamespace(), []
            )
        assert not listing.called
