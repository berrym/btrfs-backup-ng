"""Both pipelines of `run`, and `transfer`, say a target's outcome in the
same words.

The native path said "<target> is already up to date" where the snapper
path said "Transferred 0 snapshot(s) to <target>"; the native path read the
result a failed transfer attached to its exception and said "aborted after 3
of 5 snapshot(s)" where the snapper path logged the exception text alone.
One place now puts a target's outcome into words (cli/common.py), and every
caller goes through it.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import common, run as run_cli
from btrfs_backup_ng.core.operations import TransferResult

CLI = Path(__file__).resolve().parent.parent / "src" / "btrfs_backup_ng" / "cli"
LOG = "btrfs_backup_ng.cli.run"


def _partial_failure() -> __util__.AbortError:
    err = __util__.SnapshotTransferError("2 of 5 snapshot transfer(s) failed: d, e")
    err.result = TransferResult(
        transferred=["a", "b", "c"], failed=[("d", "x"), ("e", "y")]
    )
    return err


def _native(monkeypatch, outcome):
    monkeypatch.setattr(run_cli, "sync_snapshots", MagicMock(side_effect=outcome))
    target = SimpleNamespace(
        path="/mnt/t", compress=None, rate_limit=None, ssh_sudo=False
    )
    return run_cli._transfer_to_target(
        MagicMock(), MagicMock(), target, None, True, None, None, False, {}, 0
    )


def _snapper(monkeypatch, outcome):
    monkeypatch.setattr(
        "btrfs_backup_ng.core.operations.sync_snapper_snapshots",
        MagicMock(side_effect=outcome),
    )
    monkeypatch.setattr(
        "btrfs_backup_ng.snapper.SnapperScanner", lambda *a, **k: object()
    )
    monkeypatch.setattr(run_cli, "assert_target_mounted", lambda *a: None)
    monkeypatch.setattr(
        run_cli, "snapper_destination_options", lambda *a: {"compress": "none"}
    )
    monkeypatch.setattr(run_cli, "_snapper_catch_up_selector", lambda *a: None)
    monkeypatch.setattr(run_cli, "_prune_snapper_after_transfer", lambda *a: True)
    monkeypatch.setattr(run_cli.endpoint, "choose_endpoint", lambda *a: MagicMock())
    for check in ("assert_encryption_applied", "assert_compression_applied"):
        monkeypatch.setattr(run_cli.endpoint, check, lambda *a: None)
    volume = SimpleNamespace(
        path="/home",
        snapper=SimpleNamespace(config_name="root"),
        targets=[
            SimpleNamespace(
                path="/mnt/t",
                require_mount=False,
                compress=None,
                rate_limit=None,
                encrypt=None,
            )
        ],
    )
    config = SimpleNamespace(global_config=SimpleNamespace(transfer_timeout=0))
    return run_cli._backup_snapper_volume(volume, config)


def _lines(caplog, level):
    return [r.message for r in caplog.records if r.levelno == level]


class TestBothPathsSayTheSameThing:
    @pytest.mark.parametrize(
        "path, outcome",
        [
            ("native", [TransferResult()]),
            ("snapper", [0]),
        ],
    )
    def test_an_up_to_date_target_is_said_to_be_up_to_date(
        self, monkeypatch, caplog, path, outcome
    ):
        with caplog.at_level(logging.INFO, logger=LOG):
            (_native if path == "native" else _snapper)(monkeypatch, outcome)
        info = _lines(caplog, logging.INFO)
        assert "/mnt/t is already up to date" in info, info
        assert not any("Transferred 0" in line for line in info), info

    @pytest.mark.parametrize(
        "path, outcome",
        [
            ("native", [TransferResult(transferred=["a", "b"])]),
            ("snapper", [2]),
        ],
    )
    def test_a_delivery_is_said_with_its_count(
        self, monkeypatch, caplog, path, outcome
    ):
        with caplog.at_level(logging.INFO, logger=LOG):
            (_native if path == "native" else _snapper)(monkeypatch, outcome)
        assert "Transferred 2 snapshot(s) to /mnt/t" in _lines(caplog, logging.INFO)

    @pytest.mark.parametrize("path", ["native", "snapper"])
    def test_a_partial_failure_says_how_far_it_got(self, monkeypatch, caplog, path):
        with caplog.at_level(logging.ERROR, logger=LOG):
            (_native if path == "native" else _snapper)(monkeypatch, _partial_failure())
        errors = _lines(caplog, logging.ERROR)
        assert any(
            line.startswith("Transfer to /mnt/t aborted after 3 of 5 snapshot(s): ")
            for line in errors
        ), errors

    @pytest.mark.parametrize("path", ["native", "snapper"])
    def test_a_total_failure_does_not_claim_progress(self, monkeypatch, caplog, path):
        err = __util__.AbortError("nothing moved")
        err.result = TransferResult(failed=[("a", "x")])
        with caplog.at_level(logging.ERROR, logger=LOG):
            (_native if path == "native" else _snapper)(monkeypatch, err)
        errors = _lines(caplog, logging.ERROR)
        assert "Transfer to /mnt/t aborted: nothing moved" in errors, errors
        assert not any(" of " in line for line in errors)

    @pytest.mark.parametrize("path", ["native", "snapper"])
    def test_an_unexpected_error_is_a_failure_with_its_reason(
        self, monkeypatch, caplog, path
    ):
        with caplog.at_level(logging.ERROR, logger=LOG):
            (_native if path == "native" else _snapper)(
                monkeypatch, RuntimeError("boom")
            )
        assert "Transfer to /mnt/t failed: boom" in _lines(caplog, logging.ERROR)


class TestThereIsOnePlaceThatSaysIt:
    """A second copy of a sentence is how the two paths drifted apart. The
    sentences live in cli/common.py and nowhere else under cli/."""

    SENTENCES = (
        "%s is already up to date",
        "aborted after %d of %d",
        "Transfer to %s aborted",
        "Transfer to %s failed",
        "Skipping optional target %s",
        "Failed to prepare destination %s",
        "No transfers ran: all %d target",
        r"Transferred %d snapshot\(s\)",
    )

    def test_no_cli_module_but_common_spells_an_outcome_sentence(self):
        offenders = []
        for module in sorted(CLI.glob("*.py")):
            if module.name == "common.py":
                continue
            text = module.read_text(encoding="utf-8")
            for sentence in self.SENTENCES:
                for match in re.finditer(sentence, text):
                    line = text.count("\n", 0, match.start()) + 1
                    offenders.append(f"{module.name}:{line}: {sentence}")
        assert offenders == [], offenders

    def test_the_reporter_reads_the_count_from_the_result(self, caplog):
        log = logging.getLogger("btrfs_backup_ng.cli.common")
        with caplog.at_level(logging.ERROR, logger=log.name):
            common.report_transfer_failure(log, "/x", _partial_failure())
            common.report_transfer_failure(log, "/y", RuntimeError("no"), indent="  ")
        messages = _lines(caplog, logging.ERROR)
        assert messages == [
            "Transfer to /x aborted after 3 of 5 snapshot(s): "
            "2 of 5 snapshot transfer(s) failed: d, e",
            "  Transfer to /y failed: no",
        ]
