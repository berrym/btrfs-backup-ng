"""A transfer's outcome must reach the caller that reports it.

sync_snapshots returns a TransferResult and attaches it to the exception it
raises on failure (operations._raise_transfer_failures), specifically so a
caller can say "3 of 5 transferred, 2 failed" rather than inferring success from
the absence of an exception. Every caller discarded it.

Measured before the change: _transfer_to_target returned True both for a result
carrying three delivered snapshots and for the empty result an up-to-date
destination produces. The caller then did stats["completed"] += 1 for either,
and that counter is what the completion notification reports as "transfers
completed". A run that moved nothing at all reported the same number as one that
delivered every backup.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from btrfs_backup_ng.cli import run as run_mod
from btrfs_backup_ng.core.operations import TransferResult


def _target_config():
    tc = MagicMock()
    tc.compress = None
    tc.rate_limit = None
    tc.ssh_sudo = False
    tc.path = "raw:///x"
    return tc


def _transfer(result=None, raises=None):
    with patch.object(
        run_mod, "sync_snapshots", side_effect=raises, return_value=result
    ):
        return run_mod._transfer_to_target(
            MagicMock(),
            MagicMock(),
            _target_config(),
            None,
            True,
            None,
            None,
            False,
            {},
            0,
        )


class TestTheOutcomeSurvivesTheCall:
    def test_a_delivery_reports_what_it_delivered(self):
        out = _transfer(TransferResult(transferred=["a", "b", "c"]))
        assert out is not None and out.transferred_count == 3

    def test_an_up_to_date_destination_is_distinguishable_from_a_delivery(self):
        """Both are successes. Only one of them moved data, and the summary and
        the notification both report a count."""
        delivered = _transfer(TransferResult(transferred=["a"]))
        up_to_date = _transfer(TransferResult())

        assert delivered is not None and up_to_date is not None
        assert delivered.transferred_count != up_to_date.transferred_count

    def test_a_failure_is_reported_as_none(self):
        from btrfs_backup_ng import __util__

        assert _transfer(raises=__util__.AbortError("boom")) is None
        assert _transfer(raises=RuntimeError("boom")) is None

    def test_a_partial_failure_reports_how_far_it_got(self, caplog):
        """err.result exists for this and nothing read it, so "3 of 5 delivered"
        and "nothing moved" were the same log line."""
        import logging

        from btrfs_backup_ng import __util__

        err = __util__.AbortError("2 failed")
        err.result = TransferResult(transferred=["a", "b", "c"], failed=[("d", "x")])

        with caplog.at_level(logging.ERROR, logger="btrfs_backup_ng.cli.run"):
            assert _transfer(raises=err) is None

        assert "3 of 4" in caplog.text, (
            f"the abort did not say how far it got: {caplog.text!r}"
        )

    def test_a_total_failure_does_not_claim_partial_progress(self, caplog):
        import logging

        from btrfs_backup_ng import __util__

        err = __util__.AbortError("nothing moved")
        err.result = TransferResult(failed=[("a", "x")])

        with caplog.at_level(logging.ERROR, logger="btrfs_backup_ng.cli.run"):
            assert _transfer(raises=err) is None

        assert " of " not in caplog.text


class TestTheRunSummaryCountsSnapshotsNotTargets:
    def test_every_snapshot_counter_adds_the_delivered_count(self):
        """`completed` counts targets and is what the notification reports. A
        separate snapshot count is what makes the summary line true.

        Checked by AST at EVERY augmented assignment, not by searching the source
        for one occurrence: there are two scoring sites, parallel and sequential,
        and a substring check is satisfied by whichever one was left alone.
        """
        import ast
        import inspect

        tree = ast.parse(inspect.getsource(run_mod))
        sites = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.AugAssign):
                continue
            target = node.target
            if not (
                isinstance(target, ast.Subscript)
                and isinstance(target.slice, ast.Constant)
                and target.slice.value == "snapshots_transferred"
            ):
                continue
            sites.append((node.lineno, ast.dump(node.value)))

        assert len(sites) >= 3, (
            f"expected the two scoring sites plus the roll-up, found {len(sites)}"
        )
        wrong = [
            line
            for line, value in sites
            if "transferred_count" not in value and "snapshots_transferred" not in value
        ]
        assert not wrong, (
            f"snapshots_transferred incremented by something other than the "
            f"delivered count at line(s) {wrong}"
        )

    def test_no_scoring_site_reads_the_outcome_as_a_boolean(self):
        """`if outcome:` would score an up-to-date target as a failure, since an
        empty TransferResult is falsy on a dataclass with empty lists only if
        __bool__/__len__ exist -- and relying on that is the same class of
        accident as the original bool."""
        import ast
        import inspect

        tree = ast.parse(inspect.getsource(run_mod))
        bad = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id == "outcome"
        ]
        assert not bad, f"outcome used as a truth value at line(s) {bad}"


def test_an_empty_result_is_not_falsy():
    """Pins why the explicit `is not None` matters: if TransferResult ever grows
    a __bool__, an up-to-date destination would start scoring as a failure."""
    assert bool(TransferResult()) is True
