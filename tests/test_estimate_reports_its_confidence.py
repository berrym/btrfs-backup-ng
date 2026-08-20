"""`estimate` must not quote a floor as though it were a measurement.

The incremental figure comes from `btrfs send --no-data`, a stream carrying
metadata and no file contents; the code that produces it says outright that the
real transfer runs roughly 10-100x larger. It was then printed as "Total data to
transfer: 12.4 MiB" with no caveat, and emitted as a bare number in --json.

Anyone sizing a link or a maintenance window off that would be out by up to two
orders of magnitude. Found while answering #93, where the same underestimate
came up as a reason NOT to derive a transfer timeout from it.

A full transfer is measured directly (btrfs subvolume show / filesystem du / du)
and is not affected -- so the labelling has to distinguish the two, not blanket
everything with a disclaimer nobody reads.
"""

from __future__ import annotations

from btrfs_backup_ng.core.estimate import (
    UNDERESTIMATING_METHODS,
    SnapshotEstimate,
    TransferEstimate,
    print_estimate,
)


def _incremental(method, size=1000):
    return SnapshotEstimate(
        name="snap-incr",
        full_size=50_000,
        incremental_size=size,
        parent_name="snap-parent",
        is_incremental=True,
        method=method,
    )


def _full(method="filesystem_du", size=60_000):
    return SnapshotEstimate(name="snap-full", full_size=size, method=method)


class TestTheTotalSaysWhetherItIsAFloor:
    def test_an_underestimating_incremental_marks_the_total(self):
        estimate = TransferEstimate()
        estimate.add_snapshot(_incremental("send_no_data"))
        assert estimate.total_is_lower_bound is True

    def test_a_measured_full_transfer_does_not(self):
        """The case #93 cares about is measured directly; it must stay unqualified."""
        estimate = TransferEstimate()
        estimate.add_snapshot(_full())
        assert estimate.total_is_lower_bound is False

    def test_one_underestimate_among_many_is_enough(self):
        """The sum is only as trustworthy as its least trustworthy term."""
        estimate = TransferEstimate()
        estimate.add_snapshot(_full())
        estimate.add_snapshot(_incremental("send_no_data"))
        assert estimate.total_is_lower_bound is True

    def test_the_size_diff_fallback_counts_too(self):
        """It misses anything rewritten in place, so it is a floor as well."""
        estimate = TransferEstimate()
        estimate.add_snapshot(_incremental("size_diff"))
        assert estimate.total_is_lower_bound is True

    def test_both_named_methods_are_covered(self):
        for method in UNDERESTIMATING_METHODS:
            estimate = TransferEstimate()
            estimate.add_snapshot(_incremental(method))
            assert estimate.total_is_lower_bound is True, method

    def test_an_incremental_with_no_size_does_not_taint_the_total(self):
        """Estimation failed for it, so it contributed its full size instead."""
        estimate = TransferEstimate()
        snapshot = _incremental("send_no_data", size=None)
        estimate.add_snapshot(snapshot)
        assert estimate.total_is_lower_bound is False


class TestTheOutputSaysSo:
    def _output(self, capsys, *snapshots):
        estimate = TransferEstimate()
        for snapshot in snapshots:
            estimate.add_snapshot(snapshot)
        estimate.new_snapshot_count = len(snapshots)
        print_estimate(estimate, "src", "dst")
        return capsys.readouterr().out

    def test_the_total_is_qualified_when_it_is_a_floor(self, capsys):
        out = self._output(capsys, _incremental("send_no_data"))
        assert "AT LEAST" in out
        assert "no file contents" in out
        assert "10-100" in out

    def test_the_row_is_marked_too(self, capsys):
        """Someone scanning the table for the big one must not be misled by a row."""
        out = self._output(capsys, _incremental("send_no_data"))
        assert ">=" in out

    def test_a_measured_total_is_not_hedged(self, capsys):
        """A disclaimer on everything is a disclaimer nobody reads."""
        out = self._output(capsys, _full())
        assert "AT LEAST" not in out
        assert ">=" not in out
        assert "Total data to transfer:" in out


class TestTheJsonCarriesItToo:
    def test_the_flag_is_emitted(self, capsys):
        from btrfs_backup_ng.cli.estimate import _print_json

        estimate = TransferEstimate()
        estimate.add_snapshot(_incremental("send_no_data"))
        estimate.new_snapshot_count = 1
        _print_json(estimate, "src", "dst", None, None)

        import json

        data = json.loads(capsys.readouterr().out)
        assert data["total_transfer_is_lower_bound"] is True, (
            "the machine-readable form is the more misleading of the two without this"
        )

    def test_it_is_false_for_a_measured_transfer(self, capsys):
        from btrfs_backup_ng.cli.estimate import _print_json

        estimate = TransferEstimate()
        estimate.add_snapshot(_full())
        estimate.new_snapshot_count = 1
        _print_json(estimate, "src", "dst", None, None)

        import json

        assert (
            json.loads(capsys.readouterr().out)["total_transfer_is_lower_bound"]
            is False
        )
