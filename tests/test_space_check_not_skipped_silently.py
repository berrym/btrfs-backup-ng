"""A space check must not be skipped by claiming there is no data.

A snapshot whose size could not be determined contributes nothing to the
transfer total, so "nothing to transfer" and "nothing could be measured"
reached the same line and both printed "No data to transfer - skipping space
check." The check was skipped precisely when the numbers behind it were absent.
"""

import types

import pytest

from btrfs_backup_ng.cli.estimate import _print_space_check
from btrfs_backup_ng.core.estimate import SnapshotEstimate, TransferEstimate


class _Endpoint:
    def __init__(self):
        self.asked = False

    def get_space_info(self):
        self.asked = True
        return {"free": 10 * 1024**3, "total": 100 * 1024**3, "used": 90 * 1024**3}


def _run(estimate):
    endpoint = _Endpoint()
    args = types.SimpleNamespace(check_space=True, safety_margin=10)
    _print_space_check(args, endpoint, estimate)
    return endpoint


def test_an_unmeasurable_snapshot_is_not_reported_as_no_data(capsys):
    estimate = TransferEstimate()
    estimate.add_snapshot(SnapshotEstimate(name="home.1", full_size=None))

    _run(estimate)

    out = capsys.readouterr().out
    assert "No data to transfer" not in out, (
        "an unmeasurable snapshot was reported as nothing to transfer"
    )
    assert "NOT verified" in out
    assert "could not be determined" in out


def test_a_genuinely_empty_plan_still_says_no_data(capsys):
    """The honest case must keep its honest message."""
    _run(TransferEstimate())

    assert "No data to transfer" in capsys.readouterr().out


def test_a_measured_transfer_actually_checks_space():
    """The check still runs when there are real numbers to check against."""
    estimate = TransferEstimate()
    estimate.add_snapshot(SnapshotEstimate(name="home.1", full_size=5 * 1024**3))

    assert _run(estimate).asked, "the space check never consulted the destination"


@pytest.mark.parametrize(
    "snapshot,expected",
    [
        (SnapshotEstimate(name="a", full_size=None), 1),
        (SnapshotEstimate(name="b", full_size=0), 0),
        (SnapshotEstimate(name="c", full_size=1024), 0),
        (
            SnapshotEstimate(
                name="d", full_size=None, incremental_size=None, is_incremental=True
            ),
            1,
        ),
        (
            SnapshotEstimate(
                name="e", full_size=None, incremental_size=512, is_incremental=True
            ),
            0,
        ),
    ],
)
def test_unmeasured_count_distinguishes_unknown_from_zero(snapshot, expected):
    """A measured zero is a fact; an unknown size is the absence of one."""
    estimate = TransferEstimate()
    estimate.add_snapshot(snapshot)

    assert estimate.unmeasured_count == expected
