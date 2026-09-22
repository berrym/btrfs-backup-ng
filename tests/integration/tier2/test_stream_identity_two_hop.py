"""Tier 2: the stream identity is two-hop on a real btrfs chain.

Three loopback filesystems hold a chain O -> S -> R: O is an original read-only
snapshot on the first, S its received copy on the second (the backup), R a
received copy of S on the third (a transfer onward from the mirror, or a restore).
The btrfs fact these cells rest on: ``btrfs send`` of S emits S's received_uuid,
so R.received_uuid == O.uuid, not S.uuid. The unit suite proves the rule against
fakes; this proves the fact the rule encodes, on the filesystem type it applies to,
and drives the engine's own planner and transfer across the second hop.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.core.planning import plan_transfer_sequence
from btrfs_backup_ng.endpoint.local import LocalEndpoint

from .conftest import create_snapshot, requires_btrfs, send_snapshot

PREFIX = "home-"
FIRST = "home-20260101-120000"
SECOND = "home-20260102-120000"


def _subvolume_show(path: Path) -> dict[str, str]:
    out = subprocess.run(
        ["btrfs", "subvolume", "show", str(path)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    fields: dict[str, str] = {}
    for line in out.splitlines():
        key, sep, value = line.strip().partition(":")
        if sep:
            fields[key.strip()] = value.strip()
    return fields


def _endpoint(path: Path) -> LocalEndpoint:
    return LocalEndpoint(config={"path": str(path), "snap_prefix": PREFIX})


def _listed(path: Path, name: str):
    for snap in _endpoint(path).list_snapshots(flush_cache=True):
        if snap.get_name() == name:
            return snap
    raise AssertionError(f"{name} is not listed at {path}")


def _original(first: Path, name: str, payload: str) -> Path:
    """A read-only snapshot on the first filesystem carrying ``payload``."""
    data = first / "data"
    if not data.exists():
        subprocess.run(
            ["btrfs", "subvolume", "create", str(data)],
            check=True,
            capture_output=True,
        )
    (data / "file.txt").write_text(payload)
    return create_snapshot(data, first / name, readonly=True)


@pytest.mark.tier2
@requires_btrfs
class TestStreamIdentityTwoHop:
    def test_second_hop_copy_corresponds_to_the_first_hop_copy(
        self, btrfs_three_volumes
    ):
        """R corresponds to S. The old rule compared R.received_uuid with S.uuid,
        which btrfs never makes equal; the ids read straight from ``subvolume
        show`` are asserted first so a failure names the fact, not the rule."""
        first, second, third = btrfs_three_volumes
        original = _original(first, FIRST, "payload")
        send_snapshot(original, second)  # O -> S
        send_snapshot(second / FIRST, third)  # S -> R

        o = _subvolume_show(original)
        s = _subvolume_show(second / FIRST)
        r = _subvolume_show(third / FIRST)
        assert s["Received UUID"] == o["UUID"]
        assert r["Received UUID"] == o["UUID"], "the stream carried O's identity"
        assert r["Received UUID"] != s["UUID"], "not the first-hop copy's own uuid"

        s_listed = _listed(second, FIRST)
        r_listed = _listed(third, FIRST)
        assert s_listed.uuid == s["UUID"]
        assert s_listed.stream_uuid == o["UUID"]
        assert r_listed.received_uuid == o["UUID"]

        dest = _endpoint(third)
        assert dest.correspondent_of(s_listed) is not None
        assert dest.correspondent_of(s_listed).get_name() == FIRST
        assert set(dest.correspondents_of([s_listed])) == {FIRST}

    def test_a_truncated_second_hop_receive_does_not_correspond(
        self, btrfs_three_volumes
    ):
        """A receive cut off mid-stream leaves a SUBVOLUME under the right name
        with no received_uuid. It must read as absent -- a rerun re-sends it --
        never as the copy by virtue of its name."""
        first, second, third = btrfs_three_volumes
        original = _original(first, FIRST, "x" * 65536)
        send_snapshot(original, second)  # O -> S

        # S -> R, truncated: only the first 4 KiB of the stream reach receive.
        pipeline = subprocess.run(
            [
                "sh",
                "-c",
                "set -o pipefail; btrfs send "
                + str(second / FIRST)
                + " 2>/dev/null | head -c 4096 | btrfs receive "
                + str(third),
            ],
            capture_output=True,
            text=True,
        )
        assert pipeline.returncode != 0, pipeline.stderr
        partial = third / FIRST
        assert partial.is_dir()
        assert os.stat(partial).st_ino == 256, "the leftover is a subvolume"
        assert _subvolume_show(partial)["Received UUID"] == "-"

        s_listed = _listed(second, FIRST)
        partial_listed = _listed(third, FIRST)  # listed by name, so not vacuous
        assert partial_listed.received_uuid == ""

        dest = _endpoint(third)
        assert dest.correspondent_of(s_listed) is None
        assert dest.correspondents_of([s_listed]) == {}

        subprocess.run(
            ["btrfs", "subvolume", "delete", str(partial)],
            check=True,
            capture_output=True,
        )

    def test_second_hop_planning_is_incremental(self, btrfs_three_volumes):
        """The engine's planner, asked to move the mirror's two copies onward
        when the third filesystem already holds R1: one incremental send of S2
        parented on S1, executed through ``send_snapshot``. The received R2 then
        carries O2's identity and a second plan is empty."""
        first, second, third = btrfs_three_volumes
        o1 = _original(first, FIRST, "one")
        o2 = _original(first, SECOND, "two")
        send_snapshot(o1, second)  # O1 -> S1
        send_snapshot(o2, second, parent=o1)  # O2 -> S2, incremental
        send_snapshot(second / FIRST, third)  # S1 -> R1

        s1 = _listed(second, FIRST)
        s2 = _listed(second, SECOND)
        dest = _endpoint(third)

        plan = plan_transfer_sequence([s1, s2], dest)
        assert [
            (snap.get_name(), parent and parent.get_name()) for snap, parent in plan
        ] == [(SECOND, FIRST)]

        (snap, parent) = plan[0]
        ops.send_snapshot(snap, dest, parent=parent, options={"check_space": False})

        r2 = _subvolume_show(third / SECOND)
        assert r2["Received UUID"] == _subvolume_show(o2)["UUID"]
        assert (third / SECOND / "file.txt").read_text() == "two"
        # A later run lists the destination afresh (an endpoint caches its listing
        # for the life of the process) and finds nothing left to move.
        assert plan_transfer_sequence([s1, s2], _endpoint(third)) == []
