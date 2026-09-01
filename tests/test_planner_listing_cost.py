"""Presence detection must cost ONE listing, not one per snapshot.

Reported as issue #106: `plan_transfer_sequence` took about 3.5 minutes for a
44-snapshot source over ssh. `snapshots_present_on` asked
`destination_endpoint.correspondent_of(s)` for every source snapshot, and the
base `correspondent_of` calls `self.list_snapshots()` to find its match. On an
ssh:// destination that is a remote `btrfs subvolume list` per source snapshot --
about three seconds each -- so the run spent over two minutes deciding what to
send before sending anything, and logged "Found 44 remote snapshots" once per
snapshot.

The listing is the expensive part and does not change while it is being
consulted, so it is taken once. `correspondents_of` is the batch form, with the
SAME per-endpoint rule (received_uuid for btrfs, name for raw) and the same
never-raises contract.

These tests pin the COST as well as the behaviour, because the behaviour was
never wrong -- only the number of round trips.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.core.planning import snapshots_present_on
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint


def _snap(name, uuid="", received=""):
    m = MagicMock()
    m.get_name.return_value = name
    m.uuid = uuid
    m.received_uuid = received
    return m


class _CountingEndpoint:
    """Counts listings. Each one is a remote round trip in the real thing."""

    def __init__(self, destination):
        self._destination = destination
        self.listings = 0

    def list_snapshots(self, flush_cache=False):
        self.listings += 1
        return self._destination

    correspondent_of = Endpoint.correspondent_of
    correspondents_of = Endpoint.correspondents_of


class _CountingRawEndpoint(_CountingEndpoint):
    correspondent_of = RawEndpoint.correspondent_of
    correspondents_of = RawEndpoint.correspondents_of


class TestPresenceDetectionCostsOneListing:
    @pytest.mark.parametrize("count", [1, 5, 44, 200])
    def test_one_listing_regardless_of_snapshot_count(self, count):
        source = [_snap(f"s{i}", uuid=f"u{i}") for i in range(count)]
        endpoint = _CountingEndpoint([])
        snapshots_present_on(source, endpoint)
        assert endpoint.listings == 1, (
            f"{count} source snapshots caused {endpoint.listings} listings. On an "
            f"ssh:// destination each is a remote btrfs subvolume list, which is "
            f"what made a 44-snapshot plan take minutes (#106)."
        )

    def test_raw_destinations_too(self):
        source = [_snap(f"s{i}") for i in range(44)]
        endpoint = _CountingRawEndpoint([])
        snapshots_present_on(source, endpoint)
        assert endpoint.listings == 1


class TestTheBatchFormAgreesWithTheSingularOne:
    """The optimisation must not change a single verdict."""

    SHAPES = {
        "normal match": (
            [_snap("a", "u1"), _snap("b", "u2")],
            [_snap("x", received="u1")],
        ),
        "source has no uuid": ([_snap("a", "")], [_snap("x", received="u1")]),
        "empty destination": ([_snap("a", "u1")], []),
        "two destinations share a received_uuid": (
            [_snap("a", "u1")],
            [_snap("x", received="u1"), _snap("y", received="u1")],
        ),
        "destination has no received_uuid": (
            [_snap("a", "u1")],
            [_snap("a", received="")],
        ),
        "name collides but uuid does not": (
            [_snap("a", "u1")],
            [_snap("a", received="other")],
        ),
    }

    @pytest.mark.parametrize("shape", list(SHAPES))
    @pytest.mark.parametrize("kind", ["btrfs", "raw"])
    def test_same_verdict(self, shape, kind):
        source, destination = self.SHAPES[shape]
        cls = _CountingEndpoint if kind == "btrfs" else _CountingRawEndpoint
        endpoint = cls(destination)
        singular = {
            s.get_name() for s in source if endpoint.correspondent_of(s) is not None
        }
        batch = set(endpoint.correspondents_of(source))
        assert singular == batch, (
            f"{kind}/{shape}: singular said {singular}, batch said {batch}"
        )

    def test_the_batch_form_returns_the_same_objects(self):
        """Callers may use the correspondent itself, not just its presence."""
        received = _snap("x", received="u1")
        endpoint = _CountingEndpoint([received])
        source = _snap("a", "u1")
        assert endpoint.correspondents_of([source])["a"] is received
        assert endpoint.correspondent_of(source) is received


class TestItNeverRaises:
    """A listing failure must read as 'nothing corresponds', which degrades to
    full transfers -- never to an unapplyable send -p."""

    @pytest.mark.parametrize("kind", ["btrfs", "raw"])
    def test_a_failing_listing_yields_nothing(self, kind):
        cls = _CountingEndpoint if kind == "btrfs" else _CountingRawEndpoint

        class Broken(cls):
            def list_snapshots(self, flush_cache=False):
                raise OSError("remote went away")

        endpoint = Broken([])
        assert endpoint.correspondents_of([_snap("a", "u1")]) == {}
        assert endpoint.correspondent_of(_snap("a", "u1")) is None

    def test_the_planner_degrades_rather_than_raising(self):
        class Broken(_CountingEndpoint):
            def list_snapshots(self, flush_cache=False):
                raise OSError("remote went away")

        assert snapshots_present_on([_snap("a", "u1")], Broken([])) == set()


class TestUnknownIdentityIsNeverPresence:
    """A snapshot whose identity is unknown must never be reported as present.

    Presence means "already backed up", so a false positive SKIPS the transfer:
    the snapshot is never sent and the run reports success. That is a phantom
    backup, and it is the failure this project guards hardest against.

    Two guards prevent it -- a source with an empty uuid is skipped, and a
    destination with an empty received_uuid is not indexed. Each is individually
    a no-op (removing either alone changes nothing), so single-line mutation
    cannot catch their loss. Removing BOTH matches an unidentifiable source
    against a never-received destination, which is why the invariant is asserted
    directly here instead.
    """

    def test_an_unidentifiable_source_never_matches(self):
        source = _snap("orphan", uuid="")
        destination = _snap("never-received", received="")
        endpoint = _CountingEndpoint([destination])

        assert endpoint.correspondents_of([source]) == {}, (
            "a snapshot with no uuid was matched against a destination with no "
            "received_uuid -- it would be skipped as already backed up and never "
            "transferred"
        )
        assert endpoint.correspondent_of(source) is None
        assert snapshots_present_on([source], endpoint) == set()

    def test_a_real_uuid_still_matches_a_real_received_uuid(self):
        """The guards must not block genuine correspondence."""
        destination = _snap("received", received="u1")
        endpoint = _CountingEndpoint([destination])
        assert snapshots_present_on([_snap("a", uuid="u1")], endpoint) == {"a"}

    @pytest.mark.parametrize(
        ("src_uuid", "dest_received"),
        [("", ""), ("", "u1"), ("u1", ""), ("u1", "other")],
    )
    def test_no_partial_identity_counts_as_presence(self, src_uuid, dest_received):
        endpoint = _CountingEndpoint([_snap("d", received=dest_received)])
        source = _snap("s", uuid=src_uuid)
        expected = {"s"} if (src_uuid and src_uuid == dest_received) else set()
        assert snapshots_present_on([source], endpoint) == expected
