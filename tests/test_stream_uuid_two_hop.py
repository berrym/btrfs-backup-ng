"""Identity is two-hop: correspondence compares against ``stream_uuid``.

``btrfs send`` emits a subvolume's received_uuid in place of its own uuid when
it has one, and ``btrfs receive`` records whatever the stream carried. So a copy
of a copy of O (a transfer onward from a mirror, a restore from a backup) has
received_uuid == O.uuid -- NOT the uuid of the copy it was sent from. The
engine's rule used to compare the candidate's received_uuid with the source's
``uuid``, which matched only the first hop; every second hop found nothing and
degraded to full sends. Every snapshot object now exposes ``stream_uuid``
(received_uuid or uuid) and the rule compares against that.

The chain is written O -> S -> R throughout: O the original, S its received
copy (the first hop), R a received copy of S (the second hop).

Mutation guard: reverting the comparison to ``snapshot.uuid`` fails the
second-hop tests here and nothing else -- the one-hop cases carry an empty
received_uuid, so ``stream_uuid == uuid`` and they cannot tell the two rules
apart. That indifference is the proof the change is non-behavioural for one hop.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.core.chunked_transfer import TransferManifest
from btrfs_backup_ng.core.planning import plan_transfer_sequence
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _snap(name, stamp, uuid="", received_uuid=""):
    s = __util__.Snapshot(
        "/snaps",
        "home-",
        None,
        time_obj=time.strptime(stamp, "%Y%m%d-%H%M%S"),
        name=name,
    )
    s.uuid = uuid
    s.received_uuid = received_uuid
    return s


class _FakeDest:
    """A destination double with the PRODUCTION correspondence bound verbatim, so
    only the listing is the test's, never the rule."""

    correspondent_of = Endpoint.correspondent_of
    correspondents_of = Endpoint.correspondents_of

    def __init__(self, dest_snaps=()):
        self._snaps = list(dest_snaps)

    def list_snapshots(self, flush_cache=False):
        return list(self._snaps)


# --------------------------------------------------------------------------- #
# stream_uuid on every snapshot object
# --------------------------------------------------------------------------- #
def test_snapshot_stream_uuid_is_received_uuid_or_uuid():
    original = _snap("home-20240101-000000", "20240101-000000", uuid="O")
    assert original.stream_uuid == "O"
    copy = _snap("home-20240101-000000", "20240101-000000", uuid="S", received_uuid="O")
    assert copy.stream_uuid == "O"
    unknown = _snap("home-20240101-000000", "20240101-000000")
    assert unknown.stream_uuid == ""


def test_snapper_btrfs_backup_stream_uuid_is_its_received_uuid():
    backup = ops._SnapperBtrfsBackup(7, "O")
    assert backup.stream_uuid == "O"


# --------------------------------------------------------------------------- #
# O -> S -> R: the second hop corresponds
# --------------------------------------------------------------------------- #
def test_second_hop_copy_corresponds_to_the_first_hop_copy():
    """R was received from a send of S; S was received from O. R.received_uuid is
    O's uuid (what the stream carried), and the rule must still find R when the
    source it is asked about is S. Under the old rule S.uuid ("S") != "O" and R
    was invisible."""
    s = _snap("home-20240101-000000", "20240101-000000", uuid="S", received_uuid="O")
    r = _snap("home-20240101-000000", "20240101-000000", uuid="R", received_uuid="O")
    dest = _FakeDest([r])

    assert dest.correspondent_of(s) is r
    assert dest.correspondents_of([s]) == {s.get_name(): r}


def test_one_hop_copy_still_corresponds_to_the_original():
    """The first hop is unchanged by construction: O was never received, so its
    stream_uuid IS its uuid. Indifferent to the mutation, by design."""
    o = _snap("home-20240101-000000", "20240101-000000", uuid="O")
    s = _snap("home-20240101-000000", "20240101-000000", uuid="S", received_uuid="O")
    dest = _FakeDest([s])

    assert dest.correspondent_of(o) is s
    assert dest.correspondents_of([o]) == {o.get_name(): s}


def test_a_partial_receive_does_not_correspond_at_the_second_hop():
    """A truncated receive leaves a subvolume with the right NAME and no
    received_uuid. It corresponds to nothing -- the name is never consulted."""
    s = _snap("home-20240101-000000", "20240101-000000", uuid="S", received_uuid="O")
    partial = _snap("home-20240101-000000", "20240101-000000", uuid="P")
    dest = _FakeDest([partial])

    assert dest.correspondent_of(s) is None
    assert dest.correspondents_of([s]) == {}


def test_a_source_without_identity_corresponds_to_nothing():
    """Unknown identity is never a match, whichever hop: an unenriched source
    (empty uuid and received_uuid) must not be paired with a candidate whose
    received_uuid is also empty."""
    s = _snap("home-20240101-000000", "20240101-000000")
    blank = _snap("home-20240101-000000", "20240101-000000")
    dest = _FakeDest([blank])

    assert dest.correspondent_of(s) is None
    assert dest.correspondents_of([s]) == {}


def test_second_hop_plan_skips_the_present_copy_and_chains_off_it():
    """Planning a transfer onward from a mirror: S1 already has its second-hop
    copy R1 at the destination, S2 does not. The plan is one incremental send of
    S2 parented on S1. Under the old rule nothing at the destination corresponded,
    so S1 was re-sent in full and S2 chained off the duplicate."""
    s1 = _snap("home-20240101-000000", "20240101-000000", uuid="S1", received_uuid="O1")
    s2 = _snap("home-20240102-000000", "20240102-000000", uuid="S2", received_uuid="O2")
    r1 = _snap("home-20240101-000000", "20240101-000000", uuid="R1", received_uuid="O1")
    dest = _FakeDest([r1])

    plan = plan_transfer_sequence([s1, s2], dest)

    assert plan == [(s2, s1)]


# --------------------------------------------------------------------------- #
# the raw sidecar carries the source's stream identity
# --------------------------------------------------------------------------- #
def test_raw_sidecar_round_trips_source_uuid():
    snap = RawSnapshot(
        name="home-20240101-000000",
        stream_path=Path("/b/home-20240101-000000.btrfs"),
        source_uuid="O",
    )
    data = snap.to_dict()
    assert data["source_uuid"] == "O"
    restored = RawSnapshot.from_dict(data, snap.stream_path)
    assert restored.source_uuid == "O"
    assert restored.stream_uuid == "O"


def test_legacy_sidecar_without_source_uuid_reads_as_unknown_identity():
    """A sidecar written before the field existed has no source_uuid; the reader
    tolerates that and the stream corresponds to nothing by identity (today's
    name behaviour, unchanged). An explicit null reads the same way."""
    legacy = {
        "version": 2,
        "name": "home-20240101-000000",
        "uuid": "",
        "parent_uuid": None,
        "parent_name": None,
        "created": "2024-01-01T00:00:00+00:00",
        "size": 1,
    }
    snap = RawSnapshot.from_dict(legacy, Path("/b/home-20240101-000000.btrfs"))
    assert snap.source_uuid == ""
    assert snap.stream_uuid == ""

    snap = RawSnapshot.from_dict(
        {**legacy, "source_uuid": None}, Path("/b/home-20240101-000000.btrfs")
    )
    assert snap.stream_uuid == ""


def test_raw_stream_uuid_does_not_fall_back_to_the_uuid_field():
    """``uuid`` was documented as the source subvolume's own uuid, which is NOT the
    stream identity when that subvolume was itself received. Only source_uuid
    says what a receive of the stream will record."""
    snap = RawSnapshot(
        name="home-20240101-000000",
        stream_path=Path("/b/home-20240101-000000.btrfs"),
        uuid="S",
    )
    assert snap.stream_uuid == ""


def _receive_and_commit(ep, tmp_path, name, source_uuid):
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload-bytes-1234567890")
    with open(src, "rb") as stdin:
        proc = ep.receive(stdin, snapshot_name=name, source_uuid=source_uuid)
        proc.communicate()
    assert proc.returncode == 0
    ep.commit_receive()


def test_raw_receive_records_source_uuid_and_the_listing_exposes_it(tmp_path):
    """End to end on a real raw endpoint: receive() is handed the source's
    stream_uuid, commit writes it into the sidecar, and a later listing of that
    raw store -- as a SOURCE -- exposes it as each stream's stream_uuid."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    _receive_and_commit(ep, tmp_path, "home-20240101-000000", source_uuid="O")

    doc = json.loads((tmp_path / "home-20240101-000000.btrfs.meta").read_text())
    assert doc["source_uuid"] == "O"

    (listed,) = RawEndpoint(config={"path": str(tmp_path)}).list_snapshots(
        flush_cache=True
    )
    assert listed.stream_uuid == "O"


def test_raw_receive_without_source_uuid_writes_an_empty_field(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path)})
    _receive_and_commit(ep, tmp_path, "home-20240101-000000", source_uuid="")

    doc = json.loads((tmp_path / "home-20240101-000000.btrfs.meta").read_text())
    assert doc["source_uuid"] == ""


def test_raw_destination_correspondence_stays_by_name(tmp_path):
    """A stream file has no received_uuid, so as a DESTINATION a raw store still
    corresponds by name -- the source's stream_uuid plays no part."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    _receive_and_commit(ep, tmp_path, "home-20240101-000000", source_uuid="O")

    source = _snap(
        "home-20240101-000000", "20240101-000000", uuid="S", received_uuid="O"
    )
    other = _snap(
        "home-20240102-000000", "20240102-000000", uuid="S", received_uuid="O"
    )
    assert ep.correspondent_of(source) is not None
    assert ep.correspondent_of(other) is None


def test_remediation_carries_source_uuid_into_the_encrypted_sidecar(
    tmp_path, monkeypatch
):
    """``raw encrypt`` re-wraps the same bytes; the encrypted twin's sidecar must
    keep the stream identity or the remediated backup would lose correspondence."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    ep = RawEndpoint(config={"path": str(tmp_path)})
    _receive_and_commit(ep, tmp_path, "home-20240101-000000", source_uuid="O")
    (plain,) = ep.list_snapshots(flush_cache=True)
    assert plain.stream_uuid == "O"

    encrypted = ep.remediate_plaintext(plain, encrypt="openssl_enc")

    assert encrypted.source_uuid == "O"
    doc = json.loads(encrypted.metadata_path.read_text())
    assert doc["source_uuid"] == "O"


# --------------------------------------------------------------------------- #
# the engine hands the source's stream identity to the receive
# --------------------------------------------------------------------------- #
def test_send_snapshot_hands_the_stream_uuid_to_the_receive(monkeypatch):
    """The source is itself a received copy (the second hop), so what the receive
    is told is the ORIGINAL's uuid -- the identity the stream carries -- not the
    copy's own uuid."""
    monkeypatch.setattr(ops, "_ensure_destination_exists", lambda e: None)
    monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
    captured = MagicMock(return_value=[0, 0])
    monkeypatch.setattr(ops, "_do_process_transfer", captured)

    snapshot = _snap(
        "home-20240101-000000", "20240101-000000", uuid="S", received_uuid="O"
    )
    snapshot.endpoint = MagicMock()
    snapshot.endpoint.send.return_value = MagicMock(returncode=0)
    dest = MagicMock()
    dest.config = {"path": "/dest"}
    dest._is_remote = False

    ops.send_snapshot(snapshot, dest, options={"check_space": False})

    assert captured.call_args.kwargs["source_uuid"] == "O"


def test_chunked_manifest_round_trips_source_uuid():
    manifest = TransferManifest(
        transfer_id="t1",
        snapshot_name="home-20240101-000000",
        snapshot_path="/src/home-20240101-000000",
        parent_name=None,
        parent_path=None,
        destination="raw:///b",
        total_size=None,
        chunk_size=1,
        checksum_algorithm="sha256",
        source_uuid="O",
    )
    data = manifest.to_dict()
    assert data["source_uuid"] == "O"
    assert TransferManifest.from_dict(data).source_uuid == "O"

    legacy = {k: v for k, v in data.items() if k != "source_uuid"}
    assert TransferManifest.from_dict(legacy).source_uuid == ""
