"""A raw commit publishes an increment only onto the parent it was received against.

A raw store keeps a ``btrfs send`` stream as a file, and its commit is a rename.
The parent an increment applies onto is pinned on the destination for the span
of the receive, and a prune honours that pin; a pin covers only what reads
pins. A parent removed by hand, by another tool, or after its pin was judged
stale was simply gone at commit, and the commit renamed the stream into place
and reported a success. Nothing at a raw store can send a full stream in its
place, so that backup could never be restored (#135). A parent replaced under
its name by a different stream -- a second config sharing the prefix, a re-sent
copy of another snapshot -- is the same loss one step later: the increment
would be restored onto the wrong base.

The commit now re-reads the store under the target lock, the lock a prune of
the store holds while it deletes, and refuses to publish unless the parent is
there and carries the identity the receive began against: the uuid its sidecar
records, or the sha256 the commit sealed for a sidecar older than that. A
parent recording neither can only be asked to be present. The refusal fails the
transfer, the transfer's own ``.part`` is removed, and a snapshot chained on it
is not started.

The tests drive the real transfer loop into a real raw store with a send that
is a shell process, so the receive, the commit and the cleanup are the code the
engine runs. What changes the store mid-receive is the send itself, before it
writes its first byte -- after the receive has opened its ``.part`` and after
the engine has pinned and probed the parent.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import shlex
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.endpoint.raw import LOCK_FILENAME, RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

q = shlex.quote


class _Snap:
    def __init__(self, source, name: str) -> None:
        self.endpoint = source
        self.name = name
        self.stream_uuid = f"uuid-of-{name}"
        self.locks: set = set()
        self.parent_locks: set = set()

    def get_name(self) -> str:
        return self.name

    def get_path(self) -> Path:
        return Path("/mnt/snapshots") / self.name

    def __str__(self) -> str:
        return self.name


class _Source:
    """The source side of a transfer. Its send is a shell process whose output
    is the stream; ``before`` runs first, with the receive already open on the
    store, and is what changes the store under the receive."""

    def __init__(self, before: str = "") -> None:
        self.before = before
        self.pins: list[tuple[str, bool, bool]] = []

    def set_lock(self, snapshot, lock_id, state, parent=False) -> None:
        self.pins.append((snapshot.get_name(), state, parent))

    def send(self, snapshot, parent=None, clones=None):
        script = f"{self.before}\nprintf 'stream of %s' {q(snapshot.get_name())}"
        return subprocess.Popen(
            ["sh", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )


def _store(tmp_path) -> tuple[RawEndpoint, Path]:
    location = tmp_path / "raw"
    location.mkdir()
    return RawEndpoint(config={"path": str(location), "snap_prefix": ""}), location


def _stored(
    location: Path,
    name: str,
    *,
    source_uuid: str = "",
    checksum: str | None = None,
    sidecar: bool = True,
) -> RawSnapshot:
    """A stream the store already holds, with the record its commit would
    have written -- or, with ``sidecar=False``, a legacy stream listed from
    its filename alone."""
    snap = RawSnapshot(
        name=name,
        stream_path=location / f"{name}.btrfs",
        source_uuid=source_uuid,
        checksum_value=checksum,
    )
    snap.stream_path.write_bytes(f"stream of {name}".encode())
    if sidecar:
        snap.save_metadata()
    return snap


def _another_record(tmp_path, location: Path, name: str, **fields) -> Path:
    """A sidecar for a different stream stored under ``name``, waiting beside
    the store to be swapped in."""
    other = RawSnapshot(name=name, stream_path=location / f"{name}.btrfs", **fields)
    path = tmp_path / "another.meta"
    path.write_bytes(other.serialize())
    return path


def _remove(snap: RawSnapshot) -> str:
    return f"rm -f {q(str(snap.stream_path))} {q(str(snap.metadata_path))}"


def _replace_record(with_: Path, snap: RawSnapshot) -> str:
    return f"mv -f {q(str(with_))} {q(str(snap.metadata_path))}"


def _transfer(source: _Source, store: RawEndpoint, *plan):
    return ops._execute_transfers(source, store, list(plan), {})


def _published(location: Path, name: str) -> bool:
    return (location / f"{name}.btrfs").exists()


class TestTheCommitRefusesAParentThatIsNotTheOneItReceivedAgainst:
    def test_a_parent_removed_during_the_receive_is_a_failed_transfer(self, tmp_path):
        store, here = _store(tmp_path)
        parent = _stored(here, "home.1", source_uuid="u1")
        source = _Source(before=_remove(parent))
        child, grandchild = _Snap(source, "home.2"), _Snap(source, "home.3")

        result = _transfer(
            source, store, (child, _Snap(source, "home.1")), (grandchild, child)
        )

        assert result.transferred == []
        failed = {s.get_name(): str(e) for s, e in result.failed}
        assert (
            "home.2 was received against home.1, which is no longer at"
            in (failed["home.2"])
        )
        assert "was not transferred" in failed["home.3"], "the chain went on"
        assert not _published(here, "home.2"), "an unrestorable increment was published"
        assert not (here / "home.2.btrfs.meta").exists()
        assert list(here.glob("*.part")) == [], "the refused stream was left behind"

    def test_a_parent_replaced_under_its_name_is_refused(self, tmp_path):
        store, here = _store(tmp_path)
        parent = _stored(here, "home.1", source_uuid="u1")
        another = _another_record(tmp_path, here, "home.1", source_uuid="u2")
        source = _Source(before=_replace_record(another, parent))
        child = _Snap(source, "home.2")

        result = _transfer(source, store, (child, _Snap(source, "home.1")))

        assert [s for s, _ in result.failed] == [child]
        message = str(result.failed[0][1])
        assert "is not the one the receive began against" in message
        assert "source uuid u1 then, u2 now" in message
        assert not _published(here, "home.2")
        assert list(here.glob("*.part")) == []

    def test_the_sealed_checksum_stands_in_for_a_record_without_a_uuid(self, tmp_path):
        store, here = _store(tmp_path)
        parent = _stored(here, "home.1", checksum="c1")
        another = _another_record(tmp_path, here, "home.1", checksum_value="c2")
        source = _Source(before=_replace_record(another, parent))
        child = _Snap(source, "home.2")

        result = _transfer(source, store, (child, _Snap(source, "home.1")))

        assert [s for s, _ in result.failed] == [child]
        assert "sha256 c1 then, c2 now" in str(result.failed[0][1])
        assert not _published(here, "home.2")

    def test_a_parent_that_lost_its_record_reads_as_changed(self, tmp_path):
        store, here = _store(tmp_path)
        parent = _stored(here, "home.1", source_uuid="u1")
        source = _Source(before=f"rm -f {q(str(parent.metadata_path))}")
        child = _Snap(source, "home.2")

        result = _transfer(source, store, (child, _Snap(source, "home.1")))

        assert [s for s, _ in result.failed] == [child]
        assert "source uuid u1 then, none now" in str(result.failed[0][1])
        assert not _published(here, "home.2")

    def test_a_parent_recording_no_identity_is_asked_only_to_be_present(self, tmp_path):
        store, here = _store(tmp_path)
        parent = _stored(here, "home.1", sidecar=False)
        # Another stream written over it is indistinguishable: nothing recorded
        # an identity to compare. Presence is the whole question, and it holds.
        source = _Source(
            before=f"printf 'another stream' > {q(str(parent.stream_path))}"
        )
        child = _Snap(source, "home.2")

        result = _transfer(source, store, (child, _Snap(source, "home.1")))

        assert result.transferred == [child], result.failed
        assert _published(here, "home.2")

    def test_the_same_parent_is_published_with_its_chain_recorded(self, tmp_path):
        store, here = _store(tmp_path)
        _stored(here, "home.1", source_uuid="u1")
        source = _Source()
        child = _Snap(source, "home.2")

        result = _transfer(source, store, (child, _Snap(source, "home.1")))

        assert result.transferred == [child], result.failed
        record = json.loads((here / "home.2.btrfs.meta").read_text())
        assert record["parent_name"] == "home.1"
        assert record["source_uuid"] == "uuid-of-home.2"
        assert list(here.glob("*.part")) == []


def _flock_is_held(lockfile: Path) -> bool:
    """Whether another open file description holds the exclusive flock -- a
    second descriptor in this process is refused like any other holder's."""
    with open(lockfile, "rb") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fh, fcntl.LOCK_UN)
        return False


class TestTheQuestionIsAskedUnderTheTargetLock:
    """A prune of the store deletes under the target lock, so an answer read
    under it cannot be overtaken by one before the rename."""

    def test_the_live_listing_is_taken_while_the_lock_is_held(self, tmp_path):
        store, here = _store(tmp_path)
        _stored(here, "home.1", source_uuid="u1")
        asked: list[bool] = []
        listing = store.list_snapshots

        def observed(flush_cache: bool = False):
            if flush_cache:
                asked.append(_flock_is_held(here / LOCK_FILENAME))
            return listing(flush_cache=flush_cache)

        store.list_snapshots = observed  # type: ignore[method-assign]
        source = _Source()
        child = _Snap(source, "home.2")

        result = _transfer(source, store, (child, _Snap(source, "home.1")))

        assert result.transferred == [child], result.failed
        assert asked == [True], "the parent was resolved from a stale or unlocked view"


class TestTheRefusalRemovesOnlyThisRunsPart:
    def test_a_peers_part_and_a_committed_neighbour_survive(self, tmp_path):
        store, here = _store(tmp_path)
        parent = _stored(here, "home.1", source_uuid="u1")
        neighbour = _stored(here, "home.3", source_uuid="u3")
        peer = here / "home.2.btrfs.4242.ff.deadbeef.part"
        peer.write_bytes(b"a peer's stream, still arriving")
        source = _Source(before=_remove(parent))
        child = _Snap(source, "home.2")

        result = _transfer(source, store, (child, _Snap(source, "home.1")))

        assert [s for s, _ in result.failed] == [child]
        assert [p.name for p in here.glob("*.part")] == [peer.name]
        assert neighbour.stream_path.exists() and neighbour.metadata_path.exists()


class TestASnapperBackupIntoARawStoreIsHeldToTheSameRule:
    def test_a_parent_stream_removed_during_the_receive_fails_the_backup(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
        monkeypatch.setattr(ops, "_write_snapper_metadata", lambda *a, **k: None)
        store, here = _store(tmp_path)
        parent = _stored(here, "home-4", source_uuid="u4")
        source = _Source(before=_remove(parent))
        monkeypatch.setattr(
            ops,
            "_create_snapper_snapshot_wrapper",
            lambda snap, ep=None: _Snap(source, f"home-{snap.number}"),
        )
        snapper = {
            n: SimpleNamespace(
                number=n,
                subvolume_path=tmp_path / "src" / str(n) / "snapshot",
                info_xml_path=tmp_path / "src" / str(n) / "info.xml",
            )
            for n in (4, 5)
        }

        with pytest.raises(__util__.SnapshotTransferError, match="no longer at"):
            ops.send_snapper_snapshot(
                snapper[5], store, parent_snapper_snapshot=snapper[4], options={}
            )

        assert not _published(here, "home-5")
        assert list(here.glob("*.part")) == []


@contextlib.contextmanager
def _unlocked(self, **kwargs):
    yield


class TestTheRawSshCommitAsksTheSameQuestion:
    """The remote commit inherits the check; the listing it asks is the
    remote one, and the rename follows the answer, inside the lock."""

    def _endpoint(self, listing) -> SSHRawEndpoint:
        ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
        ep._pending_metadata = {  # type: ignore[typeddict-item]
            "name": "home.2",
            "stream_path": "/backup/home.2.btrfs",
            "part_path": "/backup/home.2.btrfs.1.a.b.part",
            "parent_name": "home.1",
            "parent_identity": ("source uuid", "u1"),
        }
        ep.list_snapshots = listing  # type: ignore[method-assign]
        ep._exec_remote_command = MagicMock(  # type: ignore[method-assign]
            return_value=MagicMock(returncode=0, stderr=b"", stdout=b"1")
        )
        return ep

    @staticmethod
    def _renames(ep) -> list[str]:
        return [
            c.args[0][2]
            for c in ep._exec_remote_command.call_args_list
            if c.args[0][0] == "sh" and "mv -f" in c.args[0][2]
        ]

    def test_a_parent_gone_from_the_remote_is_refused_before_the_rename(self):
        ep = self._endpoint(lambda flush_cache=False: [])
        with patch.object(SSHRawEndpoint, "target_lock", _unlocked):
            with pytest.raises(RuntimeError, match="no longer at /backup"):
                ep.commit_receive()
        assert self._renames(ep) == [], "the stream was published anyway"

    def test_a_parent_that_changed_identity_is_refused(self):
        stored = RawSnapshot(
            name="home.1", stream_path=Path("/backup/home.1.btrfs"), source_uuid="u9"
        )
        ep = self._endpoint(lambda flush_cache=False: [stored])
        with patch.object(SSHRawEndpoint, "target_lock", _unlocked):
            with pytest.raises(RuntimeError, match="source uuid u1 then, u9 now"):
                ep.commit_receive()
        assert self._renames(ep) == []

    def test_the_parent_is_asked_live_inside_the_lock_and_the_rename_follows(self):
        events: list[str] = []
        stored = RawSnapshot(
            name="home.1", stream_path=Path("/backup/home.1.btrfs"), source_uuid="u1"
        )

        def listing(flush_cache=False):
            events.append(f"listed live={flush_cache}")
            return [stored]

        @contextlib.contextmanager
        def recording(self, **kwargs):
            events.append("acquired")
            try:
                yield
            finally:
                events.append("released")

        ep = self._endpoint(listing)

        def remote(cmd, *a, **kw):
            events.append(cmd[2] if cmd[0] == "sh" else " ".join(cmd))
            return MagicMock(returncode=0, stderr=b"", stdout=b"1")

        ep._exec_remote_command = MagicMock(side_effect=remote)  # type: ignore[method-assign]
        with patch.object(SSHRawEndpoint, "target_lock", recording):
            ep.commit_receive()

        inside = events[events.index("acquired") + 1 : events.index("released")]
        assert inside[0] == "listed live=True", inside
        assert any(s.startswith("sync && mv -f") for s in inside[1:]), inside
