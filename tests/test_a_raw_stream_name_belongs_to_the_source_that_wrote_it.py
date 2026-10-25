"""A raw stream's name belongs to the source that wrote it.

A raw store names a stream after the snapshot it was sent from: the snapshot
prefix and a one-second timestamp. Two volumes that share a prefix into one
store name their streams alike, and in one run they do so in the same second.
The first volume published its stream; the second volume's planner found that
name at the store, called the second volume already backed up, and never sent
it. Both reported success and the run exited 0, with one backup for two volumes
(#138). A second apart, the second volume's increment chained onto the first
volume's stream by name, an unrestorable chain; a later run, the second volume's
commit renamed its stream over the first volume's.

The store now tells streams apart by the source they were sent from, the uuid
the sidecar records beside the name. A stream from another source is not this
snapshot's backup, so the planner sends the shadowed snapshot instead of
skipping it and never chains onto another volume's stream; and the commit,
under the target lock, refuses to rename over a stream it cannot show to be
this source's own. A record that names no source is matched by name alone,
which is all that can be asked of it, and is not overwritten, since it cannot
be told from another volume's.

The transfer tests drive the real transfer loop into a real raw store with a
send that is a shell process, so the receive, the commit and the cleanup are the
code the engine runs.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import shlex
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.core.planning import plan_transfer_sequence, snapshots_present_on
from btrfs_backup_ng.endpoint.raw import LOCK_FILENAME, RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

q = shlex.quote


class _Snap:
    """A source snapshot: ``source`` is the volume it belongs to, and its
    stream carries that volume's uuid. ``stamp`` orders it for the planner."""

    def __init__(self, source, name: str, *, stamp: str | None = None) -> None:
        self.endpoint = source
        self.name = name
        self.stream_uuid = source.uuid
        self.time_obj = time.strptime(stamp, "%Y%m%d-%H%M%S") if stamp else None
        self.locks: set = set()
        self.parent_locks: set = set()

    def get_name(self) -> str:
        return self.name

    def get_path(self) -> Path:
        return Path("/mnt/snapshots") / self.name

    def __str__(self) -> str:
        return self.name


class _Volume:
    """One backed-up volume. Its send is a shell process whose output is the
    stream, marked with the volume's name; ``before`` runs first, with the
    receive already open on the store."""

    def __init__(self, uuid: str, before: str = "") -> None:
        self.uuid = uuid
        self.before = before
        self.pins: list[tuple[str, bool, bool]] = []

    def set_lock(self, snapshot, lock_id, state, parent=False) -> None:
        self.pins.append((snapshot.get_name(), state, parent))

    def send(self, snapshot, parent=None, clones=None):
        script = (
            f"{self.before}\nprintf 'stream of %s from %s' "
            f"{q(snapshot.get_name())} {q(self.uuid)}"
        )
        return subprocess.Popen(
            ["sh", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )


def _store(tmp_path) -> tuple[RawEndpoint, Path]:
    location = tmp_path / "raw"
    location.mkdir()
    return RawEndpoint(config={"path": str(location), "snap_prefix": ""}), location


def _stored(
    location: Path, name: str, *, source_uuid: str = "", sidecar: bool = True
) -> RawSnapshot:
    """A stream the store already holds, with the record its commit would have
    written -- or, with ``sidecar=False``, a stream listed from its filename
    alone."""
    snap = RawSnapshot(
        name=name, stream_path=location / f"{name}.btrfs", source_uuid=source_uuid
    )
    snap.stream_path.write_bytes(f"stream of {name} from {source_uuid}".encode())
    if sidecar:
        snap.save_metadata()
    return snap


def _as_it_was(snap: RawSnapshot) -> tuple[bytes, bytes | None]:
    return (
        snap.stream_path.read_bytes(),
        snap.metadata_path.read_bytes() if snap.metadata_path.exists() else None,
    )


def _transfer(volume: _Volume, store: RawEndpoint, *plan):
    return ops._execute_transfers(volume, store, list(plan), {})


def _published(location: Path, name: str) -> bool:
    return (location / f"{name}.btrfs").exists()


A, B = "uuid-of-volume-a", "uuid-of-volume-b"


class TestPresenceIsTheNameAndTheSourceThatWroteIt:
    """The planner's presence authority, asked of a raw store."""

    def test_another_volumes_stream_under_the_name_is_not_this_volumes_backup(
        self, tmp_path
    ):
        store, here = _store(tmp_path)
        stored = _stored(here, "home-1", source_uuid=A)
        ours = _Snap(_Volume(A), "home-1")
        theirs = _Snap(_Volume(B), "home-1")

        assert store.correspondent_of(ours) == stored
        assert store.correspondent_of(theirs) is None, (
            "volume B was told its backup exists because volume A's stream has the "
            "same name: B is never sent, and both volumes report success (#138)"
        )
        assert snapshots_present_on([ours, theirs], store) == {"home-1"}
        assert store.correspondents_of([theirs]) == {}

    def test_a_record_that_names_no_source_is_matched_by_name_alone(self, tmp_path):
        store, here = _store(tmp_path)
        legacy = _stored(here, "home-1", source_uuid="")
        inferred = _stored(here, "home-2", sidecar=False)

        assert store.correspondent_of(_Snap(_Volume(B), "home-1")) == legacy
        assert store.correspondent_of(_Snap(_Volume(B), "home-2")) == inferred

    def test_a_source_whose_identity_is_unknown_is_matched_by_name_alone(
        self, tmp_path
    ):
        store, here = _store(tmp_path)
        stored = _stored(here, "home-1", source_uuid=A)

        assert store.correspondent_of(_Snap(_Volume(""), "home-1")) == stored

    def test_the_planner_sends_the_shadowed_snapshot_and_chains_only_onto_its_own(
        self, tmp_path
    ):
        """Volume A's stream is at the store under the name volume B's first
        snapshot also has. B's first snapshot is planned, not skipped, and
        B's second snapshot chains onto B's first, which this run will have
        sent -- never onto A's stream by name."""
        store, here = _store(tmp_path)
        _stored(here, "home-20261022-122634", source_uuid=A)
        b = _Volume(B)
        first = _Snap(b, "home-20261022-122634", stamp="20261022-122634")
        second = _Snap(b, "home-20261022-122700", stamp="20261022-122700")

        plan = plan_transfer_sequence([first, second], store)

        assert plan == [(first, None), (second, first)], (
            "the planner skipped volume B's snapshot because volume A's stream has "
            "its name, or chained B's increment onto A's stream"
        )

    def test_a_volumes_own_backup_is_still_present_and_still_a_parent(self, tmp_path):
        store, here = _store(tmp_path)
        _stored(here, "home-20261022-122634", source_uuid=A)
        a = _Volume(A)
        first = _Snap(a, "home-20261022-122634", stamp="20261022-122634")
        second = _Snap(a, "home-20261022-122700", stamp="20261022-122700")

        assert plan_transfer_sequence([first, second], store) == [(second, first)]


class TestTheCommitRefusesToOverwriteAnotherSourcesStream:
    def test_another_volumes_stream_is_left_as_it_was(self, tmp_path):
        store, here = _store(tmp_path)
        theirs = _stored(here, "home-1", source_uuid=A)
        before = _as_it_was(theirs)
        b = _Volume(B)
        first, second = _Snap(b, "home-1"), _Snap(b, "home-2")

        result = _transfer(b, store, (first, None), (second, first))

        assert result.transferred == []
        failed = {s.get_name(): str(e) for s, e in result.failed}
        assert "refusing to overwrite another source's backup" in failed["home-1"]
        assert f"sent from {A}; this one was sent from {B}" in failed["home-1"]
        assert "snapshot prefix of its own" in failed["home-1"]
        assert "was not transferred" in failed["home-2"], "the chain went on"
        assert _as_it_was(theirs) == before, "volume A's backup was overwritten"
        assert list(here.glob("*.part")) == [], "the refused stream was left behind"

    def test_a_stream_that_names_no_source_is_not_overwritten(self, tmp_path):
        store, here = _store(tmp_path)
        legacy = _stored(here, "home-1", source_uuid="")
        before = _as_it_was(legacy)
        b = _Volume(B)

        result = _transfer(b, store, (_Snap(b, "home-1"), None))

        assert [s.get_name() for s, _ in result.failed] == ["home-1"]
        message = str(result.failed[0][1])
        assert "nothing records which source it was sent from" in message
        assert "cannot be told from another volume's" in message
        assert _as_it_was(legacy) == before

    def test_a_stream_without_a_record_is_not_overwritten(self, tmp_path):
        store, here = _store(tmp_path)
        inferred = _stored(here, "home-1", sidecar=False)
        before = _as_it_was(inferred)
        b = _Volume(B)

        result = _transfer(b, store, (_Snap(b, "home-1"), None))

        assert [s.get_name() for s, _ in result.failed] == ["home-1"]
        assert "nothing records which source" in str(result.failed[0][1])
        assert _as_it_was(inferred) == before
        assert not inferred.metadata_path.exists(), "a record was written anyway"

    def test_the_same_source_may_refresh_its_own_stream(self, tmp_path):
        store, here = _store(tmp_path)
        own = _stored(here, "home-1", source_uuid=A)
        own.stream_path.write_bytes(b"an older copy of the same stream")
        a = _Volume(A)
        snap = _Snap(a, "home-1")

        result = _transfer(a, store, (snap, None))

        assert result.transferred == [snap], result.failed
        assert own.stream_path.read_bytes() == f"stream of home-1 from {A}".encode()
        assert json.loads(own.metadata_path.read_text())["source_uuid"] == A

    def test_a_source_of_unknown_identity_does_not_overwrite_a_known_one(
        self, tmp_path
    ):
        store, here = _store(tmp_path)
        theirs = _stored(here, "home-1", source_uuid=A)
        before = _as_it_was(theirs)
        unknown = _Volume("")

        result = _transfer(unknown, store, (_Snap(unknown, "home-1"), None))

        assert [s.get_name() for s, _ in result.failed] == ["home-1"]
        assert "a source whose identity is unknown" in str(result.failed[0][1])
        assert _as_it_was(theirs) == before

    def test_a_stream_arriving_under_the_name_during_the_receive_is_refused(
        self, tmp_path
    ):
        """Another volume, in another process, publishes under this name after
        the planner looked and before the commit. The commit asks the store
        again, at that moment, and not the listing the planner took."""
        store, here = _store(tmp_path)
        store.list_snapshots()  # the planner's look: nothing there
        arriving = RawSnapshot(
            name="home-1", stream_path=here / "home-1.btrfs", source_uuid=A
        )
        record = tmp_path / "arriving.meta"
        record.write_bytes(arriving.serialize())
        b = _Volume(
            B,
            before=(
                f"printf 'stream of home-1 from %s' {q(A)} > {q(str(arriving.stream_path))}"
                f" && mv -f {q(str(record))} {q(str(arriving.metadata_path))}"
            ),
        )

        result = _transfer(b, store, (_Snap(b, "home-1"), None))

        assert [s.get_name() for s, _ in result.failed] == ["home-1"]
        assert f"sent from {A}" in str(result.failed[0][1])
        assert (
            arriving.stream_path.read_bytes() == f"stream of home-1 from {A}".encode()
        )
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
    """A peer's commit publishes under the target lock, so an answer read
    under it cannot be overtaken by a publish before the rename."""

    def test_the_live_listing_is_taken_while_the_lock_is_held(self, tmp_path):
        store, here = _store(tmp_path)
        asked: list[bool] = []
        listing = store.list_snapshots

        def observed(flush_cache: bool = False):
            if flush_cache:
                asked.append(_flock_is_held(here / LOCK_FILENAME))
            return listing(flush_cache=flush_cache)

        store.list_snapshots = observed  # type: ignore[method-assign]
        a = _Volume(A)
        snap = _Snap(a, "home-1")

        result = _transfer(a, store, (snap, None))

        assert result.transferred == [snap], result.failed
        assert asked == [True], "the name was judged from a stale or unlocked view"


class TestTheRefusalRemovesOnlyThisRunsPart:
    def test_a_peers_part_and_a_committed_neighbour_survive(self, tmp_path):
        store, here = _store(tmp_path)
        _stored(here, "home-1", source_uuid=A)
        neighbour = _stored(here, "home-0", source_uuid=B)
        peer = here / "home-1.btrfs.4242.ff.deadbeef.part"
        peer.write_bytes(b"a peer's stream, still arriving")
        b = _Volume(B)

        result = _transfer(b, store, (_Snap(b, "home-1"), None))

        assert [s.get_name() for s, _ in result.failed] == ["home-1"]
        assert [p.name for p in here.glob("*.part")] == [peer.name]
        assert neighbour.stream_path.exists() and neighbour.metadata_path.exists()


class TestASnapperBackupIntoARawStoreIsHeldToTheSameRule:
    def test_another_sources_stream_under_the_slots_name_fails_the_backup(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
        monkeypatch.setattr(ops, "_write_snapper_metadata", lambda *a, **k: None)
        store, here = _store(tmp_path)
        theirs = _stored(here, "home-5", source_uuid=A)
        before = _as_it_was(theirs)
        b = _Volume(B)
        monkeypatch.setattr(
            ops,
            "_create_snapper_snapshot_wrapper",
            lambda snap, ep=None: _Snap(b, f"home-{snap.number}"),
        )
        snapper = SimpleNamespace(
            number=5,
            subvolume_path=tmp_path / "src" / "5" / "snapshot",
            info_xml_path=tmp_path / "src" / "5" / "info.xml",
        )

        with pytest.raises(
            __util__.SnapshotTransferError, match="refusing to overwrite"
        ):
            ops.send_snapper_snapshot(snapper, store, options={})

        assert _as_it_was(theirs) == before
        assert list(here.glob("*.part")) == []


@contextlib.contextmanager
def _unlocked(self, **kwargs):
    yield


class TestTheRawSshCommitRefusesTheSameWay:
    """The remote commit inherits the check; the listing it asks is the
    remote one, and the rename follows the answer, inside the lock."""

    def _endpoint(self, listing, *, source_uuid: str = B) -> SSHRawEndpoint:
        ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
        ep._pending_metadata = {  # type: ignore[typeddict-item]
            "name": "home-1",
            "stream_path": "/backup/home-1.btrfs",
            "part_path": "/backup/home-1.btrfs.1.a.b.part",
            "parent_name": None,
            "parent_identity": None,
            "source_uuid": source_uuid,
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
            if c.args[0][0] == "sh" and c.args[0][2].startswith("sync && mv -f")
        ]

    @staticmethod
    def _remote(name: str, source_uuid: str) -> RawSnapshot:
        return RawSnapshot(
            name=name,
            stream_path=Path(f"/backup/{name}.btrfs"),
            source_uuid=source_uuid,
        )

    def test_another_sources_stream_on_the_remote_is_refused_before_the_rename(self):
        ep = self._endpoint(lambda flush_cache=False: [self._remote("home-1", A)])
        with patch.object(SSHRawEndpoint, "target_lock", _unlocked):
            with pytest.raises(
                RuntimeError, match=f"sent from {A}; this one was sent from {B}"
            ):
                ep.commit_receive()
        assert self._renames(ep) == [], "the stream was published anyway"

    def test_a_remote_stream_that_names_no_source_is_refused(self):
        ep = self._endpoint(lambda flush_cache=False: [self._remote("home-1", "")])
        with patch.object(SSHRawEndpoint, "target_lock", _unlocked):
            with pytest.raises(RuntimeError, match="nothing records which source"):
                ep.commit_receive()
        assert self._renames(ep) == []

    def test_the_same_source_refreshes_its_own_stream(self):
        ep = self._endpoint(lambda flush_cache=False: [self._remote("home-1", B)])
        with patch.object(SSHRawEndpoint, "target_lock", _unlocked):
            ep.commit_receive()
        assert len(self._renames(ep)) == 1

    def test_the_name_is_judged_live_inside_the_lock_and_the_rename_follows(self):
        events: list[str] = []

        def listing(flush_cache=False):
            events.append(f"listed live={flush_cache}")
            return []

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
