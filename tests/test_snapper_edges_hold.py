"""Seven edges of the snapper backup and restore paths, each pinned.

Each class here names one defect that was measured on the working tree and
holds the fix that closes it:

- the engine's partial-transfer cleanup, given the snapper layout's
  receiver, could delete a directory of the operator's beside
  ``.snapshots``, because it derived a path from the endpoint's current
  path and the snapshot's name after the receiver had moved that path;
- a pin that could not be written to a location's lock FILE aborted the
  restore even when the location was mounted read-only (where nothing can
  delete anything), and ``--skip-remote-lock`` did not cover that store;
  and a restore's source got this tool's bookkeeping tree created under it;
- two ``snapper backup`` runs into one local target opened the same slot,
  the second removing the first's in-flight temp: the backup direction had
  no writer lock while the restore direction did;
- a regular file named like a slot made ``publish_fresh`` ask for the same
  number a thousand times;
- a ``--dry-run`` restore created the config's lock file and the source's
  bookkeeping tree;
- a pin stayed on the backup when the send died of anything but the
  transfer error the executor expected (Ctrl-C included);
- a pin on a ``raw://`` stream lived only in the process that took it, and a
  snapper slot's deletion never asked the lock store at all, so the
  documentation's "a prune cannot delete what is being read" held only for
  ``raw+ssh://``.
"""

from __future__ import annotations

import errno
import itertools
import logging
import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import prune as prune_cli
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.core.layout import SnapperLayout
from btrfs_backup_ng.core.restore import _restore_endpoint_config
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot
from btrfs_backup_ng.snapper import SnapperScanner
from btrfs_backup_ng.sshutil import lock


def _real_shell(ep, script):
    r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return r.returncode, r.stdout


@pytest.fixture
def real_shell(monkeypatch):
    monkeypatch.setattr(ops, "_snapper_run_shell", _real_shell)


def _endpoint(base: Path) -> LocalEndpoint:
    base.mkdir(parents=True, exist_ok=True)
    return LocalEndpoint(
        config={"path": str(base), "snap_prefix": "", "fs_checks": "skip"}
    )


def _snap(name: str):
    return SimpleNamespace(locks=set(), parent_locks=set(), get_name=lambda: name)


# --------------------------------------------------------------------------- #
# a. cleanup never leaves the slot it owns
# --------------------------------------------------------------------------- #
class TestCleanupIsAnchoredToTheSlot:
    def _layout(self, tmp_path):
        config = tmp_path / "config"
        (config / ".snapshots").mkdir(parents=True)
        # The operator's own directory, beside .snapshots, named like the
        # copy the engine would look for.
        keep = config / "snapshot-7"
        keep.mkdir()
        (keep / "precious").write_text("do not touch")
        # Receive 1 opens 1, receive 2 opens 2, and the publish asks again
        # for the number free at that moment: still 2.
        numbers = itertools.chain([1], itertools.repeat(2))
        layout = SnapperLayout(_endpoint(config), next_number=lambda: next(numbers))
        layout.begin([], info_xml_for=lambda s, n: None)
        return config, keep, layout

    def test_the_receiver_owns_its_partial_and_removes_only_that(
        self, tmp_path, real_shell, monkeypatch
    ):
        """One receive fails and stays open; the next opens a new slot and
        its publish fails; the engine then asks for the partial's cleanup by
        name. Only the open slot's temp goes; the operator's directory beside
        .snapshots is untouched, and no delete command names it."""
        config, keep, layout = self._layout(tmp_path)
        receiver = layout.receive_endpoint
        argv_seen: list[list[str]] = []
        real_run = subprocess.run

        def record(argv, *a, **k):
            if isinstance(argv, list):
                argv_seen.append([str(x) for x in argv])
            return real_run(argv, *a, **k)

        monkeypatch.setattr(ops.subprocess, "run", record)
        monkeypatch.setattr(LocalEndpoint, "receive", lambda self, *a, **k: None)
        # Receive 1: opens slot 1 and "fails" (nothing verified, slot left open).
        receiver.receive(None, "snapshot-7")
        assert layout.open_number == 1
        # Receive 2: abandons 1, opens 2, the copy lands, the publish fails.
        receiver.receive(None, "snapshot-7")
        assert layout.open_number == 2
        (Path(layout.incoming_dir(2)) / "snapshot").mkdir()
        monkeypatch.setattr(
            __util__,
            "rename_noreplace",
            lambda a, b: (_ for _ in ()).throw(OSError("no")),
        )
        with pytest.raises(__util__.SnapshotTransferError):
            receiver.add_snapshot(_snap("snapshot-7"))
        # The endpoint points back at the config; the engine's cleanup runs.
        assert str(receiver.config["path"]) == str(config)
        ops._cleanup_partial_local_subvolume(
            receiver, "snapshot-7", created_by_this_run=True
        )
        assert (keep / "precious").read_text() == "do not touch"
        assert not Path(layout.incoming_dir(1)).exists()
        assert not Path(layout.incoming_dir(2)).exists()
        named = [a for a in argv_seen if any(str(keep) == x for x in a)]
        assert named == [], f"a delete command named the operator's directory: {named}"

    def test_the_receiver_answers_the_pre_existence_probe_itself(
        self, tmp_path, real_shell
    ):
        """Whatever sits beside .snapshots under the copy's name is not the
        copy: every receive lands in a slot opened for it, so the artifact
        never pre-exists and the cleanup that follows is always this run's."""
        config, keep, layout = self._layout(tmp_path)
        assert keep.is_dir()
        assert (
            ops.destination_artifact_exists(layout.receive_endpoint, "snapshot-7")
            is False
        )


# --------------------------------------------------------------------------- #
# b. a read-only source, and a source that is only read
# --------------------------------------------------------------------------- #
class TestAReadOnlySourceNeedsNoLockFile:
    """The lock FILE store (a local btrfs location) decides read-only by the
    same rule every pin writer uses: the kernel says the location's
    filesystem is mounted read-only (``local_path_is_read_only``)."""

    def _location(self, tmp_path):
        location = tmp_path / "medium"
        location.mkdir()
        return LocalEndpoint(
            config={"path": str(location), "snap_prefix": "", "fs_checks": "skip"}
        )

    def _refuse_writes(self, monkeypatch):
        def refuse(*a, **k):
            raise OSError(errno.EACCES, "cannot write")

        monkeypatch.setattr(__util__, "atomic_write_bytes", refuse)

    def _mounted_read_only(self, monkeypatch):
        from btrfs_backup_ng.sshutil import lock

        real = os.statvfs

        def statvfs(path):
            st = real(path)
            return SimpleNamespace(f_flag=st.f_flag | os.ST_RDONLY)

        monkeypatch.setattr(lock.os, "statvfs", statvfs)

    def test_a_pin_on_a_read_only_location_is_not_an_error(
        self, tmp_path, monkeypatch, caplog
    ):
        ep = self._location(tmp_path)
        self._refuse_writes(monkeypatch)
        self._mounted_read_only(monkeypatch)
        snap = _snap("home-1")
        with caplog.at_level(logging.INFO):
            ep.set_lock(snap, "restore:s1", True)
        assert "restore:s1" in snap.locks
        assert "mounted read-only" in caplog.text

    def test_any_other_failure_to_pin_still_refuses(self, tmp_path, monkeypatch):
        ep = self._location(tmp_path)
        self._refuse_writes(monkeypatch)
        with pytest.raises(__util__.AbortError, match="skip-remote-lock") as info:
            ep.set_lock(_snap("home-1"), "restore:s1", True)
        assert ".." not in str(info.value)

    def test_skip_remote_lock_covers_the_lock_file_store(
        self, tmp_path, monkeypatch, shared_log
    ):
        ep = self._location(tmp_path)
        ep.config["skip_remote_lock"] = True
        self._refuse_writes(monkeypatch)
        ep.set_lock(_snap("home-1"), "restore:s1", True)
        warnings = shared_log.messages(logging.WARNING)
        assert any("WITHOUT protection" in m for m in warnings), warnings

    def test_a_release_that_cannot_be_written_is_a_warning(
        self, tmp_path, monkeypatch, shared_log
    ):
        ep = self._location(tmp_path)
        self._refuse_writes(monkeypatch)
        ep.set_lock(_snap("home-1"), "restore:s1", False)
        warnings = shared_log.messages(logging.WARNING)
        assert any("Could not clear the lock" in m for m in warnings), warnings

    def test_a_source_that_is_only_read_gets_no_bookkeeping_tree(self, tmp_path):
        """The restore-side endpoint configuration says the location is only
        read, and the local endpoint's prepare then creates nothing under it:
        a read-only medium, or another tool's location, serves as it is."""
        location = tmp_path / "medium"
        location.mkdir()
        config = _restore_endpoint_config(str(location), {"fs_checks": "skip"})
        assert config["create_tree"] is False
        LocalEndpoint(config=config).prepare()
        assert not (location / ".btrfs-backup-ng").exists()
        # The default, for a destination, still builds it.
        LocalEndpoint(
            config={"path": str(location), "snap_prefix": "", "fs_checks": "skip"}
        ).prepare()
        assert (location / ".btrfs-backup-ng" / "snapshots").is_dir()


class TestEveryPinWriterDecidesReadOnlyTheSameWay:
    """Making raw:// pins durable made a raw restore from a read-only medium
    refuse ("Could not lock ... pass --skip-remote-lock") where 8bbb2af
    restored: the read-only rule lived only in the lock FILE store, and
    ``record_pin`` -- ssh://, raw+ssh:// and now raw:// -- had none. One
    rule now, in the lock module: a location whose filesystem is mounted
    read-only cannot have anything deleted from it, so the pin is not needed;
    said at INFO, and the restore goes on. Decided by exit status from the
    kernel's mount table plus a failed write attempt, never by a tool's
    message."""

    @staticmethod
    def _probe(path, env=None, shell="sh"):
        return subprocess.run(
            [shell, "-c", lock.read_only_probe_script(str(path))],
            env={**os.environ, **(env or {})},
            capture_output=True,
        ).returncode

    @pytest.fixture
    def ro_table(self, tmp_path):
        """A mount table that calls the pytest temp directory's mount read-only."""
        table = tmp_path.parent / "mounts"
        real = os.path.realpath(str(tmp_path))
        table.write_text(
            f"/dev/x / btrfs rw,relatime 0 0\n/dev/y {real} ext4 ro,noatime 0 0\n"
        )
        return str(table)

    @pytest.fixture
    def unwritable(self, tmp_path, monkeypatch):
        """A store this account cannot write and cannot elevate for: the
        disaster-recovery medium's shape. The elevated fallback is removed
        from the lock manager because this machine's passwordless sudo would
        otherwise write into the 0555 directory as root."""
        if os.geteuid() == 0:
            pytest.skip("root can write anywhere but a read-only mount")
        real_build = RawEndpoint._build_lock_manager

        def unelevated(self):
            manager = real_build(self)
            manager._run_elevated = None
            return manager

        monkeypatch.setattr(RawEndpoint, "_build_lock_manager", unelevated)
        d = tmp_path / "store"
        d.mkdir()
        d.chmod(0o555)
        yield d
        d.chmod(0o755)

    def test_the_probe_reads_the_mount_table_by_exit_status(
        self, tmp_path, ro_table, unwritable
    ):
        assert self._probe(tmp_path) == lock.WRITABLE
        assert self._probe(tmp_path / "missing") == 3
        # Unwritable and the table says ro: read-only.
        assert self._probe(unwritable, {"BBNG_MOUNT_TABLE": ro_table}) == lock.READ_ONLY
        # Unwritable but the real table says rw: a permission problem, not ro.
        assert self._probe(unwritable) == lock.WRITABLE
        # Writable settles it whatever a table claims.
        assert self._probe(tmp_path, {"BBNG_MOUNT_TABLE": ro_table}) == lock.WRITABLE
        # No table and no mount(8) on PATH: unknown, which the caller treats
        # as not read-only.
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        for tool in ("sh", "awk", "grep", "mkdir", "rmdir"):
            (bin_dir / tool).symlink_to(shutil.which(tool))
        assert (
            self._probe(
                unwritable,
                {"BBNG_MOUNT_TABLE": "/nonexistent", "PATH": str(bin_dir)},
            )
            == lock.UNKNOWN_MOUNT
        )

    def test_the_probe_has_no_newline_and_no_case_pattern(self):
        """One line, so it survives every transport; no ``case`` inside a
        command substitution, which bash 3.2 (macOS /bin/sh) cannot parse."""
        script = lock.read_only_probe_script("/x")
        assert "\n" not in script
        assert "case " not in script

    def _manager(self, rc):
        class Manager:
            location = "/medium"

            def holds_shared(self, name, holder):
                return False

            def acquire_shared_persistent(self, *a):
                raise lock.RemoteLockUnavailable(
                    "the lock directory could not be created."
                )

            def release_shared(self, *a):
                raise lock.RemoteLockUnavailable("gone")

            def location_is_read_only(self):
                return rc

        return Manager()

    def test_record_pin_skips_the_pin_on_a_read_only_location(self, caplog):
        with caplog.at_level(logging.INFO):
            lock.record_pin(self._manager(True), _snap("home-1"), "restore:s1", True)
        assert "/medium is mounted read-only" in caplog.text
        assert "not pinned" in caplog.text

    def test_record_pin_still_refuses_elsewhere_without_a_doubled_period(self):
        with pytest.raises(__util__.AbortError) as info:
            lock.record_pin(self._manager(False), _snap("home-1"), "restore:s1", True)
        message = str(info.value)
        assert "could not be created. Refusing" in message
        assert ".." not in message

    def test_reason_of_drops_a_final_period_only(self):
        assert lock.reason_of(RuntimeError("busy.")) == "busy"
        assert lock.reason_of(RuntimeError("a. b")) == "a. b"
        assert lock.reason_of(RuntimeError("x")) == "x"

    def test_a_raw_store_on_a_read_only_medium_is_pinned_without_error(
        self, unwritable, ro_table, monkeypatch, caplog
    ):
        """The measured regression: a raw:// location whose lock store cannot
        be created and whose filesystem is read-only. The restore's pin is
        skipped with a notice and the transfer goes on."""
        monkeypatch.setenv("BBNG_MOUNT_TABLE", ro_table)
        ep = RawEndpoint(config={"path": str(unwritable), "snap_prefix": ""})
        snap = _snap("home-1")
        with caplog.at_level(logging.INFO):
            ep.set_lock(snap, "restore:s1", True)
        assert "restore:s1" in snap.locks
        assert "mounted read-only" in caplog.text
        assert not (unwritable / ".btrfs-backup-ng.locks").exists()

    def test_a_raw_store_that_merely_refuses_the_write_still_aborts(
        self, unwritable, monkeypatch
    ):
        monkeypatch.delenv("BBNG_MOUNT_TABLE", raising=False)
        ep = RawEndpoint(config={"path": str(unwritable), "snap_prefix": ""})
        with pytest.raises(__util__.AbortError, match="skip-remote-lock") as info:
            ep.set_lock(_snap("home-1"), "restore:s1", True)
        assert ".." not in str(info.value)

    @pytest.mark.parametrize(
        "rc,expected",
        [
            (lock.READ_ONLY, True),
            (lock.WRITABLE, False),
            (lock.UNKNOWN_MOUNT, False),
            (3, False),
            (127, False),
        ],
    )
    def test_the_manager_says_read_only_on_that_exit_status_alone(self, rc, expected):
        """Only the READ_ONLY status is a yes: an unknown mount table, a
        missing directory or a probe that could not run all mean "not
        read-only", so the refusal and its opt-out still apply there."""
        manager = lock.RemoteLockManager(lambda script: (rc, "", ""), "/medium")
        assert manager.location_is_read_only() is expected

    def test_a_probe_that_does_not_run_is_not_read_only(self):
        def broken(script):
            raise OSError("no shell")

        manager = lock.RemoteLockManager(broken, "/medium")
        assert manager.location_is_read_only() is False

    def test_the_local_rule_asks_the_kernel(self, tmp_path, monkeypatch):
        assert lock.local_path_is_read_only(tmp_path) is False
        real = os.statvfs
        monkeypatch.setattr(
            lock.os,
            "statvfs",
            lambda p: SimpleNamespace(f_flag=real(p).f_flag | os.ST_RDONLY),
        )
        assert lock.local_path_is_read_only(tmp_path) is True
        assert lock.local_path_is_read_only(tmp_path / "missing") is False


# --------------------------------------------------------------------------- #
# c. one writer at a time, in both directions
# --------------------------------------------------------------------------- #
class TestOneWriterIntoALocalTarget:
    def _snapper_snapshot(self, tmp_path, number=3):
        subvol = tmp_path / "src" / ".snapshots" / str(number) / "snapshot"
        subvol.mkdir(parents=True)
        return SimpleNamespace(
            number=number,
            subvolume_path=subvol,
            info_xml_path=subvol.parent / "info.xml",
            date=__import__("datetime").datetime(2026, 9, 24, 12, 0, 0),
            get_backup_name=lambda fmt=None: f"root-{number}-20260924",
        )

    def test_a_backup_is_refused_while_another_writer_holds_the_target(
        self, tmp_path, real_shell, monkeypatch
    ):
        target = tmp_path / "target"
        ep = _endpoint(target)
        monkeypatch.setattr(
            ops, "_create_snapper_snapshot_wrapper", lambda s, d=None: _snap("w")
        )
        monkeypatch.setattr(ops, "send_snapshot", lambda *a, **k: None)
        other = SnapperLayout(_endpoint(target))
        with other.writer_lock("Restoring into snapper config 'root'"):
            with pytest.raises(
                __util__.SnapshotTransferError, match="another operation holds the lock"
            ):
                ops.send_snapper_snapshot(self._snapper_snapshot(tmp_path), ep)
        assert not (target / ".snapshots" / "3.incoming").exists()
        assert not (target / ".snapshots" / "3").exists()

    def test_a_sync_is_refused_while_another_writer_holds_the_target(
        self, tmp_path, real_shell, monkeypatch
    ):
        target = tmp_path / "target"
        ep = _endpoint(target)
        snapshot = self._snapper_snapshot(tmp_path)
        monkeypatch.setattr(
            ops, "get_snapper_snapshots_for_backup", lambda *a, **k: [snapshot]
        )
        planned: list = []
        monkeypatch.setattr(
            ops, "_snapper_dest_view", lambda d: planned.append(d) or SimpleNamespace()
        )
        other = SnapperLayout(_endpoint(target))
        with other.writer_lock("Snapper backup into the same target"):
            with pytest.raises(
                __util__.SnapshotTransferError, match="another operation holds the lock"
            ):
                ops.sync_snapper_snapshots(SimpleNamespace(), "root", ep)
        assert planned == [], "the sync read the destination before it held the lock"

    def test_the_sync_holds_the_lock_for_the_whole_run(
        self, tmp_path, real_shell, monkeypatch
    ):
        target = tmp_path / "target"
        ep = _endpoint(target)
        snapshot = self._snapper_snapshot(tmp_path)
        monkeypatch.setattr(
            ops, "get_snapper_snapshots_for_backup", lambda *a, **k: [snapshot]
        )
        wrapper = _snap("root-3-20260924")
        monkeypatch.setattr(
            ops, "_create_snapper_snapshot_wrapper", lambda s, d=None: wrapper
        )
        monkeypatch.setattr(ops, "_snapper_dest_view", lambda d: SimpleNamespace())
        monkeypatch.setattr(
            "btrfs_backup_ng.core.planning.plan_transfer_sequence",
            lambda w, v, only=None: [(wrapper, None)],
        )
        seen = {}

        def fake_send(snap, dest, parent_snapper_snapshot=None, options=None, **kw):
            seen["held"] = kw.get("writer_lock_held")
            # A second writer cannot get in while the sync runs.
            with pytest.raises(RuntimeError, match="another operation holds the lock"):
                with SnapperLayout(_endpoint(target)).writer_lock("second"):
                    pass

        monkeypatch.setattr(ops, "send_snapper_snapshot", fake_send)
        assert ops.sync_snapper_snapshots(SimpleNamespace(), "root", ep) == 1
        assert seen["held"] is True
        # Released afterwards.
        with SnapperLayout(_endpoint(target)).writer_lock("after"):
            pass

    def test_the_writer_lock_makes_snapshots_below_an_existing_target_only(
        self, tmp_path
    ):
        target = tmp_path / "target"
        target.mkdir()
        with SnapperLayout(_endpoint(target)).writer_lock("first backup"):
            assert (target / ".snapshots").is_dir()
        missing = tmp_path / "unmounted" / "target"
        layout = SnapperLayout(
            LocalEndpoint(
                config={"path": str(missing), "snap_prefix": "", "fs_checks": "skip"}
            )
        )
        with pytest.raises(RuntimeError, match="Nothing was created"):
            with layout.writer_lock("backup"):
                pass
        assert not (tmp_path / "unmounted").exists()

    def test_a_remote_or_raw_destination_takes_no_local_lock(self, tmp_path):
        remote = SimpleNamespace(_is_remote=True, config={"path": "/x"})
        with ops._local_snapper_writer_lock(remote, "s"):
            pass
        raw = RawEndpoint(config={"path": str(tmp_path), "snap_prefix": ""})
        with ops._local_snapper_writer_lock(raw, "s"):
            pass
        assert not (tmp_path / ".snapshots").exists()


# --------------------------------------------------------------------------- #
# d. a regular file named like a slot
# --------------------------------------------------------------------------- #
class TestAFileNamedLikeASlot:
    def test_it_occupies_its_number(self, tmp_path):
        snapshots = tmp_path / ".snapshots"
        (snapshots / "41" / "snapshot").mkdir(parents=True)
        (snapshots / "42").write_text("a file, not a slot")
        config = SimpleNamespace(snapshots_dir=snapshots)
        assert SnapperScanner.get_next_snapshot_number(None, config) == 43  # type: ignore[arg-type]

    def test_publish_fresh_refuses_a_number_that_stands_still(
        self, tmp_path, real_shell, monkeypatch
    ):
        config = tmp_path / "config"
        (config / ".snapshots").mkdir(parents=True)
        (config / ".snapshots" / "42").write_text("a file")
        layout = SnapperLayout(_endpoint(config), next_number=lambda: 42)
        layout.open_slot(42)
        (Path(layout.incoming_dir(42)) / "snapshot").mkdir()
        attempts = []
        real = __util__.rename_noreplace

        def counted(src, dst):
            attempts.append(dst)
            return real(src, dst)

        monkeypatch.setattr(__util__, "rename_noreplace", counted)
        with pytest.raises(__util__.SnapshotTransferError, match="not a snapshot slot"):
            layout.publish_fresh(lambda n: None)
        assert len(attempts) == 1, "the same number was tried again and again"
        assert (config / ".snapshots" / "42").read_text() == "a file"
        assert not Path(layout.incoming_dir(42)).exists()


# --------------------------------------------------------------------------- #
# f. a pin never outlives the transfer
# --------------------------------------------------------------------------- #
class TestAPinNeverOutlivesTheTransfer:
    def _run(self, monkeypatch, release_on_failure, exc):
        calls: list[tuple[str, bool]] = []

        class Source:
            def set_lock(self, snapshot, lock_id, state, parent=False):
                calls.append((snapshot.get_name(), state))

        class Dest:
            config = {"path": "/dest"}

            def get_id(self):
                return "dest"

            def list_snapshots(self, flush_cache=False):
                return []

        def dying(*a, **k):
            raise exc

        monkeypatch.setattr(ops, "send_snapshot", dying)
        monkeypatch.setattr(ops, "destination_artifact_exists", lambda d, n: False)
        snapshot = _snap("home-1")
        with pytest.raises(type(exc)):
            ops._execute_transfers(
                Source(),
                Dest(),
                [(snapshot, None)],
                {},
                lock_id="restore:s1",
                release_on_failure=release_on_failure,
            )
        return calls

    def test_ctrl_c_releases_a_restores_pin(self, monkeypatch):
        calls = self._run(monkeypatch, True, KeyboardInterrupt())
        assert calls == [("home-1", True), ("home-1", False)]

    def test_any_unexpected_exception_releases_it_too(self, monkeypatch):
        calls = self._run(monkeypatch, True, RuntimeError("the pipe vanished"))
        assert calls == [("home-1", True), ("home-1", False)]

    def test_a_backup_keeps_its_pin_as_before(self, monkeypatch):
        calls = self._run(monkeypatch, False, KeyboardInterrupt())
        assert calls == [("home-1", True)]


# --------------------------------------------------------------------------- #
# g. pins protect on every location
# --------------------------------------------------------------------------- #
class TestPinsProtectEverywhere:
    def _stream(self, location: Path, name: str) -> RawSnapshot:
        stream = location / f"{name}.btrfs"
        stream.write_bytes(b"x")
        (location / f"{name}.btrfs.meta").write_text("{}")
        snap = RawSnapshot(name=name, stream_path=stream)
        return snap

    def test_a_raw_pin_is_seen_by_another_process(self, tmp_path):
        location = tmp_path / "raw"
        location.mkdir()
        reader = RawEndpoint(config={"path": str(location), "snap_prefix": ""})
        pruner = RawEndpoint(config={"path": str(location), "snap_prefix": ""})
        stream = self._stream(location, "home-1")
        reader.set_lock(stream, "restore:s1", True)
        assert (location / ".btrfs-backup-ng.locks").is_dir()
        # The pruner lists the location afresh: its own objects, empty
        # in-memory lock sets -- what a prune in another process sees.
        theirs = RawSnapshot(name="home-1", stream_path=stream.stream_path)
        result = pruner._delete_snapshots_locked([theirs])
        assert result.deleted_count == 0
        assert [reason for _s, reason in result.skipped] == [
            "locked by another process at this location"
        ]
        assert stream.stream_path.exists()
        reader.set_lock(stream, "restore:s1", False)
        result = pruner._delete_snapshots_locked([theirs])
        assert result.deleted_count == 1
        assert not stream.stream_path.exists()

    def test_a_raw_pin_is_reported_by_the_lock_readers(self, tmp_path):
        location = tmp_path / "raw"
        location.mkdir()
        ep = RawEndpoint(config={"path": str(location), "snap_prefix": ""})
        ep.set_lock(self._stream(location, "home-1"), "restore:s1", True)
        assert ep._read_locks() == {"home-1": {"locks": ["restore:s1"]}}
        assert RawEndpoint.persists_locks is True

    def _slot_deleter(self, monkeypatch, locks):
        deleted: list[str] = []
        monkeypatch.setattr(
            prune_cli,
            "_delete_snapper_slot_btrfs",
            lambda ep, slot_dir, remote: deleted.append(slot_dir),
        )

        class Endpoint:
            config = {"path": "/backups"}

            def _read_locks(self):
                if isinstance(locks, Exception):
                    raise locks
                return locks

        monkeypatch.setattr(
            prune_cli, "choose_endpoint", lambda p, c: Endpoint(), raising=False
        )
        monkeypatch.setattr(
            "btrfs_backup_ng.endpoint.choose_endpoint", lambda p, c: Endpoint()
        )
        return deleted

    def test_a_pinned_snapper_slot_is_not_deleted(self, monkeypatch, caplog):
        deleted = self._slot_deleter(
            monkeypatch, {"snapshot-3": {"locks": ["restore:s1"]}}
        )
        backups = [{"number": 3}, {"number": 4}]
        with caplog.at_level(logging.INFO):
            count, errors = prune_cli.delete_snapper_backups("/backups", backups, {})
        assert (count, errors) == (1, [])
        assert deleted == ["/backups/.snapshots/4"]
        assert "pinned by a restore in progress" in caplog.text

    def test_an_unreadable_lock_store_deletes_no_slot(self, monkeypatch):
        deleted = self._slot_deleter(monkeypatch, __util__.AbortError("corrupt"))
        count, errors = prune_cli.delete_snapper_backups(
            "/backups", [{"number": 3}], {}
        )
        assert (count, deleted) == (0, [])
        assert errors and "lock store could not be read" in errors[0]
