"""Locks that survive the process that took them.

A restore reads a snapshot on a remote target while a prune -- in another
process, possibly on another machine -- deletes from that same target. The lock
protecting the snapshot under restore used to live in the restoring process's
memory, where the pruning process could not see it. The guard was there; it was
looking at the wrong thing.

``restore --status`` reported this as "this target does not persist locks",
which is honest and useless: a backup tool that reports it cannot protect a
restore is not protecting the restore.

These tests drive the real POSIX protocol -- mkdir, stat, mv, rmdir -- against a
local sandbox standing in for the target. Nothing about the protocol is mocked,
because a mock of its replies would pass whatever the protocol did.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from btrfs_backup_ng.sshutil.lock import (
    HOLDERS_DIR_NAME,
    LOCK_DIR_NAME,
    RemoteLockBusy,
    RemoteLockManager,
    RemoteLockUnavailable,
    blocked_by_remote_lock,
    encode_name,
    read_persisted_locks,
    snapshot_lock_name,
    write_persisted_locks,
)

_real_run = subprocess.run


def _runner(sandbox: Path):
    def run(script: str):
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return run


def _manager(sandbox: Path, **kw) -> RemoteLockManager:
    return RemoteLockManager(_runner(sandbox), str(sandbox), hostname="testhost", **kw)


def _snap(name: str):
    return SimpleNamespace(locks=set(), parent_locks=set(), get_name=lambda: name)


class TestExclusion:
    """The one property everything else rests on."""

    def test_a_second_holder_is_refused(self, tmp_path):
        first, second = _manager(tmp_path), _manager(tmp_path)
        assert first.acquire_once("target", "prune") == "acquired"
        with pytest.raises(RemoteLockBusy):
            second.acquire_once("target", "restore")

    def test_the_refusal_names_the_holder(self, tmp_path):
        """An operator has to be able to find the process that is blocking them."""
        _manager(tmp_path).acquire_once("target", "prune")
        with pytest.raises(RemoteLockBusy) as caught:
            _manager(tmp_path).acquire_once("target", "restore")
        info = caught.value.info or {}
        assert info.get("operation") == "prune"
        assert info.get("hostname") == "testhost"
        assert info.get("pid") == os.getpid()

    def test_release_lets_the_next_contender_in(self, tmp_path):
        first = _manager(tmp_path)
        first.acquire_once("target", "prune")
        first.release("target")
        assert _manager(tmp_path).acquire_once("target", "restore") == "acquired"

    def test_concurrent_contenders_produce_exactly_one_winner(self, tmp_path):
        """mkdir is the atomic primitive; this is what it is being trusted for."""
        wins: list[str] = []
        barrier = threading.Barrier(12)

        def contend(i: int) -> None:
            barrier.wait()
            try:
                _manager(tmp_path).acquire_once("target", f"op-{i}")
                wins.append(f"op-{i}")
            except RemoteLockBusy:
                pass

        threads = [threading.Thread(target=contend, args=(i,)) for i in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(wins) == 1, f"expected one winner, got {wins}"


class TestTheScriptsAreNotBashOnly:
    """Remote targets are not all Fedora.

    Two shipped bugs in this project came from a remote command string that was
    written against bash and run under dash. Every lock script is POSIX by
    construction; this runs them under each shell present rather than trusting
    that, so a bashism cannot pass review unnoticed.
    """

    def test_the_protocol_behaves_identically_under_every_posix_shell(self, tmp_path):
        from .lockshell import available_shells

        shells = available_shells()
        assert "dash" in shells, (
            "dash is not installed, so this test would silently prove nothing"
        )

        results = {}
        for shell in shells:
            sandbox = tmp_path / shell
            sandbox.mkdir()

            def run(script, _sh=shell):
                proc = subprocess.run(
                    [_sh, "-c", script], capture_output=True, text=True
                )
                return proc.returncode, proc.stdout, proc.stderr

            manager = RemoteLockManager(run, str(sandbox), hostname="h")
            outcome = {}
            outcome["exclusive"] = manager.acquire_once("target", "prune")
            try:
                RemoteLockManager(run, str(sandbox), hostname="h2").acquire_once(
                    "target", "other"
                )
                outcome["second_exclusive"] = "ACQUIRED (exclusion broken)"
            except RemoteLockBusy:
                outcome["second_exclusive"] = "refused"
            manager.acquire_shared("snap-a", "restore:1")
            RemoteLockManager(run, str(sandbox), hostname="h2").acquire_shared(
                "snap-a", "restore:2"
            )
            outcome["shared_holders"] = len(manager.live_locks()["snap-a"])
            outcome["names"] = sorted(manager.live_lock_names())
            outcome["status"] = read_persisted_locks(manager)
            manager.release_shared("snap-a", "restore:1")
            outcome["after_one_release"] = len(manager.live_locks()["snap-a"])
            results[shell] = outcome

        reference = results[shells[0]]
        for shell, outcome in results.items():
            assert outcome == reference, (
                f"{shell} disagrees with {shells[0]}: {outcome} != {reference}"
            )


class TestSharedPinsDoNotBlockEachOther:
    """A pin protects a snapshot from DELETION, not from other readers.

    The contract being mirrored is ``snapshot.locks``, a SET of lock ids. Making
    the remote pin exclusive made a second restore of the same snapshot fail
    against the first -- a concurrency regression the local path never had.
    """

    def test_two_restores_can_pin_the_same_snapshot(self, tmp_path):
        first, second = _manager(tmp_path), _manager(tmp_path)
        first.acquire_shared("snap-a", "restore:1")
        second.acquire_shared("snap-a", "restore:2")  # must not raise
        assert len(_manager(tmp_path).live_locks()["snap-a"]) == 2

    def test_a_backup_can_pin_what_a_restore_is_reading(self, tmp_path):
        """A transfer pins its incremental parent; a restore may be reading it."""
        _manager(tmp_path).acquire_shared("snap-a", "restore:1")
        _manager(tmp_path).acquire_shared("snap-a", "p:transfer:9")
        assert len(_manager(tmp_path).live_locks()["snap-a"]) == 2

    def test_the_snapshot_stays_pinned_until_the_LAST_holder_leaves(self, tmp_path):
        first, second = _manager(tmp_path), _manager(tmp_path)
        first.acquire_shared("snap-a", "restore:1")
        second.acquire_shared("snap-a", "restore:2")

        first.release_shared("snap-a", "restore:1")
        assert blocked_by_remote_lock(_manager(tmp_path), [_snap("a")]) == {"a"}, (
            "one holder let go and the pin vanished while another still held it"
        )

        second.release_shared("snap-a", "restore:2")
        assert blocked_by_remote_lock(_manager(tmp_path), [_snap("a")]) == set()

    def test_many_concurrent_holders_all_succeed(self, tmp_path):
        """The inverse of the exclusive test: here EVERY contender must win."""
        wins: list = []
        barrier = threading.Barrier(10)

        def pin(i: int) -> None:
            barrier.wait()
            _manager(tmp_path).acquire_shared("snap-a", f"restore:{i}")
            wins.append(i)

        threads = [threading.Thread(target=pin, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(wins) == 10
        assert len(_manager(tmp_path).live_locks()["snap-a"]) == 10

    def test_a_holder_releases_only_its_own_pin(self, tmp_path):
        manager = _manager(tmp_path)
        manager.acquire_shared("snap-a", "restore:1")
        manager.acquire_shared("snap-a", "restore:2")
        manager.release_shared("snap-a", "restore:1")
        holders = _manager(tmp_path).live_locks()["snap-a"]
        assert [h.lock_id for h in holders] == ["restore:2"]


class TestNamesAreNotMangledIntoEachOther:
    """Four defects found by adversarial review, all one root cause.

    The on-disk name was produced by replacing every unsafe character with an
    underscore. That map is lossy, and it was being used as an identity.
    """

    def test_a_snapshot_whose_name_needs_encoding_is_still_seen(self, tmp_path):
        """The writer stored a rewritten name; the guard asked for the real one,
        so the pin existed and was invisible and a prune would delete it."""
        weird = "root:2024/01"
        _manager(tmp_path).acquire_shared(f"snap-{weird}", "restore:1")
        assert blocked_by_remote_lock(_manager(tmp_path), [_snap(weird)]) == {weird}

    def test_two_lock_ids_that_used_to_collide_get_separate_records(self, tmp_path):
        """`restore:x/y` and `restore:x:y` both became `restore_x_y`, so one
        holder releasing removed the other's pin."""
        manager = _manager(tmp_path)
        manager.acquire_shared("snap-a", "restore:x/y")
        manager.acquire_shared("snap-a", "restore:x:y")
        assert len(_manager(tmp_path).live_locks()["snap-a"]) == 2

        manager.release_shared("snap-a", "restore:x/y")
        assert blocked_by_remote_lock(_manager(tmp_path), [_snap("a")]) == {"a"}, (
            "one holder released and took another holder's pin with it"
        )

    def test_encoding_is_injective_over_awkward_names(self, tmp_path):
        candidates = [
            "restore:x/y",
            "restore:x:y",
            "restore_x_y",
            "root:2024/01",
            "root_2024_01",
            "a b",
            "a-b",
        ]
        assert len({encode_name(c) for c in candidates}) == len(candidates)

    def test_status_reports_the_real_name_not_the_encoded_one(self, tmp_path):
        """The directory name is an encoding and cannot be decoded back, so the
        lock's real name travels in its payload. Reading identity off the
        directory instead would show an operator a name that matches no snapshot
        they have, and --unlock would key off that same wrong name."""
        weird = "root:2024/01"
        _manager(tmp_path).acquire_shared(f"snap-{weird}", "restore:1")
        assert read_persisted_locks(_manager(tmp_path)) == {
            weird: {"locks": ["restore:1"]}
        }

    def test_unlock_clears_a_pin_on_an_awkwardly_named_snapshot(self, tmp_path):
        weird = "root:2024/01"
        manager = _manager(tmp_path)
        manager.acquire_shared(f"snap-{weird}", "restore:1")
        manager.acquire_shared(f"snap-{weird}", "restore:2")

        write_persisted_locks(_manager(tmp_path), {weird: {"locks": ["restore:2"]}})
        assert read_persisted_locks(_manager(tmp_path)) == {
            weird: {"locks": ["restore:2"]}
        }

    def test_a_target_path_with_a_space_does_not_misdirect_the_sweeper(self, tmp_path):
        """The remote used to emit full paths on a space-delimited line. A target
        directory containing a space produced a truncated path, which the sweeper
        handed to `rm -f` -- deleting something unrelated, reporting success, and
        leaving the real holder in place."""
        spaced = tmp_path / "my backups"
        spaced.mkdir()
        decoy = tmp_path / "my"
        decoy.write_text("not a lock file")

        manager = _manager(spaced, stale_after=1)
        manager.acquire_shared("snap-a", "restore:1")
        holder = next((spaced / LOCK_DIR_NAME).glob("*/holders/*"))
        old = time.time() - 10_000
        os.utime(holder, (old, old))

        assert manager.sweep_dead_holders() == 1
        assert not holder.exists(), "the real holder survived the sweep"
        assert decoy.exists(), "the sweeper deleted an unrelated path"

    def test_an_unnameable_holder_can_still_be_unlocked(self, tmp_path):
        """--unlock addressed holders by lock id, which is exactly what a holder
        with an unreadable payload does not have. It could never be cleared."""
        holders = (
            tmp_path
            / LOCK_DIR_NAME
            / f"{encode_name('snap-a')}.lock"
            / HOLDERS_DIR_NAME
        )
        holders.mkdir(parents=True)
        (holders / "mystery").write_text("{{{")

        assert blocked_by_remote_lock(_manager(tmp_path), [_snap("a")]) == {"a"}, (
            "a pin nobody can name must still block a delete"
        )
        write_persisted_locks(_manager(tmp_path), {})
        assert _manager(tmp_path).live_lock_names() == set()

    def test_an_unnameable_holder_on_an_encoded_name_still_blocks(self, tmp_path):
        """Both failures at once: the payload is unreadable AND the snapshot name
        needed encoding, so neither the listed key nor the real name matches.
        The directory name is exact in every case, so it is what decides."""
        weird = "root:2024/01"
        holders = (
            tmp_path
            / LOCK_DIR_NAME
            / f"{encode_name(f'snap-{weird}')}.lock"
            / HOLDERS_DIR_NAME
        )
        holders.mkdir(parents=True)
        (holders / "mystery").write_text("{{{")
        assert blocked_by_remote_lock(_manager(tmp_path), [_snap(weird)]) == {weird}

    def test_an_unnameable_holder_reads_back_recognisably(self, tmp_path):
        holders = (
            tmp_path
            / LOCK_DIR_NAME
            / f"{encode_name('snap-a')}.lock"
            / HOLDERS_DIR_NAME
        )
        holders.mkdir(parents=True)
        (holders / "mystery").write_text("{{{")
        assert read_persisted_locks(_manager(tmp_path)) == {"a": {"locks": ["unknown"]}}


class TestTheAcquisitionWindow:
    """Between the mkdir that wins a lock and the touch that heartbeats it."""

    def test_a_lock_taken_microseconds_ago_is_not_treated_as_dead(self, tmp_path):
        """Ageing a missing heartbeat from epoch zero made a brand-new lock look
        infinitely old, so a contender arriving inside that window broke it and
        took a lock somebody already held. Two winners, intermittently."""
        manager = _manager(tmp_path)
        (tmp_path / LOCK_DIR_NAME / f"{encode_name('target')}.lock").mkdir(parents=True)
        with pytest.raises(RemoteLockBusy):
            manager.acquire_once("target", "thief")

    def test_a_genuinely_dead_lock_is_still_breakable(self, tmp_path):
        """The other half: closing the window must not make dead locks eternal."""
        manager = _manager(tmp_path, stale_after=1)
        lock = tmp_path / LOCK_DIR_NAME / f"{encode_name('target')}.lock"
        lock.mkdir(parents=True)
        old = time.time() - 10_000
        os.utime(lock, (old, old))
        assert manager.acquire_once("target", "later") == "stale-broken"


class TestTheOperatorCanOptOut:
    """--skip-remote-lock, for a destination that can be read but not written."""

    def _endpoint(self, tmp_path, **extra):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path), **extra}
        return endpoint

    def test_without_the_flag_an_unrecordable_lock_stops_the_run(self, tmp_path):
        from btrfs_backup_ng import __util__

        readonly = tmp_path / "readonly"
        readonly.mkdir()
        os.chmod(readonly, 0o500)  # the read-only destination this is all about
        try:
            endpoint = self._endpoint(readonly)
            endpoint._build_lock_manager = lambda: _manager(readonly)
            with pytest.raises(__util__.AbortError, match="skip-remote-lock"):
                endpoint.set_lock(_snap("a"), "restore:1", True)
        finally:
            os.chmod(readonly, 0o700)

    def test_the_flag_lets_it_continue(self, tmp_path, caplog):
        endpoint = self._endpoint(tmp_path, skip_remote_lock=True)

        def unusable():
            raise RemoteLockUnavailable("read-only target")

        endpoint._build_lock_manager = unusable
        snapshot = _snap("a")
        endpoint.set_lock(snapshot, "restore:1", True)  # must not raise
        assert "restore:1" in snapshot.locks, "the in-memory pin is still kept"

    def test_the_flag_does_not_silence_the_readers(self, tmp_path):
        """It relaxes the abort ONLY. Nothing may start reporting a target as
        unlocked without having looked -- that is the false all-clear this
        whole mechanism exists to remove."""
        endpoint = self._endpoint(tmp_path, skip_remote_lock=True)
        assert endpoint._lock_target_path() == str(tmp_path)
        _manager(tmp_path).acquire_shared("snap-a", "restore:elsewhere")
        assert blocked_by_remote_lock(_manager(tmp_path), [_snap("a")]) == {"a"}


class TestTheConfigOptionIsReal:
    def test_a_target_can_carry_skip_remote_lock(self):
        """The flag has a config counterpart, or a site that needs it has to
        remember to type it on every run."""
        from btrfs_backup_ng.config.schema import TargetConfig

        assert TargetConfig(path="/backup").skip_remote_lock is False
        assert TargetConfig(path="/backup", skip_remote_lock=True).skip_remote_lock

    def test_it_reaches_the_endpoint_config(self):
        from btrfs_backup_ng.cli.common import thread_ssh_target_config
        from btrfs_backup_ng.config.schema import TargetConfig

        kwargs: dict = {}
        thread_ssh_target_config(
            kwargs, TargetConfig(path="/backup", skip_remote_lock=True)
        )
        assert kwargs["skip_remote_lock"] is True


class TestAbandonedHoldersAreSwept:
    def test_a_long_dead_holder_file_is_removed(self, tmp_path):
        manager = _manager(tmp_path, stale_after=1)
        manager.acquire_shared("snap-a", "restore:1")
        holder = (
            tmp_path
            / LOCK_DIR_NAME
            / f"{encode_name('snap-a')}.lock"
            / HOLDERS_DIR_NAME
            / encode_name("restore:1")
        )
        old = time.time() - 10_000
        os.utime(holder, (old, old))
        assert manager.sweep_dead_holders() == 1
        assert not holder.exists()

    def test_a_merely_unrefreshed_holder_is_left_alone(self, tmp_path):
        """Past the stale threshold but not past all doubt: ignored for locking
        purposes, but not deleted, because deleting someone else's live pin is
        far worse than leaving a small file behind."""
        manager = _manager(tmp_path, stale_after=100)
        manager.acquire_shared("snap-a", "restore:1")
        holder = (
            tmp_path
            / LOCK_DIR_NAME
            / f"{encode_name('snap-a')}.lock"
            / HOLDERS_DIR_NAME
            / encode_name("restore:1")
        )
        old = time.time() - 150  # stale, but under 2x the threshold
        os.utime(holder, (old, old))
        assert manager.live_lock_names() == set()
        assert manager.sweep_dead_holders() == 0
        assert holder.exists()


class TestCleanupOnExit:
    def test_an_interrupted_run_releases_its_pins(self, tmp_path):
        """Ctrl-C on a restore should free the snapshot now, not in three
        minutes when the stale window expires."""
        from btrfs_backup_ng.sshutil import lock as lock_mod

        manager = _manager(tmp_path)
        manager.acquire_shared_persistent("snap-a", "restore:1")
        assert manager.live_lock_names() == {"snap-a"}

        lock_mod._release_all_held()  # what atexit and the signal handler call
        assert manager.live_lock_names() == set()

    def test_cleanup_never_raises_on_a_broken_transport(self, tmp_path):
        """Exit-time cleanup that raises would mask the real failure."""
        from btrfs_backup_ng.sshutil import lock as lock_mod

        def broken(_script):
            raise OSError("no route to host")

        manager = RemoteLockManager(broken, str(tmp_path), hostname="h")
        lock_mod._register_for_cleanup(manager, "snap-a\x00restore:1")
        lock_mod._release_all_held()  # must not raise


class TestStaleness:
    """A crashed holder must not lock the target out forever."""

    def test_a_dead_holder_is_broken_after_the_threshold(self, tmp_path):
        holder = _manager(tmp_path, stale_after=1)
        holder.acquire_once("target", "prune")
        holder._stop_heartbeat("target")  # simulate the process dying
        time.sleep(2)
        assert _manager(tmp_path, stale_after=1).acquire_once("t2", "x") == "acquired"
        assert (
            _manager(tmp_path, stale_after=1).acquire_once("target", "restore")
            == "stale-broken"
        )

    def test_a_live_holder_is_not_broken(self, tmp_path):
        """The inverse: a heartbeat that is being refreshed must hold the lock."""
        holder = _manager(tmp_path, stale_after=60)
        holder.acquire_once("target", "prune")
        with pytest.raises(RemoteLockBusy):
            _manager(tmp_path, stale_after=60).acquire_once("target", "restore")

    def test_staleness_is_judged_on_the_target_not_the_client(self, tmp_path):
        """No client clock is ever compared, so client skew cannot break a lock.

        The age expression must be computed from the target's own `date` and the
        target's own `stat`; if either came from this side, two hosts four
        seconds apart (which is what the development pair measured) would each
        reach a different verdict about the same lock.
        """
        script = _manager(tmp_path)._acquire_script("target", "{}", "tok")
        assert "date +%s" in script, "the target must supply the current time"
        assert "stat -c %Y" in script or "stat -f %m" in script


class TestListing:
    def test_live_locks_are_listed_and_stale_ones_are_not(self, tmp_path):
        manager = _manager(tmp_path, stale_after=1)
        manager.acquire_shared("snap-a", "restore:1")
        manager.acquire_shared("snap-b", "restore:2")
        assert manager.live_lock_names() == {"snap-a", "snap-b"}
        time.sleep(2)
        beat = (
            f"{tmp_path}/{LOCK_DIR_NAME}/{encode_name('snap-b')}.lock"
            f"/{HOLDERS_DIR_NAME}/{encode_name('restore:2')}"
        )
        manager._run(f"touch {beat}")  # b's holder keeps beating; a's does not
        assert manager.live_lock_names() == {"snap-b"}

    def test_a_lock_with_unreadable_details_is_still_a_lock(self, tmp_path):
        """ "Something holds this and we cannot say what" must never round down
        to "nothing holds this" -- that is the whole failure being fixed."""
        manager = _manager(tmp_path)
        manager.acquire_shared("snap-a", "restore:1")
        broken = (
            tmp_path
            / LOCK_DIR_NAME
            / f"{encode_name('snap-broken')}.lock"
            / HOLDERS_DIR_NAME
        )
        broken.mkdir(parents=True)
        (broken / "someholder").write_text("not json {{")

        live = manager.live_locks()
        assert "snap-broken" in live, "a lock vanished because its details did"
        assert live["snap-broken"][0].info == {}
        assert live["snap-a"][0].info["operation"] == "restore:1"


class TestTheGuardAProcessConsults:
    def test_a_snapshot_locked_by_another_process_is_blocked(self, tmp_path):
        """The headline case, stated exactly.

        The restoring process takes the lock. The pruning process is a DIFFERENT
        manager with its own empty in-memory state -- as a separate process
        genuinely is -- and must still see the lock.
        """
        restoring = _manager(tmp_path)
        restoring.acquire_shared_persistent("snap-root.20240101T120000", "restore:abc")

        pruning = _manager(tmp_path)
        blocked = blocked_by_remote_lock(
            pruning, [_snap("root.20240101T120000"), _snap("root.20240102T120000")]
        )
        assert blocked == {"root.20240101T120000"}

    def test_an_unreadable_lock_state_is_not_answered(self, tmp_path):
        """Neither available answer is honest, so it must raise rather than pick.

        Answering "nothing is locked" prunes on an unanswered question and can
        delete what a restore is reading. Answering "everything is locked" skips
        every deletion, which is how retention stops running while the operator
        is told it succeeded.
        """

        def broken(_script):
            raise OSError("no route to host")

        manager = RemoteLockManager(broken, str(tmp_path), hostname="h")
        with pytest.raises(RemoteLockUnavailable):
            blocked_by_remote_lock(manager, [_snap("root.20240101T120000")])

    def test_a_query_that_could_not_run_is_not_a_clean_answer(self, tmp_path):
        """The scripts end in `exit 0`, so a non-zero status means the shell
        never ran -- unreachable host, unreadable lock directory. Empty output
        then means "could not ask", and must not be read as "nothing is locked".

        This is the shape the hardware run exposed: the query failed at the
        transport, produced no output, and the prune deleted on the strength of
        it.
        """

        def failed(_script):
            return 255, "", "ssh: connect to host nas port 22: No route to host"

        manager = RemoteLockManager(failed, str(tmp_path), hostname="h")
        with pytest.raises(RemoteLockUnavailable):
            manager.live_lock_names()
        with pytest.raises(RemoteLockUnavailable):
            manager.live_locks()
        with pytest.raises(RemoteLockUnavailable):
            blocked_by_remote_lock(manager, [_snap("root.20240101T120000")])

    def test_nothing_locked_blocks_nothing(self, tmp_path):
        """The guard must not be a blanket refusal, or pruning never runs."""
        assert (
            blocked_by_remote_lock(_manager(tmp_path), [_snap("a"), _snap("b")])
            == set()
        )


class TestWriterAndReaderAgree:
    """A lock written under one key and read under another is a silent hole."""

    @pytest.mark.parametrize(
        "snapshot,expected",
        [
            (
                SimpleNamespace(get_name=lambda: "root.20240101T120000"),
                "root.20240101T120000",
            ),
            (
                SimpleNamespace(get_path=lambda: "/backup/root.20240101T120000"),
                "root.20240101T120000",
            ),
            (SimpleNamespace(name="root.20240101T120000"), "root.20240101T120000"),
            ("/backup/root.20240101T120000", "root.20240101T120000"),
        ],
    )
    def test_every_shape_delete_is_called_with_yields_the_same_name(
        self, snapshot, expected
    ):
        assert snapshot_lock_name(snapshot) == expected

    def test_the_name_the_guard_looks_up_is_the_name_set_lock_wrote(self, tmp_path):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        endpoint._lock_manager = lambda: _manager(tmp_path)

        snapshot = _snap("root.20240101T120000")
        endpoint.set_lock(snapshot, "restore:abc", True)

        assert blocked_by_remote_lock(_manager(tmp_path), [snapshot]) == {
            "root.20240101T120000"
        }


class TestReleaseIsNotPremature:
    def test_a_parent_lock_still_held_keeps_the_remote_lock(self, tmp_path):
        """A snapshot can be pinned directly AND as an incremental parent.

        Clearing the first of the two must not drop the protection while the
        second still holds it.
        """
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        # ONE manager, as a single process has: the endpoint caches it per path
        # so that holds() recognises this process's own lock and so that a
        # release can stop the heartbeat the acquire started.
        shared = _manager(tmp_path)
        endpoint._lock_manager = lambda: shared

        snapshot = _snap("root.20240101T120000")
        endpoint.set_lock(snapshot, "restore:abc", True)
        endpoint.set_lock(snapshot, "transfer:xyz", True, parent=True)

        endpoint.set_lock(snapshot, "restore:abc", False)
        assert _manager(tmp_path).live_lock_names() == {"snap-root.20240101T120000"}, (
            "the remote lock was released while a parent lock still held it"
        )

        endpoint.set_lock(snapshot, "transfer:xyz", False, parent=True)
        assert _manager(tmp_path).live_lock_names() == set()


class TestOneProcessOneManager:
    """The endpoint must reuse its manager, or it locks against itself."""

    def test_the_same_path_gets_the_same_manager(self, tmp_path):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        endpoint._build_lock_manager = lambda: _manager(tmp_path)
        assert endpoint._lock_manager() is endpoint._lock_manager()

    def test_a_changed_path_gets_a_new_manager(self, tmp_path):
        """The config can be rewritten between operations; a manager holding the
        old path would lock somewhere other than where the work is happening."""
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        endpoint._build_lock_manager = lambda: _manager(Path(endpoint.config["path"]))
        first = endpoint._lock_manager()
        endpoint.config["path"] = str(tmp_path / "elsewhere")
        assert endpoint._lock_manager() is not first

    def test_locking_a_snapshot_twice_in_one_process_is_not_contention(self, tmp_path):
        """Pinned directly AND as an incremental parent is normal, not a clash."""
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        # A partially-built endpoint has no ssh transport, so the shell runner is
        # pointed at this machine. The caching under test is the endpoint's.
        endpoint._build_lock_manager = lambda: _manager(tmp_path)

        snapshot = _snap("root.20240101T120000")
        endpoint.set_lock(snapshot, "restore:abc", True)
        endpoint.set_lock(snapshot, "transfer:xyz", True, parent=True)  # must not raise
        assert endpoint._lock_manager().live_lock_names() == {
            "snap-root.20240101T120000"
        }

    def test_threads_racing_for_the_manager_get_the_same_one(self, tmp_path):
        """Transfers run threaded. Two threads building a manager each would
        recreate the duplicate-instance bug the cache exists to prevent."""
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        endpoint._build_lock_manager = lambda: _manager(tmp_path)

        seen: list = []
        barrier = threading.Barrier(8)

        def grab():
            barrier.wait()
            seen.append(endpoint._lock_manager())

        threads = [threading.Thread(target=grab) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len({id(m) for m in seen}) == 1, "threads got different managers"


class TestStatusAndUnlockTellTheTruth:
    """What `restore --status` used to be unable to say about a remote target."""

    def test_status_reports_a_lock_taken_by_another_process(self, tmp_path):
        _manager(tmp_path).acquire_shared_persistent(
            "snap-root.20240101T120000", "restore:abc"
        )
        locks = read_persisted_locks(_manager(tmp_path))
        assert locks == {"root.20240101T120000": {"locks": ["restore:abc"]}}

    def test_a_lock_with_unreadable_details_is_reported_not_omitted(self, tmp_path):
        root = tmp_path / LOCK_DIR_NAME
        root.mkdir(parents=True, exist_ok=True)
        broken = root / "snap-x.lock"
        broken.mkdir()
        (broken / "heartbeat").touch()
        (broken / "info.json").write_text("{{{")
        assert read_persisted_locks(_manager(tmp_path)) == {"x": {"locks": ["unknown"]}}

    def test_unlock_releases_what_the_new_state_drops(self, tmp_path):
        manager = _manager(tmp_path)
        manager.acquire_shared("snap-a", "restore:1")
        manager.acquire_shared("snap-b", "restore:2")

        write_persisted_locks(_manager(tmp_path), {"b": {"locks": ["restore:2"]}})
        assert _manager(tmp_path).live_lock_names() == {"snap-b"}

    def test_unlock_drops_one_holder_and_leaves_the_other(self, tmp_path):
        """Clearing one session must not unpin a snapshot another still holds."""
        manager = _manager(tmp_path)
        manager.acquire_shared("snap-a", "restore:1")
        manager.acquire_shared("snap-a", "restore:2")

        write_persisted_locks(_manager(tmp_path), {"a": {"locks": ["restore:2"]}})
        remaining = read_persisted_locks(_manager(tmp_path))
        assert remaining == {"a": {"locks": ["restore:2"]}}

    def test_unlock_does_not_disturb_a_running_operations_target_lock(self, tmp_path):
        """--unlock clears leftover snapshot pins; it must not yank the lock a
        prune is holding right now, which is not a snapshot pin at all."""
        manager = _manager(tmp_path)
        manager.acquire_persistent("target", "prune")  # exclusive, not a pin
        manager.acquire_shared("snap-a", "restore:1")

        write_persisted_locks(_manager(tmp_path), {})
        assert _manager(tmp_path).live_lock_names() == {"target"}


class TestReleaseIsNotRecursive:
    def test_an_unexpected_file_is_left_alone_rather_than_deleted(self, tmp_path):
        """release() is rmdir, never rm -rf: if something unexpected is inside
        the lock directory, it must fail and leave it rather than delete whatever
        it happens to find."""
        manager = _manager(tmp_path)
        manager.acquire_once("target", "prune")
        stray = (
            tmp_path
            / LOCK_DIR_NAME
            / f"{encode_name('target')}.lock"
            / "somebodys-data"
        )
        stray.write_text("do not delete me")

        manager.release("target")
        assert stray.exists(), "release deleted a file it did not create"
