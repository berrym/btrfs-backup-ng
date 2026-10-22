"""The lock store is never written through a symbolic link.

The store sits at a predictable path beside the backups it protects, and
the account writing it is often root. Before this, the scripts that record a
lock created their directories with ``mkdir -p`` and wrote their records
with ``>``, both of which follow a symbolic link: one planted at the store's
name, at a lock directory under it or at a holder's record made the lock
tree get built wherever the link pointed -- the lock silently defeated,
root-owned files created at a path the planter chose -- and the record write
truncate whatever file the link pointed at.

On this machine the three creating operations now run from Python against
descriptors opened ``O_NOFOLLOW`` (``LocalLockManager``), so the kernel
refuses the link and nothing can be swapped between check and use. On a
remote, where only POSIX sh is available, the emitted script creates one
component at a time with a plain ``mkdir``, refuses a link or a group- or
world-writable directory where it finds one, and writes under ``set -C``.
Both transports are driven here for real: the local one through
``local_manager``, the remote one by running the very scripts an ssh
endpoint would send, under each POSIX shell present, against a sandbox.
"""

from __future__ import annotations

import hashlib
import logging
import os
import stat
import subprocess
import time
from pathlib import Path

import pytest

from btrfs_backup_ng.sshutil.lock import (
    HOLDERS_DIR_NAME,
    LOCK_DIR_NAME,
    RemoteLockManager,
    RemoteLockUnavailable,
    csh_unsafe,
    encode_name,
    holders_beside,
    local_manager,
)

from .lockshell import shell_params

SNAPSHOT = "snap-home-20260101-000000"
PIN = "/mnt/backup/home-20260101-000000"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rig(tmp_path: Path) -> tuple[Path, Path, Path]:
    """A store, a directory outside it, and a file the planter wants
    truncated -- the shapes a local account that can write the store can
    set up."""
    store = tmp_path / "store"
    outside = tmp_path / "outside"
    victim = tmp_path / "elsewhere" / "existing_file"
    store.mkdir()
    outside.mkdir()
    victim.parent.mkdir()
    victim.write_text("do not truncate me\n" * 6)
    return store, outside, victim


def _lock_dir(store: Path, name: str = SNAPSHOT) -> Path:
    return store / LOCK_DIR_NAME / f"{encode_name(name)}.lock"


def _holder(store: Path, name: str = SNAPSHOT, lock_id: str = PIN) -> Path:
    return _lock_dir(store, name) / HOLDERS_DIR_NAME / encode_name(lock_id)


def _only_the_link(store: Path, link: Path) -> None:
    """Nothing but the planted link exists under ``store``."""
    found = sorted(p for p in store.rglob("*"))
    assert found == [link], f"the store gained entries: {found}"
    assert link.is_symlink()


def _shell_manager(shell: str, target: Path) -> RemoteLockManager:
    """What an ssh endpoint runs on its remote: the same scripts, under
    ``shell``, with no elevation to fall back on."""

    def run(script: str):
        proc = subprocess.run([shell, "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return RemoteLockManager(run, str(target), hostname="remote")


# --------------------------------------------------------- local transport


class TestALinkAtAStoreComponentIsRefusedLocally:
    """Shape one: a link at a path component. ``mkdir -p`` built the lock
    tree on the far side of it; now the component is refused and nothing is
    created outside the store."""

    def test_a_shared_pin_through_a_linked_store_creates_nothing_outside(
        self, tmp_path
    ):
        store, outside, _ = _rig(tmp_path)
        link = store / LOCK_DIR_NAME
        link.symlink_to(outside)

        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            local_manager(store).acquire_shared(SNAPSHOT, PIN)

        assert list(outside.iterdir()) == []
        _only_the_link(store, link)

    def test_an_exclusive_lock_through_a_linked_store_creates_nothing_outside(
        self, tmp_path
    ):
        store, outside, _ = _rig(tmp_path)
        link = store / LOCK_DIR_NAME
        link.symlink_to(outside)

        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            local_manager(store).acquire_once("target", "prune")

        assert list(outside.iterdir()) == []
        _only_the_link(store, link)

    def test_a_link_at_the_lock_directory_is_refused(self, tmp_path):
        store, outside, _ = _rig(tmp_path)
        (store / LOCK_DIR_NAME).mkdir()
        _lock_dir(store).symlink_to(outside)

        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            local_manager(store).acquire_shared(SNAPSHOT, PIN)
        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            local_manager(store).acquire_once(SNAPSHOT, "receive")

        assert list(outside.iterdir()) == []

    def test_a_link_at_the_holders_directory_is_refused(self, tmp_path):
        store, outside, _ = _rig(tmp_path)
        _lock_dir(store).mkdir(parents=True)
        (_lock_dir(store) / HOLDERS_DIR_NAME).symlink_to(outside)

        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            local_manager(store).acquire_shared(SNAPSHOT, PIN)

        assert list(outside.iterdir()) == []

    def test_the_records_beside_a_lock_file_refuse_a_linked_directory(self, tmp_path):
        """The same machinery writes who-holds-what beside a lock FILE, in
        ``<file>.holders``; a link planted there is refused the same way."""
        store, outside, _ = _rig(tmp_path)
        lock_file = store / LOCK_DIR_NAME
        lock_file.write_text("{}")
        link = store / f"{LOCK_DIR_NAME}.holders"
        link.symlink_to(outside)

        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            holders_beside(lock_file).acquire_shared(SNAPSHOT, PIN)

        assert list(outside.iterdir()) == []
        assert link.is_symlink()


class TestALinkAtTheRecordIsRefusedLocally:
    """Shape two: a link at the record's own name. ``>`` opened whatever it
    pointed at for writing and truncated it; now the open is ``O_NOFOLLOW``
    and the file it pointed at is untouched, byte for byte."""

    def test_a_shared_record_that_is_a_link_does_not_truncate_its_target(
        self, tmp_path
    ):
        store, _, victim = _rig(tmp_path)
        _holder(store).parent.mkdir(parents=True)
        _holder(store).symlink_to(victim)
        before = _sha256(victim)

        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            local_manager(store).acquire_shared(SNAPSHOT, PIN)

        assert _sha256(victim) == before
        assert _holder(store).is_symlink()

    def test_a_fifo_where_the_record_belongs_is_refused_not_written(self, tmp_path):
        """A FIFO is not a link, and ``O_NOFOLLOW`` alone would open it --
        and block forever waiting for a reader. It is refused as not a
        regular file instead."""
        store, _, _ = _rig(tmp_path)
        _holder(store).parent.mkdir(parents=True)
        os.mkfifo(_holder(store))

        with pytest.raises(RemoteLockUnavailable, match="not a regular file"):
            local_manager(store).acquire_shared(SNAPSHOT, PIN)

        assert stat.S_ISFIFO(_holder(store).lstat().st_mode)


class TestAWritableStoreIsRefusedLocally:
    """Any account that can write a lock directory can plant a link in it,
    so a store its group or everyone may write is refused, loudly, with the
    directory and the fix named."""

    @pytest.mark.parametrize("mode", [0o775, 0o757, 0o777])
    def test_the_store_directory(self, tmp_path, mode):
        store, _, _ = _rig(tmp_path)
        (store / LOCK_DIR_NAME).mkdir(mode=0o755)
        (store / LOCK_DIR_NAME).chmod(mode)

        with pytest.raises(RemoteLockUnavailable) as caught:
            local_manager(store).acquire_shared(SNAPSHOT, PIN)
        assert str(store / LOCK_DIR_NAME) in str(caught.value)
        assert "group or by everyone" in str(caught.value)
        assert "chmod go-w" in str(caught.value)

        with pytest.raises(RemoteLockUnavailable, match="group or by everyone"):
            local_manager(store).acquire_once("target", "prune")
        assert not _lock_dir(store, "target").exists()

    def test_the_holders_directory(self, tmp_path):
        store, _, _ = _rig(tmp_path)
        holders = _holder(store).parent
        holders.mkdir(parents=True)
        holders.chmod(0o775)

        with pytest.raises(RemoteLockUnavailable) as caught:
            local_manager(store).acquire_shared(SNAPSHOT, PIN)
        assert str(holders) in str(caught.value)
        assert not _holder(store).exists()

    def test_a_refusal_is_never_answered_by_elevating(self, tmp_path, monkeypatch):
        """Elevation is for a store the account may not write. A store that
        is unsafe would be written as root by an elevated retry -- the very
        write that was refused."""
        store, outside, _ = _rig(tmp_path)
        (store / LOCK_DIR_NAME).symlink_to(outside)
        manager = local_manager(store)
        elevated: list[str] = []
        monkeypatch.setattr(
            manager, "_run_elevated", lambda s: elevated.append(s) or (0, "", "")
        )

        with pytest.raises(RemoteLockUnavailable):
            manager.acquire_shared(SNAPSHOT, PIN)
        with pytest.raises(RemoteLockUnavailable):
            manager.acquire_once("target", "prune")

        assert elevated == []

    def test_a_beat_on_a_store_turned_unsafe_warns_and_does_not_elevate(
        self, tmp_path, monkeypatch, caplog
    ):
        """The record of a held pin is replaced by a link while the pin is
        held. The beat, which rewrites the record, must refuse: the file the
        link points at keeps every byte, nothing is retried as root, and the
        refusal is said out loud rather than at debug level."""
        store, _, victim = _rig(tmp_path)
        caplog.set_level(logging.WARNING, logger="btrfs_backup_ng.sshutil.lock")
        manager = local_manager(store)
        manager._heartbeat_interval = 0.05
        elevated: list[str] = []
        monkeypatch.setattr(
            manager, "_run_elevated", lambda s: elevated.append(s) or (0, "", "")
        )
        manager.acquire_shared_persistent(SNAPSHOT, PIN)
        record = _holder(store)
        assert record.is_file()
        record.unlink()
        record.symlink_to(victim)
        before = _sha256(victim)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not caplog.records:
            time.sleep(0.02)
        manager.release_shared(SNAPSHOT, PIN)

        warning = caplog.records[0].getMessage()
        assert "not refreshed" in warning and "symbolic link" in warning
        assert _sha256(victim) == before
        assert elevated == []


class TestTheLocalStoreIsWrittenAsItShouldBe:
    """The ordinary case still works, and what is created is owner-writable
    only: the directories 0755 whatever the umask, the records 0600, as the
    ledger file beside them is."""

    def test_directories_and_records(self, tmp_path):
        store, _, _ = _rig(tmp_path)
        old = os.umask(0o000)
        try:
            manager = local_manager(store)
            manager.acquire_shared(SNAPSHOT, PIN)
            manager.acquire_once("target", "prune")
        finally:
            os.umask(old)

        for directory in (
            store / LOCK_DIR_NAME,
            _lock_dir(store, "target"),
            _holder(store).parent.parent,
            _holder(store).parent,
        ):
            assert stat.S_IMODE(directory.stat().st_mode) == 0o755, directory
        assert stat.S_IMODE(_holder(store).stat().st_mode) == 0o600
        (owner,) = _lock_dir(store, "target").glob("owner.*")
        assert stat.S_IMODE(owner.stat().st_mode) == 0o600
        assert manager.live_lock_names() == {SNAPSHOT, "target"}

        # The second acquire of a shared pin rewrites the record in place.
        manager.acquire_shared(SNAPSHOT, PIN)
        assert PIN in _holder(store).read_text()
        manager.release_shared(SNAPSHOT, PIN)
        manager.release("target")
        assert manager.live_lock_names() == set()


# -------------------------------------------------------- remote transport


class TestTheEmittedShellRefusesTheSameShapes:
    """What an ssh endpoint sends is POSIX sh, which has no ``O_NOFOLLOW``.
    The script creates one component at a time, refuses what it can see and
    writes under noclobber; run here under each shell present, against the
    same planted shapes, it must refuse and must not write through."""

    @pytest.mark.parametrize("shell", shell_params())
    def test_a_linked_store_component_is_refused_and_nothing_lands_outside(
        self, tmp_path, shell
    ):
        store, outside, _ = _rig(tmp_path)
        link = store / LOCK_DIR_NAME
        link.symlink_to(outside)
        manager = _shell_manager(shell, store)

        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            manager.acquire_shared(SNAPSHOT, PIN)
        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            manager.acquire_once("target", "prune")

        assert list(outside.iterdir()) == []
        _only_the_link(store, link)

    @pytest.mark.parametrize("shell", shell_params())
    def test_a_linked_lock_directory_is_refused_rather_than_judged(
        self, tmp_path, shell
    ):
        """``mkdir`` refuses to create through the link; the script must
        then refuse too, not judge -- and break -- whatever the link points
        at, which could be another target's lock."""
        store, outside, _ = _rig(tmp_path)
        (store / LOCK_DIR_NAME).mkdir()
        _lock_dir(store, "target").symlink_to(outside)
        (outside / "owner.deadbeef").write_text("{}")
        old = 1_000_000
        os.utime(outside / "owner.deadbeef", (old, old))
        manager = _shell_manager(shell, store)

        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            manager.acquire_once("target", "prune")

        assert (outside / "owner.deadbeef").exists(), "the far lock was broken"

    @pytest.mark.parametrize("shell", shell_params())
    def test_a_linked_record_does_not_truncate_its_target(self, tmp_path, shell):
        store, _, victim = _rig(tmp_path)
        _holder(store).parent.mkdir(parents=True)
        _holder(store).symlink_to(victim)
        before = _sha256(victim)

        with pytest.raises(RemoteLockUnavailable, match="symbolic link"):
            _shell_manager(shell, store).acquire_shared(SNAPSHOT, PIN)

        assert _sha256(victim) == before
        assert _holder(store).is_symlink()

    @pytest.mark.parametrize("shell", shell_params())
    def test_a_writable_store_is_refused(self, tmp_path, shell):
        store, _, _ = _rig(tmp_path)
        (store / LOCK_DIR_NAME).mkdir(mode=0o755)
        (store / LOCK_DIR_NAME).chmod(0o775)
        manager = _shell_manager(shell, store)

        with pytest.raises(RemoteLockUnavailable, match="chmod go-w"):
            manager.acquire_shared(SNAPSHOT, PIN)
        with pytest.raises(RemoteLockUnavailable, match="group or by everyone"):
            manager.acquire_once("target", "prune")

        assert list((store / LOCK_DIR_NAME).iterdir()) == []

    @pytest.mark.parametrize("shell", shell_params())
    def test_the_ordinary_case_still_works_and_rewrites_the_record(
        self, tmp_path, shell
    ):
        store, _, _ = _rig(tmp_path)
        manager = _shell_manager(shell, store)
        manager.acquire_shared(SNAPSHOT, PIN)
        first = _holder(store).read_text()
        manager.acquire_shared(SNAPSHOT, PIN)  # the beat runs this again
        assert _holder(store).read_text() == first
        assert manager.acquire_once("target", "prune") == "acquired"
        assert manager.live_lock_names() == {SNAPSHOT, "target"}
        for directory in (store / LOCK_DIR_NAME, _holder(store).parent):
            assert stat.S_IMODE(directory.stat().st_mode) == 0o755

    def test_the_scripts_say_what_they_do(self, tmp_path):
        """String-level, for the half that cannot be received here: every
        creating script runs under noclobber, creates one component at a
        time, refuses a writable directory and a planted record, and never
        ``mkdir -p``s. Still csh-safe and one line."""
        manager = _shell_manager("sh", tmp_path)
        manager._tokens["target"] = "tok"
        scripts = {
            "acquire": manager._acquire_script("target", "{}", "tok"),
            "shared": manager._shared_record_script(SNAPSHOT, PIN, "op"),
            "refresh": manager._refresh_script("target"),
        }
        for name, script in scripts.items():
            assert "mkdir -p" not in script, name
            assert "bbng_safe_leaf" in script or "[ -L " in script, name
            assert csh_unsafe(script) == [], name
            assert "\n" not in script
        for name in ("acquire", "shared"):
            assert scripts[name].startswith("set -C; "), name
            assert "bbng_tree" in scripts[name], name
            assert "bbng_safe_dir" in scripts[name], name
        assert "bbng_safe_leaf" in scripts["shared"]
        assert "bbng_safe_leaf" in scripts["refresh"]


class TestARecordIsNeverDisclosedThroughALinkOnRead:
    """A planted link at a holder or owner record must not make a listing
    read the file it points at into the scan's OUTPUT -- the raw text a
    ``restore --status`` or ``doctor`` listing is built from and a debug log
    can carry. Asserting the parsed holders would miss it: a secret that is
    not the record's own JSON parses as an unknown holder, so the leak hides
    one layer below. These drive the real scan and inspect what ``cat``
    produced.
    """

    @staticmethod
    def _scan_output(mgr: object) -> str:
        """Every byte the scan scripts emit, captured around ``_run``."""
        captured: list[str] = []
        real = mgr._run

        def spy(script: str):
            rc, out, err = real(script)
            captured.append(out)
            return rc, out, err

        mgr._run = spy  # type: ignore[assignment]
        mgr.shared_records()
        mgr.live_locks()
        mgr.is_locked("snap-y")
        return "".join(captured)

    def test_a_shared_record_that_is_a_link_is_not_read_into_the_output(
        self, tmp_path: Path
    ) -> None:
        store = tmp_path / "store"
        store.mkdir()
        (tmp_path / "secret").write_text("TOP-SECRET-ROOT-CONTENT\n")
        holders = (
            store / LOCK_DIR_NAME / f"{encode_name('snap-x')}.lock" / HOLDERS_DIR_NAME
        )
        holders.mkdir(parents=True)
        (holders / encode_name("id-x")).symlink_to(tmp_path / "secret")

        out = self._scan_output(local_manager(str(store)))
        assert "TOP-SECRET" not in out, (
            f"a symlinked holder record was read into the scan output: {out!r}"
        )

    def test_an_owner_record_that_is_a_link_is_not_read_into_the_output(
        self, tmp_path: Path
    ) -> None:
        store = tmp_path / "store"
        store.mkdir()
        (tmp_path / "secret").write_text("TOP-SECRET-OWNER-CONTENT\n")
        lock_dir = store / LOCK_DIR_NAME / f"{encode_name('snap-y')}.lock"
        lock_dir.mkdir(parents=True)
        (lock_dir / "owner.deadbeef").symlink_to(tmp_path / "secret")

        out = self._scan_output(local_manager(str(store)))
        assert "TOP-SECRET" not in out, (
            f"a symlinked owner record was read into the scan output: {out!r}"
        )


class TestTheRemoteBeatHasNoDeletionWindow:
    """The emitted shared-record script refreshes an existing record in place
    rather than removing and recreating it, so a listing by a prune can never
    fall in a window where a live holder's record is absent.
    """

    def test_the_emitted_script_refreshes_in_place_and_never_removes(self) -> None:
        mgr = RemoteLockManager(lambda s: (0, "", ""), "/backup", hostname="h")
        script = mgr._shared_record_script("snap-x", "id-x", "backup")
        holder = mgr._holder_file("snap-x", "id-x")
        assert "touch -c" in script, "the beat must refresh the record in place"
        assert f"rm -f {holder}" not in script and f"rm -f '{holder}'" not in script, (
            "the record must never be removed on a beat (reopens a deletion window)"
        )
