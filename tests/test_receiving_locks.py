"""Two transfers may not create the same subvolume on one destination.

Measured against a real remote before this was designed:

* two ``btrfs receive`` runs into one directory under DIFFERENT names both
  succeed -- so serialising a whole target would cost throughput to prevent a
  clash that cannot happen;
* two under the SAME name leave one failing with
  ``ERROR: creating subvolume ... failed: File exists`` -- after it has
  transferred the entire snapshot.

So the lock is scoped to the destination subvolume, and its value is timing and
attribution: the clash is refused before the stream starts, by a message that
names the holder, across machines.
"""

from __future__ import annotations

import subprocess
import threading
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
from btrfs_backup_ng.sshutil.lock import RECEIVING_LOCK_PREFIX, RemoteLockManager


def _manager(sandbox: Path, **kw) -> RemoteLockManager:
    def run(script: str):
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return RemoteLockManager(run, str(sandbox), hostname="testhost", **kw)


def _endpoint(sandbox: Path, **config):
    endpoint = SSHEndpoint.__new__(SSHEndpoint)
    endpoint.config = {"path": str(sandbox), **config}
    shared = _manager(sandbox)
    endpoint._build_lock_manager = lambda: shared
    return endpoint


class TestDifferentDestinationsRunInParallel:
    """The property that makes this a per-destination lock and not a target one."""

    def test_two_different_subvolumes_are_not_serialised(self, tmp_path):
        first, second = _endpoint(tmp_path), _endpoint(tmp_path)
        with first.receiving_lock(f"{tmp_path}/home.20240101T120000"):
            with second.receiving_lock(f"{tmp_path}/var.20240101T120000"):
                pass  # must not block or raise

    def test_many_different_subvolumes_at_once(self, tmp_path):
        """/ , /home, /var, /opt to one destination is an ordinary setup."""
        held: list = []
        errors: list = []
        barrier = threading.Barrier(4)

        def transfer(name: str) -> None:
            try:
                barrier.wait()
                endpoint = _endpoint(tmp_path)
                with endpoint.receiving_lock(f"{tmp_path}/{name}"):
                    held.append(name)
            except Exception as exc:  # noqa: BLE001 - recorded and asserted on
                errors.append((name, exc))

        threads = [
            threading.Thread(target=transfer, args=(n,))
            for n in ("root", "home", "var", "opt")
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, f"a distinct destination was blocked: {errors}"
        assert sorted(held) == ["home", "opt", "root", "var"]


class TestTheSameDestinationIsRefused:
    def test_a_second_transfer_to_one_path_is_refused(self, tmp_path):
        first, second = _endpoint(tmp_path), _endpoint(tmp_path)
        destination = f"{tmp_path}/home.20240101T120000"
        with first.receiving_lock(destination):
            with pytest.raises(__util__.AbortError, match="Already being received"):
                with second.receiving_lock(destination):
                    pass

    def test_the_refusal_names_the_holder(self, tmp_path):
        """An operator has to be able to find the transfer blocking them."""
        first, second = _endpoint(tmp_path), _endpoint(tmp_path)
        destination = f"{tmp_path}/home.20240101T120000"
        with first.receiving_lock(destination):
            with pytest.raises(__util__.AbortError) as caught:
                with second.receiving_lock(destination):
                    pass
        message = str(caught.value)
        assert "testhost" in message, "the holder's host is not named"
        assert str(__import__("os").getpid()) in message, "the pid is not named"
        assert destination in message, "the destination is not named"
        first_line = message.splitlines()[0]
        assert len(first_line) < 200, f"unreadably long: {first_line}"
        assert first_line.count(destination) == 1, (
            f"the destination is repeated: {first_line}"
        )

    def test_the_path_is_free_again_afterwards(self, tmp_path):
        endpoint = _endpoint(tmp_path)
        destination = f"{tmp_path}/home.20240101T120000"
        with endpoint.receiving_lock(destination):
            pass
        with _endpoint(tmp_path).receiving_lock(destination):
            pass  # must not raise

    def test_a_failed_transfer_still_frees_the_path(self, tmp_path):
        endpoint = _endpoint(tmp_path)
        destination = f"{tmp_path}/home.20240101T120000"
        with pytest.raises(RuntimeError):
            with endpoint.receiving_lock(destination):
                raise RuntimeError("transfer blew up")
        with _endpoint(tmp_path).receiving_lock(destination):
            pass  # the lock did not outlive the failure


class TestItIsReentrantWithinOneProcess:
    """The snapper flow holds this across receive AND publish."""

    def test_the_same_process_may_take_it_twice(self, tmp_path):
        endpoint = _endpoint(tmp_path)
        destination = f"{tmp_path}/.snapshots/42.incoming/snapshot"
        with endpoint.receiving_lock(destination):
            with endpoint.receiving_lock(destination):
                pass

    def test_the_inner_scope_does_not_release_it_early(self, tmp_path):
        """If it did, a second machine could publish between this receive and
        this rename -- the window the transaction exists to close."""
        endpoint = _endpoint(tmp_path)
        destination = f"{tmp_path}/.snapshots/42.incoming/snapshot"
        with endpoint.receiving_lock(destination):
            with endpoint.receiving_lock(destination):
                pass
            other = _endpoint(tmp_path)
            with pytest.raises(__util__.AbortError):
                with other.receiving_lock(destination):
                    pass


class TestTheLockRootDoesNotFollowTheTransfer:
    def test_it_stays_on_the_target_when_the_path_is_repointed(self, tmp_path):
        """The snapper flow points the endpoint at `.snapshots/<n>.incoming` for
        the duration of a receive. A lock root that followed would be written
        INSIDE the slot being published, then renamed into place with it."""
        endpoint = _endpoint(tmp_path)
        endpoint.config["lock_root"] = str(tmp_path)
        incoming = tmp_path / ".snapshots" / "42.incoming"
        incoming.mkdir(parents=True)
        endpoint.config["path"] = str(incoming)

        assert endpoint._lock_target_path() == str(tmp_path)
        with endpoint.receiving_lock(f"{incoming}/snapshot"):
            assert not (incoming / ".btrfs-backup-ng.locks").exists(), (
                "the lock was written inside the slot about to be published"
            )
            assert (tmp_path / ".btrfs-backup-ng.locks").is_dir()

    def test_the_pin_is_removed_afterwards(self, tmp_path):
        """An endpoint that outlives the transfer and is later pointed at a
        different target must not keep writing locks to the old one."""
        endpoint = _endpoint(tmp_path)
        with operations._receiving_lock(
            endpoint, f"{tmp_path}/x", lock_root=str(tmp_path)
        ):
            assert endpoint.config["lock_root"] == str(tmp_path)
        assert "lock_root" not in endpoint.config

    def test_an_existing_pin_is_restored_not_clobbered(self, tmp_path):
        endpoint = _endpoint(tmp_path)
        endpoint.config["lock_root"] = "/original"
        with operations._receiving_lock(
            endpoint, f"{tmp_path}/x", lock_root=str(tmp_path)
        ):
            pass
        assert endpoint.config["lock_root"] == "/original"


class TestTheDestinationIsTheReceivedSubvolume:
    def test_it_is_named_after_the_source_basename(self, tmp_path):
        """`btrfs receive` names the subvolume after the SOURCE basename, not
        after the snapshot name. For snapper that is always "snapshot"."""
        endpoint = _endpoint(tmp_path)
        endpoint._normalize_path = lambda p: str(p)
        assert (
            endpoint._receive_destination("/mnt/@home/.snapshots/42/snapshot")
            == f"{tmp_path}/snapshot"
        )
        assert (
            endpoint._receive_destination("/mnt/snapshots/home.20240101T120000")
            == f"{tmp_path}/home.20240101T120000"
        )

    def test_two_snapper_slots_do_not_collide(self, tmp_path):
        """Both are named "snapshot"; the SLOT is what separates them, so the
        lock has to be keyed on the incoming slot rather than the basename."""
        first, second = _endpoint(tmp_path), _endpoint(tmp_path)
        with first.receiving_lock(f"{tmp_path}/.snapshots/41.incoming/snapshot"):
            with second.receiving_lock(f"{tmp_path}/.snapshots/42.incoming/snapshot"):
                pass


class TestALocalTargetIsUnaffected:
    def test_a_local_endpoint_is_a_no_op(self, tmp_path):
        """Only remote endpoints record locks; a local one has no
        cross-machine contention and no receiving_lock to call."""

        class Local:
            config: dict = {"path": "/backup"}

        with operations._receiving_lock(Local(), "/backup/x"):
            pass


class TestTheOperatorCanOptOut:
    def test_skip_remote_lock_lets_an_unlockable_target_through(self, tmp_path):
        readonly = tmp_path / "readonly"
        readonly.mkdir()
        import os

        os.chmod(readonly, 0o500)
        try:
            endpoint = _endpoint(readonly, skip_remote_lock=True)
            endpoint._build_lock_manager = lambda: _manager(readonly)
            with endpoint.receiving_lock(f"{readonly}/home"):
                pass  # must not raise
        finally:
            os.chmod(readonly, 0o700)

    def test_without_it_an_unlockable_target_stops_the_transfer(self, tmp_path):
        readonly = tmp_path / "readonly"
        readonly.mkdir()
        import os

        os.chmod(readonly, 0o500)
        try:
            endpoint = _endpoint(readonly)
            endpoint._build_lock_manager = lambda: _manager(readonly)
            with pytest.raises(__util__.AbortError, match="skip-remote-lock"):
                with endpoint.receiving_lock(f"{readonly}/home"):
                    pass
        finally:
            os.chmod(readonly, 0o700)


class TestTheLockNameIsDistinctFromASnapshotPin:
    def test_receiving_and_pinning_do_not_share_a_name(self, tmp_path):
        """A pin on a snapshot and the right to create it are different rights;
        sharing a name would make one silently satisfy the other."""
        endpoint = _endpoint(tmp_path)
        with endpoint.receiving_lock(f"{tmp_path}/home.20240101T120000"):
            names = _manager(tmp_path).live_lock_names()
        assert any(n.startswith(RECEIVING_LOCK_PREFIX) for n in names)
        assert not any(n.startswith("snap-") for n in names)
