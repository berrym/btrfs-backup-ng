"""Remote privilege escalation must stay inside the documented sudoers policy.

The remote setup this project documents grants ``NOPASSWD: /usr/bin/btrfs`` and
nothing else. Elevating any other binary therefore makes sudo demand a password
on a non-interactive connection that cannot supply one -- the command fails, and
its caller cannot tell "sudo refused" apart from "the path is not there".

That is not hypothetical. ``delete_snapshots`` used ``sudo test -d`` to confirm a
snapshot existed before deleting it. Under a btrfs-only policy that check always
failed, every snapshot was skipped with a "does not exist" warning, and remote
pruning silently deleted nothing while reporting success.

Reproduced on a real host with exactly that policy::

    $ sudo -n test -d /home/mberry/btrfs-backup-test
    sudo: a password is required                       # rc=1
    $ sudo -n btrfs subvolume show /home/mberry/btrfs-backup-test/test-...
                                                       # rc=0

Mutation-verified: re-admitting ``test`` to the elevation predicate, or restoring
the ``test -d`` precondition in delete_snapshots, fails these tests.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

# Everything the codebase asks a remote host to do, privileged or not.
REMOTE_COMMANDS = [
    pytest.param(
        ["btrfs", "subvolume", "delete", "/mnt/b/snap"], True, id="btrfs-delete"
    ),
    pytest.param(["btrfs", "subvolume", "show", "/mnt/b/snap"], True, id="btrfs-show"),
    pytest.param(["btrfs", "receive", "/mnt/b"], True, id="btrfs-receive"),
    pytest.param(["btrfs", "--version"], True, id="btrfs-version"),
    pytest.param(["test", "-d", "/mnt/b"], False, id="test-d"),
    pytest.param(["test", "-e", "/mnt/b/snap"], False, id="test-e"),
    pytest.param(["mkdir", "-p", "/mnt/b"], False, id="mkdir"),
    pytest.param(["rm", "-rf", "/mnt/b/tmp"], False, id="rm"),
    pytest.param(["touch", "/mnt/b/x"], False, id="touch"),
    pytest.param(["cat", "/mnt/b/x"], False, id="cat"),
    pytest.param(["find", "/mnt/b", "-name", "x"], False, id="find"),
]


def _endpoint(ssh_sudo: bool = True) -> SSHEndpoint:
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"ssh_sudo": ssh_sudo, "passwordless": True}
    ep.hostname = "host"
    return ep


class TestOnlyBtrfsIsElevated:
    """The sudoers policy is ``btrfs`` only, so only btrfs may be elevated."""

    @pytest.mark.parametrize(("command", "expect_sudo"), REMOTE_COMMANDS)
    def test_elevation_matches_the_documented_policy(self, command, expect_sudo):
        ep = _endpoint(ssh_sudo=True)
        built = ep._build_remote_command(list(command))
        got_sudo = built[0] == "sudo"

        assert got_sudo is expect_sudo, (
            f"{command[0]!r} elevation mismatch: built {built!r}. Only /usr/bin/btrfs "
            "is granted by the documented sudoers policy; elevating anything else "
            "makes sudo demand a password the connection cannot supply."
        )
        if expect_sudo:
            # The binary sudo runs must be the one sudoers actually names.
            assert "btrfs" in built, built

    @pytest.mark.parametrize(("command", "_expect_sudo"), REMOTE_COMMANDS)
    def test_nothing_is_elevated_when_ssh_sudo_is_off(self, command, _expect_sudo):
        ep = _endpoint(ssh_sudo=False)
        assert ep._build_remote_command(list(command)) == list(command)


class TestRemoteDeleteUnderBtrfsOnlySudoers:
    """delete_snapshots must actually delete under a btrfs-only policy.

    The fake below is the whole point: it behaves like the real remote host,
    where sudo refuses any binary the policy does not name.
    """

    @staticmethod
    def _endpoint_with_policy():
        ep = _endpoint(ssh_sudo=True)
        ep._normalize_path = MagicMock(side_effect=lambda p: str(p))
        calls: list[list[str]] = []
        existing = {"/mnt/backup/snap-1"}
        deleted: list[str] = []

        def fake_exec(cmd, **kwargs):
            cmd = [str(c) for c in cmd]
            calls.append(cmd)
            built = ep._build_remote_command(cmd)

            # A btrfs-only sudoers policy: sudo refuses anything else.
            if built[0] == "sudo" and "btrfs" not in built:
                return SimpleNamespace(
                    returncode=1, stdout=b"", stderr=b"sudo: a password is required"
                )

            if cmd[:3] == ["btrfs", "subvolume", "show"]:
                rc = 0 if cmd[3] in existing else 1
                return SimpleNamespace(returncode=rc, stdout=b"", stderr=b"")
            if cmd[:3] == ["btrfs", "subvolume", "delete"]:
                if cmd[3] in existing:
                    existing.discard(cmd[3])
                    deleted.append(cmd[3])
                    return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
                return SimpleNamespace(
                    returncode=1, stdout=b"", stderr=b"No such file or directory"
                )
            if cmd[0] == "test":
                # Unprivileged `test` cannot see a root-owned snapshot directory.
                return SimpleNamespace(returncode=1, stdout=b"", stderr=b"")
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

        ep._exec_remote_command = MagicMock(side_effect=fake_exec)
        ep._exec_remote_command_with_retry = MagicMock(side_effect=fake_exec)
        ep._SSHEndpoint__cached_snapshots = None
        return ep, calls, deleted

    def test_snapshot_is_actually_deleted(self):
        ep, calls, deleted = self._endpoint_with_policy()
        snapshot = SimpleNamespace(get_path=lambda: "/mnt/backup/snap-1")

        ep.delete_snapshots([snapshot])

        assert deleted == ["/mnt/backup/snap-1"], (
            "the snapshot was never deleted; under a btrfs-only sudoers policy the "
            f"existence precondition must not need a non-btrfs binary. calls={calls}"
        )

    def test_precondition_uses_a_btrfs_subcommand(self):
        ep, calls, _deleted = self._endpoint_with_policy()
        snapshot = SimpleNamespace(get_path=lambda: "/mnt/backup/snap-1")

        ep.delete_snapshots([snapshot])

        assert not any(c[0] == "test" for c in calls), (
            f"a non-btrfs binary was used as the deletion precondition: {calls}"
        )
        assert ["btrfs", "subvolume", "show", "/mnt/backup/snap-1"] in calls, calls

    def test_missing_subvolume_is_skipped_not_deleted(self):
        """A genuinely absent path must still be skipped, not blindly deleted."""
        ep, calls, deleted = self._endpoint_with_policy()
        snapshot = SimpleNamespace(get_path=lambda: "/mnt/backup/does-not-exist")

        ep.delete_snapshots([snapshot])

        assert deleted == []
        assert ["btrfs", "subvolume", "show", "/mnt/backup/does-not-exist"] in calls


def test_receive_keeps_its_dedicated_sudo_form():
    """btrfs receive uses `sudo -n -P -p ''`; the change must not disturb it."""
    ep = _endpoint(ssh_sudo=True)
    built = ep._build_remote_command(["btrfs", "receive", "/mnt/backup"])
    assert built[:5] == ["sudo", "-n", "-P", "-p", ""], built
    assert built[5:] == ["btrfs", "receive", "/mnt/backup"], built


def test_password_mode_uses_sudo_s():
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"ssh_sudo": True, "passwordless": False}
    ep.hostname = "host"
    built = ep._build_remote_command(["btrfs", "subvolume", "list", "/mnt"])
    assert built[0] == "sudo" and built[1] == "-S", built
