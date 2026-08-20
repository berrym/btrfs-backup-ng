"""Permission advice must match the endpoint that actually failed.

`_log_subprocess_error` decided whether to print SSH-specific guidance with

    ... and hasattr(destination_endpoint, "_is_remote")

using the attribute's PRESENCE as a stand-in for "this endpoint is remote". That
worked only while `_is_remote` was set exclusively by the ssh endpoints. Once
Endpoint declared it -- which is what lets a type checker see it at all --
hasattr became true for every endpoint, and a LOCAL backup hitting a permission
error would be told to pass --ssh-sudo and edit a remote sudoers file.

Harmless to the data, but it is wrong guidance delivered at the exact moment
someone is debugging a failure, and it was introduced by a change whose whole
point was that the attribute should be declared rather than sniffed.

The whole suite stayed green with the bug present, which is why these exist.
"""

from __future__ import annotations

import logging
import subprocess

import pytest

from btrfs_backup_ng.core import operations

SSH_ADVICE = "--ssh-sudo"
SUDOERS_ADVICE = "NOPASSWD"


def _permission_error():
    err = subprocess.CalledProcessError(1, ["btrfs", "receive"])
    err.stderr = b"ERROR: cannot open directory: Permission denied"
    err.stdout = b""
    return err


def _local_endpoint():
    """A real Endpoint: _is_remote is declared False on the base."""
    from btrfs_backup_ng.endpoint.common import Endpoint

    endpoint = Endpoint.__new__(Endpoint)
    endpoint.config = {"path": "/mnt/backup"}
    return endpoint


def _remote_endpoint():
    endpoint = _local_endpoint()
    endpoint._is_remote = True
    return endpoint


class TestLocalFailuresDoNotGetSshAdvice:
    def test_no_ssh_sudo_suggestion_for_a_local_destination(self, caplog):
        with caplog.at_level(logging.ERROR):
            operations._log_subprocess_error(_permission_error(), _local_endpoint())
        text = caplog.text
        assert SSH_ADVICE not in text, (
            "a local backup was told to use --ssh-sudo:\n" + text
        )
        assert SUDOERS_ADVICE not in text, (
            "a local backup was told to edit a REMOTE sudoers file:\n" + text
        )

    def test_the_underlying_error_is_still_reported(self, caplog):
        """Guard against over-correcting into silence: the operator still needs
        to see what actually went wrong."""
        with caplog.at_level(logging.ERROR):
            operations._log_subprocess_error(_permission_error(), _local_endpoint())
        assert "Permission denied" in caplog.text


class TestRemoteFailuresStillGetSshAdvice:
    def test_ssh_advice_is_given_for_a_remote_destination(self, caplog):
        with caplog.at_level(logging.ERROR):
            operations._log_subprocess_error(_permission_error(), _remote_endpoint())
        text = caplog.text
        assert SSH_ADVICE in text, "a remote permission failure lost its guidance"
        assert SUDOERS_ADVICE in text


class TestPresenceIsNeverUsedAsAProxyForValue:
    """The root cause, pinned directly: `_is_remote` is declared on the base, so
    testing hasattr() tells you nothing. Any site that checks presence without
    the value is asking a question that now always answers yes."""

    def test_is_remote_is_declared_on_the_base_endpoint(self):
        from btrfs_backup_ng.endpoint.common import Endpoint

        assert Endpoint._is_remote is False
        assert hasattr(Endpoint, "_is_remote")

    @pytest.mark.parametrize(
        "cls_name",
        ["LocalEndpoint", "SSHEndpoint", "RawEndpoint", "SSHRawEndpoint"],
    )
    def test_hasattr_is_true_everywhere_so_it_cannot_discriminate(self, cls_name):
        import btrfs_backup_ng.endpoint.local as local_mod
        import btrfs_backup_ng.endpoint.raw as raw_mod
        import btrfs_backup_ng.endpoint.ssh as ssh_mod

        classes = {
            "LocalEndpoint": local_mod.LocalEndpoint,
            "SSHEndpoint": ssh_mod.SSHEndpoint,
            "RawEndpoint": raw_mod.RawEndpoint,
            "SSHRawEndpoint": raw_mod.SSHRawEndpoint,
        }
        assert hasattr(classes[cls_name], "_is_remote"), (
            "if this ever fails, presence-based checks would start discriminating "
            "again and the trap returns"
        )
