"""A remote command that fails with an AbortError is not attempted again.

`_exec_remote_command_with_retry` retried every exception, so a sudo password
refused for its content was tried twice per command and logged "Command
execution failed on attempt 1, retrying" each time -- 12 such warnings in
one measured run against a real remote -- before it failed anyway. An AbortError
is a decision to stop; a dropped connection is still retried.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


@pytest.fixture
def endpoint(monkeypatch):
    monkeypatch.setattr(SSHEndpoint, "_sudo_password_cache", {})
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"path": "/remote/dest", "username": "u", "ssh_sudo": True}
    ep.hostname = "nas"
    ep.ssh_manager = SimpleNamespace(_cached_ssh_password=None)
    ep._cached_sudo_password = None
    ep._build_remote_command = lambda command: ["sudo", "-S", *command]  # type: ignore[method-assign]
    return ep


def _count_attempts(ep):
    """Wrap the real single attempt so the retries can be counted."""
    attempts: list[int] = []
    real = ep._exec_remote_command

    def counted(command, **kwargs):
        attempts.append(1)
        return real(command, **kwargs)

    ep._exec_remote_command = counted
    return attempts


def test_a_refused_password_is_attempted_once(endpoint, monkeypatch, shared_log):
    monkeypatch.setenv("BTRFS_BACKUP_SUDO_PASSWORD", "ab\ncd")
    attempts = _count_attempts(endpoint)

    with pytest.raises(__util__.AbortError, match="contains a line break"):
        endpoint._exec_remote_command_with_retry(
            ["btrfs", "subvolume", "list", "/remote/dest"], max_retries=2
        )

    assert len(attempts) == 1
    assert not any("retrying" in m for m in shared_log.messages(logging.WARNING))


def test_a_transient_failure_is_still_retried(endpoint):
    """Control: the retry the change must not take away."""
    attempts: list[int] = []

    def dropped(command, **kwargs):
        attempts.append(1)
        raise RuntimeError("connection dropped")

    endpoint._exec_remote_command = dropped
    with pytest.raises(RuntimeError):
        endpoint._exec_remote_command_with_retry(["btrfs", "x"], max_retries=2)
    assert len(attempts) == 3
