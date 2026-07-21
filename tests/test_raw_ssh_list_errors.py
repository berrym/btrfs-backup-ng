"""raw+ssh listing must not report an unreachable host as an empty target (T9).

A connection/auth/DNS failure to a raw+ssh target must FAIL loudly, not silently
return "0 snapshots" / "all ok" -- otherwise a down server (or lost backups) reads
as an intentionally-empty target, a dangerous false all-clear for a backup tool.
"""

from __future__ import annotations

import subprocess

import pytest

import btrfs_backup_ng.endpoint.raw as raw_mod
from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint, _check_remote_listing


def _cp(rc, stderr=""):
    return subprocess.CompletedProcess(
        args=["ssh"], returncode=rc, stdout="", stderr=stderr
    )


# --- the guard helper --------------------------------------------------------


def test_guard_raises_on_ssh_connection_failure():
    with pytest.raises(RuntimeError, match="Cannot reach|NOT an empty"):
        _check_remote_listing(
            _cp(255, "ssh: connect ... Connection refused"), "nas", "/b"
        )


def test_guard_noop_on_success():
    _check_remote_listing(_cp(0), "nas", "/b")  # reachable + ran -> no raise


def test_guard_warns_but_does_not_raise_on_other_nonzero(monkeypatch):
    """A reachable host whose listing command failed for another reason (e.g. a
    missing dir) is logged, not raised -- and never silently swallowed."""
    calls = []
    monkeypatch.setattr(raw_mod.logger, "warning", lambda *a, **k: calls.append(a))
    _check_remote_listing(_cp(1, ""), "nas", "/b")  # must not raise
    assert calls  # a warning was emitted (not silently swallowed)


# --- endpoint behaviour ------------------------------------------------------


def test_list_snapshots_unreachable_host_raises(monkeypatch):
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    monkeypatch.setattr(
        raw_mod.subprocess,
        "run",
        lambda *a, **k: _cp(255, "ssh: connect to host nas port 22: No route to host"),
    )
    with pytest.raises(RuntimeError, match="Cannot reach"):
        ep.list_snapshots(flush_cache=True)


def test_list_snapshots_reachable_empty_target_returns_empty(monkeypatch):
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    monkeypatch.setattr(raw_mod.subprocess, "run", lambda *a, **k: _cp(0, ""))
    assert ep.list_snapshots(flush_cache=True) == []


def test_ssh_command_fails_fast_on_a_down_host():
    """The ssh command must carry BatchMode + ConnectTimeout so a down/black-holing
    host errors quickly (ssh exit 255) instead of hanging past the OS TCP timeout --
    otherwise the 'never report a down server as empty' guard is never even reached."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    cmd = ep._build_ssh_command()
    assert "BatchMode=yes" in cmd
    assert "ConnectTimeout=10" in cmd
    assert "ServerAliveInterval=5" in cmd


def test_streams_without_sidecar_unreachable_raises(monkeypatch):
    """The backfill enumeration path must also refuse to treat an unreachable host as
    'no legacy streams'."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    monkeypatch.setattr(
        raw_mod.subprocess, "run", lambda *a, **k: _cp(255, "Connection refused")
    )
    with pytest.raises(RuntimeError, match="Cannot reach"):
        ep.streams_without_sidecar()
