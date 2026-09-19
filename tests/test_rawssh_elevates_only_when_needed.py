"""ssh_sudo on a raw+ssh target must mean "elevate if needed", not "always".

A raw+ssh target is a FILE store: no btrfs command ever runs on the remote. The
endpoint nonetheless elevated every remote command, so setting ssh_sudo -- a
documented target option -- made a valid config fail against the btrfs-only
sudoers policy the README tells people to install. It failed first at
`sudo mkdir -p` in _prepare, and behind that at find/cat/stat/mv/rm.
"""

import subprocess

from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

CONFIG = {"path": "/backup/raw", "hostname": "host.invalid", "username": "u"}


def _endpoint(monkeypatch, *, ssh_sudo, probe_rc, record=None):
    endpoint = SSHRawEndpoint(config=dict(CONFIG, ssh_sudo=ssh_sudo))

    def fake_run(cmd, *a, **kw):
        if record is not None:
            record.append(cmd[-1])
        return subprocess.CompletedProcess(cmd, probe_rc, b"", b"")

    monkeypatch.setattr(subprocess, "run", fake_run)
    return endpoint


def test_no_ssh_sudo_never_elevates_and_never_probes(monkeypatch):
    calls = []
    endpoint = _endpoint(monkeypatch, ssh_sudo=False, probe_rc=0, record=calls)

    assert endpoint._should_elevate is False
    assert calls == [], "probed the remote despite ssh_sudo being off"


def test_a_target_the_user_owns_is_not_elevated(monkeypatch):
    """The case the btrfs-only sudoers policy used to break."""
    endpoint = _endpoint(monkeypatch, ssh_sudo=True, probe_rc=0)

    assert endpoint._should_elevate is False


def test_a_target_the_user_cannot_use_still_elevates(monkeypatch):
    """ssh_sudo remains a real capability for a root-owned destination."""
    endpoint = _endpoint(monkeypatch, ssh_sudo=True, probe_rc=1)

    assert endpoint._should_elevate is True


def test_the_probe_runs_once_per_endpoint(monkeypatch):
    calls = []
    endpoint = _endpoint(monkeypatch, ssh_sudo=True, probe_rc=0, record=calls)

    for _ in range(5):
        endpoint._should_elevate

    assert len(calls) == 1, f"probed {len(calls)} times; it must be decided once"


def test_the_probe_creates_nothing(monkeypatch):
    """It runs on read-only commands too: `raw verify` against a mistyped path
    must not answer the question by bringing the directory into existence."""
    calls = []
    endpoint = _endpoint(monkeypatch, ssh_sudo=True, probe_rc=0, record=calls)

    endpoint._should_elevate

    assert calls, "no probe ran"
    for command in calls:
        for destructive in ("mkdir", "mktemp", "touch", "rm ", "install"):
            assert destructive not in command, (
                f"the probe has a side effect: {command!r}"
            )


def test_a_direct_listing_is_not_reported_as_never_run(monkeypatch):
    """The guard proves elevation by a sentinel that _elevate_shell adds ONLY
    when it elevates. Telling it "elevated" while running direct made it hunt a
    sentinel nobody emitted and call a successful find "never run"."""
    endpoint = SSHRawEndpoint(config=dict(CONFIG, ssh_sudo=True))
    endpoint._file_ops_direct = True  # already probed: the user owns the target

    def fake_run(cmd, *a, **kw):
        # A clean, unelevated find: rc=0, nothing on stderr, no sentinel.
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert endpoint.list_snapshots() == []
