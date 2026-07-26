"""R12b -- configurable ssh_host_key_policy (accept-new | strict) across all transports.

Covers: config validation (fail-closed on invalid), and that each of the three transports
(subprocess master, paramiko, raw+ssh) emits the correct policy for each setting. The
paramiko test also guards the endpoint config-restore whitelist (drop the key there ->
strict silently degrades -> the test fails).
"""

from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.config.loader import ConfigError, _parse_target
from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint
from btrfs_backup_ng.sshutil.master import SSHMasterManager


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("SUDO_USER", raising=False)


# ------------------------------------ config validation (fail closed on invalid)


def test_loader_defaults_to_accept_new():
    assert _parse_target({"path": "ssh://h/p"}).ssh_host_key_policy == "accept-new"


def test_loader_accepts_strict():
    tc = _parse_target({"path": "ssh://h/p", "ssh_host_key_policy": "strict"})
    assert tc.ssh_host_key_policy == "strict"


def test_loader_rejects_invalid_policy_fail_closed():
    """A security selector must fail closed on an unrecognized value, never silently fall
    back to a (possibly weaker) default."""
    with pytest.raises(ConfigError):
        _parse_target({"path": "ssh://h/p", "ssh_host_key_policy": "off"})


# ------------------------------------ subprocess transport (master.py)


def _mgr(policy):
    return SSHMasterManager(hostname="h", username="u", host_key_policy=policy)


def test_master_strict_emits_yes():
    cmd = _mgr("strict")._ssh_base_cmd()
    assert "StrictHostKeyChecking=yes" in cmd
    assert "StrictHostKeyChecking=accept-new" not in cmd


def test_master_accept_new_emits_accept_new():
    cmd = _mgr("accept-new")._ssh_base_cmd()
    assert "StrictHostKeyChecking=accept-new" in cmd
    assert "StrictHostKeyChecking=yes" not in cmd


# ------------------------------------ paramiko transport (ssh.py) + config whitelist


def _paramiko_policy_used(policy, monkeypatch):
    from btrfs_backup_ng.endpoint import choose_endpoint
    import btrfs_backup_ng.endpoint.ssh as ssh_mod

    cfg = {"path": "ssh://u@h/p", "snap_prefix": ""}
    if policy is not None:
        cfg["ssh_host_key_policy"] = policy
    ep = choose_endpoint("ssh://u@h/p", cfg)
    fake = MagicMock()
    monkeypatch.setattr(ssh_mod, "paramiko", fake)
    ep._new_verified_paramiko_client()
    return fake


def test_paramiko_strict_uses_reject_policy(monkeypatch):
    """strict -> RejectPolicy (unknown host refused). Also guards the ssh.py config
    whitelist: drop 'ssh_host_key_policy' there and the endpoint never sees strict."""
    fake = _paramiko_policy_used("strict", monkeypatch)
    assert fake.RejectPolicy.called
    assert not fake.AutoAddPolicy.called


def test_paramiko_accept_new_uses_autoadd(monkeypatch):
    fake = _paramiko_policy_used("accept-new", monkeypatch)
    assert fake.AutoAddPolicy.called
    assert not fake.RejectPolicy.called


def test_paramiko_default_is_accept_new(monkeypatch):
    fake = _paramiko_policy_used(None, monkeypatch)
    assert fake.AutoAddPolicy.called
    assert not fake.RejectPolicy.called


# ------------------------------------ raw+ssh transport (raw.py)


def _raw_cmd(policy):
    cfg = {"path": "/backup", "hostname": "nas"}
    if policy is not None:
        cfg["ssh_host_key_policy"] = policy
    return SSHRawEndpoint(config=cfg)._build_ssh_command()


def test_raw_strict_emits_yes():
    assert "StrictHostKeyChecking=yes" in _raw_cmd("strict")


def test_raw_accept_new_emits_accept_new():
    assert "StrictHostKeyChecking=accept-new" in _raw_cmd("accept-new")


def test_raw_default_is_accept_new():
    assert "StrictHostKeyChecking=accept-new" in _raw_cmd(None)
