"""R12b -- configurable ssh_host_key_policy (accept-new | strict) across all transports.

Covers: config validation (fail-closed on invalid), and that each of the three transports
(subprocess master, ssh:// endpoint, raw+ssh) emits the correct policy for each
setting. The endpoint test also guards the config whitelist (drop the key there ->
strict silently degrades -> the test fails).
"""

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


# ------------------------------------ ssh:// endpoint + config whitelist


def _endpoint_policy_cmd(policy):
    """Build a real ssh:// endpoint through choose_endpoint and return the ssh
    options its master manager would use.

    This goes through the endpoint's config whitelist, which the tests above do
    not: they construct SSHMasterManager directly. Dropping ssh_host_key_policy
    from that whitelist makes `strict` degrade to accept-new in silence -- a
    weakened MITM mitigation with nothing to announce it. The paramiko test used
    to be what caught that; it went with the transport, so the guard is restored
    here against the path that remains.
    """
    from btrfs_backup_ng.endpoint import choose_endpoint

    cfg = {"path": "ssh://u@h/p", "snap_prefix": ""}
    if policy is not None:
        cfg["ssh_host_key_policy"] = policy
    endpoint = choose_endpoint("ssh://u@h/p", cfg)
    return endpoint.ssh_manager._ssh_base_cmd()


def test_endpoint_strict_survives_the_config_whitelist():
    cmd = _endpoint_policy_cmd("strict")
    assert "StrictHostKeyChecking=yes" in cmd, (
        "strict did not reach the ssh command; the endpoint config whitelist "
        "may have dropped ssh_host_key_policy"
    )
    assert "StrictHostKeyChecking=accept-new" not in cmd


def test_endpoint_accept_new_survives_the_config_whitelist():
    cmd = _endpoint_policy_cmd("accept-new")
    assert "StrictHostKeyChecking=accept-new" in cmd
    assert "StrictHostKeyChecking=yes" not in cmd


def test_endpoint_defaults_to_accept_new():
    """Fail-safe, not fail-open: no policy configured must not mean no checking."""
    cmd = _endpoint_policy_cmd(None)
    assert "StrictHostKeyChecking=accept-new" in cmd
    assert "StrictHostKeyChecking=no" not in cmd


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


# ------------------------------------ threading parity guard (silent-degrade regression)


def test_every_handler_threading_ssh_sudo_also_threads_host_key_policy():
    """Silent-degrade guard: any CLI handler that threads a target's ssh_sudo into endpoint
    config MUST also thread ssh_host_key_policy -- else a config'd `strict` is silently
    ignored on that command (the R12b review found exactly this gap on `run`). Scans the
    cli/ handlers; if one reads target.ssh_sudo it must also read target.ssh_host_key_policy."""
    import pathlib

    cli_dir = pathlib.Path("src/btrfs_backup_ng/cli")
    offenders = []
    for f in cli_dir.glob("*.py"):
        text = f.read_text()
        # A handler does CONFIG-driven ssh threading if it assigns/branches on a target's
        # ssh_sudo (not merely displays it). Such a handler must also thread the policy from
        # the target. (Arg-driven handlers like restore thread it from args instead.)
        threads_config_ssh = (
            "= target.ssh_sudo" in text or "if target.ssh_sudo:" in text
        )
        if threads_config_ssh and "target.ssh_host_key_policy" not in text:
            offenders.append(f.name)
    assert offenders == [], (
        f"these handlers thread target.ssh_sudo but not ssh_host_key_policy "
        f"(strict would silently degrade): {offenders}"
    )


# ------------------------------------ R12d: snapper subcommands get the flag (R12b deferral)


@pytest.mark.parametrize(
    "argv",
    [
        ["snapper", "backup", "--ssh-host-key-policy", "strict", "src", "ssh://h/p"],
        ["snapper", "restore", "--ssh-host-key-policy", "strict", "ssh://h/p", "/dest"],
    ],
)
def test_snapper_subcommands_accept_ssh_host_key_policy(argv):
    from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

    ns = create_subcommand_parser().parse_args(argv)
    assert ns.ssh_host_key_policy == "strict"
