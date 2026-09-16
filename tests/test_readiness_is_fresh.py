"""The gate that decides whether to move data must not read a stale verdict.

SSHEndpoint caches diagnostics for 300 seconds under "{hostname}:{path}". The
pre-transfer check logs "Verifying SSH connectivity and filesystem readiness" and
then read that cache, so the verdict could be five minutes old: the host could
have gone away, the destination been unmounted, or the remote filesystem gone
read-only, and the transfer would still start.

A force_refresh parameter existed and no caller in the tree passed it.

It is worse than a stale pass. The cached value is written back to
config["passwordless_sudo_available"], which _build_remote_command consults for
EVERY remote command, and the sudo branch is selected by the same boolean -- so a
stale True keeps the run on the direct path and skips the one route that checks
the transport is still alive.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


def _endpoint():
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"path": "/dest", "ssh_sudo": False}
    ep.hostname = "host"
    ep._diagnostics_cache = {}
    ep._diagnostics_cache_timeout = 300
    return ep


ALL_GOOD = {
    "ssh_connection": True,
    "btrfs_command": True,
    "passwordless_sudo": True,
    "write_permissions": True,
    "btrfs_filesystem": True,
}


class TestTheCacheItself:
    def test_a_fresh_entry_is_reused_without_force(self):
        ep = _endpoint()
        ep._diagnostics_cache["host:/dest"] = (dict(ALL_GOOD), time.time())
        with patch.object(SSHEndpoint, "_exec_remote_command") as probe:
            ep._run_diagnostics("/dest")
        assert not probe.called

    def test_force_refresh_ignores_a_fresh_entry(self):
        ep = _endpoint()
        ep._diagnostics_cache["host:/dest"] = (dict(ALL_GOOD), time.time())
        with patch.object(
            SSHEndpoint, "_exec_remote_command", return_value=MagicMock(returncode=1)
        ) as probe:
            ep._run_diagnostics("/dest", force_refresh=True)
        assert probe.called, "force_refresh did not reach the remote"

    def test_a_stale_pass_is_still_written_into_the_config(self):
        """Why a stale verdict is not merely a stale report: this boolean picks
        the transfer strategy and is read by every remote command build."""
        ep = _endpoint()
        ep._diagnostics_cache["host:/dest"] = (dict(ALL_GOOD), time.time())
        ep.config["passwordless_sudo_available"] = False

        ep._run_diagnostics("/dest")

        assert ep.config["passwordless_sudo_available"] is True


class TestTheTransferGateRefreshes:
    """Checked at the source, because driving send_receive end to end needs a
    live remote; the behavioural half is the tier3 matrix."""

    @staticmethod
    def _gate_source():
        import inspect

        return inspect.getsource(SSHEndpoint.send_receive)

    def test_the_pre_transfer_check_forces_a_refresh(self):
        source = self._gate_source()
        assert "_run_diagnostics(dest_path, force_refresh=True)" in source, (
            "the pre-transfer readiness gate reads a verdict that can be 300s old"
        )

    def test_force_refresh_has_at_least_one_caller_now(self):
        """It had none. A parameter nothing passes is a parameter that does
        nothing, however correct its implementation."""
        import ast
        import inspect

        from btrfs_backup_ng.endpoint import ssh as ssh_mod

        tree = ast.parse(inspect.getsource(ssh_mod))
        forced = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_run_diagnostics"
            and any(
                kw.arg == "force_refresh"
                and isinstance(kw.value, ast.Constant)
                and kw.value.value is True
                for kw in node.keywords
            )
        ]
        assert forced, "no caller forces a fresh diagnostics run"

    def test_the_inner_check_does_not_refresh_again(self):
        """send_receive refreshes on the way in and _try_direct_transfer reads
        what it wrote. Forcing both would pay for all eight probes twice per
        snapshot to answer the same question."""
        import inspect

        source = inspect.getsource(SSHEndpoint._try_direct_transfer)
        assert "_run_diagnostics(dest_path)" in source
        assert "force_refresh=True" not in source


@pytest.mark.parametrize(
    "key",
    ["ssh_connection", "btrfs_command", "write_permissions", "btrfs_filesystem"],
)
def test_every_gating_key_can_still_fail_the_transfer(key):
    """The refresh must not have made the verdict decorative."""
    import inspect

    source = inspect.getsource(SSHEndpoint.send_receive)
    assert f'diagnostics["{key}"]' in source
    assert "Pre-transfer diagnostics failed" in source
