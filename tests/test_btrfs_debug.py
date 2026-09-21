"""``--btrfs-debug`` / ``[global] btrfs_debug`` reaches every endpoint and means something.

Legacy mode has had ``--btrfs-debug`` since the original tool; it put ``-vv``
on ``btrfs send`` and ``btrfs receive``, whose output went to DEVNULL, so the
option did nothing anyone could see. The config-driven commands did not have
it: nine of them hard-coded ``"btrfs_debug": False`` into endpoint kwargs.
Now one helper answers every command, the stderr drain logs each line as it
arrives with the process's name, and the option implies DEBUG logging so the
lines are not produced and dropped.
"""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
from pathlib import Path

import pytest

from btrfs_backup_ng.cli.common import btrfs_debug_enabled, get_log_level
from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser
from btrfs_backup_ng.config.loader import ConfigError, load_config
from btrfs_backup_ng.core import transfer as T
from btrfs_backup_ng.endpoint.local import LocalEndpoint

CLI = Path(__file__).resolve().parent.parent / "src" / "btrfs_backup_ng" / "cli"


def _config(tmp_path, global_lines: str):
    p = tmp_path / "c.toml"
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    p.write_text(
        f'[global]\n{global_lines}\n[[volumes]]\npath = "{src}"\n'
        f'[[volumes.targets]]\npath = "{tmp_path}/dst"\n'
    )
    return p


class TestTheConfigKey:
    def test_true_loads(self, tmp_path):
        config, warnings = load_config(_config(tmp_path, "btrfs_debug = true"))
        assert config.global_config.btrfs_debug is True
        assert not [w for w in warnings if "btrfs_debug" in w], warnings

    def test_default_is_off(self, tmp_path):
        config, _ = load_config(_config(tmp_path, ""))
        assert config.global_config.btrfs_debug is False

    @pytest.mark.parametrize("bad", ['"yes"', "1", '"true"'])
    def test_a_non_boolean_is_refused_at_load(self, tmp_path, bad):
        """A string or an int would be truthy and quietly put -vv on every
        send and receive."""
        with pytest.raises(ConfigError, match="btrfs_debug"):
            load_config(_config(tmp_path, f"btrfs_debug = {bad}"))


class TestTheFlag:
    def test_every_subcommand_accepts_it_before_the_command(self):
        parser = create_subcommand_parser()
        for cmd in ("run", "snapshot", "transfer", "prune", "list", "status"):
            args = parser.parse_args(["--btrfs-debug", cmd])
            assert args.btrfs_debug is True, cmd

    def test_it_implies_debug_logging(self):
        assert get_log_level(argparse.Namespace(btrfs_debug=True)) == "DEBUG"
        assert get_log_level(argparse.Namespace()) != "DEBUG"


class TestOneHelperAnswersEveryCommand:
    def test_flag_or_config_enables_it(self, tmp_path):
        config, _ = load_config(_config(tmp_path, "btrfs_debug = true"))
        assert btrfs_debug_enabled(argparse.Namespace(), config) is True
        assert btrfs_debug_enabled(argparse.Namespace(btrfs_debug=True), None) is True
        config_off, _ = load_config(_config(tmp_path, ""))
        assert btrfs_debug_enabled(argparse.Namespace(), config_off) is False
        assert btrfs_debug_enabled(None, None) is False

    def test_no_command_hard_codes_it(self):
        """The nine literals are gone and cannot return: every endpoint kwargs
        dict in the CLI layer names the helper."""
        offenders = [
            str(p.relative_to(CLI))
            for p in sorted(CLI.glob("*.py"))
            if re.search(
                r'"btrfs_debug":\s*(True|False)', p.read_text(encoding="utf-8")
            )
        ]
        assert offenders == [], offenders

    def test_every_kwargs_site_uses_the_helper(self):
        users = {
            str(p.relative_to(CLI))
            for p in CLI.glob("*.py")
            if "btrfs_debug_enabled(" in p.read_text(encoding="utf-8")
            and p.name != "common.py"
        }
        expected = {
            "run.py",
            "snapshot.py",
            "transfer.py",
            "prune.py",
            "list_cmd.py",
            "status.py",
            "verify.py",
            "restore.py",
        }
        assert expected <= users, expected - users


class TestItMeansSomething:
    def test_the_drain_logs_each_line_with_the_process_name(self, caplog):
        proc = subprocess.Popen(
            ["sh", "-c", "printf 'At subvol x\\nutimes x/a\\nutimes x/b\\n' >&2"],
            stderr=subprocess.PIPE,
        )
        with caplog.at_level(logging.DEBUG, logger="btrfs_backup_ng.core.transfer"):
            T.tail_stderr(proc, log_as="btrfs receive")
            proc.wait(timeout=10)
            T.stderr_text(proc)
        lines = [r.getMessage() for r in caplog.records]
        assert "btrfs receive: At subvol x" in lines
        assert "btrfs receive: utimes x/b" in lines

    def test_without_the_name_nothing_is_logged(self, caplog):
        proc = subprocess.Popen(["sh", "-c", "echo quiet >&2"], stderr=subprocess.PIPE)
        with caplog.at_level(logging.DEBUG, logger="btrfs_backup_ng.core.transfer"):
            T.tail_stderr(proc)
            proc.wait(timeout=10)
            T.stderr_text(proc)
        assert not [r for r in caplog.records if "quiet" in r.getMessage()]

    def test_the_endpoint_puts_vv_on_and_names_the_process(
        self, tmp_path, monkeypatch, caplog
    ):
        ep = LocalEndpoint(
            config={"path": str(tmp_path), "fs_checks": "skip", "btrfs_debug": True}
        )
        assert ep.btrfs_flags == ["-vv"]
        seen = {}

        def fake_cmd(dest):
            return [
                ("sh", False),
                ("-c", False),
                ("cat >/dev/null; echo 'At subvol y' >&2", False),
            ]

        monkeypatch.setattr(ep, "_build_receive_command", fake_cmd)
        feed = subprocess.Popen(["echo", "s"], stdout=subprocess.PIPE)
        with caplog.at_level(logging.DEBUG, logger="btrfs_backup_ng.core.transfer"):
            proc = ep.receive(feed.stdout, "snap")
            feed.stdout.close()
            proc.wait(timeout=10)
            feed.wait(timeout=5)
            T.stderr_text(proc)
        assert any(
            r.getMessage() == "btrfs receive: At subvol y" for r in caplog.records
        )
        del seen

    def test_off_by_default_the_endpoint_does_not_name_it(
        self, tmp_path, monkeypatch, caplog
    ):
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        assert ep.btrfs_flags == []
        monkeypatch.setattr(
            ep,
            "_build_receive_command",
            lambda d: [
                ("sh", False),
                ("-c", False),
                ("cat >/dev/null; echo 'At subvol z' >&2", False),
            ],
        )
        feed = subprocess.Popen(["echo", "s"], stdout=subprocess.PIPE)
        with caplog.at_level(logging.DEBUG, logger="btrfs_backup_ng.core.transfer"):
            proc = ep.receive(feed.stdout, "snap")
            feed.stdout.close()
            proc.wait(timeout=10)
            feed.wait(timeout=5)
        assert not any("At subvol z" in r.getMessage() for r in caplog.records)


class TestASuccessfulTransferLogsEveryLine:
    def test_nothing_is_left_in_the_queue_when_send_snapshot_returns(
        self, caplog, monkeypatch
    ):
        """The logger runs on its own thread so the console cannot slow the
        transfer; the price is that someone must wait for it. Nothing did on
        the success path, and a run exited with 348 of 24,000 lines logged."""
        import contextlib
        from unittest.mock import MagicMock

        import btrfs_backup_ng.core.operations as ops

        def _send():
            p = subprocess.Popen(
                ["sh", "-c", "echo stream"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            T.tail_stderr(p, log_as="btrfs send")
            return p

        def receive(stdin, snapshot_name="", parent_name=None):
            p = subprocess.Popen(
                [
                    "sh",
                    "-c",
                    'cat >/dev/null; i=0; while [ $i -lt 5000 ]; do echo "utimes f$i" >&2; i=$((i+1)); done',
                ],
                stdin=stdin,
                stderr=subprocess.PIPE,
            )
            T.tail_stderr(p, log_as="btrfs receive")
            return p

        snapshot = MagicMock()
        snapshot.get_name.return_value = "snap-1"
        snapshot.endpoint.send.side_effect = lambda *a, **k: _send()
        snapshot.endpoint.config = {}
        dest = MagicMock()
        dest.receive.side_effect = receive
        dest.config = {"path": "/nowhere"}
        dest._is_remote = False
        monkeypatch.setattr(ops, "_ensure_destination_exists", lambda *a, **k: None)
        monkeypatch.setattr(
            ops, "_receiving_lock", lambda *a, **k: contextlib.nullcontext()
        )
        with caplog.at_level(logging.DEBUG, logger="btrfs_backup_ng.core.transfer"):
            ops.send_snapshot(snapshot, dest, options={"compress": "none"})
        logged = [
            r
            for r in caplog.records
            if r.getMessage().startswith("btrfs receive: utimes")
        ]
        assert len(logged) == 5000, len(logged)


class TestTheSshDirectPathIsNotLeftOut:
    """ssh:// targets take the direct-pipe strategy, which builds its own
    local send and remote receive. Both must carry -vv under btrfs_debug, or
    the option is silent on the most important transport."""

    def test_the_remote_receive_command_carries_vv(self):
        from btrfs_backup_ng.endpoint.ssh import _build_receive_command

        assert "btrfs receive -vv " in _build_receive_command("/d", verbose=True)
        assert "btrfs receive -vv " in _build_receive_command(
            "/d", use_sudo=True, verbose=True
        )
        assert "-vv" not in _build_receive_command("/d")

    def test_btrfs_flags_follow_the_config_without_init(self):
        """Endpoints are built without __init__ throughout the unit tests; the
        flags are derived from config, not stored at construction."""
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"btrfs_debug": True}
        assert ep.btrfs_flags == ["-vv"]
        ep.config = {}
        assert ep.btrfs_flags == []
