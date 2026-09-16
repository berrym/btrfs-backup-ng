"""A command must not exit 0 for a state it is meant to detect.

Five commands reported success for exactly the condition an operator runs them
to find. They share one shape: a verdict derived from a variable that only ever
changes inside an ``except`` handler, or from ``any()`` over an empty list, so
every non-exceptional bad state slid past.

  status          all_healthy was falsified only in except handlers, so "no
                  snapshots", "no snapshot dir" and "no backups" all printed
                  "All systems operational" and exited 0 -- a system that has
                  never backed up, or whose destination drive was wiped. This is
                  the command a cron or monitoring wrapper checks.
  list            a source that could not be enumerated printed "(none)",
                  byte-identical to a volume that genuinely has none, and exited
                  0. During restore triage those two sentences mean opposite
                  things. The TARGET branch already recorded its errors.
  raw verify      any([]) is False, so an empty target took the "nothing was
                  corrupt" branch. "Everything I checked was fine" is not a pass
                  when the number checked was zero.
  config validate the readiness loop iterates enabled volumes, so zero volumes
                  meant zero problems and the all-clear. Reached by an ordinary
                  typo: [[volume]] for [[volumes]].
  uninstall       `found` was set before the privilege check, so a non-root run
                  that removed nothing still exited 0 while the timer stayed
                  installed and kept firing.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class TestRawVerifyOnAnEmptyTarget:
    def test_it_does_not_report_a_clean_pass(self, tmp_path):
        from btrfs_backup_ng.cli import raw_cmd

        args = MagicMock()
        args.raw_action = "verify"
        args.target = str(tmp_path)
        args.snapshot = None
        args.json = False

        rc = raw_cmd.execute_raw(args)

        assert rc == 2, f"an empty raw target verified clean with exit {rc}"

    def test_it_matches_the_general_verify_command(self):
        """The two disagreeing about whether an empty location is healthy is what
        let this sit. `verify` puts "No snapshots found at backup location" into
        report.errors, and errors mean exit 2."""
        import inspect

        from btrfs_backup_ng.cli import verify as verify_cli

        source = inspect.getsource(verify_cli)
        assert "if report.errors:\n        return 2" in source


class TestConfigValidateWithNoVolumes:
    def _validate(self, tmp_path, body):
        from btrfs_backup_ng.cli import config_cmd

        cfg = tmp_path / "c.toml"
        cfg.write_text(body)
        args = MagicMock()
        args.config = str(cfg)
        args.config_action = "validate"
        return config_cmd.execute_config(args)

    def test_a_typo_that_parses_to_no_volumes_is_not_valid(self, tmp_path, capsys):
        """[[volume]] instead of [[volumes]] parses as an unknown top-level key
        and is ignored, so the file is structurally fine and backs nothing up."""
        rc = self._validate(
            tmp_path,
            '[[volume]]\npath = "/home"\n[[volume.target]]\npath = "/mnt/b"\n',
        )

        out = capsys.readouterr().out
        assert rc != 0, f"a config that backs nothing up validated with exit {rc}"
        assert "backs nothing up" in out
        assert "[[volumes]]" in out, "the message does not name the correct section"

    def test_a_real_config_still_validates(self, tmp_path, capsys):
        """The guard must not reject a config that is merely unrunnable HERE --
        exit 2 for that is a deliberate distinction this must not collapse."""
        rc = self._validate(
            tmp_path,
            '[[volumes]]\npath = "/nonexistent-volume"\n'
            '[[volumes.targets]]\npath = "/mnt/b"\n',
        )

        assert rc == 2, f"expected the 'valid file, wrong machine' code, got {rc}"
        assert "backs nothing up" not in capsys.readouterr().out


class TestUninstallThatRemovedNothing:
    """`locations` is a literal inside the function -- a system path and a user
    path -- so the system one is redirected by patching Path, which is what the
    function calls to build it."""

    @staticmethod
    def _run(monkeypatch, tmp_path, *, euid):
        from pathlib import Path as _RealPath

        from btrfs_backup_ng.cli import install

        system_dir = tmp_path / "etc-systemd-system"
        system_dir.mkdir()
        (system_dir / "btrfs-backup-ng.service").write_text("[Unit]\n")
        (system_dir / "btrfs-backup-ng.timer").write_text("[Timer]\n")
        user_dir = tmp_path / "user-systemd"  # deliberately empty

        def fake_path(arg):
            if str(arg) == "/etc/systemd/system":
                return system_dir
            return _RealPath(arg)

        fake_path.home = lambda: tmp_path / "home"
        (tmp_path / "home" / ".config" / "systemd" / "user").mkdir(parents=True)
        assert user_dir is not None

        monkeypatch.setattr(install, "Path", fake_path)
        monkeypatch.setattr(install.os, "geteuid", lambda: euid)
        return install.execute_uninstall(MagicMock())

    def test_a_privilege_skip_is_not_success(self, monkeypatch, tmp_path, capsys):
        rc = self._run(monkeypatch, tmp_path, euid=1000)
        out = capsys.readouterr().out

        assert "Run with sudo" in out, f"the privilege branch was not reached: {out}"
        assert rc == 1, (
            f"uninstall exited {rc} having removed nothing; the units are still "
            "installed and the timer still fires"
        )
        assert "still installed" in out

    def test_a_removal_that_succeeds_still_exits_zero(self, monkeypatch, tmp_path):
        rc = self._run(monkeypatch, tmp_path, euid=0)
        assert rc == 0

    def test_nothing_installed_is_still_success(self, monkeypatch, tmp_path, capsys):
        from pathlib import Path as _RealPath

        from btrfs_backup_ng.cli import install

        empty = tmp_path / "empty"
        empty.mkdir()

        def fake_path(arg):
            if str(arg) == "/etc/systemd/system":
                return empty
            return _RealPath(arg)

        fake_path.home = lambda: tmp_path / "home"
        (tmp_path / "home" / ".config" / "systemd" / "user").mkdir(parents=True)
        monkeypatch.setattr(install, "Path", fake_path)
        monkeypatch.setattr(install.os, "geteuid", lambda: 1000)

        rc = install.execute_uninstall(MagicMock())

        assert rc == 0
        assert "No btrfs-backup-ng systemd files found" in capsys.readouterr().out


class TestStatusOnASystemThatHasNeverBackedUp:
    """The state a monitoring wrapper exists to catch, reported as operational."""

    @staticmethod
    def _run(tmp_path, *, source_snaps, target_snaps):
        from unittest.mock import patch

        from btrfs_backup_ng.config.schema import (
            Config,
            GlobalConfig,
            TargetConfig,
            VolumeConfig,
        )

        data = tmp_path / "data"
        (data / ".snapshots").mkdir(parents=True)
        volume = VolumeConfig(
            path=str(data),
            snapshot_prefix="data-",
            snapshot_dir=".snapshots",
            targets=[TargetConfig(path=str(tmp_path / "backup"))],
        )
        config = Config(global_config=GlobalConfig(), volumes=[volume])

        source_ep = MagicMock()
        source_ep.list_snapshots.return_value = source_snaps
        target_ep = MagicMock()
        target_ep.list_snapshots.return_value = target_snaps

        args = MagicMock()
        args.config = None
        args.volume = None
        args.json = False

        with (
            patch(
                "btrfs_backup_ng.cli.status.find_config_file",
                return_value=str(tmp_path / "c.toml"),
            ),
            patch("btrfs_backup_ng.cli.status.load_config", return_value=(config, [])),
            patch(
                "btrfs_backup_ng.cli.status.endpoint.choose_endpoint",
                side_effect=[source_ep, target_ep],
            ),
        ):
            from btrfs_backup_ng.cli.status import execute_status

            return execute_status(args)

    def test_a_source_with_no_snapshots_is_not_operational(self, tmp_path, capsys):
        rc = self._run(tmp_path, source_snaps=[], target_snaps=[MagicMock()])

        out = capsys.readouterr().out
        assert rc != 0, f"a volume with no snapshots reported exit {rc}: {out}"
        assert "All systems operational" not in out

    def test_a_target_with_no_backups_is_not_operational(self, tmp_path, capsys):
        snap = MagicMock()
        snap.get_name.return_value = "data-20240101-120000"
        rc = self._run(tmp_path, source_snaps=[snap], target_snaps=[])

        out = capsys.readouterr().out
        assert rc != 0, f"a target holding no backups reported exit {rc}: {out}"
        assert "All systems operational" not in out

    def test_a_healthy_system_still_passes(self, tmp_path, capsys):
        """The guard must not make `status` useless by failing always."""
        snap = MagicMock()
        snap.get_name.return_value = "data-20240101-120000"
        rc = self._run(tmp_path, source_snaps=[snap], target_snaps=[snap])

        assert rc == 0, capsys.readouterr().out


class TestListWhenALocationCannotBeRead:
    @staticmethod
    def _run(tmp_path, *, source_raises):
        from unittest.mock import patch

        from btrfs_backup_ng.config.schema import (
            Config,
            GlobalConfig,
            TargetConfig,
            VolumeConfig,
        )

        data = tmp_path / "data"
        (data / ".snapshots").mkdir(parents=True)
        volume = VolumeConfig(
            path=str(data),
            snapshot_prefix="data-",
            snapshot_dir=".snapshots",
            targets=[TargetConfig(path=str(tmp_path / "backup"))],
        )
        config = Config(global_config=GlobalConfig(), volumes=[volume])

        ep = MagicMock()
        if source_raises:
            ep.list_snapshots.side_effect = PermissionError("Permission denied")
        else:
            ep.list_snapshots.return_value = []

        args = MagicMock()
        args.config = None
        args.volume = None
        args.json = False

        with (
            patch(
                "btrfs_backup_ng.cli.list_cmd.find_config_file",
                return_value=str(tmp_path / "c.toml"),
            ),
            patch(
                "btrfs_backup_ng.cli.list_cmd.load_config", return_value=(config, [])
            ),
            patch(
                "btrfs_backup_ng.cli.list_cmd.endpoint.choose_endpoint",
                return_value=ep,
            ),
        ):
            from btrfs_backup_ng.cli.list_cmd import execute_list

            return execute_list(args)

    def test_an_unreadable_source_is_not_reported_as_none(self, tmp_path, capsys):
        """ "(none)" and "could not be read" mean opposite things during restore
        triage, and both printed the same sentence and exited 0."""
        rc = self._run(tmp_path, source_raises=True)

        out = capsys.readouterr().out
        assert rc != 0, f"an unreadable source listed cleanly with exit {rc}"
        assert "could not be listed" in out
        assert "Source snapshots: (none)" not in out

    def test_a_genuinely_empty_source_still_says_none_and_passes(
        self, tmp_path, capsys
    ):
        rc = self._run(tmp_path, source_raises=False)

        assert rc == 0
        assert "Source snapshots: (none)" in capsys.readouterr().out


class TestTheSharedShape:
    """The thing these five had in common, pinned so it is visible."""

    @pytest.mark.parametrize(
        ("module", "symbol"),
        [
            ("btrfs_backup_ng.cli.status", "all_healthy"),
            ("btrfs_backup_ng.cli.list_cmd", "unreadable"),
        ],
    )
    def test_a_verdict_is_not_decided_only_inside_except_handlers(self, module, symbol):
        """all_healthy was assigned False at exactly two places, both inside
        `except Exception`. Every ordinary bad state therefore left it True."""
        import ast
        import importlib
        import inspect

        mod = importlib.import_module(module)
        tree = ast.parse(inspect.getsource(mod))

        handler_lines = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler):
                for child in ast.walk(node):
                    if hasattr(child, "lineno"):
                        handler_lines.add(child.lineno)

        assignments = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, (ast.Assign, ast.AugAssign))
            and any(
                isinstance(t, ast.Name) and t.id == symbol
                for t in (
                    node.targets if isinstance(node, ast.Assign) else [node.target]
                )
            )
        ]
        outside = [ln for ln in assignments if ln not in handler_lines]
        assert outside, (
            f"{symbol} is only ever set inside an except handler, so every "
            "non-exceptional bad state leaves the verdict unchanged"
        )
