"""``prune`` prunes a snapper volume's destinations the way ``run`` does.

A snapper destination holds numbered slots (``.snapshots/<n>``), not
prefix-named snapshots. ``prune`` listed such a destination through the
native endpoint, found no prefix-named snapshot, printed "Keeping 0,
deleting 0" and deleted nothing -- while ``run`` on the same volume pruned
the same destination through ``plan_snapper_retention`` and
``delete_snapper_backups``. The man page says ``prune`` applies the
retention policies; on a snapper volume it applied none.

These drive the real ``execute_prune`` on a snapper volume with the two
snapper functions ``run`` uses observed, so the assertion is about what
``prune`` asks for and carries out, not about which lines it prints.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from types import SimpleNamespace

import pytest

from btrfs_backup_ng.cli import prune as prune_cli
from btrfs_backup_ng.cli.common import snapper_destination_options
from btrfs_backup_ng.config.loader import load_config


def _backup(number: int, when: datetime) -> dict:
    return {
        "number": number,
        "backup_name": f"snapshot-{number}",
        "metadata": SimpleNamespace(date=when, description=f"slot {number}"),
    }


HELD = [
    _backup(1, datetime(2026, 9, 20, 3, 0, 0)),
    _backup(2, datetime(2026, 9, 21, 3, 0, 0)),
    _backup(3, datetime(2026, 9, 22, 3, 0, 0)),
    _backup(4, datetime(2026, 9, 23, 3, 0, 0)),
]


def _config(tmp_path, retention='min = "1d"\ndaily = 2', targets=1, ssh=False):
    lines = [
        "[global.retention]",
        retention,
        "",
        "[[volumes]]",
        f'path = "{tmp_path / "root"}"',
        'source = "snapper"',
        "",
        "[volumes.snapper]",
        'config_name = "root"',
        'min_age = "0s"',
    ]
    for i in range(targets):
        dest = tmp_path / f"dest{i}"
        dest.mkdir()
        lines += ["", "[[volumes.targets]]"]
        if ssh:
            lines.append(f'path = "ssh://backup@nas:{dest}"')
            lines.append("ssh_sudo = true")
            lines.append("ssh_port = 2222")
        else:
            lines.append(f'path = "{dest}"')
    (tmp_path / "root").mkdir(exist_ok=True)
    cfg = tmp_path / "config.toml"
    cfg.write_text("\n".join(lines) + "\n")
    return cfg


def _args(cfg, **kw):
    return argparse.Namespace(
        config=str(cfg),
        dry_run=kw.get("dry_run", False),
        yes=kw.get("yes", True),
        force=kw.get("force", False),
        verbose=False,
        quiet=False,
        log_level=None,
    )


@pytest.fixture
def observed(monkeypatch):
    """The two snapper functions ``run`` prunes with, observed on the prune
    module: what was planned for, and what was deleted."""
    planned: list[tuple[str, object, dict]] = []
    deleted: list[tuple[str, list, dict]] = []
    native_plans: list = []

    def plan(backup_path, retention, endpoint_options=None, now=None):
        planned.append((backup_path, retention, dict(endpoint_options or {})))
        return HELD[2:], HELD[:2]

    def delete(backup_path, backups, endpoint_options=None):
        deleted.append((backup_path, list(backups), dict(endpoint_options or {})))
        return len(backups), []

    def native(*args, **kwargs):
        native_plans.append(args)
        return [], []

    monkeypatch.setattr(prune_cli, "plan_snapper_retention", plan)
    monkeypatch.setattr(prune_cli, "delete_snapper_backups", delete)
    monkeypatch.setattr(prune_cli, "plan_endpoint_retention", native)
    monkeypatch.setattr(prune_cli, "create_logger", lambda *a, **k: None)
    monkeypatch.setattr(prune_cli, "assert_target_mounted", lambda *a, **k: None)
    return SimpleNamespace(planned=planned, deleted=deleted, native=native_plans)


class TestPruneOnASnapperVolume:
    def test_deletes_what_the_snapper_plan_deletes(self, tmp_path, observed):
        cfg = _config(tmp_path)
        rc = prune_cli.execute_prune(_args(cfg))
        assert rc == 0
        config, _ = load_config(cfg)
        target = config.volumes[0].targets[0]
        assert [p[0] for p in observed.planned] == [target.path]
        assert observed.deleted == [
            (target.path, HELD[:2], snapper_destination_options(config, target))
        ]

    def test_the_plan_is_asked_with_the_target_policy_and_runs_options(
        self, tmp_path, observed
    ):
        """The same question ``run`` asks after transferring: the target's own
        resolved policy, and the destination opened with the options every
        command opens a snapper destination with (connection, encryption,
        compression), so ``prune`` sees the backups ``run`` wrote."""
        cfg = _config(tmp_path, ssh=True)
        prune_cli.execute_prune(_args(cfg))
        config, _ = load_config(cfg)
        volume = config.volumes[0]
        target = volume.targets[0]
        ((path, retention, options),) = observed.planned
        assert path == target.path
        assert retention == config.get_target_retention(volume, target)
        assert options == snapper_destination_options(config, target)
        assert options["ssh_sudo"] is True and options["port"] == 2222
        assert options["snap_prefix"] == ""

    def test_the_source_is_not_pruned(self, tmp_path, observed, caplog):
        """snapper owns its own timeline; ``run`` never prunes it and neither
        does ``prune``. The native source listing is not even attempted."""
        cfg = _config(tmp_path)
        with caplog.at_level(logging.INFO):
            prune_cli.execute_prune(_args(cfg))
        assert observed.native == []
        assert "managed by snapper" in caplog.text

    def test_every_target_is_pruned_under_its_own_policy(self, tmp_path, observed):
        cfg = _config(tmp_path, targets=2)
        prune_cli.execute_prune(_args(cfg))
        config, _ = load_config(cfg)
        paths = [t.path for t in config.volumes[0].targets]
        assert [p[0] for p in observed.planned] == paths
        assert [d[0] for d in observed.deleted] == paths


class TestDryRun:
    def test_deletes_nothing_and_says_what_it_would(self, tmp_path, observed, caplog):
        cfg = _config(tmp_path)
        with caplog.at_level(logging.INFO):
            rc = prune_cli.execute_prune(_args(cfg, dry_run=True))
        assert rc == 0
        assert observed.deleted == []
        assert len(observed.planned) == 1
        text = caplog.text
        assert "Would delete (target " in text
        assert "slot 1 (2026-09-20 03:00:00)" in text
        assert "slot 2 (2026-09-21 03:00:00)" in text
        assert "slot 3" not in text.split("Would delete")[-1] or "Keep" in text
        assert "would delete 2" in text


class TestRefusals:
    def test_a_degenerate_target_policy_is_refused_without_force(
        self, tmp_path, observed
    ):
        cfg = _config(
            tmp_path,
            retention='min = "0s"\nhourly = 0\ndaily = 0\nweekly = 0\nmonthly = 0\nyearly = 0',
        )
        rc = prune_cli.execute_prune(_args(cfg))
        assert rc == 1
        assert observed.planned == []
        assert observed.deleted == []

    def test_an_enumeration_failure_deletes_nothing_and_fails(
        self, tmp_path, observed, monkeypatch
    ):
        """A destination that could not be listed is not an empty one."""

        def broken(*a, **k):
            raise OSError("no such device")

        monkeypatch.setattr(prune_cli, "plan_snapper_retention", broken)
        cfg = _config(tmp_path)
        rc = prune_cli.execute_prune(_args(cfg))
        assert rc == 1
        assert observed.deleted == []

    def test_an_optional_absent_target_is_skipped_not_failed(
        self, tmp_path, observed, monkeypatch
    ):
        def broken(*a, **k):
            raise OSError("no such device")

        monkeypatch.setattr(prune_cli, "plan_snapper_retention", broken)
        cfg = _config(tmp_path)
        cfg.write_text(cfg.read_text() + "optional = true\n")
        rc = prune_cli.execute_prune(_args(cfg))
        assert rc == 0
        assert observed.deleted == []

    def test_declining_the_prompt_deletes_nothing(
        self, tmp_path, observed, monkeypatch
    ):
        cfg = _config(tmp_path)
        monkeypatch.setattr(prune_cli.sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr("builtins.input", lambda *a: "n")
        printed: list[str] = []
        monkeypatch.setattr(
            "builtins.print", lambda *a, **k: printed.append(" ".join(map(str, a)))
        )
        rc = prune_cli.execute_prune(_args(cfg, yes=False))
        assert rc == 0
        assert observed.deleted == []
        shown = "\n".join(printed)
        assert "slot 1 (2026-09-20 03:00:00)" in shown
        assert "slot 2 (2026-09-21 03:00:00)" in shown


class TestTheOptionsBuilderIsShared:
    def test_run_and_prune_open_the_destination_the_same_way(self, tmp_path):
        """``run`` transfers to and prunes the destination it opens with
        ``snapper_destination_options``; ``prune`` must use the same builder,
        or the two commands can see different backups at one path."""
        cfg = _config(tmp_path, ssh=True)
        config, _ = load_config(cfg)
        volume = config.volumes[0]
        target = volume.targets[0]
        options = snapper_destination_options(config, target)
        assert options["path"] == target.path
        assert options["snap_prefix"] == ""
        assert options["ssh_sudo"] is True
        assert options["port"] == 2222
        assert options["encrypt"] == "none"
        assert "compress" in options
        assert (
            options["transfer_stall_timeout"]
            == config.global_config.transfer_stall_timeout
        )
