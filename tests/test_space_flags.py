"""Space-check flags must actually take effect (T8).

``--no-check-space``/``--force``/``--safety-margin`` were parsed but never threaded into
the transfer options, so ``send_snapshot`` always ran the destination space preflight and
the flags were dead. That bit raw targets hardest: their size estimate is conservative
(≈130 MiB for 30 MB of real data, proven on hardware), so the preflight refused transfers
that would actually fit, with no working way to override it. These guard the plumbing that
makes the flags real, and the send-side contract they rely on.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import btrfs_backup_ng.cli.run as run_mod
import btrfs_backup_ng.cli.transfer as transfer_mod
import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli.common import space_options_from_args
from btrfs_backup_ng.config.schema import (
    Config,
    GlobalConfig,
    TargetConfig,
    VolumeConfig,
)


# --- the flags -> options mapping --------------------------------------------


def test_defaults_preserve_always_check():
    """No flags => today's behavior: the space check runs, nothing is forced."""
    opts = space_options_from_args(argparse.Namespace())
    assert opts["check_space"] is True
    assert opts["force"] is False
    assert "safety_margin" not in opts  # left to the send-side default


def test_no_check_space_disables_the_check():
    """Mutation guard: dropping the ``not`` (or hardcoding True) fails this."""
    opts = space_options_from_args(argparse.Namespace(no_check_space=True))
    assert opts["check_space"] is False


def test_force_is_threaded():
    opts = space_options_from_args(argparse.Namespace(force=True))
    assert opts["force"] is True


def test_safety_margin_is_threaded_when_set():
    opts = space_options_from_args(argparse.Namespace(safety_margin=25.0))
    assert opts["safety_margin"] == 25.0


# --- the plumbing actually reaches the transfer options ----------------------


def test_transfer_to_target_merges_space_options(monkeypatch):
    """_transfer_to_target must merge space_options into the options handed to
    sync_snapshots. Mutation guard: removing ``**(space_options or {})`` from the
    transfer_options dict drops check_space/force and fails this."""
    captured = {}

    def fake_sync(*a, **k):
        captured.update(k.get("options", {}))

    monkeypatch.setattr(run_mod, "sync_snapshots", fake_sync)

    target_config = MagicMock()
    target_config.compress = "none"
    target_config.rate_limit = None
    target_config.ssh_sudo = False
    target_config.path = "raw:///mnt/x"

    ok = run_mod._transfer_to_target(
        MagicMock(),
        MagicMock(),
        target_config,
        MagicMock(),
        True,
        space_options={"check_space": False, "force": True},
    )
    assert ok is True
    assert captured["check_space"] is False
    assert captured["force"] is True


def test_backup_snapper_volume_merges_space_options(monkeypatch):
    """The snapper 'run' path builds a SEPARATE options dict from _transfer_to_target;
    it too must merge the space flags. Mutation guard: removing ``**(space_options or
    {})`` from _backup_snapper_volume's options dict (or _backup_volume failing to
    forward space_options) drops check_space/force here and fails this."""
    from btrfs_backup_ng.snapper.scanner import SnapperConfig

    captured: dict = {}

    def fake_sync(*a, **k):
        captured.update(k.get("options", {}))
        return 1

    # _backup_snapper_volume imports sync_snapper_snapshots locally from core.operations,
    # so patch it at its origin module (not on cli.run).
    monkeypatch.setattr(ops, "sync_snapper_snapshots", fake_sync)
    monkeypatch.setattr(
        run_mod.endpoint, "choose_endpoint", lambda *a, **k: MagicMock(_is_remote=False)
    )

    volume = VolumeConfig(path="/", snapshot_prefix="root-")
    volume.source = "snapper"  # type: ignore[attr-defined]
    volume.snapper = MagicMock(config_name="root")  # type: ignore[attr-defined]
    volume.targets = [TargetConfig(path="/mnt/backup")]
    config = Config(global_config=GlobalConfig(), volumes=[volume])

    with patch("btrfs_backup_ng.snapper.SnapperScanner") as mock_scanner_cls:
        scanner = MagicMock()
        scanner.find_config_for_path.return_value = SnapperConfig(
            name="root", subvolume=Path("/")
        )
        mock_scanner_cls.return_value = scanner
        run_mod._backup_snapper_volume(
            volume, config, space_options={"check_space": False, "force": True}
        )

    assert captured["check_space"] is False
    assert captured["force"] is True


def test_execute_transfer_merges_space_options(monkeypatch, tmp_path):
    """The 'transfer' subcommand builds its own options dict inline; it must merge the
    space flags too. Mutation guard: removing ``**space_options_from_args(args)`` from
    execute_transfer's transfer_options fails this."""
    data = tmp_path / "data"
    (data / ".snapshots").mkdir(parents=True)
    volume = VolumeConfig(
        path=str(data),
        snapshot_prefix="data-",
        snapshot_dir=".snapshots",
        targets=[TargetConfig(path="/mnt/backup")],
    )
    config = Config(global_config=GlobalConfig(), volumes=[volume])

    captured: dict = {}
    monkeypatch.setattr(
        transfer_mod,
        "sync_snapshots",
        lambda *a, **k: captured.update(k.get("options", {})),
    )
    monkeypatch.setattr(
        transfer_mod, "find_config_file", lambda *a, **k: str(tmp_path / "c.toml")
    )
    monkeypatch.setattr(transfer_mod, "load_config", lambda *a, **k: (config, []))
    ep = MagicMock()
    ep.list_snapshots.return_value = [MagicMock()]
    monkeypatch.setattr(transfer_mod.endpoint, "choose_endpoint", lambda *a, **k: ep)

    args = MagicMock()
    args.config = None
    args.volume = None
    args.dry_run = False
    args.compress = None
    args.rate_limit = None
    args.no_check_space = True
    args.force = False
    args.safety_margin = 10.0

    transfer_mod.execute_transfer(args)
    assert captured["check_space"] is False  # --no-check-space took effect


def test_transfer_to_target_without_space_options_is_safe(monkeypatch):
    """A None space_options must not crash and must not inject space keys (so the
    send-side defaults apply)."""
    captured = {}
    monkeypatch.setattr(
        run_mod, "sync_snapshots", lambda *a, **k: captured.update(k.get("options", {}))
    )
    tc = MagicMock()
    tc.compress = "none"
    tc.rate_limit = None
    tc.ssh_sudo = False
    tc.path = "raw:///x"
    run_mod._transfer_to_target(MagicMock(), MagicMock(), tc, MagicMock(), True)
    assert "check_space" not in captured  # send-side default (True) governs


# --- the send-side contract the flags rely on --------------------------------


def _spy_send_snapshot(monkeypatch, options):
    """Drive send_snapshot far enough to observe the preflight decision, then abort the
    actual send. Returns the _verify_destination_space spy."""
    spy = MagicMock()
    monkeypatch.setattr(ops, "_verify_destination_space", spy)
    monkeypatch.setattr(ops, "_ensure_destination_exists", lambda *a, **k: None)
    monkeypatch.setattr(ops, "_cleanup_processes", lambda *a, **k: None)

    snap = MagicMock()
    snap.get_path.return_value = "/x"
    snap.endpoint.send.side_effect = OSError("stop after preflight decision")
    dest = MagicMock()
    dest.config = {"path": "/dest"}

    with pytest.raises(__util__.SnapshotTransferError):
        ops.send_snapshot(snap, dest, options=options)
    return spy


def test_send_snapshot_skips_preflight_when_check_space_false(monkeypatch):
    """check_space=False must SKIP the space preflight (this is what --no-check-space
    ultimately buys). Mutation guard: changing the ``if check_space and not force``
    gate so it ignores check_space makes the spy fire and fails this."""
    spy = _spy_send_snapshot(monkeypatch, {"check_space": False})
    spy.assert_not_called()


def test_send_snapshot_skips_preflight_when_forced(monkeypatch):
    spy = _spy_send_snapshot(monkeypatch, {"check_space": True, "force": True})
    spy.assert_not_called()


def test_send_snapshot_runs_preflight_by_default(monkeypatch):
    """The default (no flags) still runs the preflight -- no regression."""
    spy = _spy_send_snapshot(monkeypatch, {})
    spy.assert_called_once()
