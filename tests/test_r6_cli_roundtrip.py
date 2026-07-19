"""Enforcement: the CLI dispatch paths must thread raw-target encryption into the
destination endpoint and fail closed -- the exact seam the R6 plaintext bug lived
in. These drive the REAL cli functions (not a mocked choose_endpoint), let
choose_endpoint build a REAL RawEndpoint, and mock only the btrfs snapshot/transfer
work, so a dropped thread/assert is actually caught.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import btrfs_backup_ng.cli.run as run_mod
import btrfs_backup_ng.cli.transfer as transfer_mod
from btrfs_backup_ng import endpoint as ep_pkg
from btrfs_backup_ng.config.schema import (
    Config,
    GlobalConfig,
    TargetConfig,
    VolumeConfig,
)


def _spy_choose(captured):
    """A choose_endpoint that returns a mock SOURCE endpoint but builds the REAL
    destination endpoint (so encryption threading is actually exercised), recording
    the dest config + endpoint."""
    real_choose = ep_pkg.choose_endpoint

    def spy(spec, common_config=None, source=False, **kw):
        if source:
            m = MagicMock()
            m.snapshot.return_value = MagicMock()
            m.list_snapshots.return_value = [MagicMock()]
            return m
        ep = real_choose(spec, common_config, source=source, **kw)
        captured["config"] = dict(common_config or {})
        captured["endpoint"] = ep
        return ep

    return spy


class TestRunThreadsEncryption:
    def _volume(self, tmp_path, **target_kw):
        src = tmp_path / "src"
        src.mkdir(exist_ok=True)
        dest = tmp_path / "dest"
        dest.mkdir(exist_ok=True)
        target = TargetConfig(path=f"raw://{dest}", **target_kw)
        return VolumeConfig(
            path=str(src),
            snapshot_prefix="t-",
            snapshot_dir=str(tmp_path / "snaps"),
            targets=[target],
        )

    def test_encryption_reaches_the_real_destination_endpoint(
        self, tmp_path, monkeypatch
    ):
        volume = self._volume(tmp_path, encrypt="gpg", gpg_recipient="KEYID")
        config = Config(global_config=GlobalConfig(), volumes=[volume])
        captured: dict = {}
        monkeypatch.setattr(run_mod.endpoint, "choose_endpoint", _spy_choose(captured))
        monkeypatch.setattr(run_mod, "_transfer_to_target", lambda *a, **k: True)

        ok, _stats, errors = run_mod._backup_volume(volume, config, parallel_targets=1)

        assert ok, errors
        # The REAL RawEndpoint the CLI built and handed to the transfer is encrypting.
        assert captured["config"].get("encrypt") == "gpg"
        assert captured["endpoint"].encrypt == "gpg"

    def test_aborts_fail_closed_when_threading_is_dropped(self, tmp_path, monkeypatch):
        # Simulate a regression that stops threading encryption: the fail-closed
        # assert must abort the target rather than write plaintext.
        volume = self._volume(tmp_path, encrypt="gpg", gpg_recipient="KEYID")
        config = Config(global_config=GlobalConfig(), volumes=[volume])
        captured: dict = {}
        monkeypatch.setattr(run_mod.endpoint, "choose_endpoint", _spy_choose(captured))
        monkeypatch.setattr(run_mod, "_transfer_to_target", lambda *a, **k: True)
        monkeypatch.setattr(run_mod, "thread_raw_encryption", lambda kw, t: None)

        ok, _stats, errors = run_mod._backup_volume(volume, config, parallel_targets=1)

        assert ok is False
        assert any("PLAINTEXT" in e or "did not receive it" in e for e in errors), (
            errors
        )

    def test_plaintext_target_is_unaffected(self, tmp_path, monkeypatch):
        # A plaintext raw target must still work (no false abort).
        volume = self._volume(tmp_path)  # no encryption
        config = Config(global_config=GlobalConfig(), volumes=[volume])
        captured: dict = {}
        monkeypatch.setattr(run_mod.endpoint, "choose_endpoint", _spy_choose(captured))
        monkeypatch.setattr(run_mod, "_transfer_to_target", lambda *a, **k: True)

        ok, _stats, errors = run_mod._backup_volume(volume, config, parallel_targets=1)
        assert ok, errors
        assert captured["endpoint"].encrypt is None


class TestTransferThreadsEncryption:
    @staticmethod
    def _run(tmp_path, monkeypatch, *, drop_thread=False):
        from types import SimpleNamespace

        src = tmp_path / "src"
        src.mkdir()
        (src / ".snapshots").mkdir()  # transfer skips volumes without a snapshot dir
        dest = tmp_path / "dest"
        dest.mkdir()
        target = TargetConfig(
            path=f"raw://{dest}", encrypt="gpg", gpg_recipient="KEYID"
        )
        volume = VolumeConfig(
            path=str(src),
            snapshot_prefix="t-",
            snapshot_dir=".snapshots",
            targets=[target],
        )
        config = Config(global_config=GlobalConfig(), volumes=[volume])

        captured: dict = {}
        monkeypatch.setattr(
            transfer_mod.endpoint, "choose_endpoint", _spy_choose(captured)
        )
        monkeypatch.setattr(transfer_mod, "sync_snapshots", lambda *a, **k: MagicMock())
        monkeypatch.setattr(transfer_mod, "load_config", lambda *a, **k: (config, []))
        monkeypatch.setattr(
            transfer_mod, "find_config_file", lambda *a, **k: "cfg.toml"
        )
        if drop_thread:
            monkeypatch.setattr(
                transfer_mod, "thread_raw_encryption", lambda kw, t: None
            )

        args = SimpleNamespace(
            config=None, volume=None, dry_run=False, compress=None, rate_limit=None
        )
        rc = transfer_mod.execute_transfer(args)
        return rc, captured

    def test_encryption_reaches_endpoint_via_transfer_command(
        self, tmp_path, monkeypatch
    ):
        rc, captured = self._run(tmp_path, monkeypatch)
        assert rc == 0
        assert captured.get("config", {}).get("encrypt") == "gpg"
        assert captured["endpoint"].encrypt == "gpg"

    def test_aborts_fail_closed_when_threading_dropped(self, tmp_path, monkeypatch):
        # Dropping the thread must trip the fail-closed assert -> non-zero exit,
        # never a silent plaintext transfer.
        rc, _captured = self._run(tmp_path, monkeypatch, drop_thread=True)
        assert rc != 0
