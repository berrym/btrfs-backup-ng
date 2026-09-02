"""Per-target `optional`, and per-key retention inheritance.

Two decisions that go together. The source stops pruning while a REQUIRED target
is missing, because that target is still owed those snapshots; `optional = true`
is how an operator says a target is allowed to be away, which releases both the
run's exit code and source retention. And a narrower retention scope now inherits
what it did not write, so `[volumes.retention] daily = 7` no longer discards a
global `min = "2d"` and silently substitutes the built-in default.
"""

from __future__ import annotations

import pytest

from btrfs_backup_ng.config.loader import load_config
from btrfs_backup_ng.config.schema import (
    Config,
    GlobalConfig,
    RetentionConfig,
    VolumeConfig,
)


def _load(tmp_path, text):
    cfg = tmp_path / "config.toml"
    cfg.write_text(text)
    res = load_config(str(cfg))
    return res[0] if isinstance(res, tuple) else res


class TestRetentionInheritsPerKey:
    CONFIG = """
[global.retention]
min = "2d"
daily = 14
weekly = 4
monthly = 6

[[volumes]]
path = "/var/log"

[volumes.retention]
daily = 7
weekly = 2

[[volumes.targets]]
path = "/mnt/archive"

[volumes.targets.retention]
keep = 30
"""

    def test_the_volume_keeps_what_it_did_not_write(self, tmp_path):
        """Mutation guard: whole-object replacement gives min='1d' and
        monthly=12 -- the dataclass defaults, not the file's values."""
        config = self._config(tmp_path)
        effective = config.get_effective_retention(config.volumes[0])
        assert effective.min == "2d", "a global safety floor was discarded"
        assert effective.monthly == 6, "a global bucket was replaced by a default"
        assert effective.daily == 7, "the volume's own value must win"
        assert effective.weekly == 2

    def test_a_target_inherits_through_the_volume(self, tmp_path):
        config = self._config(tmp_path)
        volume = config.volumes[0]
        target = config.get_target_retention(volume, volume.targets[0])
        assert target.keep == 30, "the target's own key must win"
        assert target.min == "2d", "global's floor must survive two levels"
        assert target.daily == 7, "the volume's value must survive one level"

    def test_the_source_inherits_through_the_volume(self, tmp_path):
        config = self._config(tmp_path)
        volume = config.volumes[0]
        source = config.get_source_retention(volume)
        assert source.min == "2d"
        assert source.daily == 7
        assert source.monthly == 6

    def _config(self, tmp_path):
        return _load(tmp_path, self.CONFIG)

    def test_a_config_with_no_scoped_keys_is_unchanged(self, tmp_path):
        """The compatibility promise: setting none of the newer keys resolves to
        exactly the global policy."""
        config = _load(
            tmp_path,
            '[global.retention]\nmin = "3d"\ndaily = 9\n\n'
            '[[volumes]]\npath = "/home"\n\n[[volumes.targets]]\npath = "/mnt/b"\n',
        )
        volume = config.volumes[0]
        for resolved in (
            config.get_effective_retention(volume),
            config.get_source_retention(volume),
            config.get_target_retention(volume, volume.targets[0]),
        ):
            assert resolved.min == "3d"
            assert resolved.daily == 9

    def test_a_policy_built_in_code_still_replaces_wholesale(self):
        """A RetentionConfig constructed programmatically states a complete
        policy on purpose; only the loader knows which keys a FILE named."""
        config = Config(
            global_config=GlobalConfig(retention=RetentionConfig(daily=7, weekly=4)),
            volumes=[
                VolumeConfig(
                    path="/home", retention=RetentionConfig(daily=14, weekly=8)
                )
            ],
        )
        effective = config.get_effective_retention(config.volumes[0])
        assert effective.daily == 14
        assert effective.weekly == 8


class TestOptionalTargets:
    def _config(self, tmp_path, optional):
        return _load(
            tmp_path,
            '[[volumes]]\npath = "/home"\n\n'
            '[[volumes.targets]]\npath = "/mnt/monthly-drive"\n'
            + (f"optional = {optional}\n" if optional else ""),
        )

    def test_optional_defaults_to_false(self, tmp_path):
        """A target is required unless it says otherwise; the safe default."""
        config = self._config(tmp_path, None)
        assert config.volumes[0].targets[0].optional is False

    @pytest.mark.parametrize(
        "written,expected", [("true", True), ("false", False), ('"yes"', True)]
    )
    def test_optional_is_parsed(self, tmp_path, written, expected):
        config = self._config(tmp_path, written)
        assert config.volumes[0].targets[0].optional is expected

    def test_a_value_with_no_reading_is_refused(self, tmp_path):
        """Mutation guard: plain truthiness reads "false" as True, the mistake
        that made require_mount = "false" enable the check."""
        from btrfs_backup_ng.config import ConfigError

        with pytest.raises(ConfigError):
            self._config(tmp_path, '"maybe"')

    def test_an_absent_optional_target_does_not_fail_the_run(
        self, tmp_path, monkeypatch
    ):
        """It is reported and skipped, and -- because it is not counted as a
        failure -- the source is still pruned."""
        from btrfs_backup_ng.cli import run as run_cli

        src = tmp_path / "src"
        (src / ".snapshots").mkdir(parents=True)
        config = _load(
            tmp_path,
            f'[[volumes]]\npath = "{src}"\nsnapshot_prefix = "home-"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path}/absent"\n'
            f"require_mount = true\noptional = true\n",
        )
        pruned = []
        monkeypatch.setattr(
            run_cli,
            "_prune_after_transfer",
            lambda *a, **kw: pruned.append(kw.get("prune_source")) or True,
        )

        class _Src:
            def snapshot(self, **_kw):
                return object()

            def prepare(self):
                return None

            def __getattr__(self, _n):
                return lambda *a, **k: None

        monkeypatch.setattr(
            run_cli.endpoint, "choose_endpoint", lambda *a, **kw: _Src()
        )
        ok, stats, errors = run_cli._backup_volume(config.volumes[0], config, 1)
        assert stats["failed"] == 0, f"an optional target was counted: {stats}"
        assert not errors, errors

    def test_a_required_absent_target_still_fails_the_run(self, tmp_path, monkeypatch):
        """Mutation guard: treating every target as optional would pass the test
        above while removing the protection entirely."""
        from btrfs_backup_ng.cli import run as run_cli

        src = tmp_path / "src"
        (src / ".snapshots").mkdir(parents=True)
        config = _load(
            tmp_path,
            f'[[volumes]]\npath = "{src}"\nsnapshot_prefix = "home-"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path}/absent"\n'
            f"require_mount = true\n",
        )

        class _Src:
            def snapshot(self, **_kw):
                return object()

            def prepare(self):
                return None

            def __getattr__(self, _n):
                return lambda *a, **k: None

        monkeypatch.setattr(
            run_cli.endpoint, "choose_endpoint", lambda *a, **kw: _Src()
        )
        ok, stats, errors = run_cli._backup_volume(config.volumes[0], config, 1)
        assert ok is False
        assert errors, "a required target that could not be prepared must be reported"
