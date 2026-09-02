"""`run` must not delete what a target is still owed.

`run` prunes the source after transferring, on the stated grounds that lock
reconcile holds any snapshot a failed transfer still needs. A target REFUSED
before transfer -- by require_mount, or any other prepare failure -- never
reaches the code that takes those locks, so nothing held the snapshots it owed
and the source was pruned anyway. If EVERY target was refused the run returned
before pruning, so the partial failure, the dangerous case, was the one that
deleted.
"""

from __future__ import annotations

from types import SimpleNamespace

from btrfs_backup_ng.cli import run as run_cli
from btrfs_backup_ng.config.schema import RetentionConfig

# Every bucket is written out. Unset keys are filled from hard-coded defaults
# (hourly=24, weekly=4, monthly=12), not from what the file says, so a policy
# that names only `daily` keeps far more than it appears to.
RETENTION_BLOCK = (
    "[global.retention]\n"
    'min = "0s"\n'
    "hourly = 0\ndaily = 1\nweekly = 0\nmonthly = 0\nyearly = 0\n\n"
)


def _retention(**kw):
    base = dict(min="0s", hourly=0, daily=1, weekly=0, monthly=0, yearly=0, keep=0)
    base.update(kw)
    return RetentionConfig(**base)


class _Snap:
    def __init__(self, name):
        self._name = name

    def get_name(self):
        return self._name


class _Endpoint:
    def __init__(self, label):
        self.label = label
        self.deleted = []

    def __repr__(self):
        return self.label


class TestTheSourceFateSharesWithEveryTarget:
    def _prune(self, prune_source):
        source = _Endpoint("source")
        errors = []
        volume = SimpleNamespace(path="/home", snapshot_prefix="home-", targets=[])
        config = SimpleNamespace(
            get_source_retention=lambda v: _retention(),
            get_target_retention=lambda v, t: _retention(),
            global_config=SimpleNamespace(timestamp_format=None),
        )
        planned = []

        def fake_plan(ep, retention, prefix, fmt):
            planned.append(ep.label)
            return [], [_Snap("home-20260101-000000")]

        import btrfs_backup_ng.cli.run as mod

        original_plan = mod.plan_endpoint_retention
        original_exec = mod.execute_retention_deletes
        mod.plan_endpoint_retention = fake_plan
        mod.execute_retention_deletes = lambda ep, to_delete: (len(to_delete), [])
        try:
            run_cli._prune_after_transfer(
                volume, config, source, [], errors, prune_source=prune_source
            )
        finally:
            mod.plan_endpoint_retention = original_plan
            mod.execute_retention_deletes = original_exec
        return planned

    def test_the_source_is_pruned_when_everything_succeeded(self):
        """Mutation guard: never pruning the source also passes the test below,
        and would make retention useless."""
        assert self._prune(prune_source=True) == ["source"]

    def test_the_source_is_not_pruned_when_a_target_failed(self):
        """The snapshots the failed target still owes have no lock holding them."""
        assert self._prune(prune_source=False) == []


class TestTheWiringItselfIsCorrect:
    """The function honours `prune_source`; this proves `_backup_volume` sets it.

    Testing the callee alone left the call site free to pass a constant -- which
    is precisely how the original defect survived: the protection existed and was
    never handed the information it needed.
    """

    def _drive(self, tmp_path, monkeypatch, target_succeeds):
        from btrfs_backup_ng.config.loader import load_config

        src = tmp_path / "src"
        (src / ".snapshots").mkdir(parents=True)
        dest = tmp_path / "dest"
        dest.mkdir()
        cfg = tmp_path / "config.toml"
        cfg.write_text(
            RETENTION_BLOCK
            + f'[[volumes]]\npath = "{src}"\nsnapshot_prefix = "home-"\n\n'
            + f'[[volumes.targets]]\npath = "{dest}"\n'
        )
        res = load_config(str(cfg))
        config = res[0] if isinstance(res, tuple) else res

        pruned = []
        monkeypatch.setattr(
            run_cli,
            "_prune_after_transfer",
            lambda *a, **kw: pruned.append(kw.get("prune_source")) or True,
        )
        monkeypatch.setattr(
            run_cli, "_transfer_to_target", lambda *a, **kw: target_succeeds
        )

        class _Src:
            def snapshot(self, **_kw):
                return _Snap("home-20260101-000000")

            def prepare(self):
                return None

            def __getattr__(self, _name):
                return lambda *a, **k: None

        monkeypatch.setattr(
            run_cli.endpoint, "choose_endpoint", lambda *a, **kw: _Src()
        )
        run_cli._backup_volume(config.volumes[0], config, 1)
        return pruned

    def test_a_successful_run_asks_for_the_source_to_be_pruned(
        self, tmp_path, monkeypatch
    ):
        assert self._drive(tmp_path, monkeypatch, target_succeeds=True) == [True]

    def test_a_failed_target_withholds_the_source_prune(self, tmp_path, monkeypatch):
        assert self._drive(tmp_path, monkeypatch, target_succeeds=False) == [False]
