"""`prune` must not run blind.

Two gaps in the one command that deletes. It performed no `require_mount` check
at all, making it the only command that would still operate inside the empty
mount-point directory of a drive that is not connected. And a missing source
snapshot directory skipped the volume's TARGETS as well as its source, so a
volume whose snapshot directory had been removed left its backups unpruned
forever while `prune` reported success.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from btrfs_backup_ng.cli.prune import execute_prune
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.endpoint.local import LocalEndpoint

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


class TestPruneHonoursRequireMount:
    def _config(self, tmp_path, require_mount):
        src = tmp_path / "src"
        (src / ".snapshots").mkdir(parents=True)
        dest = tmp_path / "dest"
        dest.mkdir()
        now = datetime.now()
        for day in range(2, 8):
            name = f"home-{(now - timedelta(days=day)).strftime('%Y%m%d-%H%M%S')}"
            (src / ".snapshots" / name).mkdir()
            (dest / name).mkdir()
        cfg = tmp_path / "config.toml"
        cfg.write_text(
            RETENTION_BLOCK
            + f'[[volumes]]\npath = "{src}"\nsnapshot_prefix = "home-"\n\n'
            f'[[volumes.targets]]\npath = "{dest}"\n'
            + (f"require_mount = {require_mount}\n" if require_mount else "")
        )
        return cfg, dest

    def _run(self, cfg, monkeypatch):
        deleted = {}

        def record(self, snaps, **_k):
            key = str(self.config.get("path", "?"))
            deleted.setdefault(key, []).extend(s.get_name() for s in snaps)

        monkeypatch.setattr(LocalEndpoint, "delete_snapshots", record)
        monkeypatch.setattr("sys.stdin.isatty", lambda: False)
        monkeypatch.setattr(
            "btrfs_backup_ng.cli.prune._send_prune_notifications", lambda *a, **k: None
        )
        monkeypatch.setattr(
            "btrfs_backup_ng.cli.prune.create_logger", lambda *a, **k: None
        )
        rc = execute_prune(
            argparse.Namespace(
                config=str(cfg),
                dry_run=False,
                yes=True,
                force=False,
                verbose=False,
                quiet=False,
                log_level=None,
            )
        )
        return rc, deleted

    def test_a_target_that_is_not_a_mount_point_is_refused(self, tmp_path, monkeypatch):
        """`prune` was the one command with no mount gate, so it would delete
        inside the mount-point directory of a drive that is not connected."""
        cfg, dest = self._config(tmp_path, "true")
        rc, deleted = self._run(cfg, monkeypatch)
        assert deleted.get(str(dest), []) == [], (
            "prune deleted from a target the mount gate refuses"
        )
        assert rc != 0

    def test_a_target_without_require_mount_is_unaffected(self, tmp_path, monkeypatch):
        """Mutation guard: refusing every target would also pass the test above."""
        cfg, dest = self._config(tmp_path, None)
        _rc, deleted = self._run(cfg, monkeypatch)
        assert deleted.get(str(dest)), "an unguarded target must still be pruned"


class TestAMissingSnapshotDirDoesNotSkipTheTargets:
    def test_targets_are_pruned_when_the_source_dir_is_gone(
        self, tmp_path, monkeypatch
    ):
        """`continue` skipped the whole volume, so a volume whose snapshot
        directory had been removed left its backups unpruned forever while
        `prune` reported success."""
        src = tmp_path / "src"
        src.mkdir()  # note: no .snapshots
        dest = tmp_path / "dest"
        dest.mkdir()
        now = datetime.now()
        for day in range(2, 8):
            (
                dest / f"home-{(now - timedelta(days=day)).strftime('%Y%m%d-%H%M%S')}"
            ).mkdir()
        cfg = tmp_path / "config.toml"
        cfg.write_text(
            RETENTION_BLOCK
            + f'[[volumes]]\npath = "{src}"\nsnapshot_prefix = "home-"\n\n'
            f'[[volumes.targets]]\npath = "{dest}"\n'
        )
        deleted = {}

        def record(self, snaps, **_k):
            deleted.setdefault(str(self.config.get("path", "?")), []).extend(
                s.get_name() for s in snaps
            )

        monkeypatch.setattr(LocalEndpoint, "delete_snapshots", record)
        monkeypatch.setattr("sys.stdin.isatty", lambda: False)
        monkeypatch.setattr(
            "btrfs_backup_ng.cli.prune._send_prune_notifications", lambda *a, **k: None
        )
        monkeypatch.setattr(
            "btrfs_backup_ng.cli.prune.create_logger", lambda *a, **k: None
        )
        execute_prune(
            argparse.Namespace(
                config=str(cfg),
                dry_run=False,
                yes=True,
                force=False,
                verbose=False,
                quiet=False,
                log_level=None,
            )
        )
        assert deleted.get(str(dest)), (
            "the target was skipped because the source snapshot dir was missing"
        )
