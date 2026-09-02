"""`prune` must resolve retention per endpoint, exactly as `run` does.

Per-scope retention (``source_retention`` and a per-target ``retention``) was
wired into the run pipeline but not into the standalone ``prune`` command, the
snapper prune phase, or ``run --dry-run``. Those three resolved one policy per
volume, so ``prune`` deleted snapshots that ``run`` on the same config keeps --
silently, and with a policy line that never mentioned the count.

The discriminator throughout is that the source and the target are given
policies that keep DIFFERENT numbers of snapshots. A single shared policy cannot
produce two different counts, so any regression to volume-wide resolution fails
these tests rather than merely changing a log line.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from btrfs_backup_ng.cli.prune import execute_prune
from btrfs_backup_ng.config.loader import load_config
from btrfs_backup_ng.endpoint.local import LocalEndpoint

SNAPSHOT_COUNT = 10


def _make_config(tmp_path, body: str):
    """One volume with SNAPSHOT_COUNT snapshots on the source AND on the target."""
    src = tmp_path / "src"
    snaps = src / ".snapshots"
    snaps.mkdir(parents=True)
    dest = tmp_path / "dest"
    dest.mkdir()
    now = datetime.now()
    for day in range(2, 2 + SNAPSHOT_COUNT):
        name = f"home-{(now - timedelta(days=day)).strftime('%Y%m%d-%H%M%S')}"
        (snaps / name).mkdir()
        (dest / name).mkdir()
    cfg = tmp_path / "config.toml"
    cfg.write_text(body.format(src=src, dest=dest))
    return cfg, src, dest


def _drive(cfg, monkeypatch, **argkw):
    """Run execute_prune, recording deletions per endpoint path."""
    deleted: dict[str, list[str]] = {}

    def record(self, snaps, **_k):
        key = str(self.config.get("path", "?"))
        deleted.setdefault(key, []).extend(s.get_name() for s in snaps)

    monkeypatch.setattr(LocalEndpoint, "delete_snapshots", record)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    monkeypatch.setattr(
        "btrfs_backup_ng.cli.prune._send_prune_notifications", lambda *a, **k: None
    )
    monkeypatch.setattr("btrfs_backup_ng.cli.prune.create_logger", lambda *a, **k: None)
    args = argparse.Namespace(
        config=str(cfg),
        dry_run=False,
        yes=True,
        force=argkw.get("force", False),
        verbose=False,
        quiet=False,
        log_level=None,
    )
    return execute_prune(args), deleted


def _counts(deleted, src, dest):
    src_key = str((src / ".snapshots").resolve())
    return len(deleted.get(src_key, [])), len(deleted.get(str(dest), []))


SCOPED = """
[global.retention]
min = "0s"
daily = 1

[[volumes]]
path = "{src}"
snapshot_prefix = "home-"

[volumes.source_retention]
keep = 8

[[volumes.targets]]
path = "{dest}"

[volumes.targets.retention]
keep = 2
"""


def test_source_and_target_prune_under_their_own_policies(tmp_path, monkeypatch):
    """keep=8 on the source and keep=2 on the target must delete 2 and 8.

    Mutation guard: resolving one policy per volume gives both endpoints the
    global daily=1 policy, which deletes 9 from each -- neither expected count.
    """
    cfg, src, dest = _make_config(tmp_path, SCOPED)
    rc, deleted = _drive(cfg, monkeypatch)
    src_deleted, dest_deleted = _counts(deleted, src, dest)
    assert rc == 0
    assert src_deleted == SNAPSHOT_COUNT - 8, f"source deleted {src_deleted}, want 2"
    assert dest_deleted == SNAPSHOT_COUNT - 2, f"target deleted {dest_deleted}, want 8"


def test_prune_agrees_with_the_run_pipeline_on_the_same_config(tmp_path):
    """The two commands share plan_endpoint_retention so they cannot disagree
    about a policy; they must also not disagree about WHICH policy applies."""
    cfg, _src, _dest = _make_config(tmp_path, SCOPED)
    res = load_config(str(cfg))
    config = res[0] if isinstance(res, tuple) else res
    volume = config.volumes[0]
    target = volume.targets[0]
    assert config.get_source_retention(volume).keep == 8
    assert config.get_target_retention(volume, target).keep == 2
    # The volume-wide resolution prune used to apply is neither of them.
    assert config.get_effective_retention(volume).keep == 0


DEGENERATE_SOURCE = """
[global.retention]
min = "1d"
daily = 0

[[volumes]]
path = "{src}"
snapshot_prefix = "home-"

[volumes.source_retention]
min = "1d"
hourly = 0
daily = 0
weekly = 0
monthly = 0
yearly = 0

[[volumes.targets]]
path = "{dest}"

[volumes.targets.retention]
keep = 2
"""


def test_a_degenerate_source_policy_stops_the_source_only(tmp_path, monkeypatch):
    """The guardrail is per endpoint now that the policies are.

    Mutation guard: a volume-wide gate cancels the healthy target too (0 deleted
    there), and a gate applied to the wrong scope lets the source through.
    """
    cfg, src, dest = _make_config(tmp_path, DEGENERATE_SOURCE)
    rc, deleted = _drive(cfg, monkeypatch)
    src_deleted, dest_deleted = _counts(deleted, src, dest)
    assert rc != 0, "a refused endpoint must fail the command"
    assert src_deleted == 0, "the degenerate source must not be pruned"
    assert dest_deleted == SNAPSHOT_COUNT - 2, (
        f"the target's own policy is healthy and must still apply; "
        f"deleted {dest_deleted}"
    )


def test_dry_run_reports_each_scope_and_does_not_cry_degenerate(tmp_path, capsys):
    """`run --dry-run` reported the volume policy, so a config whose every scope
    is healthy could be announced as 'WOULD FAIL -- keeps only the latest'."""
    from btrfs_backup_ng.cli import run as run_cli

    cfg, _src, _dest = _make_config(tmp_path, SCOPED)
    res = load_config(str(cfg))
    config = res[0] if isinstance(res, tuple) else res
    run_cli._dry_run(config)
    out = capsys.readouterr().out
    assert "keep=8" in out, "the source policy must be the one reported"
    assert "keep=2" in out, "each target's own policy must be reported"
    assert "WOULD FAIL" not in out, (
        "every scope here is healthy; the volume policy is not what governs"
    )
