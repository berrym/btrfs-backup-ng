"""R10d: prune confirmation gate + degenerate-policy guardrail.

Prune now runs a plan-then-execute flow: it computes the whole deletion plan, then (for a real
run) asks for a single confirmation on an interactive TTY unless --yes, and REFUSES a degenerate
policy (keeps only the latest snapshot) unless --force -- enforced even non-interactively.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from btrfs_backup_ng.cli.prune import _is_degenerate_policy, execute_prune
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.endpoint.local import LocalEndpoint


# --------------------------------------------------------------------------- #
# _is_degenerate_policy (pure)
# --------------------------------------------------------------------------- #
def test_degenerate_all_zero_buckets_with_tiny_min():
    """All buckets 0 AND a near-zero min (<=1d) keeps only the latest -> degenerate. Mutation
    guard: ignoring min (all-zero => degenerate) would misclassify a short-window policy."""
    assert _is_degenerate_policy(
        RetentionConfig(min="0s", hourly=0, daily=0, weekly=0, monthly=0, yearly=0)
    )
    assert _is_degenerate_policy(
        RetentionConfig(min="1d", hourly=0, daily=0, weekly=0, monthly=0, yearly=0)
    )


def test_not_degenerate_short_window_policy():
    """All buckets 0 but a real min window (30d) is a legitimate short-retention policy, NOT
    degenerate. Mutation guard: an all-zero-only check flags this."""
    assert not _is_degenerate_policy(
        RetentionConfig(min="30d", hourly=0, daily=0, weekly=0, monthly=0, yearly=0)
    )


def test_not_degenerate_with_a_bucket():
    """Any positive bucket count means real periodic retention -> not degenerate. Mutation guard:
    dropping the bucket check flags a normal policy."""
    assert not _is_degenerate_policy(
        RetentionConfig(min="1d", hourly=0, daily=7, weekly=0, monthly=0, yearly=0)
    )


# --------------------------------------------------------------------------- #
# execute_prune: gate + guardrail (drives the real CLI orchestration)
# --------------------------------------------------------------------------- #
def _make_config(tmp_path, retention_lines):
    """A minimal TOML config: one volume whose (empty) source .snapshots exists so the volume is
    not skipped, and one local target dir holding 10 prunable snapshots (2..11 days old)."""
    src = tmp_path / "src"
    (src / ".snapshots").mkdir(parents=True)
    dest = tmp_path / "dest"
    dest.mkdir()
    now = datetime.now()
    for d in range(2, 12):
        (dest / f"home-{(now - timedelta(days=d)).strftime('%Y%m%d-%H%M%S')}").mkdir()
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        f"[global.retention]\n{retention_lines}\n\n"
        f'[[volumes]]\npath = "{src}"\nsnapshot_prefix = "home-"\n\n'
        f'[[volumes.targets]]\npath = "{dest}"\n'
    )
    return cfg


def _args(cfg, **kw):
    return argparse.Namespace(
        config=str(cfg),
        dry_run=kw.get("dry_run", False),
        yes=kw.get("yes", False),
        force=kw.get("force", False),
        verbose=False,
        quiet=False,
        log_level=None,
    )


def _drive(
    tmp_path, retention_lines, monkeypatch, *, isatty, input_answer=None, **argkw
):
    """Run execute_prune with the delete primitive spied and stdin/confirmation controlled.
    Returns (exit_code, number_of_delete_calls, input_was_called)."""
    cfg = _make_config(tmp_path, retention_lines)
    deletes = []
    monkeypatch.setattr(
        LocalEndpoint,
        "delete_snapshots",
        lambda self, snaps, **k: deletes.append(list(snaps)),
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: isatty)
    monkeypatch.setattr(
        "btrfs_backup_ng.cli.prune._send_prune_notifications", lambda *a, **k: None
    )
    # Don't let the command reconfigure logging (it would detach caplog's handler).
    monkeypatch.setattr("btrfs_backup_ng.cli.prune.create_logger", lambda *a, **k: None)
    called = {"input": False}

    def fake_input(*_a):
        called["input"] = True
        return input_answer if input_answer is not None else ""

    monkeypatch.setattr("builtins.input", fake_input)
    rc = execute_prune(_args(cfg, **argkw))
    return rc, len(deletes), called["input"]


NORMAL = 'min = "1d"\nhourly = 0\ndaily = 3\nweekly = 0\nmonthly = 0\nyearly = 0'
DEGENERATE = 'min = "1d"\nhourly = 0\ndaily = 0\nweekly = 0\nmonthly = 0\nyearly = 0'


def test_degenerate_policy_refused_without_force(tmp_path, monkeypatch):
    """A degenerate policy (keeps only latest) is REFUSED with a non-zero exit and NO deletions,
    even non-interactively. Mutation guard: dropping the guardrail deletes almost everything."""
    rc, ndel, _ = _drive(tmp_path, DEGENERATE, monkeypatch, isatty=False, force=False)
    assert rc == 1
    assert ndel == 0


def test_degenerate_policy_proceeds_with_force(tmp_path, monkeypatch):
    """--force overrides the degenerate-policy refusal and the prune proceeds."""
    rc, ndel, _ = _drive(tmp_path, DEGENERATE, monkeypatch, isatty=False, force=True)
    assert ndel > 0  # deletions happened


def test_non_tty_proceeds_without_prompt(tmp_path, monkeypatch):
    """A normal policy on a non-interactive run (cron) proceeds without prompting. Mutation
    guard: requiring a prompt in non-TTY would hang or delete nothing here (input not called)."""
    rc, ndel, input_called = _drive(tmp_path, NORMAL, monkeypatch, isatty=False)
    assert rc == 0 and ndel > 0 and input_called is False


def test_tty_confirmation_decline_aborts(tmp_path, monkeypatch):
    """On a TTY without --yes, answering 'n' aborts with ZERO deletions. Mutation guard: not
    honoring the decline deletes anyway."""
    rc, ndel, input_called = _drive(
        tmp_path, NORMAL, monkeypatch, isatty=True, input_answer="n"
    )
    assert input_called is True
    assert ndel == 0


def test_tty_confirmation_accept_proceeds(tmp_path, monkeypatch):
    """On a TTY, answering 'y' proceeds with the deletions."""
    rc, ndel, input_called = _drive(
        tmp_path, NORMAL, monkeypatch, isatty=True, input_answer="y"
    )
    assert input_called is True
    assert ndel > 0


def test_yes_flag_skips_prompt(tmp_path, monkeypatch):
    """--yes proceeds without prompting even on a TTY (input never called). Mutation guard:
    ignoring --yes would prompt (input called) / block."""
    rc, ndel, input_called = _drive(
        tmp_path, NORMAL, monkeypatch, isatty=True, input_answer="n", yes=True
    )
    assert input_called is False
    assert (
        ndel > 0
    )  # proceeded despite the 'n' answer, because --yes skipped the prompt


def test_dry_run_never_deletes(tmp_path, monkeypatch):
    """--dry-run computes the plan and deletes nothing, regardless of policy."""
    rc, ndel, input_called = _drive(
        tmp_path, NORMAL, monkeypatch, isatty=True, dry_run=True
    )
    assert rc == 0 and ndel == 0 and input_called is False


def test_resolved_policy_log_includes_yearly(tmp_path, monkeypatch, caplog):
    """R10d/critic#10: the resolved-policy log line includes yearly (was omitted)."""
    import logging

    with caplog.at_level(logging.INFO):
        _drive(tmp_path, NORMAL, monkeypatch, isatty=False)
    assert any("yearly=" in r.getMessage() for r in caplog.records)
