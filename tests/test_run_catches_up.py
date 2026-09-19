"""`run` must send what a destination is MISSING, not only the newest snapshot.

Issue #104. `run` passed the snapshot it had just created to the planner as
`only=`, pinning the plan to that one snapshot. A target that missed a run --
drive unplugged, host down, a transfer that failed -- stayed behind for ever,
because every later run offered it only the newest snapshot. `transfer` has
always passed None and caught up, and snapper sources have always caught up, so
`run` disagreed with the rest of the tool and with itself.
"""

import inspect

import pytest

from btrfs_backup_ng.cli import run as run_cli


class _Recorder:
    """Stands in for sync_snapshots and records the planning argument."""

    def __init__(self):
        self.kwargs = None

    def __call__(self, *a, **kw):
        self.kwargs = kw
        from btrfs_backup_ng.core.operations import TransferResult

        return TransferResult()


@pytest.fixture
def recorded(monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(run_cli, "sync_snapshots", rec)
    return rec


def _target():
    from btrfs_backup_ng.config.schema import TargetConfig

    return TargetConfig(path="/backup")


def _call(newest_only):
    return dict(
        source_endpoint=object(),
        destination_endpoint=object(),
        target_config=_target(),
        snapshot="the-newest-snapshot",
        incremental=True,
        newest_only=newest_only,
    )


def test_by_default_the_planner_is_asked_for_everything_missing(recorded):
    run_cli._transfer_to_target(**_call(newest_only=False))

    assert recorded.kwargs is not None, "sync_snapshots was never called"
    assert recorded.kwargs["snapshot"] is None, (
        "run pinned the plan to the snapshot it just created, so a destination "
        "that missed a run can never catch up"
    )


def test_newest_only_still_pins_the_plan_to_that_snapshot(recorded):
    """The pre-0.9.8 behaviour stays reachable, just no longer the default."""
    run_cli._transfer_to_target(**_call(newest_only=True))

    assert recorded.kwargs["snapshot"] == "the-newest-snapshot"


def test_the_flag_exists_on_the_run_command():
    from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

    parser = create_subcommand_parser()
    args = parser.parse_args(["run", "--newest-only"])

    assert args.newest_only is True


def test_the_flag_defaults_to_off():
    from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

    args = create_subcommand_parser().parse_args(["run"])

    assert getattr(args, "newest_only", False) is False


@pytest.mark.parametrize("func", ["_backup_volume", "_transfer_to_target"])
def test_both_functions_accept_the_choice(func):
    """It has to reach the transfer, not stop at the CLI."""
    sig = inspect.signature(getattr(run_cli, func))

    assert "newest_only" in sig.parameters


def test_every_transfer_path_threads_it():
    """run has a sequential and a ThreadPoolExecutor path; a flag wired into
    only one of them works until someone sets parallel_targets."""
    source = inspect.getsource(run_cli)
    assert source.count("newest_only,") >= 4, (
        "newest_only is not passed at every call site; one of the parallel or "
        "sequential paths still drops it"
    )
