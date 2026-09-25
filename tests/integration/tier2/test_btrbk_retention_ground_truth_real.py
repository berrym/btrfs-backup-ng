"""Tier 2: the imported retention against the installed btrbk, live.

``tests/btrbk_ground_truth.py`` records what btrbk 0.32.7 kept for one set
of dated snapshots at one clock; the unit tests of the importer read that
table. This drives the same set through the real binary NOW -- a loopback
btrfs with the thirteen read-only snapshots re-dated so the newest is today,
one ``btrbk -n -S run`` per variant -- and holds this tool's engine, fed the
imported policy at the same clock, to the same agreement the unit tests
assert against the table:

- EVERY variant keeps at least what btrbk keeps: a snapshot btrbk keeps and
  this tool deletes is a defect in the importer or the engine, whatever the
  clock;
- the variants whose meaning coincides keep EXACTLY what btrbk keeps;
- the others keep MORE, by design: a policy this tool refuses to prune under
  (btrbk: keep only the newest) gets the default schedule, and btrbk's
  inclusive numbers are written one higher (``2d`` -> ``3d``, ``2d`` ->
  ``daily = 3``), which contains btrbk's window from any position in the
  day, week or month.

Needs root, btrfs and btrbk on the runner; skips otherwise and says so.
"""

from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from btrfs_backup_ng.btrbk_import import convert_to_toml, parse_btrbk_config
from btrfs_backup_ng.config.loader import load_config
from btrfs_backup_ng.retention import apply_retention
from tests import btrbk_ground_truth as truth

from .conftest import requires_btrfs

requires_btrbk = pytest.mark.skipif(
    shutil.which("btrbk") is None, reason="the btrbk binary is not installed"
)

pytestmark = [pytest.mark.tier2, requires_btrfs, requires_btrbk]

#: The variants whose imported meaning is btrbk's at every clock: nothing
#: set, and a schedule under the default ``all`` minimum -- keep everything.
#: These keep EXACTLY what btrbk keeps. Every other variant contains a number
#: the import writes one higher, a policy it refuses and widens, or a
#: same-day snapshot that is future-dated before 10:00 and quarantined here,
#: and keeps MORE at some clocks -- never less.
EXACT = {
    (),
    ("snapshot_preserve 2d",),
    ("target_preserve 2d",),
}


def _shift(stamp: str, days: int) -> str:
    when = datetime.strptime(stamp, truth.FORMAT)
    return (when + timedelta(days=days)).strftime(truth.FORMAT)


def _sh(*argv: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(list(argv), capture_output=True, text=True, check=check)


def _rig(source: Path, target: Path) -> tuple[list[str], int]:
    """The thirteen snapshots on ``source``, re-dated so the newest is today;
    returns the shifted names and the shift in days."""
    data = source / "data"
    _sh("btrfs", "subvolume", "create", str(data))
    (data / "payload").write_bytes(b"x" * 4096)
    snaps = source / "snaps"
    snaps.mkdir()
    days = (datetime.now().date() - truth.NOW.date()).days
    names = []
    for stamp in truth.EXISTING:
        moved = _shift(stamp, days)
        _sh(
            "btrfs",
            "subvolume",
            "snapshot",
            "-r",
            str(data),
            str(snaps / f"data.{moved}"),
        )
        names.append(moved)
    (target / "bk").mkdir()
    return names, days


def _btrbk_conf(source: Path, target: Path, lines: tuple[str, ...]) -> str:
    return (
        "timestamp_format long\n"
        + "".join(f"{line}\n" for line in lines)
        + f"volume {source}\n  snapshot_dir snaps\n  subvolume data\n"
        f"    target send-receive {target / 'bk'}\n"
    )


def _btrbk_kept(conf: Path, present: set[str]) -> tuple[set[str], set[str]]:
    """``(kept, created)``: the names of ``present`` btrbk's dry run did NOT
    mark for deletion, and the snapshot the run itself creates (``+++``) --
    which is btrbk's "latest" and must be in the set this tool judges too,
    or this tool's newest is the previously-newest and the two disagree about
    exactly the snapshot ``latest`` retires. The recorded table includes it
    as ``NEW`` for the same reason."""
    out = _sh("btrbk", "-c", str(conf), "-n", "-S", "run", check=False)
    text = out.stdout + out.stderr
    deleted = {
        line.split("data.", 1)[1].strip()
        for line in text.splitlines()
        if line.startswith("--- ") and "data." in line
    }
    created = {
        line.split("data.", 1)[1].strip()
        for line in text.splitlines()
        if line.startswith("+++ ") and "data." in line
    }
    # The dry run creates a snapshot it does not send, so the newest snapshot
    # the two sides already share stays the incremental parent of that
    # transfer and btrbk preserves it for that reason alone ("preserve forced:
    # latest common target"), outside its retention policy. A real run sends
    # first and the sent snapshot becomes the parent; this tool prunes after
    # sending, so its "latest" is that one. The schedule table names the
    # reason per snapshot; a snapshot kept only as the pending transfer's
    # parent is not a retention decision to hold this tool to.
    chain_parent = {
        line.split("data.", 1)[1].split()[0]
        for line in text.splitlines()
        if "preserve forced: latest common" in line and "data." in line
    }
    return (present | created) - deleted - chain_parent, created


def _ours_kept(
    conf_text: str, present: set[str], tmp_path: Path, target_side: bool
) -> set[str]:
    """What this tool keeps of ``present`` under the IMPORTED policy, at the
    clock btrbk just ran at."""
    toml, _ = convert_to_toml(parse_btrbk_config(conf_text))
    path = tmp_path / "imported.toml"
    path.write_text(toml)
    config, _ = load_config(path)
    volume = config.volumes[0]
    retention = (
        config.get_target_retention(volume, volume.targets[0])
        if target_side
        else config.get_effective_retention(volume)
    )
    keep, _ = apply_retention(
        sorted("data." + n for n in present),
        retention,
        prefix="data.",
        timestamp_format=truth.FORMAT,
        now=datetime.now(),
    )
    return {n[len("data.") :] for n in keep}


def _compare(variants, tmp_path, source, target, present, target_side):
    """Run every variant through both; return {lines: (btrbk_kept, ours_kept)}."""
    results = {}
    for lines in variants:
        prefix = ("snapshot_preserve_min all",) if target_side else ()
        text = _btrbk_conf(source, target, prefix + lines)
        conf = tmp_path / "v.conf"
        conf.write_text(text)
        theirs, created = _btrbk_kept(conf, present)
        results[lines] = (
            theirs,
            _ours_kept(text, present | created, tmp_path, target_side),
        )
    return results


def _judge(results, present):
    """Every variant keeps at least what btrbk keeps; the EXACT ones keep
    exactly that. Returns the variants that kept more, with what."""
    keeps_less = {
        lines: theirs - ours
        for lines, (theirs, ours) in results.items()
        if theirs - ours
    }
    assert not keeps_less, f"kept by btrbk and deleted here: {keeps_less}"
    more = {}
    for lines, (theirs, ours) in results.items():
        if lines in EXACT:
            assert ours == theirs, (lines, "ours only:", ours - theirs)
        elif ours != theirs:
            more[lines] = sorted(ours - theirs)
    return more


class TestTheImportedPolicyAgainstBtrbk:
    def test_source_side(self, btrfs_source_and_dest, tmp_path):
        source, target = btrfs_source_and_dest
        names, _days = _rig(source, target)
        present = set(names)
        results = _compare(
            list(truth.SOURCE_KEEPS), tmp_path, source, target, present, False
        )
        more = _judge(results, present)
        print("source variants keeping more than btrbk:", more)

    def test_target_side(self, btrfs_source_and_dest, tmp_path):
        source, target = btrfs_source_and_dest
        _rig(source, target)
        # Populate the target for real: one run (a new snapshot, sent), then
        # resume (every older snapshot sent).
        populate = tmp_path / "populate.conf"
        populate.write_text(_btrbk_conf(source, target, ()))
        _sh("btrbk", "-c", str(populate), "run", check=False)
        _sh("btrbk", "-c", str(populate), "resume", check=False)
        present = {p.name.split("data.", 1)[1] for p in (target / "bk").iterdir()}
        assert len(present) >= len(truth.EXISTING), "the target was not populated"
        results = _compare(
            list(truth.TARGET_KEEPS), tmp_path, source, target, present, True
        )
        more = _judge(results, present)
        print("target variants keeping more than btrbk:", more)


def test_the_shift_keeps_the_time_of_day():
    assert _shift("20260923T1000", 104) == "20270105T1000"
