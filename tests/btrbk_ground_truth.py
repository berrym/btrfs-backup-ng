"""What btrbk 0.32.7 keeps, measured on real btrfs.

One fixed set of dated snapshots, one dry run per retention variant
(``btrbk -c <conf> -n -S run``), and the names btrbk marked for deletion read
off its action list. The measurement ran on 2026-09-23 at 19:19 local time on
a loopback rig: a ``data`` subvolume, thirteen read-only snapshots of it named
``data.<timestamp>`` in btrbk's ``long`` format, and for the target-side
variants a real ``btrbk run`` + ``resume`` first, so the target held every
snapshot. The dry run itself "creates" one more snapshot (``NEW`` below) and
sends it, and btrbk's ``latest`` is that one.

``tests/test_btrbk_import_retention_semantics.py`` reads this table;
``tests/integration/tier2/test_btrbk_retention_ground_truth_real.py``
re-measures it against the installed binary where root, btrfs and btrbk are
available, so a btrbk that changes its mind is reported rather than silently
agreed with.
"""

from __future__ import annotations

from datetime import datetime

#: The clock the measurement ran at. Every variant below is relative to it.
NOW = datetime(2026, 9, 23, 19, 19, 0)

#: btrbk ``timestamp_format long``.
FORMAT = "%Y%m%dT%H%M"

#: The snapshot btrbk's dry run creates and transfers during the measurement.
NEW = "20260923T1919"

#: The thirteen pre-existing snapshots, newest first.
EXISTING = [
    "20260923T1000",
    "20260923T0900",
    "20260923T0800",
    "20260922T0300",
    "20260921T0300",
    "20260920T0300",
    "20260915T0300",
    "20260910T0300",
    "20260901T0300",
    "20260815T0300",
    "20260701T0300",
    "20260101T0300",
    "20250601T0300",
]

ALL = frozenset(EXISTING) | {NEW}

#: Source side: ``snapshot_preserve`` / ``snapshot_preserve_min`` lines and the
#: set of names btrbk KEPT (everything not in its ``---`` delete list).
SOURCE_KEEPS: dict[tuple[str, ...], frozenset[str]] = {
    (): ALL,
    ("snapshot_preserve 2d",): ALL,
    ("snapshot_preserve_min latest",): frozenset({NEW}),
    ("snapshot_preserve_min 2d",): frozenset(
        {NEW, "20260923T1000", "20260923T0900", "20260923T0800", "20260922T0300"}
    )
    | {"20260921T0300"},
    ("snapshot_preserve_min 2d", "snapshot_preserve 3d 2w"): frozenset(
        {
            NEW,
            "20260923T1000",
            "20260923T0900",
            "20260923T0800",
            "20260922T0300",
            "20260921T0300",
            "20260920T0300",
            "20260915T0300",
            "20260910T0300",
        }
    ),
    ("snapshot_preserve_min 3m",): ALL - {"20260101T0300", "20250601T0300"},
    ("snapshot_preserve *d", "snapshot_preserve_min latest"): ALL
    - {"20260923T1000", "20260923T0900"},
    ("snapshot_preserve 14d 8w *m", "snapshot_preserve_min latest"): ALL
    - {"20260923T1000", "20260923T0900"},
    ("snapshot_preserve no", "snapshot_preserve_min latest"): frozenset({NEW}),
}

#: Target side, measured with ``snapshot_preserve_min all`` on the source so
#: only the target policy decides. ``LATEST_ON_TARGET`` is the newest backup
#: the target held before the dry run; btrbk preserved it under
#: ``target_preserve_min no`` (it is the incremental parent of what the run
#: sends), and deleted it under ``target_preserve_min latest`` once the run's
#: own transfer became the latest.
LATEST_ON_TARGET = "20260923T1919"
TARGET_KEEPS: dict[tuple[str, ...], frozenset[str]] = {
    (): ALL,
    ("target_preserve 2d",): ALL,
    ("target_preserve_min no", "target_preserve 2d"): frozenset(
        {NEW, "20260923T0800", "20260922T0300", "20260921T0300"}
    ),
    ("target_preserve_min no",): frozenset({NEW}),
    ("target_preserve_min no", "target_preserve no"): frozenset({NEW}),
    ("target_preserve_min latest", "target_preserve no"): frozenset({NEW}),
    ("target_preserve_min 2d",): frozenset(
        {
            NEW,
            "20260923T1000",
            "20260923T0900",
            "20260923T0800",
            "20260922T0300",
            "20260921T0300",
        }
    ),
    ("target_preserve_min 1w",): frozenset(
        {
            NEW,
            "20260923T1000",
            "20260923T0900",
            "20260923T0800",
            "20260922T0300",
            "20260921T0300",
            "20260920T0300",
            "20260915T0300",
        }
    ),
    ("target_preserve_min 0d",): frozenset(
        {NEW, "20260923T1000", "20260923T0900", "20260923T0800"}
    ),
}
