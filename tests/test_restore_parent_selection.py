"""Choosing an incremental parent, and proving a backup before streaming it.

Restoring reads the destination under the prefix the SOURCE ended up with, then
picks a parent from what is there. Getting that wrong is expensive in both
directions: too strict and every restore silently becomes a full transfer, too
loose and a snapshot from another volume is offered as the parent for a `btrfs
send -p` with no shared history.

Raw is where it is hardest. RawSnapshot reports no prefix at all, so relatedness
falls back to what the NAMES imply, via the same split every listing uses.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core import restore as core_restore


def _real_snapshots(stamps, prefix="snap-"):
    """Real __util__.Snapshot objects, not the test double.

    MockSnapshot.find_parent returns only strictly-older candidates; the real
    one falls back to the oldest present when nothing older exists, which is the
    whole case under test here. A test built on the double cannot reach it.
    """
    import time as _time
    from pathlib import Path

    from btrfs_backup_ng.__util__ import Snapshot

    return [
        Snapshot(
            Path("/backup"), prefix, None, time_obj=_time.strptime(t, "%Y%m%d-%H%M%S")
        )
        for t in stamps
    ]


def _raw_snapshots(stamps, prefix="home-"):
    """RawSnapshots as discovery produces them: named, and really dated.

    Constructed bare, ``created`` defaults to now() for every instance, so two
    of them compare EQUAL and any ordering test passes or fails by accident.
    """
    from datetime import datetime, timezone
    from pathlib import Path

    from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

    out = []
    for t in stamps:
        dt = datetime.strptime(t, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
        out.append(
            RawSnapshot(
                name=f"{prefix}{t}",
                stream_path=Path(f"/backup/{prefix}{t}.btrfs"),
                created=dt,
            )
        )
    return out


class TestReverseIncrementalsAreAllowedAgain:
    """Only UNRELATED parents are refused, not merely newer ones."""

    def test_a_newer_related_parent_is_used(self):
        """btrfs send -p accepts a newer related snapshot, and it is usually a
        far smaller transfer than a full send. Banning it outright turned
        working incremental restores into full ones."""
        older, newer = _real_snapshots(["20260101-100000", "20260102-100000"])
        backup = MagicMock()
        backup.correspondent_of.return_value = None

        parent = core_restore._choose_parent([older, newer], [newer], older, backup)
        assert parent is not None and parent.get_name() == newer.get_name(), (
            "a valid reverse incremental was refused"
        )

    def test_a_parent_from_another_volume_is_refused(self):
        """A different prefix means a different volume: no shared history, so no
        delta to send. Comparing across prefixes is also what Snapshot.__lt__
        refuses to do, raising NotImplementedError."""
        (mine,) = _real_snapshots(["20260101-100000"], prefix="home-")
        (theirs,) = _real_snapshots(["20260102-100000"], prefix="other-")

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        assert (
            core_restore._choose_parent([mine, theirs], [theirs], mine, backup) is None
        )

    def test_choosing_a_parent_never_raises_across_prefixes(self):
        """Snapshot.__lt__ raises NotImplementedError across prefixes, which used
        to abort the whole restore for raw sources."""
        (mine,) = _real_snapshots(["20260101-100000"], prefix="home-")
        (theirs,) = _real_snapshots(["20260102-100000"], prefix="")

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        core_restore._choose_parent([mine, theirs], [theirs], mine, backup)


class TestRawSourcesKeepTheirIncrementals:
    """FIX 4's prefix filter silently disabled incremental restore for raw.

    RawSnapshot defaults ``prefix`` to "" and discover_raw_snapshots never sets
    it, while a btrfs source Snapshot carries e.g. 'home-'. An exact-equality
    filter therefore discarded every raw destination snapshot and turned every
    prefixed raw restore into a full transfer -- reported as an ordinary success.
    That inverts this branch's purpose on the very path it exists to fix.
    """

    def test_a_raw_destination_snapshot_can_still_be_a_parent(self):
        import time as _time
        from pathlib import Path

        from btrfs_backup_ng.__util__ import Snapshot
        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        source = Snapshot(
            Path("/backup"),
            "home-",
            None,
            time_obj=_time.strptime("20240102-120000", "%Y%m%d-%H%M%S"),
        )
        present = RawSnapshot(
            name="home-20240101-120000", stream_path=Path("/backup/x.btrfs")
        )
        assert present.prefix == "", "fixture no longer models the defect"

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        parent = core_restore._choose_parent([source], [present], source, backup)
        assert parent is not None, (
            "a raw destination snapshot was discarded, so the restore silently "
            "degraded to a full transfer"
        )

    def test_an_unrelated_raw_snapshot_is_still_refused(self):
        """Tolerating an UNKNOWN prefix must not become tolerating anything.

        Raw snapshots all report prefix "", so accepting on "cannot tell" alone
        handed back a stream from an entirely different volume as the parent --
        a `btrfs send -p` against a subvolume with no shared history. The name
        still carries the volume, and raw correspondence is by name anyway.
        """
        import time as _time
        from pathlib import Path

        from btrfs_backup_ng.__util__ import Snapshot
        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        source = Snapshot(
            Path("/backup"),
            "home-",
            None,
            time_obj=_time.strptime("20240102-120000", "%Y%m%d-%H%M%S"),
        )
        foreign = RawSnapshot(
            name="database-20240101-120000", stream_path=Path("/backup/db.btrfs")
        )
        assert foreign.prefix == "", "fixture no longer models a raw snapshot"

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        assert (
            core_restore._choose_parent([source], [foreign], source, backup) is None
        ), "a stream from another volume was offered as an incremental parent"

    def test_a_genuine_mismatch_is_still_refused(self):
        """Tolerating UNKNOWN must not tolerate KNOWN-and-different."""
        (mine,) = _real_snapshots(["20240101-120000"], prefix="home-")
        (theirs,) = _real_snapshots(["20240102-120000"], prefix="other-")
        backup = MagicMock()
        backup.correspondent_of.return_value = None
        assert (
            core_restore._choose_parent([mine, theirs], [theirs], mine, backup) is None
        )

    def test_an_unorderable_pair_degrades_instead_of_aborting(self):
        """Keeping unknown-prefix snapshots means find_parent can still meet a
        genuine mismatch and raise. A restore must not abort for that: no parent
        is a full send, which always works."""
        (mine,) = _real_snapshots(["20240101-120000"], prefix="home-")
        (odd,) = _real_snapshots(["20240102-120000"], prefix="")
        backup = MagicMock()
        backup.correspondent_of.return_value = None
        core_restore._choose_parent([mine, odd], [odd], mine, backup)


class TestARawSnapshotIsRestorable:
    """The actual raw restore path: a RawSnapshot as the thing BEING restored.

    The earlier raw tests put a RawSnapshot in the `present` list only, i.e. as
    a parent CANDIDATE, with a btrfs Snapshot as the target. That is the mirror
    image of a raw restore and it exercises a configuration that does not occur.
    B1 exists because "every prefixed raw restore" silently became a full
    transfer, so the restored snapshot has to be the raw one.
    """

    def test_a_raw_restore_still_finds_its_incremental_parent(self):
        older, newer = _raw_snapshots(["20240101-120000", "20240102-120000"])
        assert older < newer, "fixture does not order -- created was not set"
        assert newer.prefix == "", "fixture no longer models a raw snapshot"

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        parent = core_restore._choose_parent([older, newer], [older], newer, backup)
        assert parent is not None and parent.get_name() == older.get_name(), (
            "a raw restore degraded to a full transfer, which is the regression "
            "B1 was written to remove"
        )

    def test_a_raw_restore_refuses_a_parent_from_another_volume(self):
        (target,) = _raw_snapshots(["20240102-120000"], prefix="home-")
        (foreign,) = _raw_snapshots(["20240101-120000"], prefix="database-")

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        parent = core_restore._choose_parent(
            [target, foreign], [foreign], target, backup
        )
        assert parent is None, (
            f"a stream from another volume was offered as the parent for a raw "
            f"restore: {parent.get_name() if parent else None}"
        )


class TestUnorderableSnapshotsDegradeInsteadOfAborting:
    """B2's swallow is load-bearing on the raw path and was untested.

    Keeping unknown-prefix snapshots (B1) means find_parent can still meet a
    genuine mismatch. Snapshot.__lt__ raises NotImplementedError across
    prefixes; a comparison against something with no usable time raises
    TypeError. Neither may abort a restore: no parent is a full send, which
    always works.
    """

    def test_a_prefix_mismatch_degrades_to_a_full_send(self):
        (mine,) = _real_snapshots(["20240101-120000"], prefix="home-")
        (odd,) = _real_snapshots(["20240102-120000"], prefix="")
        backup = MagicMock()
        backup.correspondent_of.return_value = None
        assert core_restore._choose_parent([mine, odd], [odd], mine, backup) is None, (
            "an unorderable pair produced a parent instead of a full send"
        )

    def test_a_comparison_that_raises_TypeError_degrades_too(self):
        """The TypeError arm is reachable independently of NotImplementedError."""

        class _Unorderable:
            prefix = ""

            def get_name(self):
                return "home-20240101-120000"

            def __lt__(self, other):
                raise TypeError("cannot order this")

            def __gt__(self, other):
                raise TypeError("cannot order this")

        (mine,) = _real_snapshots(["20240102-120000"], prefix="home-")
        backup = MagicMock()
        backup.correspondent_of.return_value = None
        # Must not raise; the restore continues as a full send.
        core_restore._choose_parent([mine], [_Unorderable()], mine, backup)


class TestTheBackupIsProvenBeforeStreaming:
    """A restore asks whether the backup can be delivered before it starts.

    Every read-side check -- a corrupt stream, a missing decompressor, an
    unsupported cipher -- otherwise lives inside ``send``, so the failure
    surfaces only once the transfer is under way. `preflight_send` runs the same
    checks first, which turns a late failure into an early, clearer one.

    This mattered far more when `--overwrite` existed, because those checks ran
    AFTER it had deleted the copy being replaced -- a corrupt backup then cost
    the operator their last good copy. That feature is not in this release; the
    early check is kept because failing before a transfer beats failing during
    one.
    """

    def _snapshot(self):
        from tests.test_restore import make_snapshots

        return make_snapshots([("snap-1", "20260101-100000")])[0]

    def test_an_undeliverable_backup_stops_before_the_transfer(self):
        from unittest.mock import patch

        import pytest

        from btrfs_backup_ng.core.restore import RestoreError

        sent = []
        backup = MagicMock()
        backup.config = {"path": "/backup"}
        backup.preflight_send.side_effect = RestoreError("the stored stream is CORRUPT")

        destination = MagicMock()
        destination.config = {"path": "/dest"}
        destination.list_snapshots.return_value = []

        with patch.object(
            core_restore, "send_snapshot", lambda *a, **k: sent.append(1)
        ):
            with patch.object(core_restore, "log_transaction", lambda **k: None):
                with pytest.raises(RestoreError, match="CORRUPT"):
                    core_restore.restore_snapshot(backup, destination, self._snapshot())
        assert sent == [], "the transfer started despite an undeliverable backup"

    def test_an_endpoint_without_a_preflight_still_restores(self):
        """The base implementation makes no claim, and a caller must tolerate an
        endpoint that has none rather than refusing to run."""
        from unittest.mock import patch

        sent = []
        backup = MagicMock(spec=["config", "set_lock", "get_id"])
        backup.config = {"path": "/backup"}
        destination = MagicMock()
        destination.config = {"path": "/dest"}
        destination.list_snapshots.return_value = []

        with patch.object(
            core_restore, "send_snapshot", lambda *a, **k: sent.append(1)
        ):
            with patch.object(core_restore, "log_transaction", lambda **k: None):
                with patch.object(
                    core_restore, "verify_restored_snapshot", lambda *a, **k: True
                ):
                    core_restore.restore_snapshot(backup, destination, self._snapshot())
        assert sent == [1]


class TestThePreviewMatchesTheRun:
    """A dry run is what an operator uses to size a restore before starting it.

    `_choose_parent` exists so the preview cannot describe a transfer the run
    will not perform. Two things still let them diverge: the preview did not
    honour `--no-incremental`, and it offered every snapshot in the chain as a
    candidate parent rather than only the ones that will already be at the
    destination when a given snapshot is reached.
    """

    def _endpoint(self, names):
        import sys

        sys.path.insert(0, "tests")
        from test_restore_destination_prefix_parity import _Endpoint

        return _Endpoint(names)

    def test_no_incremental_previews_full_sends(self, caplog):
        backup = self._endpoint(["home-20240101T120000", "home-20240102T120000"])
        with caplog.at_level("INFO"):
            core_restore.restore_snapshots(
                backup,
                self._endpoint([]),
                restore_all=True,
                dry_run=True,
                no_incremental=True,
            )
        assert "incremental" not in caplog.text, (
            "the preview promised an incremental that --no-incremental forbids"
        )

    def test_the_first_snapshot_has_no_parent_to_offer(self, caplog):
        """Nothing precedes it, so it must preview as a full send. Passing the
        whole chain let it name a LATER snapshot as its parent."""
        backup = self._endpoint(["home-20240101T120000", "home-20240102T120000"])
        with caplog.at_level("INFO"):
            core_restore.restore_snapshots(
                backup, self._endpoint([]), restore_all=True, dry_run=True
            )
        first = [
            line
            for line in caplog.text.splitlines()
            if "home-20240101T120000" in line and "Would restore" in line
        ]
        assert first, "the first snapshot was never previewed"
        assert "(full)" in first[0], (
            f"the first snapshot was previewed against a parent that will not be "
            f"at the destination yet: {first[0]}"
        )


class TestSameVolumeMatchesItsOwnComment:
    """ "Cannot-tell on BOTH names is the only case that accepts" -- the code
    accepted when EITHER was unknown, which is not the same rule."""

    def test_one_sided_uncertainty_refuses(self):
        from datetime import datetime, timezone
        from pathlib import Path

        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        def raw(name, stamp):
            return RawSnapshot(
                name=name,
                stream_path=Path(f"/backup/{name}.btrfs"),
                created=datetime.strptime(stamp, "%Y%m%d-%H%M%S").replace(
                    tzinfo=timezone.utc
                ),
            )

        target = raw("home-20240102-120000", "20240102-120000")
        unparseable = raw("not-a-snapshot-name", "20240101-120000")

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        assert (
            core_restore._choose_parent([target], [unparseable], target, backup) is None
        ), "a candidate whose volume cannot be determined was used as a parent"


class TestThePreviewAndTheRunAgree:
    """Drive both paths over the same inputs and compare, rather than asserting
    on either one alone.

    A preview that disagrees with the run is worse than no preview: it is what
    an operator sizes a restore by before committing to it over a slow link.
    Two divergences survived the first parity fix -- the preview ordered
    `present` differently from the run, and it skipped the run's remap of the
    chosen parent onto the backup side (which drops to a full send when the
    parent is not there).
    """

    def _endpoints(self, backup_names, local_names):
        import sys

        sys.path.insert(0, "tests")
        from test_restore_destination_prefix_parity import _Endpoint

        return _Endpoint(backup_names), _Endpoint(local_names)

    def _plan(self, caplog, marker):
        """(name, mode, parent) for each line the given path announced."""
        out = []
        for line in caplog.text.splitlines():
            if marker not in line:
                continue
            body = line.split(marker, 1)[1].strip()
            name = body.split(" ", 1)[0]
            detail = body[body.find("(") + 1 : body.rfind(")")]
            if detail.startswith("incremental from "):
                out.append((name, "incremental", detail.split("from ", 1)[1]))
            else:
                out.append((name, "full", None))
        return out

    @pytest.mark.parametrize(
        ("backup_names", "local_names", "no_incremental"),
        [
            (["home-20240101T120000", "home-20240102T120000"], [], False),
            (["home-20240101T120000", "home-20240102T120000"], [], True),
            (
                [
                    "home-20240101T120000",
                    "home-20240102T120000",
                    "home-20240103T120000",
                ],
                ["home-20240101T120000"],
                False,
            ),
        ],
    )
    def test_the_preview_describes_what_the_run_does(
        self, caplog, backup_names, local_names, no_incremental
    ):
        backup, local = self._endpoints(backup_names, local_names)
        with caplog.at_level("INFO"):
            core_restore.restore_snapshots(
                backup,
                local,
                restore_all=True,
                dry_run=True,
                no_incremental=no_incremental,
            )
        previewed = self._plan(caplog, "Would restore:")

        caplog.clear()
        backup, local = self._endpoints(backup_names, local_names)
        with (
            patch.object(core_restore, "restore_snapshot"),
            caplog.at_level("INFO"),
        ):
            core_restore.restore_snapshots(
                backup,
                local,
                restore_all=True,
                no_incremental=no_incremental,
            )
        # "] Restoring", not "Restoring": the latter also matches the summary
        # line "Restoring all N snapshots", which is not a per-snapshot plan.
        performed = self._plan(caplog, "] Restoring")

        assert previewed, "the preview announced nothing"
        assert previewed == performed, (
            f"the dry run described a different restore from the one performed.\n"
            f"  preview: {previewed}\n"
            f"  run:     {performed}"
        )

    def test_a_parent_absent_from_the_backup_previews_as_full(self, caplog):
        """The run remaps the chosen parent onto the backup side and drops to a
        full send when it is not there -- `btrfs send -p` computes the delta on
        the BACKUP side, so a parent that exists only at the destination has no
        usable path there.

        Pinned directly because the end-to-end parity fixtures cannot reach it:
        in those, every parent the chooser returns already IS a backup snapshot,
        so the remap is a no-op and deleting it changes nothing.
        """
        backup, local = self._endpoints(
            ["home-20240101T120000", "home-20240102T120000"], []
        )
        only_at_the_destination = MagicMock()
        only_at_the_destination.get_name.return_value = "home-19990101T120000"

        with (
            patch.object(
                core_restore, "_choose_parent", return_value=only_at_the_destination
            ),
            caplog.at_level("INFO"),
        ):
            core_restore.restore_snapshots(
                backup, local, restore_all=True, dry_run=True
            )

        assert "home-19990101T120000" not in caplog.text, (
            "the preview promised an incremental against a parent that is not "
            "on the backup side, which the run would send in full instead"
        )
