"""The destination must be read under the same prefix as the source.

`_prepare_local_endpoint`'s own docstring states the invariant: "The local
endpoint must parse already-restored subvolumes under the SAME prefix, or it
fails to recognize them and the restore chain re-restores an existing parent."

That held while the prefix came from `--prefix`. It stopped holding when the
prefix is INFERRED: inference updates the backup endpoint and nothing updates
the destination, so the destination lists as empty, `skip_existing` has nothing
to skip, and a snapshot already present is re-sent onto its own name --
`creating subvolume ... failed: File exists`.

Shipped as a known issue in 0.9.5 with `--prefix` as the workaround.
"""

from __future__ import annotations

import re
from unittest.mock import patch

import pytest

from btrfs_backup_ng.core import restore as core_restore


def _parses_as_timestamp(text: str) -> bool:
    """Whether what follows the prefix looks like a snapshot timestamp."""
    return bool(re.fullmatch(r"\d{8}T\d{6}", text))


class _Snap:
    def __init__(self, name, t=0):
        self.name = name
        self.time_obj = t
        self.locks = set()
        self.parent_locks = set()

    def get_name(self):
        return self.name

    def get_path(self):
        return f"/backup/{self.name}"

    def find_parent(self, others):
        return None

    def __lt__(self, other):
        # The chain logic orders snapshots; without this the tests fail with a
        # TypeError, which would look like the defect without being it.
        return self.time_obj < other.time_obj

    def __str__(self):
        return self.name


class _Endpoint:
    """An endpoint that filters by prefix, the way a real one does."""

    def __init__(self, names, prefix=""):
        self._names = list(names)
        self.config = {"path": "/backup", "snap_prefix": prefix}
        self.listed_under = []

    def list_snapshots(self, flush_cache=False):
        """Filter the way a real endpoint does: prefix, THEN a parseable date.

        Both halves matter. Modelling only ``startswith`` makes an empty prefix
        match everything, the first listing succeeds, and inference never runs --
        so the test passes through a code path the real defect never reaches.
        That is how the first version of this test failed for the wrong reason.
        """
        prefix = self.config.get("snap_prefix", "") or ""
        self.listed_under.append(prefix)
        found = []
        for i, name in enumerate(self._names):
            if not name.startswith(prefix):
                continue
            remainder = name[len(prefix) :]
            if not _parses_as_timestamp(remainder):
                continue  # real endpoints log "Skipping non-snapshot item"
            found.append(_Snap(name, i))
        return found

    def prefixes_present(self):
        return {"home-": len(self._names)}

    def correspondent_of(self, snapshot):
        """Same-name correspondence, which is what a raw endpoint does."""
        for name in self._names:
            if name == snapshot.get_name():
                return _Snap(name)
        return None

    def set_lock(self, *a, **kw):
        pass

    def get_id(self):
        return "endpoint"


@pytest.fixture
def backup():
    # The location holds 'home-' snapshots; nothing matches the default prefix.
    return _Endpoint(["home-20240101T120000", "home-20240102T120000"], prefix="")


@pytest.fixture
def destination():
    # The destination ALREADY holds the first one -- a resumed restore.
    return _Endpoint(["home-20240101T120000"], prefix="")


class TestAnAlreadyRestoredSnapshotIsSkipped:
    def test_the_destination_is_listed_under_the_inferred_prefix(
        self, backup, destination
    ):
        """The regression, stated directly.

        After inference the source is read as 'home-'. If the destination is
        still read as '' it returns nothing, and the snapshot sitting right
        there is invisible.
        """
        core_restore.restore_snapshots(
            backup, destination, restore_all=True, dry_run=True
        )
        assert destination.listed_under, "the destination was never listed"
        assert destination.listed_under[-1] == "home-", (
            f"destination listed under {destination.listed_under[-1]!r} while the "
            f"source was read under 'home-'; an already-restored snapshot is invisible"
        )

    def test_the_present_snapshot_is_not_in_the_restore_set(
        self, backup, destination, caplog
    ):
        """The observable that matters: it must not be attempted.

        Not ``stats["skipped"]`` -- get_restore_chain STOPS at a snapshot already
        present locally rather than adding it and filtering it out later, so a
        correct run reports 0 skipped. Asserting on that counter would have
        demanded behaviour the fix does not produce.
        """
        with caplog.at_level("INFO"):
            core_restore.restore_snapshots(
                backup, destination, restore_all=True, dry_run=True
            )
        # The name being RESTORED only -- not the whole line. A correct run names
        # the existing snapshot as the incremental PARENT, so a substring match
        # over the message finds it there and fails a fix that works.
        attempted = [
            m.group(1)
            for m in (
                re.search(r"Would restore: (\S+)", r.getMessage())
                for r in caplog.records
            )
            if m
        ]
        assert "home-20240101T120000" not in attempted, (
            f"the snapshot already at the destination was going to be re-sent "
            f"onto its own name: {attempted}"
        )
        assert attempted == ["home-20240102T120000"], (
            f"expected only the missing snapshot to be restored, got {attempted}"
        )


class TestTheGuardsAroundTheSync:
    """Both guards were unobservable until these existed."""

    def test_a_memoised_destination_listing_is_not_trusted(self, backup):
        """Endpoints memoise list_snapshots. A listing cached under the OLD
        prefix returns the stale empty set with no error -- the exact signature
        of the bug being fixed -- so the listing after the sync must flush."""

        class _CachingEndpoint(_Endpoint):
            def __init__(self, names, prefix=""):
                super().__init__(names, prefix)
                self._memo = None

            def list_snapshots(self, flush_cache=False):
                if self._memo is not None and not flush_cache:
                    self.listed_under.append("<from cache>")
                    return self._memo
                self._memo = super().list_snapshots()
                return self._memo

        destination = _CachingEndpoint(["home-20240101T120000"], prefix="")
        destination.list_snapshots()  # something listed it early, under ''
        assert destination._memo == []

        core_restore.restore_snapshots(
            backup, destination, restore_all=True, dry_run=True
        )
        assert destination.listed_under[-1] != "<from cache>", (
            "the destination listing came from a memo built under the old prefix"
        )
        assert destination.listed_under[-1] == "home-"

    def test_a_non_string_prefix_is_not_copied_through(self):
        """Test doubles hand back a mock from config.get. Copied through, it
        reaches str.startswith inside the listing and raises there, far from
        the cause."""
        from unittest.mock import MagicMock

        # The real shape: the whole endpoint is a double, so `config.get(...)`
        # hands back a MagicMock. Setting the key on a REAL endpoint instead
        # just crashes that endpoint's own listing first, which is a different
        # bug and would not exercise the guard.
        backup = MagicMock()
        backup.list_snapshots.return_value = [
            _Snap("home-20240101T120000", 0),
            _Snap("home-20240102T120000", 1),
        ]
        backup.correspondent_of.return_value = None
        destination = _Endpoint(["home-20240101T120000"], prefix="")

        core_restore.restore_snapshots(
            backup, destination, restore_all=True, dry_run=True
        )
        assert isinstance(destination.config["snap_prefix"], str), (
            f"a {type(destination.config['snap_prefix']).__name__} was copied into "
            f"the destination's prefix"
        )


class TestTheFixIsANoOpWhereItShouldBe:
    def test_an_explicit_prefix_is_never_overridden(self):
        """Inference does not run when a prefix was given, so nothing changes."""
        backup = _Endpoint(["home-20240101T120000"], prefix="home-")
        destination = _Endpoint(["home-20240101T120000"], prefix="home-")
        core_restore.restore_snapshots(
            backup, destination, restore_all=True, dry_run=True
        )
        assert destination.listed_under[-1] == "home-"

    def test_a_location_that_lists_fine_leaves_the_destination_alone(self):
        """No inference means no change to the destination's prefix."""
        # No prefix at all: the bare timestamp parses, so the first listing
        # succeeds and inference never runs.
        backup = _Endpoint(["20240101T120000"], prefix="")
        destination = _Endpoint(["20240101T120000"], prefix="")
        core_restore.restore_snapshots(
            backup, destination, restore_all=True, dry_run=True
        )
        assert destination.listed_under[-1] == ""

    def test_a_failed_inference_does_not_corrupt_the_destination_prefix(self):
        """Inference that finds nothing must not set a garbage prefix."""
        backup = _Endpoint([], prefix="")
        destination = _Endpoint(["home-20240101T120000"], prefix="")
        with patch.object(core_restore, "_retry_with_inferred_prefix", lambda ep: []):
            core_restore.restore_snapshots(
                backup, destination, restore_all=True, dry_run=True
            )
        assert destination.config["snap_prefix"] == ""
