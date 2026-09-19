"""An explicit empty snapshot prefix is a choice, and must not be overridden.

Issue #6, raised by the reporter of #2 as a side-note: an empty
``snapshot_prefix`` was replaced by the auto-derived default, "which is a pity:
everything is put in a subdir anyway, one cannot have an empty prefix". The
config layer was fixed for it -- VolumeConfig derives only when the key is None
and preserves an explicit "" -- and the reporter was told it now yields
bare-timestamp names.

Restore's prefix inference then took it back. That function returns early when a
prefix "was asked for explicitly", tested as ``if configured:`` -- and "" is
falsy. So the one prefix value that means "I deliberately want no prefix" was
the one value read as "nobody said", and inference replaced it: measured, a
location holding another source's ``home.`` snapshots had the operator's
explicit choice rewritten to ``home.`` and those snapshots listed for restore.

That is the harm the inference guard was written to prevent, reached through the
only falsy prefix. A value cannot carry the distinction, so the choice is
recorded beside it.
"""

from __future__ import annotations

import pytest

from btrfs_backup_ng.config.schema import VolumeConfig
from btrfs_backup_ng.core.restore import _retry_with_inferred_prefix
from btrfs_backup_ng.endpoint.local import LocalEndpoint

OTHER_SOURCE = ["home.20240101-120000", "home.20240102-120000"]
BARE = ["20240101-120000"]


def _endpoint(tmp_path, names, **config):
    for name in names:
        (tmp_path / name).mkdir()
    return LocalEndpoint(config={"path": str(tmp_path), **config})


class TestTheConfigLayerStillHonoursIt:
    """Issue #6's own requirements, so a regression there is caught here too."""

    def test_an_unset_prefix_is_derived(self):
        assert VolumeConfig(path="/home").snapshot_prefix == "home-"

    def test_an_explicit_empty_prefix_is_preserved(self):
        assert VolumeConfig(path="/home", snapshot_prefix="").snapshot_prefix == ""

    def test_a_bare_timestamp_name_results(self):
        """An empty prefix must leave the timestamp standing alone.

        This pinned the literal "20231114-171320", which is that instant
        rendered in US Eastern time. It passed here and failed in CI, which
        runs UTC, because `Snapshot` formats LOCAL time -- so the digits
        depend on the machine. The claim worth making is that nothing is
        prepended, not what the clock says.
        """
        import time
        from pathlib import Path

        from btrfs_backup_ng import __util__

        moment = time.localtime(1700000000)
        snap = __util__.Snapshot(Path("/x"), "", None, time_obj=moment)
        name = snap.get_name()

        assert name == time.strftime(__util__.DATE_FORMAT, moment)
        assert name[0].isdigit(), f"{name!r} is not a bare timestamp"


class TestInferenceRespectsAnExplicitEmptyPrefix:
    def test_it_is_not_overridden(self, tmp_path):
        """The defect: another source's snapshots were listed for restore in
        place of the operator's explicit 'no prefix'."""
        ep = _endpoint(
            tmp_path, OTHER_SOURCE, snap_prefix="", snap_prefix_explicit=True
        )

        found = _retry_with_inferred_prefix(ep)

        assert found == [], (
            f"inference replaced an explicit empty prefix and returned "
            f"{[s.get_name() for s in found]}"
        )
        assert ep.config["snap_prefix"] == "", (
            "the operator's explicit choice was rewritten in the config"
        )

    def test_inference_still_runs_when_no_prefix_was_asked_for(self, tmp_path):
        """The feature must survive: it is what turns a wrong-prefix dead end
        into a listing."""
        ep = _endpoint(tmp_path, OTHER_SOURCE, snap_prefix="")

        found = _retry_with_inferred_prefix(ep)

        assert len(found) == len(OTHER_SOURCE)
        assert ep.config["snap_prefix"] == "home."

    def test_an_explicit_non_empty_prefix_is_still_respected(self, tmp_path):
        ep = _endpoint(
            tmp_path, OTHER_SOURCE, snap_prefix="srv.", snap_prefix_explicit=True
        )

        assert _retry_with_inferred_prefix(ep) == []
        assert ep.config["snap_prefix"] == "srv."

    def test_a_non_empty_prefix_suppresses_inference_even_if_not_marked_chosen(
        self, tmp_path
    ):
        """Pre-existing behaviour, and the reason the original `if configured:`
        guard must stay: a volume's auto-DERIVED prefix is non-empty and carries
        no explicit marker, and inference has never been allowed to replace it."""
        ep = _endpoint(tmp_path, OTHER_SOURCE, snap_prefix="srv.")

        assert _retry_with_inferred_prefix(ep) == []
        assert ep.config["snap_prefix"] == "srv."

    def test_a_bare_timestamp_location_lists_under_an_explicit_empty_prefix(
        self, tmp_path
    ):
        """The configuration working as the reporter asked for it."""
        ep = _endpoint(tmp_path, BARE, snap_prefix="", snap_prefix_explicit=True)

        assert [s.get_name() for s in ep.list_snapshots()] == BARE


class TestTheChoiceSurvivesEveryLayer:
    def test_the_endpoint_records_it(self, tmp_path):
        """The base __init__ keeps only known keys, so an unregistered one is
        silently dropped and the guard never sees it."""
        ep = _endpoint(tmp_path, [], snap_prefix="", snap_prefix_explicit=True)
        assert ep.config["snap_prefix_explicit"] is True

    def test_it_defaults_to_not_explicit(self, tmp_path):
        ep = _endpoint(tmp_path, [], snap_prefix="")
        assert ep.config["snap_prefix_explicit"] is False

    @pytest.mark.parametrize("given", ["", "home-"])
    def test_the_restore_cli_marks_a_given_prefix_as_chosen(self, given):
        import inspect

        from btrfs_backup_ng.cli import restore as restore_cli

        source = inspect.getsource(restore_cli)
        assert '"snap_prefix_explicit": getattr(args, "prefix", None) is not None' in (
            source
        ), "the restore CLI no longer records whether a prefix was given"

    def test_a_volumes_explicit_empty_prefix_reaches_args(self):
        """`if volume.snapshot_prefix and ...` is falsy for "", so a volume's
        deliberate 'no prefix' never became a choice at all. A volume prefix is
        empty ONLY when the operator wrote it: an unset one is auto-derived."""
        import inspect

        from btrfs_backup_ng.cli import restore as restore_cli

        source = inspect.getsource(restore_cli)
        assert (
            'if volume.snapshot_prefix and not getattr(args, "prefix", None):'
            not in (source)
        ), "a volume's explicit empty prefix is dropped by a truthiness test again"
        assert source.count('if getattr(args, "prefix", None) is None:') >= 2
