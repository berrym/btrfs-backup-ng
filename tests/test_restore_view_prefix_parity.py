"""Every read-only view of a location must answer for it the same way.

`--list` learned to work out the prefix a location actually uses (#98). Its
siblings did not, so three commands described the same directory three ways:

* `restore --interactive` printed "No snapshots available", and the restore then
  returned 0 -- a restore that restored nothing, reporting success -- for a
  location the same command without `-i` restores fine;
* `restore --status` reported "Available snapshots: 0" for a location `--list`
  shows as full;
* `--list` was right.

Only local and ssh btrfs are affected. A raw target lists every stream
regardless of prefix, so it never reaches the empty-listing case.
"""

from __future__ import annotations

import argparse
from unittest.mock import patch

import pytest

from btrfs_backup_ng.cli import restore as restore_cli
from btrfs_backup_ng.endpoint.local import LocalEndpoint


def _location(tmp_path, *names):
    """A destination holding real snapshot directories under one prefix."""
    for name in names:
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
    endpoint = LocalEndpoint.__new__(LocalEndpoint)
    endpoint.config = {
        "path": tmp_path,
        "snap_prefix": "",
        "lock_file_name": ".btrfs-backup-ng.locks",
    }
    return endpoint


def _snapshots(*names):
    class _S:
        def __init__(self, name):
            self.name = name

        def get_name(self):
            return self.name

        def __str__(self):
            return self.name

    return [_S(n) for n in names]


class TestTheViewsAgree:
    """The property. Each view is driven through the same endpoint state."""

    @pytest.fixture
    def endpoint(self, tmp_path):
        return _location(tmp_path)

    def _with_inference(self, endpoint, found):
        """A location where the default prefix matches nothing but 'home-' does."""

        def infer(ep):
            ep.config["snap_prefix"] = "home-"
            return found

        return (
            patch.object(restore_cli, "list_remote_snapshots", lambda ep: []),
            patch.object(restore_cli, "_retry_with_inferred_prefix", infer),
        )

    def test_list_finds_them(self, endpoint, capsys):
        found = _snapshots("home-20240101T120000", "home-20240102T120000")
        a, b = self._with_inference(endpoint, found)
        with a, b:
            snapshots, inferred = restore_cli._list_for_display(endpoint)
        assert len(snapshots) == 2
        assert inferred == "home-"

    def test_interactive_finds_them_too(self, endpoint, capsys):
        """It used to print 'No snapshots available' and the restore exited 0."""
        found = _snapshots("home-20240101T120000", "home-20240102T120000")
        a, b = self._with_inference(endpoint, found)
        with a, b:
            with patch("builtins.input", side_effect=["1", "y"]):
                selected = restore_cli._interactive_select(endpoint)
        out = capsys.readouterr().out
        assert "No snapshots available" not in out, (
            "interactive reported an empty location that --list shows as full"
        )
        assert selected is not None

    def test_status_counts_them_too(self, endpoint, capsys):
        """It used to report 'Available snapshots: 0' for the same location."""
        found = _snapshots("home-20240101T120000", "home-20240102T120000")
        a, b = self._with_inference(endpoint, found)
        with a, b:
            with patch.object(
                restore_cli, "_prepare_backup_endpoint", lambda ar, s: endpoint
            ):
                rc = restore_cli._execute_status(
                    argparse.Namespace(
                        source=str(endpoint.config["path"]),
                        fs_checks="skip",
                        prefix="",
                    )
                )
        out = capsys.readouterr().out
        assert rc == 0
        assert "Available snapshots: 0" not in out, (
            "status reported 0 for a location holding backups"
        )
        assert "Available snapshots: 2" in out

    def test_each_view_says_which_prefix_it_used(self, endpoint, capsys):
        """Inference that is not announced is a silent substitution.

        Asserting on the prefix alone would be theater: it appears in every
        snapshot name listed below, so the check passed with the announcement
        deleted. The sentence is what has to be there.
        """
        found = _snapshots("home-20240101T120000")
        a, b = self._with_inference(endpoint, found)
        with a, b:
            with patch("builtins.input", side_effect=["1", "y"]):
                restore_cli._interactive_select(endpoint)
        out = capsys.readouterr().out
        assert "No snapshots use the default prefix here" in out, (
            "the substituted prefix was used but never announced"
        )
        assert "'home-'" in out, "the announcement does not name the prefix"


class TestInferenceStillOnlyAppliesWhenItShould:
    """The guard rails the shared primitive already had must still hold."""

    def test_a_genuinely_empty_location_is_still_empty(self, tmp_path):
        endpoint = _location(tmp_path)
        with patch.object(restore_cli, "list_remote_snapshots", lambda ep: []):
            with patch.object(
                restore_cli, "_retry_with_inferred_prefix", lambda ep: []
            ):
                snapshots, inferred = restore_cli._list_for_display(endpoint)
        assert snapshots == []
        assert inferred == ""

    def test_a_location_that_lists_fine_is_not_second_guessed(self, tmp_path):
        """No inference runs when the first listing already found snapshots."""
        endpoint = _location(tmp_path)
        called = []

        def infer(ep):
            called.append(ep)
            return []

        with patch.object(
            restore_cli, "list_remote_snapshots", lambda ep: _snapshots("snap-1")
        ):
            with patch.object(restore_cli, "_retry_with_inferred_prefix", infer):
                snapshots, inferred = restore_cli._list_for_display(endpoint)
        assert len(snapshots) == 1
        assert inferred == ""
        assert not called, "inference ran for a location that listed fine"


class TestItIsOnePrimitiveNotThreeCopies:
    def test_every_view_goes_through_the_same_helper(self):
        """A fourth view added later must not have to remember the rules.

        Reads the source rather than the behaviour: the point is that no view
        calls the bare lister and re-implements the retry beside it, which is
        how the three answers diverged in the first place.
        """
        import inspect

        source = inspect.getsource(restore_cli)
        for view in ("_execute_list", "_interactive_select", "_execute_status"):
            body = source.split(f"def {view}(")[1].split("\ndef ")[0]
            assert "_list_for_display(" in body, (
                f"{view} does not use the shared lister"
            )
            assert "_retry_with_inferred_prefix(" not in body, (
                f"{view} re-implements the inference instead of sharing it"
            )
