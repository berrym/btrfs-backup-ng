"""An empty listing must say WHICH kind of empty it is.

A listing filters on ``snap_prefix``, then requires the rest of each name to
parse as a timestamp. Supply the wrong prefix -- or none -- and every real
snapshot is silently discarded. Measured on a real host, against a destination
holding a backup:

    $ btrfs-backup-ng restore --list ~/bbng-e2e/dest
    No snapshots found at backup location                  (exit 0)

    $ btrfs-backup-ng restore --list --prefix 'home-mberry-bbng-e2e-src-' ...
        1. home-mberry-bbng-e2e-src-20260818-021031

Identical wording for "this location is empty" and "your prefix matched nothing"
-- and an operator reading the first during disaster recovery concludes the
backups are gone. The listing itself is unchanged; only the empty case now
distinguishes the two and names the prefixes that would have worked.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.__util__ import infer_snapshot_prefix
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

DEST = "/backups/home"


def _endpoint(path, **config):
    base = {"path": path, "snap_prefix": "", "lock_file_name": ".locks"}
    base.update(config)
    ep = Endpoint.__new__(Endpoint)
    ep.config = base
    return ep


class TestInferringThePrefixFromAName:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("home-20260818-021031", "home-"),
            ("home-mberry-bbng-e2e-src-20260818-021031", "home-mberry-bbng-e2e-src-"),
            ("20260818-021031", ""),
            ("README.md", None),
            ("snapshot", None),
            ("", None),
        ],
    )
    def test_it_recovers_the_prefix_a_listing_would_need(self, name, expected):
        assert infer_snapshot_prefix(name) == expected

    def test_it_honours_a_configured_timestamp_format(self):
        assert infer_snapshot_prefix("daily-2026-08-18", "%Y-%m-%d") == "daily-"

    def test_the_longest_timestamp_wins(self):
        """Otherwise a coincidental short match yields a too-long prefix."""
        assert infer_snapshot_prefix("home-20260818-021031") == "home-"


class TestALocationThatIsGenuinelyEmpty:
    def test_no_explanation_is_offered(self, tmp_path):
        """Nothing there means nothing to explain; the caller keeps its plain
        'no snapshots' wording and its exit 0."""
        assert _endpoint(tmp_path).describe_empty_listing() is None

    def test_entries_that_are_not_snapshots_do_not_count(self, tmp_path):
        (tmp_path / "README.md").write_text("x")
        (tmp_path / "lost+found").mkdir()
        assert _endpoint(tmp_path).describe_empty_listing() is None


class TestALocationWhosePrefixDidNotMatch:
    def _populated(self, tmp_path, names, **config):
        for name in names:
            (tmp_path / name).mkdir()
        return _endpoint(tmp_path, **config)

    def test_it_says_the_location_is_not_empty(self, tmp_path):
        ep = self._populated(tmp_path, ["home-20260818-021031"])
        message = ep.describe_empty_listing()
        assert message is not None
        assert "NOT empty" in message

    def test_it_names_the_prefix_that_would_work(self, tmp_path):
        ep = self._populated(tmp_path, ["home-20260818-021031", "home-20260818-030000"])
        message = ep.describe_empty_listing()
        assert "'home-'" in message
        assert "2 snapshots" in message

    def test_it_tells_the_operator_what_to_run(self, tmp_path):
        ep = self._populated(tmp_path, ["home-20260818-021031"])
        assert "--prefix 'home-'" in ep.describe_empty_listing()

    def test_the_most_common_prefix_is_the_one_suggested(self, tmp_path):
        ep = self._populated(
            tmp_path,
            [
                "home-20260818-021031",
                "home-20260818-030000",
                "other-20260818-040000",
            ],
        )
        message = ep.describe_empty_listing()
        assert "--prefix 'home-'" in message
        assert "'other-'" in message, "the rarer prefix is still reported"

    def test_a_prefix_that_already_matches_is_not_suggested(self, tmp_path):
        """If these names DID match the configured prefix they would have been
        listed; re-suggesting it would send the operator in a circle."""
        ep = self._populated(tmp_path, ["home-20260818-021031"], snap_prefix="home-")
        assert ep.describe_empty_listing() is None

    def test_a_diagnostic_failure_is_never_fatal(self, tmp_path):
        ep = _endpoint(tmp_path)
        with patch.object(ep, "_listdir", side_effect=OSError("boom")):
            assert ep.describe_empty_listing() is None


class TestTheRemoteVariant:
    """ssh:// cannot use the local _listdir, and its subvolume listing is
    filesystem-wide, so it must separate 'here' from 'elsewhere'."""

    OUTPUT = (
        "ID 1 gen 1 top level 5 path @home/mberry/backups/home/home-20260818-021031\n"
        "ID 2 gen 1 top level 5 path @home/mberry/snaps/src/other-20260818-030000\n"
    )

    def _ssh(self, present):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": DEST, "snap_prefix": "", "hostname": "nas"}
        ep.hostname = "nas"
        ep._exec_remote_command = MagicMock(
            return_value=MagicMock(
                returncode=0, stdout=self.OUTPUT.encode(), stderr=b""
            )
        )
        ep._subvolume_exists_at = MagicMock(side_effect=lambda p: p in present)
        return ep

    def test_a_prefix_present_at_the_destination_is_suggested(self):
        ep = self._ssh({f"{DEST}/home-20260818-021031"})
        message = ep.describe_empty_listing()
        assert "NOT empty" in message
        assert "--prefix 'home-'" in message

    def test_snapshots_only_elsewhere_say_so_instead(self):
        """The most useful message of all: you are pointed at the wrong path."""
        ep = self._ssh(set())
        message = ep.describe_empty_listing()
        assert message is not None
        assert "elsewhere on the same filesystem" in message
        assert "points somewhere other than the destination" in message
        assert "--prefix" not in message, "suggesting a prefix here would not help"

    def test_it_does_not_enumerate_locally(self):
        ep = self._ssh({f"{DEST}/home-20260818-021031"})
        ep._listdir = MagicMock(
            side_effect=AssertionError("a remote endpoint must not stat locally")
        )
        ep.describe_empty_listing()

    def test_a_failed_remote_probe_is_not_fatal(self):
        ep = self._ssh(set())
        ep._exec_remote_command = MagicMock(return_value=MagicMock(returncode=1))
        assert ep.describe_empty_listing() is None


class TestTheCommandUsesTheExplanation:
    """Pinning the wiring, not just the endpoint method: without these, deleting
    the CLI's use of describe_empty_listing() changes nothing a test can see."""

    def _list(self, endpoint, source):
        from btrfs_backup_ng.cli import restore as restore_cli

        with patch.object(
            restore_cli, "_prepare_backup_endpoint", lambda a, s: endpoint
        ):
            with patch.object(restore_cli, "list_remote_snapshots", lambda ep: []):
                args = MagicMock()
                args.source = str(source)
                return restore_cli._execute_list(args)

    def test_a_prefix_mismatch_is_explained_and_exits_nonzero(self, tmp_path, capsys):
        (tmp_path / "home-20260818-021031").mkdir()
        rc = self._list(_endpoint(tmp_path), tmp_path)
        out = capsys.readouterr().out
        assert rc == 1, "a location holding backups must not report success"
        assert "No snapshots found at backup location" not in out
        assert "NOT empty" in out
        assert "--prefix 'home-'" in out

    def test_a_genuinely_empty_location_keeps_the_plain_wording(self, tmp_path, capsys):
        rc = self._list(_endpoint(tmp_path), tmp_path)
        out = capsys.readouterr().out
        assert rc == 0
        assert "No snapshots found at backup location" in out

    def test_the_command_survives_a_broken_diagnostic(self, tmp_path, capsys):
        ep = _endpoint(tmp_path)
        ep.describe_empty_listing = MagicMock(side_effect=RuntimeError("boom"))
        rc = self._list(ep, tmp_path)
        assert rc == 0
        assert "No snapshots found at backup location" in capsys.readouterr().out
