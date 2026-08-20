"""A raw location holding backups must never report a clean empty result.

A listing filters on snap_prefix and then requires the remainder of each name
to parse as a timestamp. The prefix diagnostics exist to tell an operator that
a location is NOT empty, it just does not use the prefix they searched for --
and to let the restore act on that instead of giving up.

Both were written for a btrfs destination, where one snapshot is one subvolume
and the directory entry IS the snapshot name. A raw destination stores files:
<name>.btrfs, plus compression and encryption suffixes. Every entry therefore
failed to parse, both diagnostics returned nothing, and a raw:// destination
holding good backups answered a prefix mismatch with "no snapshots found" and
exit 0 -- at the one moment that answer does the most damage.
"""

from __future__ import annotations


import pytest

from btrfs_backup_ng.endpoint.raw import RawEndpoint


def _populate(directory, prefix="home-", stamps=("20260810-101010", "20260811-101010")):
    for stamp in stamps:
        (directory / f"{prefix}{stamp}.btrfs").write_bytes(b"stream")
        (directory / f"{prefix}{stamp}.btrfs.meta").write_text("{}")
    return directory


class TestARawLocationWithBackupsIsNeverReportedEmpty:
    def test_a_mismatched_prefix_can_be_inferred(self, tmp_path):
        """The value the restore acts on: without it there is no retry to make."""
        _populate(tmp_path)
        endpoint = RawEndpoint(path=str(tmp_path), snap_prefix="wrong-")
        assert endpoint.list_snapshots(flush_cache=True) == []
        assert endpoint.prefixes_present() == {"home-": 2}

    def test_a_mismatched_prefix_is_explained(self, tmp_path):
        _populate(tmp_path)
        endpoint = RawEndpoint(path=str(tmp_path), snap_prefix="wrong-")
        explanation = endpoint.describe_empty_listing()
        assert explanation is not None, "a location holding backups reported as empty"
        assert "NOT empty" in explanation
        assert "home-" in explanation

    def test_sidecars_are_not_counted_as_snapshots(self, tmp_path):
        """Each stream has a .meta beside it; counting both doubles every prefix."""
        _populate(tmp_path)
        endpoint = RawEndpoint(path=str(tmp_path), snap_prefix="wrong-")
        assert endpoint.prefixes_present() == {"home-": 2}

    @pytest.mark.parametrize(
        "suffix",
        [".btrfs", ".btrfs.gz", ".btrfs.zst", ".btrfs.zst.gpg", ".btrfs.zst.enc"],
    )
    def test_compressed_and_encrypted_streams_are_recognised(self, tmp_path, suffix):
        """The suffixes stack; the name is what is left after all of them."""
        (tmp_path / f"home-20260810-101010{suffix}").write_bytes(b"stream")
        endpoint = RawEndpoint(path=str(tmp_path), snap_prefix="wrong-")
        assert endpoint.prefixes_present() == {"home-": 1}, suffix

    def test_a_matching_prefix_reports_nothing_wrong(self, tmp_path):
        """The diagnostic must stay quiet when there is no mismatch."""
        _populate(tmp_path)
        endpoint = RawEndpoint(path=str(tmp_path), snap_prefix="home-")
        assert len(endpoint.list_snapshots(flush_cache=True)) == 2
        assert endpoint.prefixes_present() == {}
        assert endpoint.describe_empty_listing() is None

    def test_a_genuinely_empty_location_reports_nothing(self, tmp_path):
        endpoint = RawEndpoint(path=str(tmp_path), snap_prefix="home-")
        assert endpoint.prefixes_present() == {}
        assert endpoint.describe_empty_listing() is None

    def test_an_unreadable_location_does_not_abort(self, tmp_path):
        """A diagnostic must never itself become the failure."""
        endpoint = RawEndpoint(path=str(tmp_path / "gone"), snap_prefix="home-")
        assert endpoint.prefixes_present() == {}
        assert endpoint.describe_empty_listing() is None

    def test_two_prefixes_are_both_reported(self, tmp_path):
        """Ambiguity is the caller's to resolve; the diagnostic must show both."""
        _populate(tmp_path, prefix="home-", stamps=("20260810-101010",))
        _populate(tmp_path, prefix="root-", stamps=("20260810-101010",))
        endpoint = RawEndpoint(path=str(tmp_path), snap_prefix="wrong-")
        assert endpoint.prefixes_present() == {"home-": 1, "root-": 1}


class TestTheSeamIsWhatMakesThisWork:
    def test_a_btrfs_entry_is_its_own_snapshot_name(self, tmp_path):
        """The base behaviour must be unchanged: entry name IS the snapshot name."""
        from btrfs_backup_ng.endpoint.local import LocalEndpoint

        endpoint = LocalEndpoint(path=str(tmp_path))
        assert endpoint._entry_snapshot_name("home-20260810-101010") == (
            "home-20260810-101010"
        )

    def test_a_raw_entry_is_stripped_to_its_snapshot_name(self, tmp_path):
        endpoint = RawEndpoint(path=str(tmp_path))
        assert endpoint._entry_snapshot_name("home-20260810-101010.btrfs.zst") == (
            "home-20260810-101010"
        )
        assert endpoint._entry_snapshot_name("home-20260810-101010.btrfs.meta") is None
