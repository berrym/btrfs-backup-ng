"""A raw listing must recognise only streams this tool wrote.

Any filename containing `.btrfs` was inferred as a stream, so btrbk's
`<stream>.info` sidecar was listed as a second snapshot, offered for restore
and counted by retention. The rule has to hold at every site that enumerates
streams -- the backfill scan most of all, since it would write a `.meta`
sidecar for the foreign file and make the phantom permanent.
"""

from __future__ import annotations

import pytest
from btrfs_backup_ng import endpoint


class TestRawDiscoveryListsOnlyItsOwnStreams:
    """`raw list` inferred a backup from any filename containing '.btrfs'.

    btrbk writes a `<stream>.info` sidecar next to its raw backups, which was
    listed as a second snapshot -- a backup that does not exist, offered for
    restore and counted by retention.
    """

    def _parse(self, name):
        from btrfs_backup_ng.endpoint.raw_metadata import parse_stream_filename

        return parse_stream_filename(name)

    @pytest.mark.parametrize(
        "name",
        [
            "s.20260110T120000.btrfs",
            "s.20260110T120000.btrfs.gz",
            "s.20260110T120000.btrfs.zst",
            "s.20260110T120000.btrfs.zst.gpg",
            "s.20260110T120000.btrfs.zst.enc",
            "s.20260110T120000.btrfs.gpg",
        ],
    )
    def test_a_real_stream_is_recognised(self, name):
        assert self._parse(name)["is_stream"] is True

    @pytest.mark.parametrize(
        "name",
        [
            "s.20260110T120000.btrfs.gz.info",
            "notes.btrfs.txt",
            "s.btrfs.bak",
            "unrelated.info",
        ],
    )
    def test_a_foreign_file_is_not_a_backup(self, name):
        assert self._parse(name)["is_stream"] is False

    def test_discovery_ignores_the_sidecar(self, tmp_path):
        """End to end, through the real discovery entry point."""
        from btrfs_backup_ng.endpoint.raw_metadata import discover_raw_snapshots

        (tmp_path / "home.20260110T120000.btrfs.gz").write_text("stream")
        (tmp_path / "home.20260110T120000.btrfs.gz.info").write_text("btrbk meta")
        found = discover_raw_snapshots(tmp_path)
        assert len(found) == 1, [s.name for s in found]


class TestEveryRawListingUsesTheSameStreamRule:
    """The is_stream rule has to hold at every site that enumerates streams.

    Applying it only to discover_raw_snapshots left the backfill scan writing a
    .meta sidecar for a foreign file -- which would make the phantom backup
    authoritative and permanent, worse than merely listing it.
    """

    def test_the_backfill_scan_ignores_a_foreign_sidecar(self, tmp_path):
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        (tmp_path / "home.20260110T120000.btrfs.gz").write_text("stream")
        (tmp_path / "home.20260110T120000.btrfs.gz.info").write_text("btrbk meta")
        ep = RawEndpoint({"path": str(tmp_path), "source": "/src"})
        names = [s.name for s in ep.streams_without_sidecar()]
        assert names == ["home.20260110T120000"], names

    def test_every_listing_site_consults_is_stream(self):
        """Mutation guard: a new enumeration site that forgets the rule
        reintroduces the phantom on that transport only."""
        import inspect

        from btrfs_backup_ng.endpoint import raw

        source = inspect.getsource(raw)
        parses = source.count("parse_stream_filename(")
        guards = source.count('parsed.get("is_stream")')
        assert parses == guards, (
            f"{parses} filename-parse sites but {guards} is_stream guards: a site "
            f"enumerates streams without the rule, so the phantom returns there"
        )


class TestSkipRemoteLockReachesTheEndpoint:
    """RawEndpoint.set_lock reads config["skip_remote_lock"], but only
    SSHEndpoint whitelisted the key, so the abort whose own message recommends
    the flag could never be relaxed on a raw+ssh target.
    """

    def test_skip_remote_lock_reaches_a_raw_ssh_endpoint(self):
        """RawEndpoint.set_lock reads config['skip_remote_lock'], but only
        SSHEndpoint whitelisted the key, so the abort telling the operator to
        pass --skip-remote-lock could never be relaxed on this transport."""
        ep = endpoint.choose_endpoint(
            "raw+ssh://host:/backups",
            {"source": "/src", "skip_remote_lock": True},
            source=False,
        )
        assert ep.config.get("skip_remote_lock") is True

    def test_skip_remote_lock_still_reaches_an_ssh_endpoint(self):
        """The transport that already worked must keep working."""
        ep = endpoint.choose_endpoint(
            "ssh://host:/backups",
            {"source": "/src", "skip_remote_lock": True},
            source=False,
        )
        assert ep.config.get("skip_remote_lock") is True
