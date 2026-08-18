"""An ssh:// listing must report what is AT the destination -- nothing else.

`btrfs subvolume list -o <path>` does not scope to <path>. Measured on a real
host: listing a destination directory and listing an unrelated sibling returned
byte-identical output, because the command enumerates the whole FILESYSTEM.

_parse_snapshot_list discarded the location with os.path.basename() and recorded
every hit as living at config["path"]. Measured consequence, on a destination
holding exactly ONE backup:

    1. home-...-021031      <- the real backup
    2. home-...-021031      <- the SOURCE copy, same name, different path
    3. home-...-021257      <- NEVER TRANSFERRED, exists only at the source
    Total: 3 snapshot(s)

That is a phantom backup, and it is worse than a missing one: an operator can
read it as "my data is safe" and delete the source. The same rule already lived
in _verify_snapshot_exists, whose docstring explains why a name-only search is
wrong -- it had simply never reached the listing.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

DEST = "/backups/home"

# Real `btrfs subvolume list -o -u -R` output shape, captured from 192.168.0.70.
# Note the paths are relative to the FILESYSTEM root and carry the top-level
# subvolume name (@home), which is why a naive suffix comparison is not enough.
OUTPUT = (
    "ID 5816 gen 341546 top level 263 received_uuid -                    "
    "uuid eceff1db-d300-c94e-8735-d5980e5ac4f9 "
    "path @home/mberry/snaps/src/home-20260818-021031\n"
    "ID 5817 gen 341550 top level 263 received_uuid "
    "eceff1db-d300-c94e-8735-d5980e5ac4f9 "
    "uuid 4f9583c7-fd4a-0d48-8fb8-4c116d2b60a6 "
    "path @home/mberry/backups/home/home-20260818-021031\n"
    "ID 5818 gen 341560 top level 263 received_uuid -                    "
    "uuid aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee "
    "path @home/mberry/snaps/src/home-20260818-021257\n"
)

# The same two-copies-of-one-name situation with the DESTINATION line FIRST.
# Ordering must not decide which line wins: "keep the first" and "keep the last"
# are both wrong, and one of these two fixtures catches each.
OUTPUT_DEST_FIRST = (
    "ID 5817 gen 341550 top level 263 received_uuid "
    "eceff1db-d300-c94e-8735-d5980e5ac4f9 "
    "uuid 4f9583c7-fd4a-0d48-8fb8-4c116d2b60a6 "
    "path @home/mberry/backups/home/home-20260818-021031\n"
    "ID 5816 gen 341546 top level 263 received_uuid -                    "
    "uuid eceff1db-d300-c94e-8735-d5980e5ac4f9 "
    "path @home/mberry/snaps/src/home-20260818-021031\n"
)


def _endpoint(**config):
    base = {
        "path": DEST,
        "hostname": "nas",
        "username": "backup",
        "snap_prefix": "home-",
        "timestamp_format": "%Y%m%d-%H%M%S",
    }
    base.update(config)
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = base
    ep.hostname = base["hostname"]
    return ep


def _names(snapshots):
    return sorted(s.get_name() for s in snapshots)


class TestOnlyWhatIsAtTheDestinationCounts:
    def test_a_never_transferred_source_snapshot_is_not_a_backup(self):
        """The dangerous one: reporting data as backed up when it is not."""
        ep = _endpoint()
        parsed = ep._parse_snapshot_list(OUTPUT, DEST)
        present = {f"{DEST}/home-20260818-021031"}
        with patch.object(
            ep, "_subvolume_exists_at", side_effect=lambda p: p in present
        ):
            scoped = ep._scope_to_destination(parsed, DEST)
        assert _names(scoped) == ["home-20260818-021031"]
        assert "home-20260818-021257" not in _names(scoped)

    def test_the_probe_asks_about_the_exact_destination_path(self):
        ep = _endpoint()
        parsed = ep._parse_snapshot_list(OUTPUT, DEST)
        with patch.object(ep, "_subvolume_exists_at", return_value=True) as probe:
            ep._scope_to_destination(parsed, DEST)
        for call in probe.call_args_list:
            asked = call[0][0]
            assert asked.startswith(f"{DEST}/"), asked
            assert asked.count("/snaps/") == 0, asked

    def test_a_destination_holding_nothing_lists_nothing(self):
        """First run to a fresh destination: the source has snapshots, the
        destination has none. Zero is the correct answer, not an error."""
        ep = _endpoint()
        parsed = ep._parse_snapshot_list(OUTPUT, DEST)
        with patch.object(ep, "_subvolume_exists_at", return_value=False):
            assert ep._scope_to_destination(parsed, DEST) == []

    def test_everything_present_survives(self):
        """Guard against over-correcting."""
        ep = _endpoint()
        parsed = ep._parse_snapshot_list(OUTPUT, DEST)
        with patch.object(ep, "_subvolume_exists_at", return_value=True):
            scoped = ep._scope_to_destination(parsed, DEST)
        assert _names(scoped) == ["home-20260818-021031", "home-20260818-021257"]


class TestSameNamedSubvolumesResolveToTheDestinationCopy:
    def test_the_name_appears_once_not_once_per_filesystem_hit(self):
        ep = _endpoint()
        parsed = ep._parse_snapshot_list(OUTPUT, DEST)
        names = [s.get_name() for s in parsed]
        assert names.count("home-20260818-021031") == 1, names

    def test_identity_comes_from_the_destination_line_not_the_source_line(self):
        """Both copies share a name; only the destination's has a received_uuid.
        Taking the source's line would leave it empty and make verify_structure
        call a good backup "unverifiable"."""
        ep = _endpoint()
        parsed = {s.get_name(): s for s in ep._parse_snapshot_list(OUTPUT, DEST)}
        snap = parsed["home-20260818-021031"]
        assert snap.uuid == "4f9583c7-fd4a-0d48-8fb8-4c116d2b60a6"
        assert snap.received_uuid == "eceff1db-d300-c94e-8735-d5980e5ac4f9"

    def test_parsing_performs_no_remote_io(self):
        """Parsing stays pure; the probe belongs to the scoping step. A parser
        that opened connections could not be tested without a host."""
        ep = _endpoint()
        ep._exec_remote_command = MagicMock(
            side_effect=AssertionError("parsing must not talk to the remote")
        )
        ep._parse_snapshot_list(OUTPUT, DEST)


class TestTheProbeUsesBtrfsSoElevationApplies:
    def _probe_argv(self, **config):
        ep = _endpoint(**config)
        seen = {}

        def record(cmd, **kw):
            seen["cmd"] = cmd
            return MagicMock(returncode=0)

        ep._exec_remote_command = MagicMock(side_effect=record)
        ep._exec_remote_command_with_retry = MagicMock(side_effect=record)
        ep._subvolume_exists_at(f"{DEST}/snap")
        return seen["cmd"]

    def test_it_is_a_btrfs_subcommand_when_elevated_too(self):
        """The sudo path is a SEPARATE call site in _subvolume_exists_at, and it
        is the one that matters: --ssh-sudo exists precisely for root-owned
        destinations, where a non-btrfs probe is refused and reads as absent."""
        assert self._probe_argv(ssh_sudo=True)[:3] == ["btrfs", "subvolume", "show"]

    def test_it_is_a_btrfs_subcommand(self):
        """--ssh-sudo elevates btrfs and nothing else, so the probe must be btrfs
        to have the same privilege that produced the listing. A `test -d` here
        would be refused on a root-owned destination and read as 'absent'."""
        assert self._probe_argv()[:3] == ["btrfs", "subvolume", "show"]

    def test_it_probes_the_exact_path(self):
        assert self._probe_argv()[-1] == f"{DEST}/snap"

    def test_a_nonzero_exit_means_absent(self):
        ep = _endpoint()
        ep._exec_remote_command = MagicMock(return_value=MagicMock(returncode=1))
        assert ep._subvolume_exists_at(f"{DEST}/snap") is False

    def test_a_zero_exit_means_present(self):
        ep = _endpoint()
        ep._exec_remote_command = MagicMock(return_value=MagicMock(returncode=0))
        assert ep._subvolume_exists_at(f"{DEST}/snap") is True


class TestTheListingItselfIsScoped:
    """The gap that mattered: every test above drove _scope_to_destination
    directly, so deleting the call from list_snapshots changed nothing they
    could see. These go through the public entry point."""

    def _listing(self, present, output=OUTPUT, **config):
        ep = _endpoint(**config)
        ep._run_diagnostics = MagicMock()
        ep._is_master_active = MagicMock(return_value=False)
        result = MagicMock(returncode=0, stdout=output.encode(), stderr=b"")
        with patch.object(ep, "_exec_remote_command", return_value=result):
            with patch.object(
                ep, "_exec_remote_command_with_retry", return_value=result
            ):
                with patch.object(
                    ep,
                    "_subvolume_exists_at",
                    side_effect=lambda path: path in present,
                ):
                    return ep.list_snapshots()

    def test_list_snapshots_excludes_a_never_transferred_snapshot(self):
        got = self._listing({f"{DEST}/home-20260818-021031"})
        assert _names(got) == ["home-20260818-021031"]

    def test_list_snapshots_does_not_report_a_name_twice(self):
        got = self._listing({f"{DEST}/home-20260818-021031"})
        assert len(got) == 1, _names(got)

    def test_list_snapshots_returns_nothing_for_an_empty_destination(self):
        assert self._listing(set()) == []

    def test_list_snapshots_keeps_what_is_really_there(self):
        got = self._listing(
            {
                f"{DEST}/home-20260818-021031",
                f"{DEST}/home-20260818-021257",
            }
        )
        assert _names(got) == ["home-20260818-021031", "home-20260818-021257"]

    def test_the_elevated_listing_is_scoped_the_same_way(self):
        got = self._listing({f"{DEST}/home-20260818-021031"}, ssh_sudo=True)
        assert _names(got) == ["home-20260818-021031"]


class TestOrderingDoesNotDecideIdentity:
    def test_destination_line_wins_when_it_comes_second(self):
        ep = _endpoint()
        snap = {s.get_name(): s for s in ep._parse_snapshot_list(OUTPUT, DEST)}
        assert snap["home-20260818-021031"].received_uuid == (
            "eceff1db-d300-c94e-8735-d5980e5ac4f9"
        )

    def test_destination_line_wins_when_it_comes_first(self):
        ep = _endpoint()
        snap = {
            s.get_name(): s for s in ep._parse_snapshot_list(OUTPUT_DEST_FIRST, DEST)
        }
        assert snap["home-20260818-021031"].received_uuid == (
            "eceff1db-d300-c94e-8735-d5980e5ac4f9"
        )
        assert snap["home-20260818-021031"].uuid == (
            "4f9583c7-fd4a-0d48-8fb8-4c116d2b60a6"
        )
