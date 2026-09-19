"""--convert-rw and --sync must do what their help text says.

Both are shipped legacy CLI flags. ``-w/--convert-rw`` promises "Convert
read-only snapshots to read-write before deleting them" and ``-s/--sync``
promises "Run 'btrfs subvolume sync' after deleting subvolumes". Both are
threaded from the parsed options into the endpoint config, and
``_build_deletion_commands`` honours both -- but it had no caller.
``delete_snapshots`` assembled its own single ``btrfs subvolume delete`` and
consulted neither, so the two options had no effect on any target.
"""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint


def _local(tmp_path, count=2, **config):
    ep = LocalEndpoint(config={"path": str(tmp_path), "snap_prefix": "root.", **config})
    snaps = [
        __util__.Snapshot(tmp_path, "root.", ep, time_obj=time.localtime(t))
        for t in (1700000000, 1700086400, 1700172800)[:count]
    ]
    return ep, snaps


def _issued(ep, snaps):
    """Every btrfs command the deletion actually issues, in order."""
    commands: list[str] = []
    with patch.object(
        LocalEndpoint,
        "_exec_command",
        side_effect=lambda o, **k: commands.append(
            " ".join(str(a) for a, _is_path in o["command"])
        ),
    ):
        ep.delete_snapshots(snaps)
    return commands


class TestTheDeletionFlagsTakeEffect:
    def test_without_the_flags_only_the_delete_is_issued(self, tmp_path):
        ep, snaps = _local(tmp_path)
        assert _issued(ep, snaps) == [
            f"btrfs subvolume delete {snap.get_path()}" for snap in snaps
        ]

    def test_convert_rw_clears_the_read_only_property_first(self, tmp_path):
        """-w/--convert-rw: "Convert read-only snapshots to read-write before
        deleting them." Ordering is the whole point -- after the delete it would
        be meaningless."""
        ep, snaps = _local(tmp_path, convert_rw=True)
        commands = _issued(ep, snaps)

        for snap in snaps:
            path = str(snap.get_path())
            assert f"btrfs property set -ts {path} ro false" in commands
            assert commands.index(
                f"btrfs property set -ts {path} ro false"
            ) < commands.index(f"btrfs subvolume delete {path}"), (
                "the read-only property was cleared after the delete, not before"
            )

    def test_sync_runs_once_after_the_batch(self, tmp_path):
        """-s/--sync: "Run 'btrfs subvolume sync' after deleting subvolumes."
        Once for the batch, not once per snapshot, and last."""
        ep, snaps = _local(tmp_path, count=3, subvolume_sync=True)
        commands = _issued(ep, snaps)

        sync = f"btrfs subvolume sync {tmp_path}"
        assert commands.count(sync) == 1, f"sync issued {commands.count(sync)} times"
        assert commands[-1] == sync

    def test_sync_is_not_run_when_nothing_was_deleted(self, tmp_path):
        """Waiting for the cleanup of deletions that did not happen."""
        ep, snaps = _local(tmp_path, subvolume_sync=True)
        for snap in snaps:
            snap.locks.add("restore")
        commands = _issued(ep, snaps)

        assert commands == []

    def test_a_failed_convert_rw_fails_that_snapshot_and_skips_its_delete(
        self, tmp_path
    ):
        """The conversion is a precondition of the delete, not an optional extra:
        running the delete anyway after it failed is how a read-only subvolume
        gets reported as deleted when btrfs refused it."""
        ep, snaps = _local(tmp_path, count=1, convert_rw=True)
        issued: list[str] = []

        def fail_the_property_set(options, **kwargs):
            text = " ".join(str(a) for a, _ in options["command"])
            issued.append(text)
            if "property set" in text:
                raise RuntimeError("Read-only file system")

        with patch.object(
            LocalEndpoint, "_exec_command", side_effect=fail_the_property_set
        ):
            result = ep.delete_snapshots(snaps)

        assert result.failed_count == 1 and result.deleted_count == 0
        assert not any("subvolume delete" in c for c in issued), (
            "the delete ran even though the read-write conversion failed"
        )

    def test_a_failed_sync_does_not_unreport_the_deletions(self, tmp_path):
        """The subvolumes are already gone from the tree. Failing the batch over
        a failed wait would report a loss that did not happen."""
        ep, snaps = _local(tmp_path, subvolume_sync=True)

        def fail_the_sync(options, **kwargs):
            # Matched on the command verbs, not on a substring of the whole line:
            # tmp_path is named after this test and so contains "sync", which
            # made the DELETES fail instead and the test pass for the wrong
            # reason.
            verbs = [str(a) for a, _is_path in options["command"]][:3]
            if verbs == ["btrfs", "subvolume", "sync"]:
                raise RuntimeError("Interrupted system call")

        with patch.object(LocalEndpoint, "_exec_command", side_effect=fail_the_sync):
            result = ep.delete_snapshots(snaps)

        assert result.deleted_count == 2 and result.ok


@pytest.mark.parametrize("flag", ["convert_rw", "subvolume_sync"])
def test_the_flag_reaches_the_endpoint_from_the_legacy_cli(flag):
    """Both are shipped legacy options; the wiring from flag to config is what
    made them look implemented while the delete path ignored them."""
    import inspect

    from btrfs_backup_ng import _legacy_main

    source = inspect.getsource(_legacy_main)
    assert f'"{flag}": options[' in source, (
        f"{flag} is no longer threaded from the legacy CLI into the endpoint config"
    )
