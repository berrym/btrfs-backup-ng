"""A configured path is a statement that something is there, not a request to make it.

Reported by a contributor against local targets (#102): a target on an external
disk may or may not be present depending on whether the disk is mounted, and the
tool created the path unconditionally. The backup then landed on the ROOT
filesystem, underneath what later becomes a mount point -- invisible once the
real disk is mounted over it, and counted against the wrong filesystem's free
space. `require_mount` does not cover this: it works only when the configured
path IS the mount point, because that one always exists.

Refusing also catches a typo, which is the same argument from the other side.

The reporter suspected his patch was incomplete, and it was. There were FOUR
places that create a configured path, and fixing only the first would have been
undone by the third:

    endpoint/local.py   _prepare, the destination and the source
    endpoint/local.py   an ABSOLUTE snapshot_dir, which can name another filesystem
    endpoint/common.py  Endpoint.receive, immediately before receiving
    endpoint/raw.py     RawEndpoint._prepare        <- the following commit

The raw endpoint is fixed separately because withdrawing its first-run
convenience is a distinct decision, not because it is a lesser problem. It is
the worse one: RawEndpoint._prepare applies no filesystem check at all.

Directories BELOW an existing configured path are still created: the
.btrfs-backup-ng tree, and a relative snapshot_dir.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from btrfs_backup_ng.__util__ import AbortError
from btrfs_backup_ng.endpoint.local import LocalEndpoint


def _endpoint(source, dest, **extra):
    config = {
        "source": str(source) if source else None,
        "path": str(dest),
        "fs_checks": "auto",
    }
    config.update(extra)
    return LocalEndpoint(config=config)


def _prepare(source, dest, **extra):
    with patch("shutil.which", return_value="/usr/bin/btrfs"):
        with patch("btrfs_backup_ng.__util__.is_subvolume", return_value=True):
            with patch("btrfs_backup_ng.__util__.is_btrfs", return_value=True):
                _endpoint(source, dest, **extra).prepare()


class TestTheUnmountedDiskScenario:
    def test_the_mount_point_tree_is_not_built_on_the_root_filesystem(self, tmp_path):
        """The reported bug, end to end."""
        source = tmp_path / "source"
        source.mkdir()
        # Nothing of this exists: it is where an external disk would be mounted.
        dest = tmp_path / "mnt" / "external" / "backups"

        with pytest.raises(AbortError):
            _prepare(source, dest)

        assert not dest.exists()
        assert not (tmp_path / "mnt").exists(), (
            "refused the destination but still created its parents"
        )

    def test_the_message_names_the_likely_cause(self, tmp_path):
        source = tmp_path / "source"
        source.mkdir()
        dest = tmp_path / "absent"

        with pytest.raises(AbortError) as excinfo:
            _prepare(source, dest)

        message = str(excinfo.value)
        assert str(dest) in message
        assert "mounted" in message.lower(), (
            "an operator hitting this has an unmounted disk and should be told so"
        )


class TestSourceAndDestination:
    def test_a_missing_destination_is_refused(self, tmp_path):
        source = tmp_path / "source"
        source.mkdir()
        with pytest.raises(AbortError):
            _prepare(source, tmp_path / "absent")

    def test_a_missing_source_is_refused(self, tmp_path):
        dest = tmp_path / "dest"
        dest.mkdir()
        with pytest.raises(AbortError):
            _prepare(tmp_path / "absent", dest)

    def test_a_missing_source_is_not_created(self, tmp_path):
        dest = tmp_path / "dest"
        dest.mkdir()
        source = tmp_path / "absent"
        with pytest.raises(AbortError):
            _prepare(source, dest)
        assert not source.exists()

    def test_both_present_succeeds(self, tmp_path):
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()
        dest.mkdir()
        _prepare(source, dest)
        assert (dest / ".btrfs-backup-ng" / "snapshots").is_dir()


class TestSnapshotFolderIsDeliberatelyUnchanged:
    """The snapshot directory is the SOURCE side, and is not what #102 is about.

    Note `local.py` reads `config["snapshot_dir"]` in `_prepare`, but nothing
    ever puts that key in an endpoint config -- the endpoint layer uses
    `snapshot_folder`, set by the base Endpoint. That block has never executed.
    It is left exactly as found rather than quietly deleted or quietly fixed.

    The live creation is `Endpoint.snapshot()`, which creates the snapshot
    folder under the source on first use. That is wanted for the relative
    default (`.snapshots`). Whether an ABSOLUTE snapshot_folder should instead
    be required to exist -- it can name another filesystem, so it carries the
    same unmounted-disk hazard -- is an open decision, not a defect fix, and is
    pinned here so that changing it is deliberate.
    """

    def test_a_relative_snapshot_folder_is_created_under_the_source(self, tmp_path):
        source = tmp_path / "source"
        source.mkdir()
        endpoint = _endpoint(source, tmp_path / "dest", snapshot_folder="snaps")
        with patch("btrfs_backup_ng.__util__.is_subvolume", return_value=True):
            with patch.object(type(endpoint), "_build_snapshot_cmd", create=True):
                folder = Path(endpoint.config["snapshot_folder"])
        assert not folder.is_absolute()

    def test_the_dead_snapshot_dir_key_is_still_absent_from_endpoint_config(
        self, tmp_path
    ):
        """If this ever starts passing a value, local.py's block goes live."""
        endpoint = _endpoint(tmp_path / "s", tmp_path / "d", snapshot_dir="whatever")
        assert endpoint.config.get("snapshot_dir") is None


class TestReceiveDoesNotRebuildWhatPrepareRefused:
    """The site that would have silently undone the fix."""

    def test_receive_refuses_a_missing_destination(self, tmp_path):
        dest = tmp_path / "mnt" / "external"
        endpoint = _endpoint(None, dest)

        with pytest.raises(AbortError) as excinfo:
            endpoint.receive(stdin=None, snapshot_name="snap")

        assert str(dest) in str(excinfo.value)
        assert not dest.exists()

    def test_receive_no_longer_merely_warns(self, tmp_path):
        """It logged a warning and carried on, leaving a less specific failure later."""
        dest = tmp_path / "absent"
        endpoint = _endpoint(None, dest)

        with pytest.raises(AbortError):
            endpoint.receive(stdin=None)
