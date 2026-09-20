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
    endpoint/raw.py     RawEndpoint._prepare, and target_lock

and, found afterwards and covered here too:

    endpoint/common.py  Endpoint.snapshot, an absolute snapshot_folder
    endpoint/local.py   the .btrfs-backup-ng tree, built with parents=True

Raw was the worst of them. It created the target on first use -- convenient
until the disk is not mounted -- and it applies no filesystem check at all: no
fs_checks, no btrfs test. Nothing else would have noticed, and raw streams are
as large as the data being backed up, so an unmounted disk filled the root
filesystem silently. Its target_lock rebuilt the directory too, for every locked
operation, which would have undone the refusal in _prepare.

Directories BELOW an existing configured path are still created: the
.btrfs-backup-ng tree, and a relative snapshot_dir.
"""

from __future__ import annotations

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


class TestSnapshotCreatesOnlyBelowTheSource:
    """Endpoint.snapshot() built its snapshot directory with parents=True. For
    an absolute snapshot_folder that meant the base was created wherever it
    pointed -- reproduced on real btrfs: an absolute folder that did not
    exist was built and the snapshot taken into it, exit 0. It is the
    endpoint-level twin of the CLI's absolute snapshot_dir rule, reached by
    legacy mode and by any API caller, and it would also rebuild a folder
    the CLI had created if the disk went away mid-run.

    Now: an absolute folder must exist; a relative one is created one
    component at a time below the source, which must itself exist."""

    def _stop_after_the_directory(self, monkeypatch):
        class Stop(Exception):
            pass

        def raiser(*a, **k):
            raise Stop()

        import btrfs_backup_ng.__util__ as util

        monkeypatch.setattr(util, "Snapshot", raiser)
        return Stop

    def test_an_absolute_folder_that_does_not_exist_is_refused(
        self, tmp_path, monkeypatch
    ):
        source = tmp_path / "source"
        source.mkdir()
        folder = tmp_path / "unmounted" / "snapshots"
        stop = self._stop_after_the_directory(monkeypatch)
        endpoint = _endpoint(source, tmp_path / "dest", snapshot_folder=str(folder))
        with pytest.raises(AbortError, match="Nothing was created"):
            endpoint.snapshot()
        assert not (tmp_path / "unmounted").exists()
        assert stop  # the seam was never reached

    def test_an_absolute_folder_that_exists_is_used(self, tmp_path, monkeypatch):
        source = tmp_path / "source"
        source.mkdir()
        folder = tmp_path / "bigdisk" / "snapshots"
        folder.mkdir(parents=True)
        stop = self._stop_after_the_directory(monkeypatch)
        endpoint = _endpoint(source, tmp_path / "dest", snapshot_folder=str(folder))
        with pytest.raises(stop):
            endpoint.snapshot()
        assert endpoint.config["path"] == folder.resolve()

    def test_a_relative_folder_is_created_under_the_source(self, tmp_path, monkeypatch):
        source = tmp_path / "source"
        source.mkdir()
        stop = self._stop_after_the_directory(monkeypatch)
        endpoint = _endpoint(source, tmp_path / "dest", snapshot_folder="snaps/hourly")
        with pytest.raises(stop):
            endpoint.snapshot()
        assert (source / "snaps" / "hourly").is_dir()

    def test_a_relative_folder_is_not_created_beside_a_missing_source(
        self, tmp_path, monkeypatch
    ):
        """Mutation guard: a parents=True here rebuilds <source>/snaps and
        the source with it."""
        source = tmp_path / "gone" / "source"
        self._stop_after_the_directory(monkeypatch)
        endpoint = _endpoint(source, tmp_path / "dest", snapshot_folder="snaps")
        with pytest.raises(AbortError, match="Source"):
            endpoint.snapshot()
        assert not (tmp_path / "gone").exists()


class TestInfrastructureBelowTheDestinationCannotRebuildIt:
    def test_prepare_creates_the_tree_one_level_at_a_time(self, tmp_path):
        source = tmp_path / "source"
        source.mkdir()
        dest = tmp_path / "dest"
        dest.mkdir()
        _prepare(source, dest)
        assert (dest / ".btrfs-backup-ng" / "snapshots").is_dir()

    def test_a_destination_that_vanishes_after_the_check_is_not_rebuilt(
        self, tmp_path, monkeypatch
    ):
        """prepare() verified the destination, then the drive went away
        before the .btrfs-backup-ng tree was made. With parents=True the
        tree -- and the destination -- were rebuilt on the root filesystem."""
        source = tmp_path / "source"
        source.mkdir()
        dest = tmp_path / "dest"
        dest.mkdir()
        real_is_btrfs = __import__("btrfs_backup_ng").__util__.is_btrfs

        def vanish(path):
            import shutil

            shutil.rmtree(dest)
            return True

        with patch("shutil.which", return_value="/usr/bin/btrfs"):
            with patch("btrfs_backup_ng.__util__.is_subvolume", return_value=True):
                with patch("btrfs_backup_ng.__util__.is_btrfs", side_effect=vanish):
                    with pytest.raises(AbortError, match="Nothing was created"):
                        _endpoint(source, dest).prepare()
        assert not dest.exists(), "the infrastructure mkdir rebuilt the destination"
        assert real_is_btrfs  # keep the reference honest


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


class TestRawTargetsGetTheSameRule:
    """Raw applies no filesystem check of its own, so nothing else catches this."""

    @staticmethod
    def _raw(dest):
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        return RawEndpoint(config={"path": str(dest), "fs_checks": "skip"})

    def test_prepare_refuses_a_target_that_does_not_exist(self, tmp_path):
        dest = tmp_path / "mnt" / "usb" / "backups"

        with pytest.raises(AbortError) as excinfo:
            self._raw(dest)._prepare()

        assert str(dest) in str(excinfo.value)
        assert "mounted" in str(excinfo.value).lower()

    def test_prepare_creates_nothing_when_it_refuses(self, tmp_path):
        dest = tmp_path / "mnt" / "usb" / "backups"

        with pytest.raises(AbortError):
            self._raw(dest)._prepare()

        assert not dest.exists()
        assert not (tmp_path / "mnt").exists(), (
            "refused the target but still built its parents on this filesystem"
        )

    def test_prepare_accepts_a_target_that_exists(self, tmp_path):
        dest = tmp_path / "rawtarget"
        dest.mkdir()

        self._raw(dest)._prepare()

        assert dest.is_dir()

    def test_target_lock_does_not_rebuild_a_missing_target(self, tmp_path):
        """It ran for every locked operation and would have undone the refusal."""
        dest = tmp_path / "mnt" / "usb" / "backups"
        endpoint = self._raw(dest)

        with pytest.raises(AbortError):
            with endpoint.target_lock():
                pass

        assert not dest.exists()

    def test_target_lock_still_works_when_the_target_exists(self, tmp_path):
        dest = tmp_path / "rawtarget"
        dest.mkdir()
        endpoint = self._raw(dest)

        with endpoint.target_lock():
            pass

        assert dest.is_dir()
