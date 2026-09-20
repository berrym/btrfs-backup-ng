"""A listing is a READ: it must never create the path it enumerates.

The listing used to mkdir its configured path, quietly undoing _prepare's
34904c6 refusal to create configured paths. Every CLI command calls prepare()
first, but that is a per-command convention, not a property of the primitive
-- and the gap was measured: prepare() passes while a removable drive is
mounted, the drive unmounts mid-run, and the next listing REBUILT the mount
point on the root filesystem and reported the pool empty, so presence checks
saw nothing and the planner scheduled full re-sends into the directory the
read had just invented.

Contract (mirrors the ssh listing at ssh.py:2604): a missing SOURCE-side
snapshot dir is a legitimate baseline (a volume never snapshotted); a missing
BACKUP DESTINATION is unreadable, never empty. The discriminator is
config["source"], which every source-side endpoint carries and destination
endpoints do not.
"""

from __future__ import annotations

import shutil

import pytest

from btrfs_backup_ng.endpoint.local import LocalEndpoint


def _dest_endpoint(path):
    return LocalEndpoint(config={"path": path, "source": None, "snap_prefix": "home."})


def _source_endpoint(path):
    return LocalEndpoint(
        config={"path": path, "source": "/some/volume", "snap_prefix": "home."}
    )


def test_the_mid_run_vanish_raises_instead_of_recreating(tmp_path):
    """The measured defect, reproduced before the fix: one real backup listed
    while the drive is present; the path vanishes; the re-list returned 0 and
    RECREATED the directory. It must instead refuse loudly and create
    nothing. Mutation guards: restoring the mkdir recreates the directory and
    lists empty; replacing the raise with `return []` presents a failed
    enumeration as an empty target."""
    dest = tmp_path / "mnt" / "backups"
    dest.mkdir(parents=True)
    (dest / "home.20260101-000000").mkdir()
    ep = _dest_endpoint(dest)
    assert [s.get_name() for s in ep.list_snapshots()] == ["home.20260101-000000"]

    shutil.rmtree(tmp_path / "mnt")

    with pytest.raises(RuntimeError, match="NOT an empty target"):
        ep.list_snapshots(flush_cache=True)
    assert not dest.exists(), "the READ recreated the directory"


def test_a_missing_destination_is_unreadable_not_empty(tmp_path):
    missing = tmp_path / "unmounted" / "backups"
    ep = _dest_endpoint(missing)
    with pytest.raises(RuntimeError, match="NOT an empty target"):
        ep.list_snapshots()
    assert not (tmp_path / "unmounted").exists(), "the listing created the path"


def test_a_missing_source_snapshot_dir_is_a_legitimate_baseline(tmp_path):
    """A volume that has never been snapshotted has no snapshot directory:
    empty is the truth, and creation belongs to snapshot(), not to a read."""
    missing = tmp_path / "vol" / ".snapshots"
    ep = _source_endpoint(missing)
    assert ep.list_snapshots() == []
    assert not (tmp_path / "vol").exists(), "the listing created the path"


@pytest.mark.parametrize("make_endpoint", [_dest_endpoint, _source_endpoint])
def test_a_file_at_the_path_is_diagnosed_for_both_roles(tmp_path, make_endpoint):
    """A non-directory at the configured path is a misconfiguration for
    either role. The old mkdir path let it escape as a raw FileExistsError,
    which is not a diagnosis; the message must say what is wrong."""
    stray = tmp_path / "backups"
    stray.write_text("not a directory")
    ep = make_endpoint(stray)
    with pytest.raises(RuntimeError, match="exists but is not a directory"):
        ep.list_snapshots()


def test_an_existing_directory_lists_exactly_as_before(tmp_path):
    (tmp_path / "home.20260101-000000").mkdir()
    (tmp_path / "home.20260102-000000").mkdir()
    ep = _dest_endpoint(tmp_path)
    assert [s.get_name() for s in ep.list_snapshots()] == [
        "home.20260101-000000",
        "home.20260102-000000",
    ]


def test_correspondents_of_still_never_raises_and_never_creates(tmp_path):
    """correspondents_of's documented contract is never-raise (a listing
    failure degrades the planner to full sends). The new refusal must arrive
    there as an empty mapping -- and the swallow path must not create the
    directory either: the full sends then fail loudly at receive instead of
    landing in a directory a read invented."""
    missing = tmp_path / "gone" / "backups"
    ep = _dest_endpoint(missing)
    assert ep.correspondents_of([]) == {}
    assert not (tmp_path / "gone").exists()
