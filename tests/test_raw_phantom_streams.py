"""A sidecar is not a backup; the stream it describes is.

``discover_raw_snapshots`` reads each ``.meta`` and reports the snapshot it
records, and nothing checked that the stream still existed. A sidecar whose
stream had been deleted was therefore listed as a present backup carrying its
RECORDED size -- measured at 999999 bytes from a directory that held only the
sidecar.

Three things then go wrong, and the third is the expensive one:

* ``raw list`` shows a backup that is not there.
* ``restore`` fails on it, at the worst possible moment.
* retention counts it against the keep budget, so a REAL backup is pruned to
  make room for one that does not exist.

The second discovery pass cannot compensate: it enumerates stream FILES, and
there is no file for it to find.
"""

from __future__ import annotations

import json

import pytest

from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot, discover_raw_snapshots


def _stream(tmp_path, name, data=b"stream-bytes"):
    path = tmp_path / f"{name}.btrfs"
    path.write_bytes(data)
    return path


def _sidecar(tmp_path, name, *, size=0, stream=True):
    if stream:
        _stream(tmp_path, name)
    snap = RawSnapshot(name=name, stream_path=tmp_path / f"{name}.btrfs", size=size)
    (tmp_path / f"{name}.btrfs.meta").write_text(json.dumps(snap.to_dict()))
    return snap


class TestAPhantomIsNotListed:
    def test_a_sidecar_without_its_stream_is_not_a_backup(self, tmp_path):
        _sidecar(tmp_path, "home.20240102-120000", size=999999, stream=False)

        found = discover_raw_snapshots(tmp_path)

        assert found == [], (
            f"a sidecar with no stream was listed as a backup: "
            f"{[(s.name, s.size) for s in found]}"
        )

    def test_it_is_reported_rather_than_dropped_quietly(self, tmp_path, capsys):
        """A sidecar without its stream means a backup is GONE. Silence there is
        how an operator discovers it during a restore instead of before one."""
        _sidecar(tmp_path, "home.20240102-120000", stream=False)

        discover_raw_snapshots(tmp_path)

        err = capsys.readouterr().err
        assert "missing" in err and "that backup is gone" in err, err

    def test_real_backups_beside_it_are_unaffected(self, tmp_path):
        _sidecar(tmp_path, "home.20240101-120000")
        _sidecar(tmp_path, "home.20240102-120000", stream=False)
        _sidecar(tmp_path, "home.20240103-120000")

        found = sorted(s.name for s in discover_raw_snapshots(tmp_path))

        assert found == ["home.20240101-120000", "home.20240103-120000"]

    def test_a_stream_without_a_sidecar_is_still_found(self, tmp_path):
        """The second pass, which is how a legacy or backfilled stream lists.
        This guard must not have disturbed it."""
        _stream(tmp_path, "home.20240104-120000")

        found = discover_raw_snapshots(tmp_path)

        assert [s.name for s in found] == ["home.20240104-120000"]


class TestTheRetentionConsequence:
    def test_a_phantom_no_longer_occupies_a_keep_slot(self, tmp_path):
        """The reason this is data loss rather than a cosmetic listing bug: with
        the phantom counted, keeping N snapshots keeps N-1 real ones and deletes
        a real backup to stay within budget."""
        for day in ("01", "02", "03"):
            _sidecar(tmp_path, f"home.202401{day}-120000")
        _sidecar(tmp_path, "home.20240104-120000", size=999999, stream=False)

        found = discover_raw_snapshots(tmp_path)

        assert len(found) == 3, (
            f"retention would budget for {len(found)} backups when only 3 exist"
        )
        assert all(s.stream_path.exists() for s in found)


@pytest.mark.parametrize("suffix", [".btrfs", ".btrfs.zst", ".btrfs.gz"])
def test_the_check_follows_the_recorded_stream_path(tmp_path, suffix):
    """The stream path comes from the sidecar, so a compressed stream must still
    be found by it rather than assumed to end in a bare .btrfs."""
    name = "home.20240101-120000"
    path = tmp_path / f"{name}{suffix}"
    path.write_bytes(b"stream")
    # A size that cannot come from the file itself, so the assertion can tell
    # WHICH pass listed it: the sidecar's authoritative record, or the second
    # pass inferring from the filename. Checking only the name cannot -- the
    # stream file is there either way, so a broken existence check silently
    # demotes a backup to filename inference and loses its recorded
    # compress/encrypt/cipher/checksum.
    snap = RawSnapshot(name=name, stream_path=path, size=4242)
    (tmp_path / f"{name}{suffix}.meta").write_text(json.dumps(snap.to_dict()))

    found = discover_raw_snapshots(tmp_path)

    assert [s.name for s in found] == [name], (
        f"a {suffix} stream was not matched to its sidecar"
    )
    assert found[0].size == 4242, (
        f"a {suffix} stream was listed from its filename rather than its "
        "sidecar, losing the authoritative metadata"
    )
