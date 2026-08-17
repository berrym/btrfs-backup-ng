"""The raw ``.part`` file is private to one transfer and opened without following links.

Two defects lived in how the temp stream file was named and opened.

The name was derived from the snapshot name alone, so two runs against the same
target -- a cron run overlapping a manual one -- were handed the SAME temp file.
Nothing serialized the write: ``target_lock`` is taken only around the rename and
the sidecar write, not the stream. Their bytes interleaved into one published
stream, the sha256 was then sealed over the corruption, and both processes exited
0. The engine's return-code gate passed, ``raw verify`` reported ok, and the
damage surfaced only at restore -- the one raw failure mode with no detector.

The file was also opened by path with no ``O_NOFOLLOW``, so a symlink planted at
the ``.part`` name made this endpoint -- typically running as root against a
directory untrusted users may write -- truncate whatever the link pointed at.

Both are closed by ``_open_part_file``: a name carrying the pid and a monotonic
stamp, opened ``O_CREAT|O_EXCL|O_NOFOLLOW`` at 0600.
"""

from __future__ import annotations

import errno
import os
from pathlib import Path

import pytest

from btrfs_backup_ng.endpoint.raw import PARTIAL_SUFFIX, RawEndpoint


def _receive(endpoint: RawEndpoint, name: str, payload: bytes, tmp_path: Path):
    src = tmp_path / f"{name}.src"
    src.write_bytes(payload)
    with open(src, "rb") as stdin:
        proc = endpoint.receive(stdin, snapshot_name=name)
        proc.communicate()
    return proc


class TestPartNameIsPrivateToOneTransfer:
    def test_two_endpoints_do_not_share_a_part_path(self, tmp_path):
        """The defect directly: same target, same snapshot name, one temp file."""
        a = RawEndpoint(config={"path": str(tmp_path)})
        b = RawEndpoint(config={"path": str(tmp_path)})

        _receive(a, "snap", b"a" * 64, tmp_path)
        path_a = Path(a._pending_metadata["part_path"])
        # b's receive must not be able to open a's in-flight file.
        _receive(b, "snap", b"b" * 64, tmp_path)
        path_b = Path(b._pending_metadata["part_path"])

        assert path_a != path_b, (
            f"both transfers were handed {path_a}; concurrent runs would interleave "
            "their bytes into one published stream and seal a sha256 over it"
        )
        assert path_a.exists() and path_b.exists()
        # Neither stream was corrupted by the other.
        assert path_a.read_bytes() == b"a" * 64
        assert path_b.read_bytes() == b"b" * 64

    def test_part_name_still_ends_with_the_suffix_discovery_ignores(self, tmp_path):
        """Discovery excludes ``.part``; a unique name must not escape that filter."""
        endpoint = RawEndpoint(config={"path": str(tmp_path)})
        _receive(endpoint, "snap", b"payload", tmp_path)
        part = Path(endpoint._pending_metadata["part_path"])
        assert part.name.endswith(PARTIAL_SUFFIX), part.name

    def test_part_name_is_a_sibling_of_the_final_stream(self, tmp_path):
        """commit_receive renames within the directory, so it must stay a sibling."""
        endpoint = RawEndpoint(config={"path": str(tmp_path)})
        _receive(endpoint, "snap", b"payload", tmp_path)
        part = Path(endpoint._pending_metadata["part_path"])
        final = Path(endpoint._pending_metadata["stream_path"])
        assert part.parent == final.parent == tmp_path


class TestPartFileIsOpenedSafely:
    def test_symlink_at_the_part_path_is_refused(self, tmp_path):
        """O_NOFOLLOW: a planted symlink must not be followed and truncated.

        Running as root against a target directory an untrusted user can write,
        following the link would truncate whatever it points at.
        """
        victim = tmp_path / "victim"
        victim.write_bytes(b"do-not-truncate")
        part = tmp_path / f"planted{PARTIAL_SUFFIX}"
        part.symlink_to(victim)

        with pytest.raises(OSError) as excinfo:
            RawEndpoint._open_part_file(part)
        # ELOOP from O_NOFOLLOW, or EEXIST from O_EXCL -- either refuses the link.
        assert excinfo.value.errno in (errno.ELOOP, errno.EEXIST), excinfo.value
        assert victim.read_bytes() == b"do-not-truncate", "the symlink was followed"

    def test_existing_file_is_refused(self, tmp_path):
        """O_EXCL: never adopt a file another transfer may still be writing."""
        part = tmp_path / f"taken{PARTIAL_SUFFIX}"
        part.write_bytes(b"someone-elses-stream")

        with pytest.raises(FileExistsError):
            RawEndpoint._open_part_file(part)
        assert part.read_bytes() == b"someone-elses-stream"

    def test_mode_is_private(self, tmp_path):
        """0600, matching every other durable artifact this endpoint writes."""
        part = tmp_path / f"fresh{PARTIAL_SUFFIX}"
        fd = RawEndpoint._open_part_file(part)
        os.close(fd)
        assert part.stat().st_mode & 0o777 == 0o600, oct(part.stat().st_mode)

    def test_received_stream_is_private(self, tmp_path):
        """The end-to-end result: a published stream is not world-readable."""
        endpoint = RawEndpoint(config={"path": str(tmp_path)})
        _receive(endpoint, "snap", b"secret-backup-bytes", tmp_path)
        endpoint.commit_receive()
        final = tmp_path / "snap.btrfs"
        assert final.exists()
        assert final.stat().st_mode & 0o077 == 0, oct(final.stat().st_mode)


def test_symlink_planted_at_the_derivable_part_name_is_not_followed(tmp_path):
    """The vulnerability behaviourally, through the real receive() path.

    The old ``.part`` name was ``<snapshot><ext>.part`` -- fully derivable by
    anyone who can see the target directory. Planting a symlink there made the
    receive open and truncate the link's target. This drives receive() rather
    than the opener, so it demonstrates the defect on the old code instead of
    merely finding a helper missing.
    """
    victim = tmp_path / "victim"
    victim.write_bytes(b"do-not-truncate")
    (tmp_path / f"snap.btrfs{PARTIAL_SUFFIX}").symlink_to(victim)

    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    _receive(endpoint, "snap", b"incoming-stream-bytes", tmp_path)

    assert victim.read_bytes() == b"do-not-truncate", (
        "the receive followed a planted symlink and truncated its target; running "
        "as root against a directory untrusted users can write, that is arbitrary "
        "file destruction"
    )


def test_stream_contents_survive_the_fd_handoff(tmp_path):
    """The pipeline writes through an inherited descriptor, not a re-opened path."""
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    payload = bytes(range(256)) * 64
    proc = _receive(endpoint, "snap", payload, tmp_path)
    assert proc.returncode == 0
    endpoint.commit_receive()
    assert (tmp_path / "snap.btrfs").read_bytes() == payload


def test_compressed_pipeline_also_writes_through_the_descriptor(tmp_path):
    """The multi-stage path lost its `> path` redirect; it must still produce bytes."""
    endpoint = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
    payload = b"compress-me" * 512
    proc = _receive(endpoint, "snap", payload, tmp_path)
    assert proc.returncode == 0
    part = Path(endpoint._pending_metadata["part_path"])
    assert part.stat().st_size > 0, "the pipeline wrote nothing"
    endpoint.commit_receive()
    published = tmp_path / "snap.btrfs.gz"
    assert published.exists() and published.stat().st_size > 0
    assert published.stat().st_mode & 0o077 == 0
