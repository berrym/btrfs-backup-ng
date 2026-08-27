"""The mount table is escaped, and four parsers ignored that.

The kernel escapes space, tab, newline and backslash in every path field of
/proc/mounts as \\040, \\011, \\012 and \\134. Comparing an undecoded field
against a real path fails for any mount point containing one of them.

That is not an edge case on a systemd desktop. udisks2 mounts removable drives
at /run/media/<user>/<volume label>, and labels routinely contain spaces -- so
the single most common external-drive layout could not be matched at all:

  * `require_mount` could never be satisfied, making the safety option unusable
    exactly where it matters most;
  * `is_btrfs` reported a btrfs drive as not-btrfs purely because of its label.

Proven against a REAL bind mount with a space before the fix, not a mocked table.
"""

from __future__ import annotations

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__util__ import unescape_mount_field


class TestTheDecoder:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (r"/run/media/u/My\040Backup", "/run/media/u/My Backup"),
            (r"/mnt/two\040words\040here", "/mnt/two words here"),
            (r"/mnt/tab\011sep", "/mnt/tab\tsep"),
            (r"/mnt/nl\012here", "/mnt/nl\nhere"),
            (r"/mnt/back\134slash", "/mnt/back\\slash"),
            ("/mnt/plain", "/mnt/plain"),
        ],
    )
    def test_octal_escapes_are_decoded(self, raw, expected):
        assert unescape_mount_field(raw) == expected

    @pytest.mark.parametrize(
        "raw", [r"/mnt/not\09x", r"/mnt/trailing\04", r"/mnt/\999high", r"/mnt/\abc"]
    )
    def test_malformed_escapes_are_left_alone(self, raw):
        """A literal backslash in a name must survive unharmed."""
        assert unescape_mount_field(raw) == raw


def _table(monkeypatch, tmp_path, lines):
    f = tmp_path / "mounts"
    f.write_text("".join(lines))
    monkeypatch.setattr(__util__, "MOUNTS_FILE", str(f))
    return f


class TestTheFedoraLayoutWorks:
    """udisks2: /run/media/<user>/<Volume Label>, label containing a space."""

    LABEL = "/run/media/mberry/My Backup"
    LINE = "/dev/sdb1 /run/media/mberry/My\\040Backup btrfs rw,relatime 0 0\n"

    def test_is_mounted_finds_a_mount_point_containing_a_space(
        self, monkeypatch, tmp_path
    ):
        _table(monkeypatch, tmp_path, [self.LINE])
        assert __util__.is_mounted(self.LABEL), (
            "the drive is mounted but is_mounted cannot see it, so require_mount "
            "can never be satisfied for a volume label containing a space"
        )

    def test_get_mount_info_decodes_the_mount_point(self, monkeypatch, tmp_path):
        _table(monkeypatch, tmp_path, [self.LINE])
        info = __util__.get_mount_info(self.LABEL + "/box1")
        assert info is not None
        assert info["mount_point"] == self.LABEL
        assert info["fs_type"] == "btrfs"

    def test_is_btrfs_recognises_a_drive_whose_label_has_a_space(
        self, monkeypatch, tmp_path
    ):
        """A btrfs drive reported as not-btrfs purely because of its label."""
        _table(monkeypatch, tmp_path, ["/dev/sda1 / ext4 rw 0 0\n", self.LINE])
        assert __util__.is_btrfs(self.LABEL + "/box1")

    def test_the_gate_accepts_the_udisks2_layout(self, monkeypatch, tmp_path):
        from btrfs_backup_ng.cli.common import assert_target_mounted

        _table(monkeypatch, tmp_path, [self.LINE])
        assert_target_mounted(self.LABEL + "/box1", self.LABEL)

    def test_the_gate_still_refuses_when_that_drive_is_absent(
        self, monkeypatch, tmp_path
    ):
        from btrfs_backup_ng.cli.common import assert_target_mounted

        # udisks2 removes the directory on unplug; /run remains.
        _table(monkeypatch, tmp_path, ["tmpfs /run tmpfs rw 0 0\n"])
        with pytest.raises(__util__.AbortError, match="is not mounted"):
            assert_target_mounted(self.LABEL + "/box1", self.LABEL)


class TestAMemoryBackedMountIsRefused:
    """/run is tmpfs and ALWAYS mounted, and udisks2 mounts drives beneath it.
    So require_mount = "/run" is present precisely when the drive is absent, and
    the backup goes into RAM. Containment cannot catch it -- the target really is
    under /run."""

    @pytest.mark.parametrize("fs_type", ["tmpfs", "ramfs", "devtmpfs"])
    def test_a_memory_filesystem_is_refused(self, monkeypatch, tmp_path, fs_type):
        from btrfs_backup_ng.cli.common import assert_target_mounted

        _table(monkeypatch, tmp_path, [f"tmpfs /run {fs_type} rw 0 0\n"])
        with pytest.raises(__util__.AbortError, match="held in memory"):
            assert_target_mounted("/run/media/mberry/USB/backups", "/run")

    def test_a_real_filesystem_is_accepted(self, monkeypatch, tmp_path):
        from btrfs_backup_ng.cli.common import assert_target_mounted

        _table(monkeypatch, tmp_path, ["/dev/sdb1 /mnt/backup btrfs rw 0 0\n"])
        assert_target_mounted("/mnt/backup/box1", "/mnt/backup")
