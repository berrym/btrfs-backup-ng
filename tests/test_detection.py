"""Tests for btrfs subvolume detection module."""

from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.detection import (
    BackupSuggestion,
    BtrfsMountInfo,
    DetectedSubvolume,
    DetectionError,
    DetectionResult,
    PermissionDeniedError,
    SubvolumeClass,
    classify_all_subvolumes,
    classify_subvolume,
    correlate_mounts_and_subvolumes,
    detect_subvolumes,
    generate_suggestions,
    list_subvolumes,
    parse_proc_mounts,
    process_detection_result,
    scan_system,
)
from btrfs_backup_ng.detection.classifier import (
    _get_priority_and_reason,
    _suggest_snapshot_dir,
)
from btrfs_backup_ng.detection.scanner import get_subvolume_details, is_removable_media


class TestBtrfsMountInfo:
    """Tests for BtrfsMountInfo dataclass."""

    def test_creation(self):
        """Test basic creation of BtrfsMountInfo."""
        mount = BtrfsMountInfo(
            device="/dev/sda1",
            mount_point="/home",
            subvol_path="/home",
            subvol_id=256,
        )
        assert mount.device == "/dev/sda1"
        assert mount.mount_point == "/home"
        assert mount.subvol_path == "/home"
        assert mount.subvol_id == 256

    def test_hash_and_equality(self):
        """Test hash and equality based on device and subvol_id."""
        mount1 = BtrfsMountInfo(
            device="/dev/sda1",
            mount_point="/home",
            subvol_path="/home",
            subvol_id=256,
        )
        mount2 = BtrfsMountInfo(
            device="/dev/sda1",
            mount_point="/mnt/home",
            subvol_path="/home",
            subvol_id=256,
        )
        mount3 = BtrfsMountInfo(
            device="/dev/sda1",
            mount_point="/home",
            subvol_path="/home",
            subvol_id=257,
        )

        assert mount1 == mount2
        assert mount1 != mount3
        assert hash(mount1) == hash(mount2)
        assert hash(mount1) != hash(mount3)

    def test_equality_with_non_mount_info(self):
        """Test equality returns NotImplemented for non-BtrfsMountInfo."""
        mount = BtrfsMountInfo(
            device="/dev/sda1",
            mount_point="/home",
            subvol_path="/home",
            subvol_id=256,
        )
        # Comparing with non-BtrfsMountInfo should use NotImplemented
        assert mount != "not a mount"
        assert mount != 256
        assert mount != None

    def test_hash_for_set_usage(self):
        """Test that BtrfsMountInfo can be used in sets."""
        mount1 = BtrfsMountInfo(
            device="/dev/sda1",
            mount_point="/home",
            subvol_path="/home",
            subvol_id=256,
        )
        mount2 = BtrfsMountInfo(
            device="/dev/sda1",
            mount_point="/mnt/home",
            subvol_path="/home",
            subvol_id=256,
        )
        # Can be used in a set
        mount_set = {mount1, mount2}
        assert len(mount_set) == 1  # Same device and subvol_id


class TestDetectedSubvolume:
    """Tests for DetectedSubvolume dataclass."""

    def test_creation(self):
        """Test basic creation of DetectedSubvolume."""
        subvol = DetectedSubvolume(id=256, path="/home")
        assert subvol.id == 256
        assert subvol.path == "/home"
        assert subvol.mount_point is None
        assert subvol.classification == SubvolumeClass.UNKNOWN

    def test_display_path_with_mount(self):
        """Test display_path returns mount_point when available."""
        subvol = DetectedSubvolume(id=256, path="/@home", mount_point="/home")
        assert subvol.display_path == "/home"

    def test_display_path_without_mount(self):
        """Test display_path returns path when no mount_point."""
        subvol = DetectedSubvolume(id=256, path="/home")
        assert subvol.display_path == "/home"

    def test_suggested_prefix_home(self):
        """Test suggested_prefix for /home (includes trailing dash)."""
        subvol = DetectedSubvolume(id=256, path="/home", mount_point="/home")
        assert subvol.suggested_prefix == "home-"

    def test_suggested_prefix_root(self):
        """Test suggested_prefix for / (includes trailing dash)."""
        subvol = DetectedSubvolume(id=5, path="/", mount_point="/")
        assert subvol.suggested_prefix == "root-"

    def test_suggested_prefix_nested(self):
        """Test suggested_prefix for nested path (includes trailing dash)."""
        subvol = DetectedSubvolume(id=260, path="/var/log", mount_point="/var/log")
        assert subvol.suggested_prefix == "var-log-"

    def test_hash_and_equality(self):
        """Test hash and equality based on id and device."""
        subvol1 = DetectedSubvolume(id=256, path="/home", device="/dev/sda1")
        subvol2 = DetectedSubvolume(id=256, path="/@home", device="/dev/sda1")
        subvol3 = DetectedSubvolume(id=257, path="/home", device="/dev/sda1")

        assert subvol1 == subvol2
        assert subvol1 != subvol3

    def test_equality_with_non_subvolume(self):
        """Test equality returns NotImplemented for non-DetectedSubvolume."""
        subvol = DetectedSubvolume(id=256, path="/home", device="/dev/sda1")
        # Comparing with non-DetectedSubvolume should use NotImplemented
        assert subvol != "not a subvolume"
        assert subvol != 256
        assert subvol != None

    def test_hash_for_set_usage(self):
        """Test that DetectedSubvolume can be used in sets."""
        subvol1 = DetectedSubvolume(id=256, path="/home", device="/dev/sda1")
        subvol2 = DetectedSubvolume(id=256, path="/@home", device="/dev/sda1")
        subvol3 = DetectedSubvolume(id=257, path="/data", device="/dev/sda1")

        # Same id and device should hash to same value
        assert hash(subvol1) == hash(subvol2)

        # Can be used in a set
        subvol_set = {subvol1, subvol2, subvol3}
        assert len(subvol_set) == 2  # subvol1 and subvol2 are equal


class TestBackupSuggestion:
    """Tests for BackupSuggestion dataclass."""

    def test_is_recommended_high_priority(self):
        """Test is_recommended for high priority suggestions."""
        subvol = DetectedSubvolume(id=256, path="/home")
        suggestion = BackupSuggestion(
            subvolume=subvol,
            suggested_prefix="home",
            priority=1,
        )
        assert suggestion.is_recommended is True

    def test_is_recommended_low_priority(self):
        """Test is_recommended for low priority suggestions."""
        subvol = DetectedSubvolume(id=260, path="/var/cache")
        suggestion = BackupSuggestion(
            subvolume=subvol,
            suggested_prefix="var-cache",
            priority=5,
        )
        assert suggestion.is_recommended is False


class TestDetectionResult:
    """Tests for DetectionResult dataclass."""

    def test_recommended_subvolumes(self):
        """Test recommended_subvolumes property."""
        home = DetectedSubvolume(id=256, path="/home")
        root = DetectedSubvolume(id=5, path="/")
        cache = DetectedSubvolume(id=260, path="/var/cache")

        result = DetectionResult(
            subvolumes=[home, root, cache],
            suggestions=[
                BackupSuggestion(subvolume=home, suggested_prefix="home", priority=1),
                BackupSuggestion(subvolume=root, suggested_prefix="root", priority=2),
                BackupSuggestion(
                    subvolume=cache, suggested_prefix="var-cache", priority=5
                ),
            ],
        )

        recommended = result.recommended_subvolumes
        assert len(recommended) == 2
        assert home in recommended
        assert root in recommended
        assert cache not in recommended

    def test_excluded_subvolumes(self):
        """Test excluded_subvolumes property."""
        home = DetectedSubvolume(
            id=256, path="/home", classification=SubvolumeClass.USER_DATA
        )
        snapshot = DetectedSubvolume(
            id=300,
            path="/.snapshots/1/snapshot",
            classification=SubvolumeClass.SNAPSHOT,
        )
        internal = DetectedSubvolume(
            id=301,
            path="/var/lib/machines",
            classification=SubvolumeClass.INTERNAL,
        )

        result = DetectionResult(subvolumes=[home, snapshot, internal])

        excluded = result.excluded_subvolumes
        assert len(excluded) == 2
        assert snapshot in excluded
        assert internal in excluded
        assert home not in excluded

    def test_optional_subvolumes(self):
        """Test optional_subvolumes property."""
        home = DetectedSubvolume(
            id=256, path="/home", classification=SubvolumeClass.USER_DATA
        )
        var_log = DetectedSubvolume(
            id=257, path="/var/log", classification=SubvolumeClass.VARIABLE
        )
        snapshot = DetectedSubvolume(
            id=300,
            path="/.snapshots/1/snapshot",
            classification=SubvolumeClass.SNAPSHOT,
        )

        # home is recommended, var_log is not
        result = DetectionResult(
            subvolumes=[home, var_log, snapshot],
            suggestions=[
                BackupSuggestion(subvolume=home, suggested_prefix="home", priority=1),
                BackupSuggestion(
                    subvolume=var_log, suggested_prefix="var-log", priority=5
                ),
            ],
        )

        optional = result.optional_subvolumes
        # var_log should be optional (not recommended, not excluded)
        assert len(optional) == 1
        assert var_log in optional
        # home is recommended, not optional
        assert home not in optional
        # snapshot is excluded, not optional
        assert snapshot not in optional

    def test_to_dict(self):
        """Test to_dict serialization."""
        mount = BtrfsMountInfo(
            device="/dev/sda1",
            mount_point="/home",
            subvol_path="/home",
            subvol_id=256,
        )
        subvol = DetectedSubvolume(
            id=256,
            path="/home",
            mount_point="/home",
            classification=SubvolumeClass.USER_DATA,
        )
        suggestion = BackupSuggestion(
            subvolume=subvol,
            suggested_prefix="home",
            priority=1,
            reason="User data",
        )

        result = DetectionResult(
            filesystems=[mount],
            subvolumes=[subvol],
            suggestions=[suggestion],
            is_partial=False,
        )

        d = result.to_dict()
        assert d["is_partial"] is False
        assert len(d["filesystems"]) == 1
        assert d["filesystems"][0]["device"] == "/dev/sda1"
        assert len(d["subvolumes"]) == 1
        assert d["subvolumes"][0]["classification"] == "user_data"
        assert len(d["suggestions"]) == 1
        assert d["suggestions"][0]["recommended"] is True


class TestParseProcMounts:
    """Tests for parse_proc_mounts function."""

    def test_parse_single_btrfs_mount(self):
        """Test parsing a single btrfs mount entry."""
        content = "/dev/sda1 /home btrfs rw,subvolid=256,subvol=/home 0 0"

        mounts = parse_proc_mounts(content)

        assert len(mounts) == 1
        assert mounts[0].device == "/dev/sda1"
        assert mounts[0].mount_point == "/home"
        assert mounts[0].subvol_id == 256
        assert mounts[0].subvol_path == "/home"

    def test_parse_multiple_mounts(self):
        """Test parsing multiple mount entries including non-btrfs."""
        content = """/dev/sda1 / btrfs rw,subvolid=5,subvol=/ 0 0
/dev/sda1 /home btrfs rw,subvolid=256,subvol=/home 0 0
/dev/sdb1 /mnt/data ext4 rw 0 0
tmpfs /tmp tmpfs rw 0 0"""

        mounts = parse_proc_mounts(content)

        assert len(mounts) == 2
        assert mounts[0].mount_point == "/"
        assert mounts[1].mount_point == "/home"

    def test_parse_empty_content(self):
        """Test parsing empty content."""
        mounts = parse_proc_mounts("")
        assert mounts == []

    def test_parse_no_btrfs(self):
        """Test parsing content with no btrfs mounts."""
        content = """/dev/sda1 / ext4 rw 0 0
/dev/sdb1 /home ext4 rw 0 0"""

        mounts = parse_proc_mounts(content)
        assert mounts == []

    def test_parse_complex_options(self):
        """Test parsing mount with complex options."""
        content = (
            "/dev/mapper/luks-xxx /home btrfs "
            "rw,relatime,compress=zstd:3,ssd,space_cache=v2,"
            "subvolid=256,subvol=/home 0 0"
        )

        mounts = parse_proc_mounts(content)

        assert len(mounts) == 1
        assert mounts[0].device == "/dev/mapper/luks-xxx"
        assert mounts[0].subvol_id == 256
        assert "compress" in mounts[0].options
        assert mounts[0].options["compress"] == "zstd:3"

    def test_parse_at_prefix_subvol(self):
        """Test parsing mount with @ prefix in subvol path."""
        content = "/dev/sda1 / btrfs rw,subvolid=256,subvol=/@ 0 0"

        mounts = parse_proc_mounts(content)

        assert len(mounts) == 1
        assert mounts[0].subvol_path == "/@"

    def test_parse_file_not_found(self):
        """Test handling of missing mounts file."""
        mounts = parse_proc_mounts(mounts_file="/nonexistent/path")
        assert mounts == []

    def test_excludes_removable_media_by_default(self):
        """Test that removable media mounts are excluded by default."""
        content = """/dev/sda1 / btrfs rw,subvolid=5,subvol=/ 0 0
/dev/sda1 /home btrfs rw,subvolid=256,subvol=/home 0 0
/dev/sdb1 /run/media/user/external btrfs rw,subvolid=5,subvol=/ 0 0
/dev/sdc1 /media/backup btrfs rw,subvolid=5,subvol=/ 0 0
/dev/sdd1 /mnt/usb btrfs rw,subvolid=5,subvol=/ 0 0"""

        mounts = parse_proc_mounts(content)

        # Only / and /home should be included
        assert len(mounts) == 2
        mount_points = {m.mount_point for m in mounts}
        assert "/" in mount_points
        assert "/home" in mount_points
        assert "/run/media/user/external" not in mount_points
        assert "/media/backup" not in mount_points
        assert "/mnt/usb" not in mount_points

    def test_includes_removable_media_when_disabled(self):
        """Test that removable media can be included if requested."""
        content = """/dev/sda1 / btrfs rw,subvolid=5,subvol=/ 0 0
/dev/sdb1 /run/media/user/external btrfs rw,subvolid=5,subvol=/ 0 0"""

        mounts = parse_proc_mounts(content, exclude_removable=False)

        assert len(mounts) == 2
        mount_points = {m.mount_point for m in mounts}
        assert "/" in mount_points
        assert "/run/media/user/external" in mount_points


class TestListSubvolumes:
    """Tests for list_subvolumes function."""

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_list_subvolumes_success(self, mock_run):
        """Test successful subvolume listing."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="""ID 256 gen 12345 top level 5 path <FS_TREE>/home
ID 257 gen 12346 top level 5 path <FS_TREE>/@
ID 258 gen 12347 top level 256 path <FS_TREE>/home/.snapshots/1/snapshot""",
        )

        subvols = list_subvolumes("/")

        assert len(subvols) == 3
        assert subvols[0].id == 256
        assert subvols[0].path == "/home"
        assert subvols[1].id == 257
        assert subvols[1].path == "/@"
        assert subvols[2].id == 258
        assert subvols[2].top_level == 256

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_list_subvolumes_fs_tree_no_slash(self, mock_run):
        """Test parsing path with <FS_TREE> but no slash after."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="ID 256 gen 12345 top level 5 path <FS_TREE>home",
        )

        subvols = list_subvolumes("/")

        assert len(subvols) == 1
        assert subvols[0].path == "/home"

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_list_subvolumes_unparseable_line(self, mock_run):
        """Test handling of lines that don't match expected format."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="""ID 256 gen 12345 top level 5 path <FS_TREE>/home
some garbage line that doesn't match
ID 257 gen 12346 top level 5 path <FS_TREE>/data""",
        )

        subvols = list_subvolumes("/")

        # Should skip the bad line
        assert len(subvols) == 2
        assert subvols[0].path == "/home"
        assert subvols[1].path == "/data"

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_list_subvolumes_permission_denied(self, mock_run):
        """Test permission denied error handling."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="ERROR: cannot read /: Permission denied",
        )

        with pytest.raises(PermissionDeniedError):
            list_subvolumes("/")

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_list_subvolumes_operation_not_permitted(self, mock_run):
        """Test operation not permitted error handling."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="ERROR: Operation not permitted",
        )

        with pytest.raises(PermissionDeniedError):
            list_subvolumes("/")

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_list_subvolumes_other_error(self, mock_run):
        """Test other error handling."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="ERROR: not a btrfs filesystem",
        )

        with pytest.raises(DetectionError, match="not a btrfs filesystem"):
            list_subvolumes("/mnt/ext4")

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_list_subvolumes_command_not_found(self, mock_run):
        """Test btrfs command not found."""
        mock_run.side_effect = FileNotFoundError()

        with pytest.raises(DetectionError, match="btrfs command not found"):
            list_subvolumes("/")

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_list_subvolumes_empty(self, mock_run):
        """Test handling of empty subvolume list."""
        mock_run.return_value = MagicMock(returncode=0, stdout="")

        subvols = list_subvolumes("/")
        assert subvols == []


class TestIsRemovableMedia:
    """Tests for is_removable_media function."""

    def test_run_media_is_removable(self):
        """Test /run/media paths are detected as removable."""
        assert is_removable_media("/run/media/user/usb") is True

    def test_media_is_removable(self):
        """Test /media paths are detected as removable."""
        assert is_removable_media("/media/backup") is True

    def test_mnt_is_removable(self):
        """Test /mnt paths are detected as removable."""
        assert is_removable_media("/mnt/usb") is True

    def test_root_not_removable(self):
        """Test / is not removable."""
        assert is_removable_media("/") is False

    def test_home_not_removable(self):
        """Test /home is not removable."""
        assert is_removable_media("/home") is False


class TestGetSubvolumeDetails:
    """Tests for get_subvolume_details function."""

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_get_details_success(self, mock_run):
        """Test getting subvolume details."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="""Name: home
UUID: abc-123-def
Parent UUID: -
Received UUID: -
Creation time: 2024-01-01 12:00:00
Subvolume ID: 256
Generation: 12345
Flags: -""",
        )

        details = get_subvolume_details("/home")

        assert details["name"] == "home"
        assert details["uuid"] == "abc-123-def"
        assert details["subvolume id"] == "256"

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_get_details_command_not_found(self, mock_run):
        """Test handling btrfs command not found."""
        mock_run.side_effect = FileNotFoundError()

        details = get_subvolume_details("/home")

        assert details == {}

    @patch("btrfs_backup_ng.detection.scanner.subprocess.run")
    def test_get_details_command_fails(self, mock_run):
        """Test handling command failure."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="ERROR: not a subvolume",
        )

        details = get_subvolume_details("/not/a/subvolume")

        assert details == {}


class TestCorrelateSubvolumes:
    """Tests for correlate_mounts_and_subvolumes function."""

    def test_correlate_by_id(self):
        """Test correlation by subvolume ID."""
        mounts = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/home",
                subvol_path="/home",
                subvol_id=256,
            )
        ]
        subvols = [DetectedSubvolume(id=256, path="/home")]

        result = correlate_mounts_and_subvolumes(mounts, subvols)

        assert result[0].mount_point == "/home"
        assert result[0].device == "/dev/sda1"

    def test_correlate_by_path(self):
        """Test correlation by path when ID doesn't match."""
        mounts = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/home",
                subvol_path="/home",
                subvol_id=999,  # Different ID
            )
        ]
        subvols = [DetectedSubvolume(id=256, path="/home")]

        result = correlate_mounts_and_subvolumes(mounts, subvols)

        assert result[0].mount_point == "/home"
        assert result[0].device == "/dev/sda1"

    def test_correlate_unmounted_subvol(self):
        """Test that unmounted subvolumes get device from filesystem."""
        mounts = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/",
                subvol_path="/",
                subvol_id=5,
            )
        ]
        subvols = [
            DetectedSubvolume(id=256, path="/home"),  # Not mounted
        ]

        result = correlate_mounts_and_subvolumes(mounts, subvols)

        # Should inherit device from the mount
        assert result[0].device == "/dev/sda1"
        assert result[0].mount_point is None

    def test_adds_mounted_subvolumes_not_in_list(self):
        """Test that mounted subvolumes not in btrfs list are added.

        The top-level subvolume (ID 5) is not shown by 'btrfs subvolume list'
        but may be mounted as /. This test ensures such subvolumes are added.
        """
        mounts = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/",
                subvol_path="/",
                subvol_id=5,  # Top-level, not in btrfs list
            ),
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/home",
                subvol_path="/home",
                subvol_id=256,
            ),
        ]
        # btrfs subvolume list only shows 256, not 5
        subvols = [DetectedSubvolume(id=256, path="/home")]

        result = correlate_mounts_and_subvolumes(mounts, subvols)

        # Should have both subvolumes now
        assert len(result) == 2
        ids = {s.id for s in result}
        assert 5 in ids
        assert 256 in ids

        # Check the added root subvolume
        root_subvol = next(s for s in result if s.id == 5)
        assert root_subvol.mount_point == "/"
        assert root_subvol.path == "/"
        assert root_subvol.device == "/dev/sda1"

    def test_adds_root_subvolume_from_mount(self):
        """Test that root / is added when only mounted but not in subvol list."""
        mounts = [
            BtrfsMountInfo(
                device="/dev/nvme0n1p2",
                mount_point="/",
                subvol_path="/",
                subvol_id=5,
            ),
        ]
        # Empty subvolume list (simulates btrfs list not showing top-level)
        subvols = []

        result = correlate_mounts_and_subvolumes(mounts, subvols)

        assert len(result) == 1
        assert result[0].id == 5
        assert result[0].path == "/"
        assert result[0].mount_point == "/"
        assert result[0].device == "/dev/nvme0n1p2"

    def test_no_duplicate_subvolumes_added(self):
        """Test that subvolumes already in list are not duplicated."""
        mounts = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/home",
                subvol_path="/home",
                subvol_id=256,
            ),
        ]
        # Subvolume already in list
        subvols = [DetectedSubvolume(id=256, path="/home")]

        result = correlate_mounts_and_subvolumes(mounts, subvols)

        # Should still be just one
        assert len(result) == 1
        assert result[0].id == 256

    def test_adds_subvolume_with_normalized_path(self):
        """Test that added subvolumes have normalized paths starting with /."""
        mounts = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/",
                subvol_path="@",  # No leading slash
                subvol_id=256,
            ),
        ]
        subvols = []

        result = correlate_mounts_and_subvolumes(mounts, subvols)

        assert len(result) == 1
        assert result[0].path == "/@"  # Should be normalized with leading /


class TestClassifySubvolume:
    """Tests for classify_subvolume function."""

    def test_classify_home_as_user_data(self):
        """Test /home is classified as USER_DATA."""
        subvol = DetectedSubvolume(id=256, path="/home", mount_point="/home")
        assert classify_subvolume(subvol) == SubvolumeClass.USER_DATA

    def test_classify_user_home_as_user_data(self):
        """Test /home/user is classified as USER_DATA."""
        subvol = DetectedSubvolume(
            id=257, path="/home/alice", mount_point="/home/alice"
        )
        assert classify_subvolume(subvol) == SubvolumeClass.USER_DATA

    def test_classify_root_as_system_root(self):
        """Test / is classified as SYSTEM_ROOT."""
        subvol = DetectedSubvolume(id=5, path="/", mount_point="/")
        assert classify_subvolume(subvol) == SubvolumeClass.SYSTEM_ROOT

    def test_classify_at_root_as_system_root(self):
        """Test /@ is classified as SYSTEM_ROOT."""
        subvol = DetectedSubvolume(id=256, path="/@", mount_point="/")
        assert classify_subvolume(subvol) == SubvolumeClass.SYSTEM_ROOT

    def test_classify_snapshots_directory(self):
        """Test .snapshots directory is classified as SNAPSHOT."""
        subvol = DetectedSubvolume(id=300, path="/.snapshots/1/snapshot")
        assert classify_subvolume(subvol) == SubvolumeClass.SNAPSHOT

    def test_classify_generic_snapshots(self):
        """Test generic .snapshots path is classified as SNAPSHOT."""
        subvol = DetectedSubvolume(id=300, path="/home/.snapshots/backup")
        assert classify_subvolume(subvol) == SubvolumeClass.SNAPSHOT

    def test_classify_timeshift_snapshots(self):
        """Test timeshift snapshots are classified as SNAPSHOT."""
        subvol = DetectedSubvolume(id=300, path="/timeshift-btrfs/snapshots/2024-01-01")
        assert classify_subvolume(subvol) == SubvolumeClass.SNAPSHOT

    def test_classify_root_snapshots_dir(self):
        """Test /.snapshots directory is classified as SNAPSHOT."""
        subvol = DetectedSubvolume(
            id=300, path="/.snapshots", mount_point="/.snapshots"
        )
        assert classify_subvolume(subvol) == SubvolumeClass.SNAPSHOT

    def test_classify_home_snapshots_dir(self):
        """Test /home/.snapshots directory is classified as SNAPSHOT."""
        subvol = DetectedSubvolume(
            id=301, path="/home/.snapshots", mount_point="/home/.snapshots"
        )
        assert classify_subvolume(subvol) == SubvolumeClass.SNAPSHOT

    def test_classify_with_parent_uuid(self):
        """Test subvolume with parent_uuid is classified as SNAPSHOT."""
        subvol = DetectedSubvolume(id=300, path="/some/path", parent_uuid="abc-123-def")
        assert classify_subvolume(subvol) == SubvolumeClass.SNAPSHOT

    def test_classify_var_lib_machines_as_internal(self):
        """Test /var/lib/machines is classified as INTERNAL."""
        subvol = DetectedSubvolume(id=400, path="/var/lib/machines")
        assert classify_subvolume(subvol) == SubvolumeClass.INTERNAL

    def test_classify_var_lib_docker_as_internal(self):
        """Test /var/lib/docker is classified as INTERNAL."""
        subvol = DetectedSubvolume(id=400, path="/var/lib/docker")
        assert classify_subvolume(subvol) == SubvolumeClass.INTERNAL

    def test_classify_var_cache_as_variable(self):
        """Test /var/cache is classified as VARIABLE."""
        subvol = DetectedSubvolume(id=500, path="/var/cache")
        assert classify_subvolume(subvol) == SubvolumeClass.VARIABLE

    def test_classify_var_log_as_variable(self):
        """Test /var/log is classified as VARIABLE."""
        subvol = DetectedSubvolume(id=500, path="/var/log")
        assert classify_subvolume(subvol) == SubvolumeClass.VARIABLE

    def test_classify_opt_as_system_data(self):
        """Test /opt is classified as SYSTEM_DATA."""
        subvol = DetectedSubvolume(id=600, path="/opt", mount_point="/opt")
        assert classify_subvolume(subvol) == SubvolumeClass.SYSTEM_DATA

    def test_classify_srv_as_system_data(self):
        """Test /srv is classified as SYSTEM_DATA."""
        subvol = DetectedSubvolume(id=600, path="/srv", mount_point="/srv")
        assert classify_subvolume(subvol) == SubvolumeClass.SYSTEM_DATA

    def test_classify_unknown_path(self):
        """Test unknown path is classified as UNKNOWN."""
        subvol = DetectedSubvolume(id=700, path="/some/random/path")
        assert classify_subvolume(subvol) == SubvolumeClass.UNKNOWN

    def test_classify_relative_path_normalized(self):
        """Test that paths without leading slash are normalized."""
        # Path like "@home" (common btrfs subvolume naming) gets normalized to "/@home"
        subvol = DetectedSubvolume(id=256, path="@home")
        result = classify_subvolume(subvol)
        # Should not crash and should return some classification
        assert result in SubvolumeClass


class TestSuggestSnapshotDir:
    """Tests for _suggest_snapshot_dir function."""

    def test_with_mount_point(self):
        """Test suggesting snapshot dir for mounted subvolume."""
        subvol = DetectedSubvolume(id=256, path="/home", mount_point="/home")
        result = _suggest_snapshot_dir(subvol)
        # Returns relative path .snapshots for mounted subvolumes
        assert result == ".snapshots"

    def test_with_root_mount(self):
        """Test suggesting snapshot dir for root mount."""
        subvol = DetectedSubvolume(id=5, path="/", mount_point="/")
        result = _suggest_snapshot_dir(subvol)
        # Returns relative path .snapshots for mounted subvolumes
        assert result == ".snapshots"

    def test_without_mount_point(self):
        """Test suggesting snapshot dir for unmounted subvolume."""
        subvol = DetectedSubvolume(id=256, path="/@home")
        result = _suggest_snapshot_dir(subvol)
        # Should use path-based suggestion
        assert ".snapshots" in result


class TestGetPriorityAndReason:
    """Tests for _get_priority_and_reason function."""

    def test_user_data_priority(self):
        """Test USER_DATA gets priority 1."""
        subvol = DetectedSubvolume(
            id=256,
            path="/home",
            mount_point="/home",
            classification=SubvolumeClass.USER_DATA,
        )
        priority, reason = _get_priority_and_reason(subvol)
        assert priority == 1

    def test_system_root_priority(self):
        """Test SYSTEM_ROOT gets priority 2."""
        subvol = DetectedSubvolume(
            id=5, path="/", mount_point="/", classification=SubvolumeClass.SYSTEM_ROOT
        )
        priority, reason = _get_priority_and_reason(subvol)
        assert priority == 2

    def test_system_data_priority(self):
        """Test SYSTEM_DATA gets priority 3."""
        subvol = DetectedSubvolume(
            id=256,
            path="/opt",
            mount_point="/opt",
            classification=SubvolumeClass.SYSTEM_DATA,
        )
        priority, reason = _get_priority_and_reason(subvol)
        assert priority == 3

    def test_variable_priority_var_log(self):
        """Test VARIABLE with /var/log gets priority 4."""
        subvol = DetectedSubvolume(
            id=256,
            path="/var/log",
            mount_point="/var/log",
            classification=SubvolumeClass.VARIABLE,
        )
        priority, reason = _get_priority_and_reason(subvol)
        assert priority == 4
        assert "Logs" in reason

    def test_variable_priority_other(self):
        """Test VARIABLE without /var/log gets priority 5."""
        subvol = DetectedSubvolume(
            id=256, path="/var/cache", classification=SubvolumeClass.VARIABLE
        )
        priority, reason = _get_priority_and_reason(subvol)
        assert priority == 5

    def test_unknown_priority(self):
        """Test UNKNOWN gets priority 4."""
        subvol = DetectedSubvolume(
            id=256, path="/custom/path", classification=SubvolumeClass.UNKNOWN
        )
        priority, reason = _get_priority_and_reason(subvol)
        assert priority == 4
        assert "Unknown" in reason


class TestClassifyAllSubvolumes:
    """Tests for classify_all_subvolumes function."""

    def test_classifies_all(self):
        """Test that all subvolumes are classified."""
        subvols = [
            DetectedSubvolume(id=256, path="/home", mount_point="/home"),
            DetectedSubvolume(id=257, path="/.snapshots/1/snapshot"),
            DetectedSubvolume(id=258, path="/var/lib/machines"),
        ]

        result = classify_all_subvolumes(subvols)

        assert result[0].classification == SubvolumeClass.USER_DATA
        assert result[1].classification == SubvolumeClass.SNAPSHOT
        assert result[1].is_snapshot is True
        assert result[2].classification == SubvolumeClass.INTERNAL


class TestSnapperAwareClassification:
    """Tests for snapper-aware classification functions."""

    def _create_mock_snapper_config(self, name: str, subvolume: str):
        """Create a mock SnapperConfig-like object."""
        from pathlib import Path
        from unittest.mock import MagicMock

        config = MagicMock()
        config.name = name
        config.subvolume = Path(subvolume)
        return config

    def test_classify_root_from_snapper_config(self):
        """Test that snapper config named 'root' classifies subvolume as SYSTEM_ROOT."""
        from btrfs_backup_ng.detection import classify_from_snapper_config

        # Create a subvolume with UNKNOWN classification (path doesn't match rules)
        subvol = DetectedSubvolume(
            id=256,
            path="/@",
            mount_point="/",
            classification=SubvolumeClass.UNKNOWN,
        )

        snapper_configs = [self._create_mock_snapper_config("root", "/")]

        classify_from_snapper_config([subvol], snapper_configs)

        assert subvol.classification == SubvolumeClass.SYSTEM_ROOT

    def test_classify_home_from_snapper_config(self):
        """Test that snapper config named 'home' classifies subvolume as USER_DATA."""
        from btrfs_backup_ng.detection import classify_from_snapper_config

        subvol = DetectedSubvolume(
            id=257,
            path="/@home",
            mount_point="/home",
            classification=SubvolumeClass.UNKNOWN,
        )

        snapper_configs = [self._create_mock_snapper_config("home", "/home")]

        classify_from_snapper_config([subvol], snapper_configs)

        assert subvol.classification == SubvolumeClass.USER_DATA

    def test_classify_opt_from_snapper_config(self):
        """Test that snapper config named 'opt' classifies subvolume as SYSTEM_DATA."""
        from btrfs_backup_ng.detection import classify_from_snapper_config

        subvol = DetectedSubvolume(
            id=258,
            path="/@opt",
            mount_point="/opt",
            classification=SubvolumeClass.UNKNOWN,
        )

        snapper_configs = [self._create_mock_snapper_config("opt", "/opt")]

        classify_from_snapper_config([subvol], snapper_configs)

        assert subvol.classification == SubvolumeClass.SYSTEM_DATA

    def test_classify_var_log_from_snapper_config(self):
        """Test that snapper config named 'var_log' classifies as VARIABLE."""
        from btrfs_backup_ng.detection import classify_from_snapper_config

        subvol = DetectedSubvolume(
            id=259,
            path="/@var_log",
            mount_point="/var/log",
            classification=SubvolumeClass.UNKNOWN,
        )

        snapper_configs = [self._create_mock_snapper_config("var_log", "/var/log")]

        classify_from_snapper_config([subvol], snapper_configs)

        assert subvol.classification == SubvolumeClass.VARIABLE

    def test_does_not_reclassify_snapshots(self):
        """Test that snapshot classification is preserved."""
        from btrfs_backup_ng.detection import classify_from_snapper_config

        subvol = DetectedSubvolume(
            id=300,
            path="/.snapshots/1/snapshot",
            classification=SubvolumeClass.SNAPSHOT,
        )

        snapper_configs = [self._create_mock_snapper_config("root", "/")]

        classify_from_snapper_config([subvol], snapper_configs)

        # Should still be SNAPSHOT
        assert subvol.classification == SubvolumeClass.SNAPSHOT

    def test_returns_path_map(self):
        """Test that classify_from_snapper_config returns path map."""
        from btrfs_backup_ng.detection import classify_from_snapper_config

        subvol = DetectedSubvolume(
            id=256,
            path="/",
            mount_point="/",
            classification=SubvolumeClass.UNKNOWN,
        )

        snapper_configs = [self._create_mock_snapper_config("root", "/")]

        path_map = classify_from_snapper_config([subvol], snapper_configs)

        assert "/" in path_map
        assert path_map["/"].name == "root"

    def test_reclassify_with_snapper_regenerates_suggestions(self):
        """Test that reclassify_with_snapper regenerates suggestions."""
        from btrfs_backup_ng.detection import reclassify_with_snapper

        # Create a result with UNKNOWN subvolume
        subvol = DetectedSubvolume(
            id=256,
            path="/@",
            mount_point="/",
            classification=SubvolumeClass.UNKNOWN,
        )

        result = DetectionResult(
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/",
                    subvol_path="/",
                    subvol_id=256,
                )
            ],
            subvolumes=[subvol],
            suggestions=[
                BackupSuggestion(
                    subvolume=subvol,
                    suggested_prefix="unknown",
                    priority=4,  # UNKNOWN priority
                )
            ],
        )

        snapper_configs = [self._create_mock_snapper_config("root", "/")]

        reclassify_with_snapper(result, snapper_configs)

        # Classification should be updated
        assert subvol.classification == SubvolumeClass.SYSTEM_ROOT

        # Suggestions should be regenerated with new priority
        assert len(result.suggestions) == 1
        assert result.suggestions[0].priority == 2  # SYSTEM_ROOT priority

    def test_partial_name_match(self):
        """Test that partial name matches work (e.g., 'home_user' -> home)."""
        from btrfs_backup_ng.detection import classify_from_snapper_config

        subvol = DetectedSubvolume(
            id=257,
            path="/home/user",
            mount_point="/home/user",
            classification=SubvolumeClass.UNKNOWN,
        )

        # Config named "home_user" should match "home" pattern
        snapper_configs = [self._create_mock_snapper_config("home_user", "/home/user")]

        classify_from_snapper_config([subvol], snapper_configs)

        assert subvol.classification == SubvolumeClass.USER_DATA

    def test_unknown_snapper_name_preserves_classification(self):
        """Test that unknown snapper names don't change classification."""
        from btrfs_backup_ng.detection import classify_from_snapper_config

        subvol = DetectedSubvolume(
            id=260,
            path="/custom",
            mount_point="/custom",
            classification=SubvolumeClass.UNKNOWN,
        )

        # Config with non-standard name
        snapper_configs = [self._create_mock_snapper_config("mycustom", "/custom")]

        classify_from_snapper_config([subvol], snapper_configs)

        # Should remain UNKNOWN since "mycustom" doesn't match any pattern
        assert subvol.classification == SubvolumeClass.UNKNOWN

    def test_adds_missing_snapper_managed_root(self):
        """Test that snapper-managed / is added when booted from snapshot.

        When the system is booted from a snapper snapshot, the original /
        may not appear in the detection results. But snapper still manages
        it and it should be offered for backup.
        """
        from btrfs_backup_ng.detection import reclassify_with_snapper

        # Simulate booting from a snapshot: / is mounted from a snapshot subvol
        snapshot_subvol = DetectedSubvolume(
            id=9097,
            path="/.snapshots/7636/snapshot",
            mount_point="/",
            classification=SubvolumeClass.SNAPSHOT,
            is_snapshot=True,
        )

        result = DetectionResult(
            subvolumes=[snapshot_subvol],
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/",
                    subvol_path="/.snapshots/7636/snapshot",
                    subvol_id=9097,
                )
            ],
        )

        # Snapper "root" config manages /
        snapper_configs = [self._create_mock_snapper_config("root", "/")]

        reclassify_with_snapper(result, snapper_configs)

        # Should have added a virtual subvolume for /
        paths = [s.path for s in result.subvolumes]
        assert "/" in paths

        # Find the added root subvol
        root_subvol = next(s for s in result.subvolumes if s.path == "/")
        assert root_subvol.classification == SubvolumeClass.SYSTEM_ROOT
        assert root_subvol.mount_point == "/"
        assert root_subvol.id == 0  # Virtual, not directly detected

        # Should have a suggestion for it
        suggestion_paths = [s.subvolume.path for s in result.suggestions]
        assert "/" in suggestion_paths

    def test_adds_missing_snapper_managed_home(self):
        """Test that snapper-managed /home is added if not detected."""
        from btrfs_backup_ng.detection import reclassify_with_snapper

        result = DetectionResult(
            subvolumes=[],  # No detected subvolumes
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/",
                    subvol_path="/",
                    subvol_id=5,
                )
            ],
        )

        snapper_configs = [self._create_mock_snapper_config("home", "/home")]

        reclassify_with_snapper(result, snapper_configs)

        # Should have added /home
        assert len(result.subvolumes) == 1
        assert result.subvolumes[0].path == "/home"
        assert result.subvolumes[0].classification == SubvolumeClass.USER_DATA

    def test_does_not_duplicate_existing_subvolume(self):
        """Test that already-detected subvolumes aren't duplicated."""
        from btrfs_backup_ng.detection import reclassify_with_snapper

        home_subvol = DetectedSubvolume(
            id=256,
            path="/home",
            mount_point="/home",
            classification=SubvolumeClass.USER_DATA,
        )

        result = DetectionResult(
            subvolumes=[home_subvol],
            filesystems=[],
        )

        snapper_configs = [self._create_mock_snapper_config("home", "/home")]

        reclassify_with_snapper(result, snapper_configs)

        # Should still have just one /home subvolume
        home_subvols = [s for s in result.subvolumes if s.path == "/home"]
        assert len(home_subvols) == 1
        assert home_subvols[0].id == 256  # Original, not virtual

    def test_adds_multiple_missing_snapper_subvolumes(self):
        """Test that multiple missing snapper-managed subvolumes are added."""
        from btrfs_backup_ng.detection import reclassify_with_snapper

        result = DetectionResult(
            subvolumes=[],
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/",
                    subvol_path="/",
                    subvol_id=5,
                )
            ],
        )

        snapper_configs = [
            self._create_mock_snapper_config("root", "/"),
            self._create_mock_snapper_config("home", "/home"),
            self._create_mock_snapper_config("opt", "/opt"),
        ]

        reclassify_with_snapper(result, snapper_configs)

        paths = {s.path for s in result.subvolumes}
        assert "/" in paths
        assert "/home" in paths
        assert "/opt" in paths

        # Check classifications
        classifications = {s.path: s.classification for s in result.subvolumes}
        assert classifications["/"] == SubvolumeClass.SYSTEM_ROOT
        assert classifications["/home"] == SubvolumeClass.USER_DATA
        assert classifications["/opt"] == SubvolumeClass.SYSTEM_DATA


class TestGenerateSuggestions:
    """Tests for generate_suggestions function."""

    def test_generates_suggestions_for_user_data(self):
        """Test suggestions are generated for USER_DATA."""
        subvol = DetectedSubvolume(
            id=256,
            path="/home",
            mount_point="/home",
            classification=SubvolumeClass.USER_DATA,
        )

        suggestions = generate_suggestions([subvol])

        assert len(suggestions) == 1
        assert suggestions[0].subvolume == subvol
        assert suggestions[0].priority == 1
        assert suggestions[0].is_recommended is True

    def test_excludes_snapshots(self):
        """Test snapshots are not suggested."""
        subvols = [
            DetectedSubvolume(
                id=256,
                path="/home",
                mount_point="/home",
                classification=SubvolumeClass.USER_DATA,
            ),
            DetectedSubvolume(
                id=300,
                path="/.snapshots/1/snapshot",
                classification=SubvolumeClass.SNAPSHOT,
            ),
        ]

        suggestions = generate_suggestions(subvols)

        assert len(suggestions) == 1
        assert suggestions[0].subvolume.path == "/home"

    def test_excludes_internal(self):
        """Test internal subvolumes are not suggested."""
        subvol = DetectedSubvolume(
            id=400,
            path="/var/lib/machines",
            classification=SubvolumeClass.INTERNAL,
        )

        suggestions = generate_suggestions([subvol])
        assert len(suggestions) == 0

    def test_sorted_by_priority(self):
        """Test suggestions are sorted by priority."""
        subvols = [
            DetectedSubvolume(
                id=500, path="/var/log", classification=SubvolumeClass.VARIABLE
            ),
            DetectedSubvolume(
                id=256, path="/home", classification=SubvolumeClass.USER_DATA
            ),
            DetectedSubvolume(
                id=5, path="/", classification=SubvolumeClass.SYSTEM_ROOT
            ),
        ]

        suggestions = generate_suggestions(subvols)

        assert len(suggestions) == 3
        assert suggestions[0].subvolume.path == "/home"  # Priority 1
        assert suggestions[1].subvolume.path == "/"  # Priority 2
        assert suggestions[2].subvolume.path == "/var/log"  # Priority 4


class TestScanSystem:
    """Tests for scan_system function."""

    @patch("btrfs_backup_ng.detection.scanner.list_subvolumes")
    @patch("btrfs_backup_ng.detection.scanner.parse_proc_mounts")
    def test_scan_system_success(self, mock_parse, mock_list):
        """Test successful system scan."""
        mock_parse.return_value = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/",
                subvol_path="/",
                subvol_id=5,
            )
        ]
        mock_list.return_value = [
            DetectedSubvolume(id=5, path="/"),
            DetectedSubvolume(id=256, path="/home"),
        ]

        result = scan_system()

        assert len(result.filesystems) == 1
        assert len(result.subvolumes) == 2
        assert result.is_partial is False

    @patch("btrfs_backup_ng.detection.scanner.list_subvolumes")
    @patch("btrfs_backup_ng.detection.scanner.parse_proc_mounts")
    def test_scan_system_no_btrfs(self, mock_parse, mock_list):
        """Test scan with no btrfs filesystems."""
        mock_parse.return_value = []

        result = scan_system()

        assert result.filesystems == []
        assert result.error_message == "No btrfs filesystems found."
        mock_list.assert_not_called()

    @patch("btrfs_backup_ng.detection.scanner.list_subvolumes")
    @patch("btrfs_backup_ng.detection.scanner.parse_proc_mounts")
    def test_scan_system_permission_denied_no_partial(self, mock_parse, mock_list):
        """Test permission denied without allow_partial."""
        mock_parse.return_value = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/",
                subvol_path="/",
                subvol_id=5,
            )
        ]
        mock_list.side_effect = PermissionDeniedError("Permission denied")

        with pytest.raises(PermissionDeniedError):
            scan_system(allow_partial=False)

    @patch("btrfs_backup_ng.detection.scanner.list_subvolumes")
    @patch("btrfs_backup_ng.detection.scanner.parse_proc_mounts")
    def test_scan_system_permission_denied_with_partial(self, mock_parse, mock_list):
        """Test permission denied with allow_partial creates fallback."""
        mock_parse.return_value = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/home",
                subvol_path="/home",
                subvol_id=256,
            )
        ]
        mock_list.side_effect = PermissionDeniedError("Permission denied")

        result = scan_system(allow_partial=True)

        assert result.is_partial is True
        assert result.error_message is not None
        # Should create subvolumes from mount info
        assert len(result.subvolumes) == 1
        assert result.subvolumes[0].mount_point == "/home"

    @patch("btrfs_backup_ng.detection.scanner.list_subvolumes")
    @patch("btrfs_backup_ng.detection.scanner.parse_proc_mounts")
    def test_scan_system_deduplicates_devices(self, mock_parse, mock_list):
        """Test that same device is only scanned once."""
        mock_parse.return_value = [
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/",
                subvol_path="/",
                subvol_id=5,
            ),
            BtrfsMountInfo(
                device="/dev/sda1",
                mount_point="/home",
                subvol_path="/home",
                subvol_id=256,
            ),
        ]
        mock_list.return_value = [
            DetectedSubvolume(id=5, path="/"),
            DetectedSubvolume(id=256, path="/home"),
        ]

        scan_system()

        # list_subvolumes should only be called once for the device
        assert mock_list.call_count == 1


class TestDetectSubvolumes:
    """Tests for detect_subvolumes high-level API."""

    @patch("btrfs_backup_ng.detection.scan_system")
    def test_detect_and_classify(self, mock_scan):
        """Test detect_subvolumes classifies and generates suggestions."""
        mock_scan.return_value = DetectionResult(
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/home",
                    subvol_path="/home",
                    subvol_id=256,
                )
            ],
            subvolumes=[
                DetectedSubvolume(id=256, path="/home", mount_point="/home"),
                DetectedSubvolume(id=300, path="/.snapshots/1/snapshot"),
            ],
        )

        result = detect_subvolumes()

        # Should have classified subvolumes
        assert result.subvolumes[0].classification == SubvolumeClass.USER_DATA
        assert result.subvolumes[1].classification == SubvolumeClass.SNAPSHOT

        # Should have generated suggestions (only for /home)
        assert len(result.suggestions) == 1
        assert result.suggestions[0].subvolume.path == "/home"


class TestProcessDetectionResult:
    """Tests for process_detection_result function."""

    def test_processes_result(self):
        """Test that process_detection_result adds classifications and suggestions."""
        result = DetectionResult(
            subvolumes=[
                DetectedSubvolume(id=256, path="/home", mount_point="/home"),
                DetectedSubvolume(id=5, path="/", mount_point="/"),
            ]
        )

        process_detection_result(result)

        # Should have classified
        assert result.subvolumes[0].classification == SubvolumeClass.USER_DATA
        assert result.subvolumes[1].classification == SubvolumeClass.SYSTEM_ROOT

        # Should have suggestions
        assert len(result.suggestions) == 2


class TestCLIIntegration:
    """Tests for CLI integration of detect command."""

    @patch("btrfs_backup_ng.detection.scan_system")
    def test_detect_json_output(self, mock_scan, capsys):
        """Test --json output mode."""
        import argparse

        from btrfs_backup_ng.cli.config_cmd import _detect_subvolumes

        mock_scan.return_value = DetectionResult(
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/home",
                    subvol_path="/home",
                    subvol_id=256,
                )
            ],
            subvolumes=[
                DetectedSubvolume(
                    id=256,
                    path="/home",
                    mount_point="/home",
                    classification=SubvolumeClass.USER_DATA,
                )
            ],
            suggestions=[],
        )

        args = argparse.Namespace(json=True, wizard=False)
        result = _detect_subvolumes(args)

        assert result == 0
        captured = capsys.readouterr()
        assert '"filesystems"' in captured.out
        assert '"/home"' in captured.out

    @patch("btrfs_backup_ng.detection.scan_system")
    def test_detect_no_btrfs(self, mock_scan, capsys):
        """Test output when no btrfs filesystems found."""
        import argparse

        from btrfs_backup_ng.cli.config_cmd import _detect_subvolumes

        mock_scan.return_value = DetectionResult(filesystems=[])

        args = argparse.Namespace(json=False, wizard=False)
        result = _detect_subvolumes(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "No btrfs filesystems found" in captured.out

    @patch("btrfs_backup_ng.detection.scan_system")
    def test_detect_displays_results(self, mock_scan, capsys):
        """Test display of detection results."""
        import argparse

        from btrfs_backup_ng.cli.config_cmd import _detect_subvolumes

        subvol = DetectedSubvolume(
            id=256,
            path="/home",
            mount_point="/home",
            classification=SubvolumeClass.USER_DATA,
        )
        mock_scan.return_value = DetectionResult(
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/home",
                    subvol_path="/home",
                    subvol_id=256,
                )
            ],
            subvolumes=[subvol],
            suggestions=[
                BackupSuggestion(
                    subvolume=subvol,
                    suggested_prefix="home",
                    priority=1,
                )
            ],
        )

        args = argparse.Namespace(json=False, wizard=False)
        result = _detect_subvolumes(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Recommended for backup" in captured.out
        assert "/home" in captured.out


class TestConfigDiffSummary:
    """Tests for _show_config_diff_summary function."""

    def test_show_added_volume(self, capsys):
        """Test showing added volumes in diff summary."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_summary

        existing = """
[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/home"
snapshot_prefix = "home"
"""
        new = """
[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/home"
snapshot_prefix = "home"

[[volumes]]
path = "/"
snapshot_prefix = "root"
"""
        config_data = {
            "volumes": [
                {"path": "/home", "snapshot_prefix": "home", "targets": []},
                {"path": "/", "snapshot_prefix": "root", "targets": [{"path": "/mnt"}]},
            ],
            "retention": {},
        }

        _show_config_diff_summary(existing, new, config_data)

        captured = capsys.readouterr()
        assert "+ Add volume: /" in captured.out
        assert "prefix: root" in captured.out

    def test_show_removed_volume(self, capsys):
        """Test showing removed volumes in diff summary."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_summary

        existing = """
[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/home"
snapshot_prefix = "home"

[[volumes]]
path = "/opt"
snapshot_prefix = "opt"
"""
        new = """
[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/home"
snapshot_prefix = "home"
"""
        config_data = {
            "volumes": [
                {"path": "/home", "snapshot_prefix": "home", "targets": []},
            ],
            "retention": {},
        }

        _show_config_diff_summary(existing, new, config_data)

        captured = capsys.readouterr()
        assert "- Remove volume: /opt" in captured.out

    def test_show_modified_volume(self, capsys):
        """Test showing modified volumes in diff summary."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_summary

        existing = """
[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/home"
snapshot_prefix = "home"

[[volumes.targets]]
path = "/mnt/backup"
"""
        new = """
[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/home"
snapshot_prefix = "home-new"
"""
        config_data = {
            "volumes": [
                {
                    "path": "/home",
                    "snapshot_prefix": "home-new",
                    "targets": [{"path": "/mnt/a"}, {"path": "/mnt/b"}],
                },
            ],
            "retention": {},
        }

        _show_config_diff_summary(existing, new, config_data)

        captured = capsys.readouterr()
        assert "~ Modify volume: /home" in captured.out
        assert "prefix: home -> home-new" in captured.out

    def test_show_retention_changes(self, capsys):
        """Test showing retention changes in diff summary."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_summary

        existing = """
[global]
snapshot_dir = ".snapshots"

[global.retention]
daily = 7
weekly = 4
"""
        new = """
[global]
snapshot_dir = ".snapshots"

[global.retention]
daily = 14
weekly = 4
"""
        config_data = {
            "volumes": [],
            "retention": {"daily": 14, "weekly": 4},
        }

        _show_config_diff_summary(existing, new, config_data)

        captured = capsys.readouterr()
        assert "~ Modify retention:" in captured.out
        assert "daily: 7 -> 14" in captured.out

    def test_show_added_email_notifications(self, capsys):
        """Test showing added email notifications in diff summary."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_summary

        existing = """
[global]
snapshot_dir = ".snapshots"
"""
        new = """
[global]
snapshot_dir = ".snapshots"

[global.notifications.email]
enabled = true
"""
        config_data = {
            "volumes": [],
            "retention": {},
            "email": {"enabled": True},
        }

        _show_config_diff_summary(existing, new, config_data)

        captured = capsys.readouterr()
        assert "+ Add email notifications" in captured.out

    def test_show_added_webhook_notifications(self, capsys):
        """Test showing added webhook notifications in diff summary."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_summary

        existing = """
[global]
snapshot_dir = ".snapshots"
"""
        new = """
[global]
snapshot_dir = ".snapshots"

[global.notifications.webhook]
enabled = true
url = "https://example.com/hook"
"""
        config_data = {
            "volumes": [],
            "retention": {},
            "webhook": {"enabled": True, "url": "https://example.com/hook"},
        }

        _show_config_diff_summary(existing, new, config_data)

        captured = capsys.readouterr()
        assert "+ Add webhook notifications" in captured.out

    def test_invalid_existing_config(self, capsys):
        """Test handling of invalid existing config in diff summary."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_summary

        existing = "this is not valid toml {{{"
        new = "[global]\nsnapshot_dir = '.snapshots'"
        config_data = {"volumes": [], "retention": {}}

        _show_config_diff_summary(existing, new, config_data)

        captured = capsys.readouterr()
        assert "Could not parse existing config" in captured.out


class TestConfigDiffText:
    """Tests for _show_config_diff_text function."""

    def test_show_text_diff_additions(self, capsys):
        """Test showing text diff with additions."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_text

        existing = """[global]
snapshot_dir = ".snapshots"
"""
        new = """[global]
snapshot_dir = ".snapshots"
incremental = true
"""

        _show_config_diff_text(existing, new)

        captured = capsys.readouterr()
        assert "+incremental = true" in captured.out

    def test_show_text_diff_removals(self, capsys):
        """Test showing text diff with removals."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_text

        existing = """[global]
snapshot_dir = ".snapshots"
old_setting = "value"
"""
        new = """[global]
snapshot_dir = ".snapshots"
"""

        _show_config_diff_text(existing, new)

        captured = capsys.readouterr()
        assert '-old_setting = "value"' in captured.out

    def test_show_text_diff_no_changes(self, capsys):
        """Test showing text diff with no changes."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_text

        content = """[global]
snapshot_dir = ".snapshots"
"""

        _show_config_diff_text(content, content)

        captured = capsys.readouterr()
        assert "No differences detected" in captured.out

    def test_show_text_diff_headers(self, capsys):
        """Test text diff includes proper headers."""
        from btrfs_backup_ng.cli.config_cmd import _show_config_diff_text

        existing = "line1\n"
        new = "line2\n"

        _show_config_diff_text(existing, new)

        captured = capsys.readouterr()
        assert "existing config" in captured.out
        assert "new config" in captured.out


class TestDetectionWizard:
    """Tests for _run_detection_wizard function."""

    def _create_mock_result(self):
        """Create a mock detection result for testing."""
        home_subvol = DetectedSubvolume(
            id=256,
            path="/home",
            mount_point="/home",
            classification=SubvolumeClass.USER_DATA,
        )
        root_subvol = DetectedSubvolume(
            id=5,
            path="/",
            mount_point="/",
            classification=SubvolumeClass.SYSTEM_ROOT,
        )
        return DetectionResult(
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/home",
                    subvol_path="/home",
                    subvol_id=256,
                )
            ],
            subvolumes=[home_subvol, root_subvol],
            suggestions=[
                BackupSuggestion(
                    subvolume=home_subvol,
                    suggested_prefix="home",
                    priority=1,
                ),
                BackupSuggestion(
                    subvolume=root_subvol,
                    suggested_prefix="root",
                    priority=2,
                ),
            ],
        )

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_volume_selection_all(
        self,
        mock_selection,
        mock_prompt,
        mock_choice,
        mock_bool,
        mock_btrbk,
        mock_find,
        mock_snapper,
        capsys,
    ):
        """Test wizard with 'all' volume selection."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config
        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        result = self._create_mock_result()

        # prompt_selection returns indices (0-based)
        mock_selection.return_value = [0, 1]  # Select both volumes

        # Use /backup paths to avoid /mnt/ require_mount check
        mock_prompt.side_effect = [
            "home",  # prefix for /home
            "/backup/home",  # target for /home
            "root",  # prefix for /
            "/backup/root",  # target for /
        ]
        mock_bool.side_effect = [
            False,  # add another target for home? no
            False,  # add another target for root? no
            False,  # configure global settings? no
        ]
        mock_choice.side_effect = [
            "print",  # print config instead of save
        ]

        exit_code = _run_detection_wizard(result)

        assert exit_code == 0
        captured = capsys.readouterr()
        assert 'path = "/home"' in captured.out
        assert 'path = "/"' in captured.out

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_volume_selection_specific(
        self,
        mock_selection,
        mock_prompt,
        mock_choice,
        mock_bool,
        mock_btrbk,
        mock_find,
        mock_snapper,
        capsys,
    ):
        """Test wizard with specific volume selection."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config
        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        result = self._create_mock_result()

        # prompt_selection returns indices (0-based) - select only first
        mock_selection.return_value = [0]

        mock_prompt.side_effect = [
            "home",  # prefix
            "/backup/home",  # target (not /mnt/)
        ]
        mock_bool.side_effect = [
            False,  # add another target? no
            False,  # configure global settings? no
        ]
        mock_choice.side_effect = [
            "print",  # print config
        ]

        exit_code = _run_detection_wizard(result)

        assert exit_code == 0
        captured = capsys.readouterr()
        assert 'path = "/home"' in captured.out

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_cancel(
        self,
        mock_selection,
        mock_prompt,
        mock_choice,
        mock_bool,
        mock_btrbk,
        mock_find,
        mock_snapper,
        capsys,
    ):
        """Test wizard cancellation."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config
        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        result = self._create_mock_result()

        mock_selection.return_value = [0]  # Select first volume
        mock_prompt.side_effect = [
            "home",  # prefix
            "/backup",  # target (not /mnt/)
        ]
        mock_bool.side_effect = [
            False,  # add another target? no
            False,  # configure global settings? no
        ]
        mock_choice.side_effect = [
            "cancel",  # cancel
        ]

        exit_code = _run_detection_wizard(result)

        assert exit_code == 0

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_with_ssh_target(
        self,
        mock_selection,
        mock_prompt,
        mock_choice,
        mock_bool,
        mock_btrbk,
        mock_find,
        mock_snapper,
        capsys,
    ):
        """Test wizard with SSH target."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config
        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        result = self._create_mock_result()

        mock_selection.return_value = [0]  # Select first volume
        mock_prompt.side_effect = [
            "home",  # prefix
            "ssh://user@host:/backup/home",  # SSH target
        ]
        mock_bool.side_effect = [
            True,  # use sudo on remote? yes
            False,  # add another target? no
            False,  # configure global settings? no
        ]
        mock_choice.side_effect = [
            "print",  # print config
        ]

        exit_code = _run_detection_wizard(result)

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "ssh://user@host:/backup/home" in captured.out
        assert "ssh_sudo = true" in captured.out

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_with_mount_target(
        self,
        mock_selection,
        mock_prompt,
        mock_choice,
        mock_bool,
        mock_btrbk,
        mock_find,
        mock_snapper,
        capsys,
    ):
        """Test wizard with mount point target."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config
        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        result = self._create_mock_result()

        mock_selection.return_value = [0]  # Select first volume
        mock_prompt.side_effect = [
            "home",  # prefix
            "/mnt/usb-drive/backup",  # Mount point target triggers require_mount
        ]
        mock_bool.side_effect = [
            True,  # require mount check? yes
            False,  # add another target? no
            False,  # configure global settings? no
        ]
        mock_choice.side_effect = [
            "print",  # print config
        ]

        exit_code = _run_detection_wizard(result)

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "/mnt/usb-drive/backup" in captured.out
        assert "require_mount = true" in captured.out

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_invalid_selection_fallback(
        self,
        mock_selection,
        mock_prompt,
        mock_choice,
        mock_bool,
        mock_btrbk,
        mock_find,
        mock_snapper,
        capsys,
    ):
        """Test wizard handles empty selection by returning default."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config
        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        result = self._create_mock_result()

        # prompt_selection handles invalid input internally,
        # returns the default (recommended) indices
        mock_selection.return_value = [0, 1]  # Both recommended

        mock_prompt.side_effect = [
            "home",  # prefix for /home
            "/backup/home",  # target (not /mnt/)
            "root",  # prefix for /
            "/backup/root",  # target (not /mnt/)
        ]
        mock_bool.side_effect = [
            False,  # add another target for home? no
            False,  # add another target for root? no
            False,  # configure global settings? no
        ]
        mock_choice.side_effect = [
            "print",  # print config
        ]

        exit_code = _run_detection_wizard(result)

        assert exit_code == 0
        captured = capsys.readouterr()
        assert 'path = "/home"' in captured.out
        assert 'path = "/"' in captured.out

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_int")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_with_global_settings(
        self,
        mock_selection,
        mock_int,
        mock_prompt,
        mock_choice,
        mock_bool,
        mock_btrbk,
        mock_find,
        mock_snapper,
        capsys,
    ):
        """Test wizard with global settings configured."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config
        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        result = self._create_mock_result()

        mock_selection.return_value = [0]  # Select first volume
        mock_prompt.side_effect = [
            "home",  # prefix
            "/backup",  # target (not /mnt/)
            "2d",  # min retention
        ]
        mock_int.side_effect = [
            48,  # hourly
            14,  # daily
            8,  # weekly
            24,  # monthly
            2,  # yearly
        ]
        mock_bool.side_effect = [
            False,  # add another target? no
            True,  # configure global settings? yes
            False,  # configure email notifications? no
        ]
        mock_choice.side_effect = [
            "print",  # print config
        ]

        exit_code = _run_detection_wizard(result)

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "hourly = 48" in captured.out
        assert "daily = 14" in captured.out

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    def test_wizard_no_suggestions(self, mock_btrbk, mock_snapper, capsys):
        """Test wizard when no suggestions available."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        result = DetectionResult(
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/",
                    subvol_path="/",
                    subvol_id=5,
                )
            ],
            subvolumes=[],
            suggestions=[],
        )

        exit_code = _run_detection_wizard(result)

        assert exit_code == 1
        captured = capsys.readouterr()
        assert "No subvolumes suitable for backup" in captured.out

    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_partial_result(self, mock_selection, mock_btrbk, capsys):
        """Test wizard shows partial result warning."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_btrbk.return_value = None  # No btrbk config

        home_subvol = DetectedSubvolume(
            id=256,
            path="/home",
            mount_point="/home",
            classification=SubvolumeClass.USER_DATA,
        )
        result = DetectionResult(
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/home",
                    subvol_path="/home",
                    subvol_id=256,
                )
            ],
            subvolumes=[home_subvol],
            suggestions=[
                BackupSuggestion(
                    subvolume=home_subvol,
                    suggested_prefix="home",
                    priority=1,
                )
            ],
            is_partial=True,
            error_message="Limited detection due to permissions",
        )

        # This will print the warning but then need input
        # We'll just verify the warning is shown
        mock_selection.side_effect = KeyboardInterrupt

        try:
            _run_detection_wizard(result)
        except KeyboardInterrupt:
            pass

        captured = capsys.readouterr()
        assert "Limited detection due to permissions" in captured.out

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_with_existing_config_diff(
        self,
        mock_selection,
        mock_prompt,
        mock_choice,
        mock_bool,
        mock_btrbk,
        mock_find,
        mock_snapper,
        capsys,
        tmp_path,
    ):
        """Test wizard shows diff with existing config."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        # Create existing config
        existing_config = tmp_path / "config.toml"
        existing_config.write_text("""[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/home"
snapshot_prefix = "home"

[[volumes.targets]]
path = "/backup"
""")

        mock_find.return_value = str(existing_config)

        result = self._create_mock_result()

        mock_selection.return_value = [0]  # Select first volume
        mock_prompt.side_effect = [
            "home-new",  # different prefix
            "/backup/new",  # different target (not /mnt/)
        ]
        mock_bool.side_effect = [
            False,  # add another target? no
            False,  # configure global settings? no
            True,  # view diff? yes
        ]
        mock_choice.side_effect = [
            "summary",  # summary diff format
            "cancel",  # cancel after seeing diff
        ]

        exit_code = _run_detection_wizard(result)

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "Existing Configuration Found" in captured.out

    @patch("btrfs_backup_ng.snapper.SnapperScanner")
    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd.find_btrbk_config")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt")
    @patch("btrfs_backup_ng.cli.config_cmd.prompt_selection")
    def test_wizard_save_to_file(
        self,
        mock_selection,
        mock_prompt,
        mock_choice,
        mock_bool,
        mock_btrbk,
        mock_find,
        mock_snapper,
        capsys,
        tmp_path,
    ):
        """Test wizard saves config to file."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None
        mock_btrbk.return_value = None  # No btrbk config
        mock_snapper.return_value.list_configs.return_value = []  # No snapper configs

        # Use a subdirectory to avoid any file existence issues
        save_dir = tmp_path / "config-dir"
        save_path = save_dir / "new-config.toml"

        result = self._create_mock_result()

        mock_selection.return_value = [0]  # Select first volume
        mock_prompt.side_effect = [
            "home",  # prefix
            "/backup",  # target (not /mnt/)
            str(save_path),  # save path
        ]
        mock_bool.side_effect = [
            False,  # add another target? no
            False,  # configure global settings? no
        ]
        mock_choice.side_effect = [
            "save",  # save config
        ]

        exit_code = _run_detection_wizard(result)

        assert exit_code == 0
        assert save_path.exists()
        content = save_path.read_text()
        assert 'path = "/home"' in content
        assert 'snapshot_prefix = "home"' in content
