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
        """Test suggested_prefix for /home."""
        subvol = DetectedSubvolume(id=256, path="/home", mount_point="/home")
        assert subvol.suggested_prefix == "home"

    def test_suggested_prefix_root(self):
        """Test suggested_prefix for /."""
        subvol = DetectedSubvolume(id=5, path="/", mount_point="/")
        assert subvol.suggested_prefix == "root"

    def test_suggested_prefix_nested(self):
        """Test suggested_prefix for nested path."""
        subvol = DetectedSubvolume(id=260, path="/var/log", mount_point="/var/log")
        assert subvol.suggested_prefix == "var-log"

    def test_hash_and_equality(self):
        """Test hash and equality based on id and device."""
        subvol1 = DetectedSubvolume(id=256, path="/home", device="/dev/sda1")
        subvol2 = DetectedSubvolume(id=256, path="/@home", device="/dev/sda1")
        subvol3 = DetectedSubvolume(id=257, path="/home", device="/dev/sda1")

        assert subvol1 == subvol2
        assert subvol1 != subvol3


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
