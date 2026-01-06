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

    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt")
    def test_wizard_volume_selection_all(
        self, mock_prompt, mock_choice, mock_bool, mock_find, capsys
    ):
        """Test wizard with 'all' volume selection."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config

        result = self._create_mock_result()

        # Use /backup paths to avoid /mnt/ require_mount check
        # Note: "add another target?" is _prompt_bool, not _prompt
        mock_prompt.side_effect = [
            "all",  # Select all volumes
            "home",  # prefix for /home
            "/backup/home",  # target for /home (not /mnt/ to avoid mount check)
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
        assert "Generated Configuration" in captured.out
        assert 'path = "/home"' in captured.out
        assert 'path = "/"' in captured.out

    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt")
    def test_wizard_volume_selection_specific(
        self, mock_prompt, mock_choice, mock_bool, mock_find, capsys
    ):
        """Test wizard with specific volume selection."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config

        result = self._create_mock_result()

        # Select only volume 1 (/home), use /backup to avoid mount check
        # Note: "add another target?" is _prompt_bool, not _prompt
        mock_prompt.side_effect = [
            "1",  # Select only first volume
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

    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt")
    def test_wizard_cancel(
        self, mock_prompt, mock_choice, mock_bool, mock_find, capsys
    ):
        """Test wizard cancellation."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config

        result = self._create_mock_result()

        # Note: "add another target?" is _prompt_bool, not _prompt
        mock_prompt.side_effect = [
            "1",  # Select first volume
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
        captured = capsys.readouterr()
        assert "cancelled" in captured.out.lower()

    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt")
    def test_wizard_with_ssh_target(
        self, mock_prompt, mock_choice, mock_bool, mock_find, capsys
    ):
        """Test wizard with SSH target."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config

        result = self._create_mock_result()

        # Note: "add another target?" is _prompt_bool, not _prompt
        mock_prompt.side_effect = [
            "1",  # Select first volume
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

    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt")
    def test_wizard_with_mount_target(
        self, mock_prompt, mock_choice, mock_bool, mock_find, capsys
    ):
        """Test wizard with mount point target."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config

        result = self._create_mock_result()

        # Note: "add another target?" is _prompt_bool, not _prompt
        mock_prompt.side_effect = [
            "1",  # Select first volume
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

    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt")
    def test_wizard_invalid_selection_fallback(
        self, mock_prompt, mock_choice, mock_bool, mock_find, capsys
    ):
        """Test wizard falls back to recommended on invalid selection."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config

        result = self._create_mock_result()

        # Invalid selection -> falls back to recommended (both), use /backup paths
        # Note: "add another target?" is _prompt_bool, not _prompt
        mock_prompt.side_effect = [
            "invalid",  # Invalid selection -> falls back to recommended
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
        assert "Invalid selection" in captured.out

    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_int")
    def test_wizard_with_global_settings(
        self, mock_int, mock_prompt, mock_choice, mock_bool, mock_find, capsys
    ):
        """Test wizard with global settings configured."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None  # No existing config

        result = self._create_mock_result()

        # Note: "add another target?" is _prompt_bool, not _prompt
        mock_prompt.side_effect = [
            "1",  # Select first volume
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

    def test_wizard_no_suggestions(self, capsys):
        """Test wizard when no suggestions available."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

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

    def test_wizard_partial_result(self, capsys):
        """Test wizard shows partial result warning."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

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
        with patch("btrfs_backup_ng.cli.config_cmd._prompt") as mock_prompt:
            mock_prompt.side_effect = KeyboardInterrupt

            try:
                _run_detection_wizard(result)
            except KeyboardInterrupt:
                pass

        captured = capsys.readouterr()
        assert "Limited detection due to permissions" in captured.out

    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt")
    def test_wizard_with_existing_config_diff(
        self, mock_prompt, mock_choice, mock_bool, mock_find, capsys, tmp_path
    ):
        """Test wizard shows diff with existing config."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

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

        # Note: "add another target?" is _prompt_bool, not _prompt
        mock_prompt.side_effect = [
            "1",  # Select first volume
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

    @patch("btrfs_backup_ng.cli.config_cmd.find_config_file")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_bool")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt_choice")
    @patch("btrfs_backup_ng.cli.config_cmd._prompt")
    def test_wizard_save_to_file(
        self, mock_prompt, mock_choice, mock_bool, mock_find, capsys, tmp_path
    ):
        """Test wizard saves config to file."""
        from btrfs_backup_ng.cli.config_cmd import _run_detection_wizard

        mock_find.return_value = None

        # Use a subdirectory to avoid any file existence issues
        save_dir = tmp_path / "config-dir"
        save_path = save_dir / "new-config.toml"

        result = self._create_mock_result()

        # Only 4 _prompt calls needed:
        # 1. Select volumes, 2. Snapshot prefix, 3. Target path, 4. Save path
        # (the "add another target?" is a _prompt_bool, not _prompt)
        mock_prompt.side_effect = [
            "1",  # Select first volume
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
