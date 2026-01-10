"""Tests for snapper scanner module."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.snapper.scanner import (
    SnapperConfig,
    SnapperNotFoundError,
    SnapperScanner,
)
from btrfs_backup_ng.snapper.snapshot import SnapperSnapshot


class TestSnapperConfig:
    """Tests for SnapperConfig dataclass."""

    def test_create_basic_config(self):
        """Test creating a basic config."""
        config = SnapperConfig(
            name="root",
            subvolume=Path("/"),
        )
        assert config.name == "root"
        assert config.subvolume == Path("/")
        assert config.fstype == "btrfs"
        assert config.space_limit == 0.5
        assert config.free_limit == 0.2

    def test_snapshots_dir(self):
        """Test snapshots_dir property."""
        config = SnapperConfig(name="root", subvolume=Path("/"))
        assert config.snapshots_dir == Path("/.snapshots")

        config2 = SnapperConfig(name="home", subvolume=Path("/home"))
        assert config2.snapshots_dir == Path("/home/.snapshots")

    def test_is_valid_with_mock_fs(self, tmp_path):
        """Test is_valid method with mock filesystem."""
        # Create snapshots dir
        snapshots_dir = tmp_path / ".snapshots"
        snapshots_dir.mkdir()

        config = SnapperConfig(
            name="test",
            subvolume=tmp_path,
            fstype="btrfs",
        )
        assert config.is_valid()

    def test_is_valid_wrong_fstype(self, tmp_path):
        """Test is_valid returns False for non-btrfs."""
        snapshots_dir = tmp_path / ".snapshots"
        snapshots_dir.mkdir()

        config = SnapperConfig(
            name="test",
            subvolume=tmp_path,
            fstype="ext4",
        )
        assert not config.is_valid()

    def test_is_valid_missing_snapshots_dir(self, tmp_path):
        """Test is_valid returns False when .snapshots missing."""
        config = SnapperConfig(
            name="test",
            subvolume=tmp_path,
            fstype="btrfs",
        )
        assert not config.is_valid()


class TestSnapperScanner:
    """Tests for SnapperScanner class."""

    def test_init_default(self):
        """Test default initialization."""
        scanner = SnapperScanner()
        assert scanner.configs_dir == Path("/etc/snapper/configs")
        assert scanner.use_snapper_command is True

    def test_init_custom_dir(self, tmp_path):
        """Test initialization with custom configs dir."""
        scanner = SnapperScanner(configs_dir=tmp_path)
        assert scanner.configs_dir == tmp_path

    def test_list_configs_no_dir(self, tmp_path):
        """Test list_configs when directory doesn't exist."""
        scanner = SnapperScanner(configs_dir=tmp_path / "nonexistent")
        with pytest.raises(SnapperNotFoundError):
            scanner.list_configs()

    def test_list_configs_empty(self, tmp_path):
        """Test list_configs with empty directory."""
        scanner = SnapperScanner(configs_dir=tmp_path)
        configs = scanner.list_configs()
        assert configs == []

    def test_list_configs_parses_files(self, tmp_path):
        """Test list_configs parses config files."""
        # Create mock config files
        root_config = tmp_path / "root"
        root_config.write_text("""
SUBVOLUME="/"
FSTYPE="btrfs"
SPACE_LIMIT="0.5"
FREE_LIMIT="0.2"
ALLOW_USERS="testuser"
""")
        home_config = tmp_path / "home"
        home_config.write_text("""
SUBVOLUME="/home"
FSTYPE="btrfs"
SPACE_LIMIT="0.3"
""")

        scanner = SnapperScanner(configs_dir=tmp_path)
        configs = scanner.list_configs()

        assert len(configs) == 2
        # Sorted by name
        assert configs[0].name == "home"
        assert configs[0].subvolume == Path("/home")
        assert configs[1].name == "root"
        assert configs[1].subvolume == Path("/")
        assert configs[1].allow_users == ["testuser"]

    def test_get_config_exists(self, tmp_path):
        """Test get_config for existing config."""
        config_file = tmp_path / "myconfig"
        config_file.write_text('SUBVOLUME="/data"\nFSTYPE="btrfs"')

        scanner = SnapperScanner(configs_dir=tmp_path)
        config = scanner.get_config("myconfig")

        assert config is not None
        assert config.name == "myconfig"
        assert config.subvolume == Path("/data")

    def test_get_config_not_exists(self, tmp_path):
        """Test get_config for non-existent config."""
        scanner = SnapperScanner(configs_dir=tmp_path)
        config = scanner.get_config("nonexistent")
        assert config is None

    def test_find_config_for_path(self, tmp_path):
        """Test find_config_for_path."""
        # Create configs
        (tmp_path / "root").write_text('SUBVOLUME="/"\nFSTYPE="btrfs"')
        (tmp_path / "home").write_text('SUBVOLUME="/home"\nFSTYPE="btrfs"')

        scanner = SnapperScanner(configs_dir=tmp_path)

        # Path exactly matching
        config = scanner.find_config_for_path("/home")
        assert config is not None
        assert config.name == "home"

        # Path under subvolume - should match most specific
        config = scanner.find_config_for_path("/home/user/documents")
        assert config is not None
        assert config.name == "home"

        # Path under root
        config = scanner.find_config_for_path("/etc/passwd")
        assert config is not None
        assert config.name == "root"

    def test_parse_config_with_comments(self, tmp_path):
        """Test parsing config file with comments."""
        config_file = tmp_path / "test"
        config_file.write_text("""# This is a comment
SUBVOLUME="/"
# Another comment
FSTYPE="btrfs"
SPACE_LIMIT="0.5"
""")
        scanner = SnapperScanner(configs_dir=tmp_path)
        config = scanner.get_config("test")

        assert config is not None
        assert config.subvolume == Path("/")
        assert config.fstype == "btrfs"

    def test_parse_config_allow_groups(self, tmp_path):
        """Test parsing ALLOW_GROUPS."""
        config_file = tmp_path / "test"
        config_file.write_text("""
SUBVOLUME="/"
FSTYPE="btrfs"
ALLOW_GROUPS="wheel admin users"
""")
        scanner = SnapperScanner(configs_dir=tmp_path)
        config = scanner.get_config("test")

        assert config.allow_groups == ["wheel", "admin", "users"]

    def test_parse_config_sync_acl(self, tmp_path):
        """Test parsing SYNC_ACL."""
        config_file = tmp_path / "test"
        config_file.write_text("""
SUBVOLUME="/"
FSTYPE="btrfs"
SYNC_ACL="yes"
""")
        scanner = SnapperScanner(configs_dir=tmp_path)
        config = scanner.get_config("test")

        assert config.sync_acl is True


class TestSnapperScannerSnapshots:
    """Tests for snapshot enumeration."""

    def setup_method(self):
        """Set up test fixtures."""
        self.info_xml_template = """<?xml version="1.0"?>
<snapshot>
  <type>{type}</type>
  <num>{num}</num>
  <date>{date}</date>
  <description>{description}</description>
  <cleanup>{cleanup}</cleanup>
</snapshot>"""

    def create_snapshot_dir(
        self,
        base_path,
        num,
        snap_type="single",
        date="2025-10-01 12:00:00",
        description="test",
        cleanup="timeline",
    ):
        """Helper to create a mock snapshot directory."""
        snap_dir = base_path / str(num)
        snap_dir.mkdir(parents=True)
        (snap_dir / "snapshot").mkdir()  # Mock subvolume
        info_xml = snap_dir / "info.xml"
        info_xml.write_text(
            self.info_xml_template.format(
                type=snap_type,
                num=num,
                date=date,
                description=description,
                cleanup=cleanup,
            )
        )
        return snap_dir

    def test_get_snapshots_via_filesystem(self, tmp_path):
        """Test getting snapshots by scanning filesystem."""
        # Set up mock snapper structure
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "root").write_text(
            f'SUBVOLUME="{tmp_path / "subvol"}"\nFSTYPE="btrfs"'
        )

        subvol = tmp_path / "subvol"
        subvol.mkdir()
        snapshots_dir = subvol / ".snapshots"
        snapshots_dir.mkdir()

        # Create some snapshots
        self.create_snapshot_dir(snapshots_dir, 1, date="2025-01-01 10:00:00")
        self.create_snapshot_dir(snapshots_dir, 2, date="2025-01-02 10:00:00")
        self.create_snapshot_dir(snapshots_dir, 3, date="2025-01-03 10:00:00")

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=False)
        snapshots = scanner.get_snapshots("root")

        assert len(snapshots) == 3
        assert snapshots[0].number == 1
        assert snapshots[1].number == 2
        assert snapshots[2].number == 3

    def test_get_snapshots_filters_by_type(self, tmp_path):
        """Test filtering snapshots by type."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "root").write_text(
            f'SUBVOLUME="{tmp_path / "subvol"}"\nFSTYPE="btrfs"'
        )

        subvol = tmp_path / "subvol"
        subvol.mkdir()
        snapshots_dir = subvol / ".snapshots"
        snapshots_dir.mkdir()

        self.create_snapshot_dir(snapshots_dir, 1, snap_type="single")
        self.create_snapshot_dir(snapshots_dir, 2, snap_type="pre")
        self.create_snapshot_dir(snapshots_dir, 3, snap_type="post")

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=False)

        # Only singles
        snapshots = scanner.get_snapshots("root", include_types=["single"])
        assert len(snapshots) == 1
        assert snapshots[0].snapshot_type == "single"

        # Pre and post
        snapshots = scanner.get_snapshots("root", include_types=["pre", "post"])
        assert len(snapshots) == 2

    def test_get_snapshots_excludes_cleanup(self, tmp_path):
        """Test excluding snapshots by cleanup algorithm."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "root").write_text(
            f'SUBVOLUME="{tmp_path / "subvol"}"\nFSTYPE="btrfs"'
        )

        subvol = tmp_path / "subvol"
        subvol.mkdir()
        snapshots_dir = subvol / ".snapshots"
        snapshots_dir.mkdir()

        self.create_snapshot_dir(snapshots_dir, 1, cleanup="timeline")
        self.create_snapshot_dir(snapshots_dir, 2, cleanup="number")
        self.create_snapshot_dir(snapshots_dir, 3, cleanup="timeline")

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=False)

        # Exclude number cleanup
        snapshots = scanner.get_snapshots("root", exclude_cleanup=["number"])
        assert len(snapshots) == 2
        assert all(s.cleanup != "number" for s in snapshots)

    def test_get_snapshots_skips_invalid(self, tmp_path):
        """Test that invalid snapshot dirs are skipped."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "root").write_text(
            f'SUBVOLUME="{tmp_path / "subvol"}"\nFSTYPE="btrfs"'
        )

        subvol = tmp_path / "subvol"
        subvol.mkdir()
        snapshots_dir = subvol / ".snapshots"
        snapshots_dir.mkdir()

        # Valid snapshot
        self.create_snapshot_dir(snapshots_dir, 1)

        # Directory without info.xml
        (snapshots_dir / "2").mkdir()
        (snapshots_dir / "2" / "snapshot").mkdir()

        # Non-numeric directory (should be skipped)
        (snapshots_dir / "invalid").mkdir()

        # Snapshot 0 (current, should be skipped)
        self.create_snapshot_dir(snapshots_dir, 0)

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=False)
        snapshots = scanner.get_snapshots("root")

        assert len(snapshots) == 1
        assert snapshots[0].number == 1

    def test_get_snapshot_specific(self, tmp_path):
        """Test getting a specific snapshot by number."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "root").write_text(
            f'SUBVOLUME="{tmp_path / "subvol"}"\nFSTYPE="btrfs"'
        )

        subvol = tmp_path / "subvol"
        subvol.mkdir()
        snapshots_dir = subvol / ".snapshots"
        snapshots_dir.mkdir()

        self.create_snapshot_dir(snapshots_dir, 100, description="specific snapshot")

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=False)
        snapshot = scanner.get_snapshot("root", 100)

        assert snapshot is not None
        assert snapshot.number == 100
        assert snapshot.description == "specific snapshot"

    def test_get_snapshot_not_found(self, tmp_path):
        """Test getting non-existent snapshot returns None."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "root").write_text(
            f'SUBVOLUME="{tmp_path / "subvol"}"\nFSTYPE="btrfs"'
        )

        subvol = tmp_path / "subvol"
        subvol.mkdir()
        snapshots_dir = subvol / ".snapshots"
        snapshots_dir.mkdir()

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=False)
        snapshot = scanner.get_snapshot("root", 999)

        assert snapshot is None

    def test_get_next_snapshot_number(self, tmp_path):
        """Test get_next_snapshot_number."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "root").write_text(
            f'SUBVOLUME="{tmp_path / "subvol"}"\nFSTYPE="btrfs"'
        )

        subvol = tmp_path / "subvol"
        subvol.mkdir()
        snapshots_dir = subvol / ".snapshots"
        snapshots_dir.mkdir()

        self.create_snapshot_dir(snapshots_dir, 10)
        self.create_snapshot_dir(snapshots_dir, 20)
        self.create_snapshot_dir(snapshots_dir, 15)

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=False)
        next_num = scanner.get_next_snapshot_number("root")

        assert next_num == 21

    def test_get_next_snapshot_number_empty(self, tmp_path):
        """Test get_next_snapshot_number with no snapshots."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "root").write_text(
            f'SUBVOLUME="{tmp_path / "subvol"}"\nFSTYPE="btrfs"'
        )

        subvol = tmp_path / "subvol"
        subvol.mkdir()
        snapshots_dir = subvol / ".snapshots"
        snapshots_dir.mkdir()

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=False)
        next_num = scanner.get_next_snapshot_number("root")

        assert next_num == 1


class TestSnapperScannerCommand:
    """Tests for snapper command integration."""

    def test_is_snapper_available_not_found(self):
        """Test is_snapper_available when snapper not installed."""
        scanner = SnapperScanner()
        with patch("subprocess.run", side_effect=FileNotFoundError):
            assert scanner.is_snapper_available() is False

    def test_is_snapper_available_found(self):
        """Test is_snapper_available when snapper is installed."""
        scanner = SnapperScanner()
        mock_result = MagicMock()
        mock_result.returncode = 0
        with patch("subprocess.run", return_value=mock_result):
            scanner._snapper_available = None  # Reset cache
            assert scanner.is_snapper_available() is True

    def test_get_snapshots_via_command(self, tmp_path):
        """Test getting snapshots via snapper command."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        subvol = tmp_path / "subvol"
        subvol.mkdir()
        (subvol / ".snapshots").mkdir()
        (configs_dir / "root").write_text(f'SUBVOLUME="{subvol}"\nFSTYPE="btrfs"')

        # Create snapshot directories for the paths
        for num in [1, 2]:
            snap_dir = subvol / ".snapshots" / str(num)
            snap_dir.mkdir()
            (snap_dir / "snapshot").mkdir()

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=True)
        scanner._snapper_available = True

        # Mock snapper JSON output
        mock_json = {
            "root": [
                {"number": 0, "type": "single", "date": "", "description": "current"},
                {
                    "number": 1,
                    "type": "single",
                    "date": "2025-01-01 10:00:00",
                    "description": "timeline",
                    "cleanup": "timeline",
                    "userdata": {},
                },
                {
                    "number": 2,
                    "type": "post",
                    "date": "2025-01-02 10:00:00",
                    "description": "update",
                    "cleanup": "number",
                    "pre-number": 1,
                    "userdata": [],
                },
            ]
        }

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(mock_json)

        with patch("subprocess.run", return_value=mock_result):
            snapshots = scanner.get_snapshots("root")

        # Should skip snapshot 0
        assert len(snapshots) == 2
        assert snapshots[0].number == 1
        assert snapshots[0].snapshot_type == "single"
        assert snapshots[1].number == 2
        assert snapshots[1].snapshot_type == "post"
        assert snapshots[1].pre_num == 1


class TestSnapperScannerEdgeCases:
    """Tests for edge cases in SnapperScanner."""

    def test_list_configs_skips_directories(self, tmp_path):
        """Test that directories in configs dir are skipped."""
        # Create a config file
        (tmp_path / "root").write_text('SUBVOLUME="/"\nFSTYPE="btrfs"')
        # Create a subdirectory (should be skipped)
        (tmp_path / "subdir").mkdir()

        scanner = SnapperScanner(configs_dir=tmp_path)
        configs = scanner.list_configs()

        assert len(configs) == 1
        assert configs[0].name == "root"

    def test_list_configs_handles_parse_error(self, tmp_path):
        """Test that parse errors in config files are handled."""
        # Create a valid config
        (tmp_path / "valid").write_text('SUBVOLUME="/"\nFSTYPE="btrfs"')
        # Create an invalid config that will cause parsing issues
        (tmp_path / "invalid").write_text("not valid config syntax\x00\x01")

        scanner = SnapperScanner(configs_dir=tmp_path)
        configs = scanner.list_configs()

        # Should return valid config, skip invalid
        assert len(configs) >= 1
        assert any(c.name == "valid" for c in configs)

    def test_get_config_handles_parse_error(self, tmp_path):
        """Test get_config handles parse errors gracefully."""
        # Create a config file that will cause an encoding error
        config_file = tmp_path / "broken"
        # Writing invalid UTF-8 bytes that will cause UnicodeDecodeError
        config_file.write_bytes(b"\xff\xfe invalid utf-8 \x80\x81")

        scanner = SnapperScanner(configs_dir=tmp_path)
        config = scanner.get_config("broken")

        # Should return None on parse error (UnicodeDecodeError)
        assert config is None

    def test_get_snapshots_invalid_config_name(self, tmp_path):
        """Test get_snapshots with invalid config name."""
        scanner = SnapperScanner(configs_dir=tmp_path)

        with pytest.raises(ValueError, match="not found"):
            scanner.get_snapshots("nonexistent")

    def test_get_snapshots_command_fallback(self, tmp_path):
        """Test fallback to filesystem when command fails."""
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        subvol = tmp_path / "subvol"
        subvol.mkdir()
        snapshots_dir = subvol / ".snapshots"
        snapshots_dir.mkdir()
        (configs_dir / "root").write_text(f'SUBVOLUME="{subvol}"\nFSTYPE="btrfs"')

        # Create a snapshot
        snap_dir = snapshots_dir / "1"
        snap_dir.mkdir()
        (snap_dir / "snapshot").mkdir()
        (snap_dir / "info.xml").write_text("""<?xml version="1.0"?>
<snapshot>
  <type>single</type>
  <num>1</num>
  <date>2025-01-01 12:00:00</date>
  <description>test</description>
  <cleanup>timeline</cleanup>
</snapshot>""")

        scanner = SnapperScanner(configs_dir=configs_dir, use_snapper_command=True)
        scanner._snapper_available = True

        # Mock command failure to trigger fallback
        with patch("subprocess.run", side_effect=Exception("command failed")):
            snapshots = scanner.get_snapshots("root")

        # Should fall back to filesystem and find the snapshot
        assert len(snapshots) == 1
        assert snapshots[0].number == 1

    def test_parse_config_invalid_space_limit(self, tmp_path):
        """Test parsing config with invalid SPACE_LIMIT."""
        config_file = tmp_path / "test"
        config_file.write_text("""
SUBVOLUME="/"
FSTYPE="btrfs"
SPACE_LIMIT="not_a_number"
""")
        scanner = SnapperScanner(configs_dir=tmp_path)
        config = scanner.get_config("test")

        # Should use default 0.5
        assert config is not None
        assert config.space_limit == 0.5

    def test_parse_config_invalid_free_limit(self, tmp_path):
        """Test parsing config with invalid FREE_LIMIT."""
        config_file = tmp_path / "test"
        config_file.write_text("""
SUBVOLUME="/"
FSTYPE="btrfs"
FREE_LIMIT="invalid"
""")
        scanner = SnapperScanner(configs_dir=tmp_path)
        config = scanner.get_config("test")

        # Should use default 0.2
        assert config is not None
        assert config.free_limit == 0.2

    def test_find_config_for_path_no_match(self, tmp_path):
        """Test find_config_for_path when no config matches."""
        (tmp_path / "home").write_text('SUBVOLUME="/home"\nFSTYPE="btrfs"')

        scanner = SnapperScanner(configs_dir=tmp_path)
        # Path that doesn't match any config subvolume
        config = scanner.find_config_for_path("/var/log")

        assert config is None


class TestSnapperSnapshot:
    """Tests for SnapperSnapshot class."""

    def test_get_backup_name(self):
        """Test backup name generation."""
        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        meta = SnapperMetadata(
            type="single",
            num=100,
            date=datetime(2025, 10, 1, 11, 42, 50),
        )
        snapshot = SnapperSnapshot(
            config_name="root",
            number=100,
            metadata=meta,
            subvolume_path=Path("/.snapshots/100/snapshot"),
            info_xml_path=Path("/.snapshots/100/info.xml"),
        )

        name = snapshot.get_backup_name()
        assert name == "root-100-20251001-114250"

    def test_snapshot_ordering(self):
        """Test snapshot comparison and ordering."""
        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        def make_snapshot(config, num):
            meta = SnapperMetadata(
                type="single", num=num, date=datetime(2025, 1, 1, 0, 0, 0)
            )
            return SnapperSnapshot(
                config_name=config,
                number=num,
                metadata=meta,
                subvolume_path=Path(f"/{config}/.snapshots/{num}/snapshot"),
                info_xml_path=Path(f"/{config}/.snapshots/{num}/info.xml"),
            )

        s1 = make_snapshot("root", 1)
        s2 = make_snapshot("root", 2)
        s3 = make_snapshot("home", 1)

        # Same config, different numbers
        assert s1 < s2
        assert not s2 < s1

        # Different configs
        assert s3 < s1  # home < root alphabetically

        # Sorting
        snapshots = [s2, s3, s1]
        sorted_snaps = sorted(snapshots)
        assert sorted_snaps == [s3, s1, s2]

    def test_snapshot_equality(self):
        """Test snapshot equality."""
        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        meta1 = SnapperMetadata(
            type="single", num=100, date=datetime(2025, 1, 1, 0, 0, 0)
        )
        meta2 = SnapperMetadata(
            type="single",
            num=100,
            date=datetime(2025, 1, 2, 0, 0, 0),  # Different date
        )

        s1 = SnapperSnapshot(
            config_name="root",
            number=100,
            metadata=meta1,
            subvolume_path=Path("/a"),
            info_xml_path=Path("/b"),
        )
        s2 = SnapperSnapshot(
            config_name="root",
            number=100,
            metadata=meta2,
            subvolume_path=Path("/c"),
            info_xml_path=Path("/d"),
        )
        s3 = SnapperSnapshot(
            config_name="root",
            number=101,
            metadata=meta1,
            subvolume_path=Path("/e"),
            info_xml_path=Path("/f"),
        )

        # Same config and number = equal
        assert s1 == s2
        # Different number = not equal
        assert s1 != s3

    def test_snapshot_hash(self):
        """Test snapshot hashing for use in sets/dicts."""
        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        meta = SnapperMetadata(
            type="single", num=100, date=datetime(2025, 1, 1, 0, 0, 0)
        )

        s1 = SnapperSnapshot(
            config_name="root",
            number=100,
            metadata=meta,
            subvolume_path=Path("/a"),
            info_xml_path=Path("/b"),
        )
        s2 = SnapperSnapshot(
            config_name="root",
            number=100,
            metadata=meta,
            subvolume_path=Path("/c"),
            info_xml_path=Path("/d"),
        )

        # Should be usable in a set
        snapshot_set = {s1, s2}
        assert len(snapshot_set) == 1  # They're equal, so only one in set

    def test_snapshot_exists(self, tmp_path):
        """Test snapshot exists method."""
        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        # Create an actual path
        snap_path = tmp_path / "snapshot"
        snap_path.mkdir()

        meta = SnapperMetadata(
            type="single", num=100, date=datetime(2025, 1, 1, 0, 0, 0)
        )
        s1 = SnapperSnapshot(
            config_name="root",
            number=100,
            metadata=meta,
            subvolume_path=snap_path,
            info_xml_path=tmp_path / "info.xml",
        )
        assert s1.exists() is True

        # Non-existent path
        s2 = SnapperSnapshot(
            config_name="root",
            number=101,
            metadata=meta,
            subvolume_path=tmp_path / "nonexistent",
            info_xml_path=tmp_path / "info2.xml",
        )
        assert s2.exists() is False

    def test_snapshot_repr(self):
        """Test snapshot string representation."""
        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        meta = SnapperMetadata(
            type="single", num=100, date=datetime(2025, 6, 15, 14, 30, 0)
        )
        s = SnapperSnapshot(
            config_name="root",
            number=100,
            metadata=meta,
            subvolume_path=Path("/a"),
            info_xml_path=Path("/b"),
        )
        repr_str = repr(s)
        assert "SnapperSnapshot" in repr_str
        assert "root" in repr_str
        assert "100" in repr_str
        assert "single" in repr_str
        assert "2025-06-15" in repr_str

    def test_snapshot_lt_not_implemented(self):
        """Test that comparing with non-SnapperSnapshot returns NotImplemented."""
        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        meta = SnapperMetadata(
            type="single", num=100, date=datetime(2025, 1, 1, 0, 0, 0)
        )
        s = SnapperSnapshot(
            config_name="root",
            number=100,
            metadata=meta,
            subvolume_path=Path("/a"),
            info_xml_path=Path("/b"),
        )
        # Comparing with non-SnapperSnapshot should return NotImplemented
        result = s.__lt__("not a snapshot")
        assert result is NotImplemented

    def test_snapshot_eq_with_non_snapshot(self):
        """Test that equality with non-SnapperSnapshot returns False."""
        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        meta = SnapperMetadata(
            type="single", num=100, date=datetime(2025, 1, 1, 0, 0, 0)
        )
        s = SnapperSnapshot(
            config_name="root",
            number=100,
            metadata=meta,
            subvolume_path=Path("/a"),
            info_xml_path=Path("/b"),
        )
        assert s != "not a snapshot"
        assert s != 100
        assert s is not None
