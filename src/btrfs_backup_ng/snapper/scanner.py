"""Snapper configuration and snapshot scanner.

This module provides functionality to discover snapper configurations
and enumerate their snapshots.
"""

import json
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ..__logger__ import logger
from .metadata import SnapperMetadata, parse_info_xml
from .snapshot import SnapperSnapshot

__all__ = ["SnapperConfig", "SnapperScanner", "SnapperNotFoundError"]

SNAPPER_CONFIGS_DIR = Path("/etc/snapper/configs")
SNAPSHOTS_DIR_NAME = ".snapshots"


class SnapperNotFoundError(Exception):
    """Raised when snapper is not installed or accessible."""


@dataclass
class SnapperConfig:
    """Represents a snapper configuration.

    Attributes:
        name: Configuration name (e.g., 'root', 'home')
        subvolume: Path to the subvolume being managed
        fstype: Filesystem type (should be 'btrfs')
        space_limit: Fraction of space that can be used by snapshots
        free_limit: Minimum free space to maintain
        allow_users: Users allowed to manage this config
        allow_groups: Groups allowed to manage this config
        sync_acl: Whether to sync ACLs
        raw_config: Original config file content as dict
    """

    name: str
    subvolume: Path
    fstype: str = "btrfs"
    space_limit: float = 0.5
    free_limit: float = 0.2
    allow_users: list[str] = field(default_factory=list)
    allow_groups: list[str] = field(default_factory=list)
    sync_acl: bool = False
    raw_config: dict[str, str] = field(default_factory=dict)

    @property
    def snapshots_dir(self) -> Path:
        """Path to the .snapshots directory for this config."""
        return self.subvolume / SNAPSHOTS_DIR_NAME

    def is_valid(self) -> bool:
        """Check if this configuration is valid and usable."""
        return (
            self.fstype == "btrfs"
            and self.subvolume.exists()
            and self.snapshots_dir.exists()
        )


class SnapperScanner:
    """Scanner for discovering snapper configurations and snapshots."""

    def __init__(
        self,
        configs_dir: Path | str = SNAPPER_CONFIGS_DIR,
        use_snapper_command: bool = True,
    ):
        """Initialize the scanner.

        Args:
            configs_dir: Path to snapper configs directory
            use_snapper_command: If True, use `snapper` command when available
                                for snapshot listing (more reliable)
        """
        self.configs_dir = Path(configs_dir)
        self.use_snapper_command = use_snapper_command
        self._snapper_available: Optional[bool] = None

    def is_snapper_available(self) -> bool:
        """Check if snapper command is available."""
        if self._snapper_available is None:
            try:
                result = subprocess.run(
                    ["snapper", "--version"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self._snapper_available = result.returncode == 0
            except FileNotFoundError:
                self._snapper_available = False
        return self._snapper_available

    def list_configs(self) -> list[SnapperConfig]:
        """List all snapper configurations.

        Returns:
            List of SnapperConfig objects

        Raises:
            SnapperNotFoundError: If snapper configs directory doesn't exist
        """
        if not self.configs_dir.exists():
            raise SnapperNotFoundError(
                f"Snapper configs directory not found: {self.configs_dir}"
            )

        configs = []
        for config_file in self.configs_dir.iterdir():
            if config_file.is_file():
                try:
                    config = self._parse_config_file(config_file)
                    configs.append(config)
                except Exception as e:
                    logger.warning(
                        "Failed to parse snapper config %s: %s", config_file.name, e
                    )

        return sorted(configs, key=lambda c: c.name)

    def get_config(self, name: str) -> Optional[SnapperConfig]:
        """Get a specific snapper configuration by name.

        Args:
            name: Configuration name

        Returns:
            SnapperConfig if found, None otherwise
        """
        config_file = self.configs_dir / name
        if not config_file.exists():
            return None

        try:
            return self._parse_config_file(config_file)
        except Exception as e:
            logger.warning("Failed to parse snapper config %s: %s", name, e)
            return None

    def find_config_for_path(self, path: Path | str) -> Optional[SnapperConfig]:
        """Find the snapper configuration that manages a given path.

        Args:
            path: Path to look up

        Returns:
            SnapperConfig if found, None otherwise
        """
        path = Path(path).resolve()
        configs = self.list_configs()

        # Find the config whose subvolume matches or contains the path
        best_match: Optional[SnapperConfig] = None
        best_match_len = 0

        for config in configs:
            subvol = config.subvolume.resolve()
            if path == subvol or path.is_relative_to(subvol):
                if len(str(subvol)) > best_match_len:
                    best_match = config
                    best_match_len = len(str(subvol))

        return best_match

    def get_snapshots(
        self,
        config: SnapperConfig | str,
        include_types: Optional[list[str]] = None,
        exclude_cleanup: Optional[list[str]] = None,
    ) -> list[SnapperSnapshot]:
        """Get all snapshots for a configuration.

        Args:
            config: SnapperConfig object or config name string
            include_types: Only include these snapshot types (default: all)
            exclude_cleanup: Exclude snapshots with these cleanup algorithms

        Returns:
            List of SnapperSnapshot objects sorted by number
        """
        if isinstance(config, str):
            config_obj = self.get_config(config)
            if config_obj is None:
                raise ValueError(f"Snapper config not found: {config}")
            config = config_obj

        # Try using snapper command first for reliability
        if self.use_snapper_command and self.is_snapper_available():
            try:
                snapshots = self._get_snapshots_via_command(config)
            except Exception as e:
                logger.warning(
                    "Failed to get snapshots via snapper command, "
                    "falling back to filesystem scan: %s",
                    e,
                )
                snapshots = self._get_snapshots_via_filesystem(config)
        else:
            snapshots = self._get_snapshots_via_filesystem(config)

        # Filter by type
        if include_types:
            snapshots = [s for s in snapshots if s.snapshot_type in include_types]

        # Filter by cleanup
        if exclude_cleanup:
            snapshots = [s for s in snapshots if s.cleanup not in exclude_cleanup]

        return sorted(snapshots)

    def _parse_config_file(self, config_file: Path) -> SnapperConfig:
        """Parse a snapper config file.

        Snapper config files are shell-style key=value files.
        """
        raw_config = {}
        with open(config_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                match = re.match(r'^([A-Z_]+)="?(.*?)"?$', line)
                if match:
                    key, value = match.groups()
                    raw_config[key] = value

        subvolume = raw_config.get("SUBVOLUME", "/")

        # Parse space limit as float
        try:
            space_limit = float(raw_config.get("SPACE_LIMIT", "0.5"))
        except ValueError:
            space_limit = 0.5

        # Parse free limit as float
        try:
            free_limit = float(raw_config.get("FREE_LIMIT", "0.2"))
        except ValueError:
            free_limit = 0.2

        # Parse user/group lists
        allow_users: list[str] = []
        if raw_config.get("ALLOW_USERS"):
            allow_users = raw_config["ALLOW_USERS"].split()

        allow_groups: list[str] = []
        if raw_config.get("ALLOW_GROUPS"):
            allow_groups = raw_config["ALLOW_GROUPS"].split()

        sync_acl = raw_config.get("SYNC_ACL", "no").lower() == "yes"

        return SnapperConfig(
            name=config_file.name,
            subvolume=Path(subvolume),
            fstype=raw_config.get("FSTYPE", "btrfs"),
            space_limit=space_limit,
            free_limit=free_limit,
            allow_users=allow_users,
            allow_groups=allow_groups,
            sync_acl=sync_acl,
            raw_config=raw_config,
        )

    def _get_snapshots_via_command(
        self, config: SnapperConfig
    ) -> list[SnapperSnapshot]:
        """Get snapshots using snapper command with JSON output."""
        result = subprocess.run(
            ["snapper", "--jsonout", "-c", config.name, "list"],
            capture_output=True,
            text=True,
            check=True,
        )

        data = json.loads(result.stdout)
        snapshots = []

        # snapper JSON output format: {"root": [...snapshots...]}
        # The key is the config name
        snapshot_list = data.get(config.name, [])

        for snap_data in snapshot_list:
            # Skip the "current" pseudo-snapshot (number 0)
            num = snap_data.get("number", 0)
            if num == 0:
                continue

            snapshot_dir = config.snapshots_dir / str(num)
            subvolume_path = snapshot_dir / "snapshot"
            info_xml_path = snapshot_dir / "info.xml"

            # Parse date from snapper output
            from datetime import datetime

            date_str = snap_data.get("date", "")
            try:
                # Snapper JSON uses format like "2025-10-01 11:42:50"
                date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                logger.warning("Invalid date for snapshot %d: %s", num, date_str)
                continue

            # Build userdata dict
            userdata = snap_data.get("userdata", {})
            if isinstance(userdata, list):
                # Handle empty userdata list
                userdata = {}

            metadata = SnapperMetadata(
                type=snap_data.get("type", "single"),
                num=num,
                date=date,
                description=snap_data.get("description", ""),
                cleanup=snap_data.get("cleanup", ""),
                pre_num=snap_data.get("pre-number"),
                userdata=userdata,
            )

            snapshot = SnapperSnapshot(
                config_name=config.name,
                number=num,
                metadata=metadata,
                subvolume_path=subvolume_path,
                info_xml_path=info_xml_path,
            )
            snapshots.append(snapshot)

        return snapshots

    def _get_snapshots_via_filesystem(
        self, config: SnapperConfig
    ) -> list[SnapperSnapshot]:
        """Get snapshots by scanning the filesystem."""
        snapshots_dir = config.snapshots_dir
        if not snapshots_dir.exists():
            return []

        snapshots = []

        for entry in snapshots_dir.iterdir():
            if not entry.is_dir():
                continue

            # Snapshot directories are numbered
            try:
                num = int(entry.name)
            except ValueError:
                continue

            # Skip 0 (current system)
            if num == 0:
                continue

            info_xml_path = entry / "info.xml"
            subvolume_path = entry / "snapshot"

            if not info_xml_path.exists():
                logger.debug("Skipping snapshot %d: no info.xml", num)
                continue

            try:
                metadata = parse_info_xml(info_xml_path)
            except Exception as e:
                logger.warning("Failed to parse info.xml for snapshot %d: %s", num, e)
                continue

            snapshot = SnapperSnapshot(
                config_name=config.name,
                number=num,
                metadata=metadata,
                subvolume_path=subvolume_path,
                info_xml_path=info_xml_path,
            )
            snapshots.append(snapshot)

        return snapshots

    def get_snapshot(
        self, config: SnapperConfig | str, number: int
    ) -> Optional[SnapperSnapshot]:
        """Get a specific snapshot by number.

        Args:
            config: SnapperConfig object or config name string
            number: Snapshot number

        Returns:
            SnapperSnapshot if found, None otherwise
        """
        if isinstance(config, str):
            config_obj = self.get_config(config)
            if config_obj is None:
                return None
            config = config_obj

        snapshot_dir = config.snapshots_dir / str(number)
        info_xml_path = snapshot_dir / "info.xml"
        subvolume_path = snapshot_dir / "snapshot"

        if not info_xml_path.exists():
            return None

        try:
            metadata = parse_info_xml(info_xml_path)
        except Exception as e:
            logger.warning("Failed to parse info.xml for snapshot %d: %s", number, e)
            return None

        return SnapperSnapshot(
            config_name=config.name,
            number=number,
            metadata=metadata,
            subvolume_path=subvolume_path,
            info_xml_path=info_xml_path,
        )

    def get_next_snapshot_number(self, config: SnapperConfig | str) -> int:
        """Get the next available snapshot number for a config.

        This scans the actual .snapshots directory to find the highest numbered
        directory, which is more reliable than using snapper's list (which may
        not see recently restored snapshots).

        Args:
            config: SnapperConfig object or config name string

        Returns:
            Next available snapshot number
        """
        if isinstance(config, str):
            config_obj = self.get_config(config)
            if config_obj is None:
                return 1
            config = config_obj

        # Scan the filesystem directly rather than using snapper list,
        # because snapper list may not see recently restored snapshots
        max_num = 0
        try:
            for item in config.snapshots_dir.iterdir():
                if item.is_dir() and item.name.isdigit():
                    num = int(item.name)
                    if num > max_num:
                        max_num = num
        except (OSError, PermissionError):
            # Fall back to snapper list if we can't read the directory
            snapshots = self.get_snapshots(config)
            if snapshots:
                max_num = max(s.number for s in snapshots)

        return max_num + 1
