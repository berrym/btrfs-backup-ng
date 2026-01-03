"""Configuration schema definitions using dataclasses.

Defines the structure for TOML configuration with sensible defaults.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RetentionConfig:
    """Retention policy configuration.

    Attributes:
        min: Minimum retention period (e.g., "1d", "2h", "30m")
        hourly: Number of hourly snapshots to keep
        daily: Number of daily snapshots to keep
        weekly: Number of weekly snapshots to keep
        monthly: Number of monthly snapshots to keep
        yearly: Number of yearly snapshots to keep
    """

    min: str = "1d"
    hourly: int = 24
    daily: int = 7
    weekly: int = 4
    monthly: int = 12
    yearly: int = 0


@dataclass
class TargetConfig:
    """Backup target configuration.

    Attributes:
        path: Target path (local path or ssh://user@host:/path)
        ssh_sudo: Whether to use sudo on remote SSH targets
        ssh_port: SSH port for remote targets
        ssh_key: Path to SSH private key
        ssh_password_auth: Allow password authentication fallback
    """

    path: str
    ssh_sudo: bool = False
    ssh_port: int = 22
    ssh_key: Optional[str] = None
    ssh_password_auth: bool = True


@dataclass
class VolumeConfig:
    """Volume backup configuration.

    Attributes:
        path: Path to the btrfs subvolume to back up
        snapshot_prefix: Prefix for snapshot names
        snapshot_dir: Directory to store snapshots (relative to volume or absolute)
        targets: List of backup targets for this volume
        retention: Volume-specific retention policy (overrides global)
        enabled: Whether this volume is enabled for backup
    """

    path: str
    snapshot_prefix: str = ""
    snapshot_dir: str = ".snapshots"
    targets: list[TargetConfig] = field(default_factory=list)
    retention: Optional[RetentionConfig] = None
    enabled: bool = True

    def __post_init__(self):
        # Generate default prefix from path if not specified
        if not self.snapshot_prefix:
            # /home -> home, /var/log -> var-log
            self.snapshot_prefix = self.path.strip("/").replace("/", "-") or "root"


@dataclass
class GlobalConfig:
    """Global configuration settings.

    Attributes:
        snapshot_dir: Default snapshot directory for all volumes
        timestamp_format: Format string for snapshot timestamps
        incremental: Whether to use incremental transfers by default
        log_file: Path to log file (None for no file logging)
        retention: Default retention policy
        parallel_volumes: Max concurrent volume backups
        parallel_targets: Max concurrent target transfers per volume
        quiet: Suppress non-essential output
        verbose: Enable verbose output
    """

    snapshot_dir: str = ".snapshots"
    timestamp_format: str = "%Y%m%d-%H%M%S"
    incremental: bool = True
    log_file: Optional[str] = None
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    parallel_volumes: int = 2
    parallel_targets: int = 3
    quiet: bool = False
    verbose: bool = False


@dataclass
class Config:
    """Root configuration object.

    Attributes:
        global_config: Global settings that apply to all volumes
        volumes: List of volume configurations
    """

    global_config: GlobalConfig = field(default_factory=GlobalConfig)
    volumes: list[VolumeConfig] = field(default_factory=list)

    def get_effective_retention(self, volume: VolumeConfig) -> RetentionConfig:
        """Get the effective retention policy for a volume.

        Volume-specific retention overrides global retention.
        """
        return volume.retention or self.global_config.retention

    def get_enabled_volumes(self) -> list[VolumeConfig]:
        """Get list of enabled volumes."""
        return [v for v in self.volumes if v.enabled]
