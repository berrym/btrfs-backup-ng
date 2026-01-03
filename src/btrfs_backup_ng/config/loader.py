"""TOML configuration loading and validation.

Handles config file discovery, parsing, and validation with helpful error messages.
"""

import tomllib
from pathlib import Path
from typing import Any

from .schema import (
    Config,
    GlobalConfig,
    RetentionConfig,
    TargetConfig,
    VolumeConfig,
)


class ConfigError(Exception):
    """Configuration loading or validation error."""

    pass


# Config file search paths in priority order
CONFIG_PATHS = [
    Path.home() / ".config" / "btrfs-backup-ng" / "config.toml",
    Path("/etc/btrfs-backup-ng/config.toml"),
]


def find_config_file(explicit_path: str | None = None) -> Path | None:
    """Find configuration file.

    Args:
        explicit_path: Explicitly specified config path (highest priority)

    Returns:
        Path to config file, or None if not found
    """
    if explicit_path:
        path = Path(explicit_path)
        if path.exists():
            return path
        raise ConfigError(f"Config file not found: {explicit_path}")

    for path in CONFIG_PATHS:
        if path.exists():
            return path

    return None


def _parse_retention(data: dict[str, Any]) -> RetentionConfig:
    """Parse retention configuration from dict."""
    return RetentionConfig(
        min=data.get("min", "1d"),
        hourly=data.get("hourly", 24),
        daily=data.get("daily", 7),
        weekly=data.get("weekly", 4),
        monthly=data.get("monthly", 12),
        yearly=data.get("yearly", 0),
    )


def _parse_target(data: dict[str, Any]) -> TargetConfig:
    """Parse target configuration from dict."""
    if "path" not in data:
        raise ConfigError("Target missing required 'path' field")

    return TargetConfig(
        path=data["path"],
        ssh_sudo=data.get("ssh_sudo", False),
        ssh_port=data.get("ssh_port", 22),
        ssh_key=data.get("ssh_key"),
        ssh_password_auth=data.get("ssh_password_auth", True),
    )


def _parse_volume(data: dict[str, Any], global_config: GlobalConfig) -> VolumeConfig:
    """Parse volume configuration from dict."""
    if "path" not in data:
        raise ConfigError("Volume missing required 'path' field")

    targets = [_parse_target(t) for t in data.get("targets", [])]

    retention = None
    if "retention" in data:
        retention = _parse_retention(data["retention"])

    return VolumeConfig(
        path=data["path"],
        snapshot_prefix=data.get("snapshot_prefix", ""),
        snapshot_dir=data.get("snapshot_dir", global_config.snapshot_dir),
        targets=targets,
        retention=retention,
        enabled=data.get("enabled", True),
    )


def _parse_global(data: dict[str, Any]) -> GlobalConfig:
    """Parse global configuration from dict."""
    retention = RetentionConfig()
    if "retention" in data:
        retention = _parse_retention(data["retention"])

    return GlobalConfig(
        snapshot_dir=data.get("snapshot_dir", ".snapshots"),
        timestamp_format=data.get("timestamp_format", "%Y%m%d-%H%M%S"),
        incremental=data.get("incremental", True),
        log_file=data.get("log_file"),
        retention=retention,
        parallel_volumes=data.get("parallel_volumes", 2),
        parallel_targets=data.get("parallel_targets", 3),
        quiet=data.get("quiet", False),
        verbose=data.get("verbose", False),
    )


def _validate_config(config: Config) -> list[str]:
    """Validate configuration and return list of warnings."""
    warnings = []

    if not config.volumes:
        warnings.append("No volumes configured")

    for i, volume in enumerate(config.volumes):
        if not volume.targets:
            warnings.append(f"Volume '{volume.path}' has no targets configured")

        # Check for duplicate targets
        target_paths = [t.path for t in volume.targets]
        if len(target_paths) != len(set(target_paths)):
            warnings.append(f"Volume '{volume.path}' has duplicate target paths")

        # Validate SSH URLs
        for target in volume.targets:
            if target.path.startswith("ssh://"):
                if ":" not in target.path[6:]:
                    warnings.append(
                        f"SSH target '{target.path}' may be missing path separator ':'"
                    )

    # Check for duplicate volume paths
    volume_paths = [v.path for v in config.volumes]
    if len(volume_paths) != len(set(volume_paths)):
        warnings.append("Duplicate volume paths detected")

    return warnings


def load_config(path: Path | str) -> tuple[Config, list[str]]:
    """Load and validate configuration from TOML file.

    Args:
        path: Path to configuration file

    Returns:
        Tuple of (Config object, list of warnings)

    Raises:
        ConfigError: If config is invalid or cannot be parsed
    """
    path = Path(path)

    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"Invalid TOML syntax: {e}")
    except OSError as e:
        raise ConfigError(f"Cannot read config file: {e}")

    # Parse global config
    global_config = _parse_global(data.get("global", {}))

    # Parse volumes
    volumes = []
    for vol_data in data.get("volumes", []):
        volumes.append(_parse_volume(vol_data, global_config))

    config = Config(global_config=global_config, volumes=volumes)

    # Validate and collect warnings
    warnings = _validate_config(config)

    return config, warnings


def generate_example_config() -> str:
    """Generate example configuration file content."""
    return """# btrfs-backup-ng configuration
# See documentation for full options

[global]
snapshot_dir = ".snapshots"
timestamp_format = "%Y%m%d-%H%M%S"
incremental = true
# log_file = "/var/log/btrfs-backup-ng.log"

# Parallelism settings
parallel_volumes = 2
parallel_targets = 3

[global.retention]
min = "1d"          # Keep all snapshots for at least 1 day
hourly = 24         # Then keep 24 hourly snapshots
daily = 7           # Then keep 7 daily snapshots
weekly = 4          # Then keep 4 weekly snapshots
monthly = 12        # Then keep 12 monthly snapshots
yearly = 0          # Don't keep yearly (0 = disabled)

# Home directory backup
[[volumes]]
path = "/home"
snapshot_prefix = "home"

[[volumes.targets]]
path = "/mnt/backup/home"

# Example SSH target
# [[volumes.targets]]
# path = "ssh://backup@server:/backups/home"
# ssh_sudo = true

# System logs backup with custom retention
# [[volumes]]
# path = "/var/log"
# snapshot_prefix = "logs"
#
# [volumes.retention]
# daily = 14
# weekly = 8
#
# [[volumes.targets]]
# path = "ssh://backup@server:/backups/logs"
"""
