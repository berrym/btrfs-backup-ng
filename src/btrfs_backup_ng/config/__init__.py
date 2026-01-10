"""Configuration system for btrfs-backup-ng.

This module provides TOML-based configuration loading, validation,
and schema definitions for automated backup management.
"""

from .loader import (
    ConfigError,
    find_config_file,
    get_default_config_path,
    get_user_config_dir,
    get_user_home,
    load_config,
)
from .schema import (
    Config,
    GlobalConfig,
    RetentionConfig,
    TargetConfig,
    VolumeConfig,
)

__all__ = [
    "GlobalConfig",
    "RetentionConfig",
    "TargetConfig",
    "VolumeConfig",
    "Config",
    "load_config",
    "find_config_file",
    "ConfigError",
    "get_user_home",
    "get_user_config_dir",
    "get_default_config_path",
]
