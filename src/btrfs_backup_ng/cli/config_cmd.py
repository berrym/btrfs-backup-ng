"""Config command: Configuration management."""

import argparse
import logging
import sys

from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from ..config.loader import generate_example_config
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_config(args: argparse.Namespace) -> int:
    """Execute the config command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    action = getattr(args, "config_action", None)

    if action == "validate":
        return _validate_config(args)
    elif action == "init":
        return _init_config(args)
    elif action == "import":
        return _import_config(args)
    else:
        print("Usage: btrfs-backup-ng config <validate|init|import>")
        return 1


def _validate_config(args: argparse.Namespace) -> int:
    """Validate configuration file."""
    try:
        config_path = find_config_file(getattr(args, "config", None))
        if config_path is None:
            print("No configuration file found.")
            print("Searched locations:")
            print("  ~/.config/btrfs-backup-ng/config.toml")
            print("  /etc/btrfs-backup-ng/config.toml")
            return 1

        print(f"Validating: {config_path}")
        config, warnings = load_config(config_path)

        if warnings:
            print("")
            print("Warnings:")
            for warning in warnings:
                print(f"  - {warning}")

        print("")
        print("Configuration is valid.")
        print(f"  Volumes: {len(config.volumes)}")
        print(f"  Enabled: {len(config.get_enabled_volumes())}")

        total_targets = sum(len(v.targets) for v in config.volumes)
        print(f"  Targets: {total_targets}")

        return 0

    except ConfigError as e:
        print(f"Configuration error: {e}")
        return 1


def _init_config(args: argparse.Namespace) -> int:
    """Generate example configuration."""
    content = generate_example_config()

    output = getattr(args, "output", None)
    if output:
        try:
            with open(output, "w") as f:
                f.write(content)
            print(f"Example configuration written to: {output}")
        except OSError as e:
            print(f"Error writing file: {e}")
            return 1
    else:
        print(content)

    return 0


def _import_config(args: argparse.Namespace) -> int:
    """Import btrbk configuration."""
    from ..btrbk_import import import_btrbk_config

    btrbk_file = getattr(args, "btrbk_config", None)
    if not btrbk_file:
        print("Error: btrbk configuration file path required")
        return 1

    try:
        toml_content, warnings = import_btrbk_config(btrbk_file)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        print(f"Error parsing btrbk config: {e}")
        return 1

    # Show warnings
    if warnings:
        print("# Conversion warnings:", file=sys.stderr)
        for warning in warnings:
            print(f"#   {warning}", file=sys.stderr)
        print("", file=sys.stderr)

    # Output TOML
    output = getattr(args, "output", None)
    if output:
        try:
            with open(output, "w") as f:
                f.write(toml_content)
            print(f"Configuration written to: {output}", file=sys.stderr)
            print(f"Review the file and adjust as needed.", file=sys.stderr)
        except OSError as e:
            print(f"Error writing file: {e}")
            return 1
    else:
        print(toml_content)

    if warnings:
        print("", file=sys.stderr)
        print(f"Conversion complete with {len(warnings)} warning(s).", file=sys.stderr)
        print(
            "Review the warnings above and adjust the configuration.", file=sys.stderr
        )

    return 0
