"""Snapper integration CLI commands.

This module provides CLI commands for discovering, listing, and backing up
snapper-managed snapshots.
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Any, cast

from ..__logger__ import create_logger
from ..snapper import SnapperScanner
from ..snapper.scanner import SnapperNotFoundError
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_snapper(args: argparse.Namespace) -> int:
    """Execute snapper subcommand.

    Args:
        args: Parsed arguments

    Returns:
        Exit code
    """
    if not args.snapper_action:
        print("No snapper action specified. Use 'snapper --help' for usage.")
        return 1

    handlers = {
        "detect": _handle_detect,
        "list": _handle_list,
        "backup": _handle_backup,
        "status": _handle_status,
        "restore": _handle_restore,
        "generate-config": _handle_generate_config,
    }

    handler = handlers.get(args.snapper_action)
    if handler:
        return handler(args)
    else:
        print(f"Unknown snapper action: {args.snapper_action}")
        return 1


def _handle_detect(args: argparse.Namespace) -> int:
    """Handle 'snapper detect' command."""
    try:
        scanner = SnapperScanner()
        configs = scanner.list_configs()
    except SnapperNotFoundError as e:
        if args.json:
            print(json.dumps({"error": str(e), "configs": []}))
        else:
            print(f"Snapper not found: {e}")
        return 1

    if args.json:
        result = {
            "configs": [
                {
                    "name": c.name,
                    "subvolume": str(c.subvolume),
                    "fstype": c.fstype,
                    "valid": c.is_valid(),
                    "snapshots_dir": str(c.snapshots_dir),
                }
                for c in configs
            ]
        }
        print(json.dumps(result, indent=2))
    else:
        if not configs:
            print("No snapper configurations found.")
            return 0

        print(f"Found {len(configs)} snapper configuration(s):\n")
        for config in configs:
            status = "OK" if config.is_valid() else "INVALID"
            print(f"  {config.name}:")
            print(f"    Subvolume:     {config.subvolume}")
            print(f"    Snapshots dir: {config.snapshots_dir}")
            print(f"    Status:        {status}")
            if config.allow_users:
                print(f"    Allowed users: {', '.join(config.allow_users)}")
            print()

    return 0


def _handle_list(args: argparse.Namespace) -> int:
    """Handle 'snapper list' command."""
    try:
        scanner = SnapperScanner()
    except SnapperNotFoundError as e:
        if args.json:
            print(json.dumps({"error": str(e)}))
        else:
            print(f"Snapper not found: {e}")
        return 1

    # Determine which configs to list
    if args.config:
        config = scanner.get_config(args.config)
        if config is None:
            if args.json:
                print(json.dumps({"error": f"Config not found: {args.config}"}))
            else:
                print(f"Snapper config not found: {args.config}")
            return 1
        configs = [config]
    else:
        try:
            configs = scanner.list_configs()
        except SnapperNotFoundError:
            configs = []

    if not configs:
        if args.json:
            print(json.dumps({"configs": []}))
        else:
            print("No snapper configurations found.")
        return 0

    # Get type filter
    include_types = args.type if args.type else None

    all_data: list[dict[str, Any]] = []

    for config in configs:
        try:
            snapshots = scanner.get_snapshots(config, include_types=include_types)
        except Exception as e:
            logger.warning("Failed to get snapshots for %s: %s", config.name, e)
            snapshots = []

        config_data = {
            "name": config.name,
            "subvolume": str(config.subvolume),
            "snapshots": [
                {
                    "number": s.number,
                    "type": s.snapshot_type,
                    "date": s.date.isoformat(),
                    "description": s.description,
                    "cleanup": s.cleanup,
                    "pre_num": s.pre_num,
                    "backup_name": s.get_backup_name(),
                }
                for s in snapshots
            ],
        }
        all_data.append(config_data)

    if args.json:
        print(json.dumps({"configs": all_data}, indent=2))
    else:
        for config_data in all_data:
            print(f"Config: {config_data['name']} ({config_data['subvolume']})")
            print("-" * 60)

            if not config_data["snapshots"]:
                print("  No snapshots found.")
            else:
                # Table header
                print(f"  {'NUM':>6}  {'TYPE':<6}  {'DATE':<19}  {'DESCRIPTION'}")
                print(f"  {'-' * 6}  {'-' * 6}  {'-' * 19}  {'-' * 20}")

                snapshots_list = cast(list[dict[str, Any]], config_data["snapshots"])
                for snap in snapshots_list:
                    date_val = str(snap["date"])
                    date_str = date_val[:19].replace("T", " ")
                    desc_val = snap["description"]
                    desc = str(desc_val)[:30] if desc_val else ""
                    print(
                        f"  {snap['number']:>6}  {snap['type']:<6}  {date_str}  {desc}"
                    )

            print()

    return 0


def _handle_backup(args: argparse.Namespace) -> int:
    """Handle 'snapper backup' command."""
    from ..core.operations import (
        get_snapper_snapshots_for_backup,
        send_snapper_snapshot,
        sync_snapper_snapshots,
    )

    # Set up Rich logging like other commands
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    try:
        scanner = SnapperScanner()
    except SnapperNotFoundError as e:
        logger.error("Snapper not found: %s", e)
        return 1

    # Validate config exists
    config = scanner.get_config(args.config)
    if config is None:
        logger.error("Snapper config not found: %s", args.config)
        return 1

    # Destination path - now just a path, not an endpoint
    # Backup layout: {target}/.snapshots/{num}/snapshot
    target_path = args.target

    # Determine if this is a local or remote transfer
    is_remote = target_path.startswith("ssh://")

    # Build transfer options
    # For local transfers: no compression (Rich progress bar)
    # For remote transfers: use zstd compression
    options = {
        "show_progress": True,
        "compress": getattr(args, "compress", None)
        or ("zstd" if is_remote else "none"),
        "rate_limit": getattr(args, "rate_limit", None),
    }

    # Get type filter
    include_types = args.type if args.type else ["single", "pre", "post"]

    # If specific snapshot requested
    if args.snapshot:
        snapshot = scanner.get_snapshot(config, args.snapshot)
        if snapshot is None:
            logger.error(
                "Snapshot %s not found in config %s", args.snapshot, args.config
            )
            return 1

        if args.dry_run:
            logger.info("Would backup snapshot %d", snapshot.number)
            return 0

        try:
            # Find a parent for incremental transfer
            # Look for backed up snapshots that exist locally
            parent = None
            dest_snapshots_dir = Path(target_path) / ".snapshots"
            if dest_snapshots_dir.exists():
                # Get all local snapshots
                all_snapshots = scanner.get_snapshots(config)
                all_by_num = {s.number: s for s in all_snapshots}

                # Find highest numbered backup that's lower than our target
                for item in sorted(
                    dest_snapshots_dir.iterdir(),
                    key=lambda x: int(x.name) if x.name.isdigit() else 0,
                    reverse=True,
                ):
                    if item.is_dir() and item.name.isdigit():
                        backup_num = int(item.name)
                        if (
                            backup_num < snapshot.number
                            and (item / "snapshot").exists()
                        ):
                            # Check if this snapshot still exists locally
                            if backup_num in all_by_num:
                                parent = all_by_num[backup_num]
                                logger.debug(
                                    "Found parent snapshot %d for incremental",
                                    backup_num,
                                )
                                break

            send_snapper_snapshot(
                snapshot, target_path, parent_snapper_snapshot=parent, options=options
            )
            return 0
        except Exception as e:
            logger.error("Failed to backup snapshot: %s", e)
            return 1

    # Otherwise, sync all eligible snapshots
    if args.dry_run:
        snapshots = get_snapper_snapshots_for_backup(
            scanner,
            args.config,
            include_types=include_types,
            min_age=args.min_age,
        )
        logger.info("Would backup %d snapshot(s):", len(snapshots))
        for snap in snapshots:
            logger.info("  %d", snap.number)
        return 0

    try:
        # Create a simple snapper config object for filtering
        class SnapperFilterConfig:
            def __init__(self):
                self.include_types = include_types
                self.exclude_cleanup = []
                self.min_age = args.min_age

        sync_snapper_snapshots(
            scanner,
            args.config,
            target_path,
            snapper_config=SnapperFilterConfig(),
            options=options,
        )
        return 0
    except Exception as e:
        logger.error("Backup failed: %s", e)
        logger.exception("Backup failed")
        return 1


def _handle_status(args: argparse.Namespace) -> int:
    """Handle 'snapper status' command."""
    from ..core.operations import _list_snapper_backups_at_destination
    from ..endpoint import choose_endpoint

    try:
        scanner = SnapperScanner()
    except SnapperNotFoundError as e:
        if args.json:
            print(json.dumps({"error": str(e)}))
        else:
            print(f"Snapper not found: {e}")
        return 1

    # Determine which configs to check
    if args.config:
        config = scanner.get_config(args.config)
        if config is None:
            if args.json:
                print(json.dumps({"error": f"Config not found: {args.config}"}))
            else:
                print(f"Snapper config not found: {args.config}")
            return 1
        configs = [config]
    else:
        try:
            configs = scanner.list_configs()
        except SnapperNotFoundError:
            configs = []

    if not configs:
        if args.json:
            print(json.dumps({"configs": []}))
        else:
            print("No snapper configurations found.")
        return 0

    # If target specified, check backup status there
    if args.target:
        try:
            endpoint_config = {"path": args.target, "snap_prefix": ""}
            endpoint = choose_endpoint(endpoint_config["path"], endpoint_config)
            backed_up = _list_snapper_backups_at_destination(endpoint)
        except Exception as e:
            if args.json:
                print(json.dumps({"error": f"Cannot access target: {e}"}))
            else:
                print(f"Cannot access target: {e}")
            return 1

        status_data = []
        for config in configs:
            try:
                snapshots = scanner.get_snapshots(config)
            except Exception:
                snapshots = []

            # Count backed up vs not backed up
            snapshot_names = {s.get_backup_name() for s in snapshots}
            backed_up_count = len(snapshot_names & backed_up)
            not_backed_up = len(snapshot_names) - backed_up_count

            status_data.append(
                {
                    "config": config.name,
                    "total_snapshots": len(snapshots),
                    "backed_up": backed_up_count,
                    "pending": not_backed_up,
                }
            )

        if args.json:
            print(json.dumps({"target": args.target, "status": status_data}, indent=2))
        else:
            print(f"Backup status at {args.target}:\n")
            for status in status_data:
                print(f"  {status['config']}:")
                print(f"    Total snapshots: {status['total_snapshots']}")
                print(f"    Backed up:       {status['backed_up']}")
                print(f"    Pending:         {status['pending']}")
                print()

    else:
        # Just show local snapshot counts
        status_data = []
        for config in configs:
            try:
                snapshots = scanner.get_snapshots(config)
            except Exception:
                snapshots = []

            # Count by type
            by_type: dict[str, int] = {}
            for s in snapshots:
                by_type[s.snapshot_type] = by_type.get(s.snapshot_type, 0) + 1

            status_data.append(
                {
                    "config": config.name,
                    "subvolume": str(config.subvolume),
                    "total_snapshots": len(snapshots),
                    "by_type": by_type,
                }
            )

        if args.json:
            print(json.dumps({"status": status_data}, indent=2))
        else:
            print("Snapper snapshot status:\n")
            for status in status_data:
                print(f"  {status['config']} ({status['subvolume']}):")
                print(f"    Total snapshots: {status['total_snapshots']}")
                by_type_dict = status["by_type"]
                if by_type_dict and isinstance(by_type_dict, dict):
                    for snap_type, count in sorted(by_type_dict.items()):
                        print(f"      {snap_type}: {count}")
                print()

    return 0


def _handle_restore(args: argparse.Namespace) -> int:
    """Handle 'snapper restore' command."""
    from ..core.restore import (
        list_snapper_backups,
        restore_snapper_snapshot,
    )

    # Set up Rich logging
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    source_path = args.source

    # List mode - show available backups
    if args.list:
        try:
            backups = list_snapper_backups(source_path)
        except Exception as e:
            if args.json:
                print(json.dumps({"error": str(e)}))
            else:
                logger.error("Failed to list backups: %s", e)
            return 1

        if args.json:
            result = {
                "source": source_path,
                "backups": [
                    {
                        "number": b["number"],
                        "type": b["metadata"].type if b.get("metadata") else None,
                        "date": str(b["metadata"].date) if b.get("metadata") else None,
                        "description": b["metadata"].description
                        if b.get("metadata")
                        else None,
                    }
                    for b in backups
                ],
            }
            print(json.dumps(result, indent=2))
        else:
            if not backups:
                logger.info("No snapper backups found at %s", source_path)
                return 0

            logger.info("Snapper backups at %s:", source_path)
            print()
            print(f"  {'NUM':>6}  {'TYPE':<6}  {'DATE':<19}  {'DESCRIPTION'}")
            print(f"  {'-' * 6}  {'-' * 6}  {'-' * 19}  {'-' * 30}")

            for b in backups:
                meta = b.get("metadata")
                snap_type = meta.type if meta else "?"
                date = str(meta.date)[:19] if meta and meta.date else "?"
                desc = (meta.description or "")[:30] if meta else ""
                print(f"  {b['number']:>6}  {snap_type:<6}  {date}  {desc}")

            print(f"\nTotal: {len(backups)} backup(s)")

        return 0

    # Validate local snapper config exists
    try:
        scanner = SnapperScanner()
        local_config = scanner.get_config(args.config)
        if local_config is None:
            logger.error("Local snapper config not found: %s", args.config)
            return 1
    except SnapperNotFoundError as e:
        logger.error("Snapper not found: %s", e)
        return 1

    # Determine what to restore
    snapshot_numbers = args.snapshot if args.snapshot else None
    restore_all = getattr(args, "all", False)

    if not snapshot_numbers and not restore_all:
        logger.error("Specify --snapshot NUM or --all to restore snapshots.")
        logger.info("Use --list to see available backups.")
        return 1

    # Get available backups
    try:
        backups = list_snapper_backups(source_path)
    except Exception as e:
        logger.error("Failed to list backups: %s", e)
        return 1

    if not backups:
        logger.error("No backups found at %s", source_path)
        return 1

    # Filter to requested snapshots
    if restore_all:
        to_restore = backups
    else:
        # snapshot_numbers is guaranteed non-None here due to early return above
        to_restore = [b for b in backups if b["number"] in (snapshot_numbers or [])]
        if not to_restore:
            logger.error("None of the requested snapshots found in backup")
            return 1

    # Sort by number (oldest first for proper incremental chain)
    to_restore.sort(key=lambda b: b["number"])

    logger.info("Will restore %d snapshot(s)", len(to_restore))

    # Build transfer options
    options = {
        "show_progress": True,
    }

    # Track restored snapshots for incremental
    restored_count = 0
    failed_count = 0
    backup_numbers = {b["number"] for b in backups}

    for i, backup in enumerate(to_restore, 1):
        backup_num = backup["number"]

        # Find parent for incremental restore
        # Look for the highest numbered backup that:
        # 1. Is lower than current backup number
        # 2. Exists in the backup set
        parent_num = None
        for candidate in sorted(backup_numbers, reverse=True):
            if candidate < backup_num:
                parent_num = candidate
                break

        # Check if parent was already restored in this session or exists locally
        # For now, use the backup-side parent for incremental send
        # The restore function will use -p with the parent backup path

        try:
            logger.info(
                "[%d/%d] Restoring snapshot %d ...", i, len(to_restore), backup_num
            )

            new_num, snapshot_path = restore_snapper_snapshot(
                backup_path=source_path,
                backup_number=backup_num,
                snapper_config_name=args.config,
                parent_backup_number=parent_num,
                options=options,
                dry_run=args.dry_run,
            )

            if not args.dry_run:
                logger.info(
                    "Snapshot %d restored as local snapshot %d", backup_num, new_num
                )
            restored_count += 1

        except Exception as e:
            logger.error("Failed to restore snapshot %d: %s", backup_num, e)
            failed_count += 1

    # Summary
    logger.info("")
    logger.info("Restore complete:")
    logger.info("  Restored: %d", restored_count)
    logger.info("  Failed: %d", failed_count)

    return 1 if failed_count > 0 else 0


def _handle_generate_config(args: argparse.Namespace) -> int:
    """Handle 'snapper generate-config' command.

    Generates TOML configuration for snapper-managed volumes.
    """
    try:
        scanner = SnapperScanner()
        all_configs = scanner.list_configs()
    except SnapperNotFoundError as e:
        if args.json:
            print(json.dumps({"error": str(e)}))
        else:
            print(f"Snapper not found: {e}")
        return 1

    if not all_configs:
        if args.json:
            print(json.dumps({"error": "No snapper configurations found"}))
        else:
            print("No snapper configurations found.")
        return 1

    # Filter configs if specific ones requested
    if args.config:
        configs = [c for c in all_configs if c.name in args.config]
        missing = set(args.config) - {c.name for c in configs}
        if missing:
            print(f"Warning: Config(s) not found: {', '.join(missing)}")
    else:
        configs = all_configs

    if not configs:
        print("No matching snapper configurations.")
        return 1

    # Get snapshot types (default to single)
    include_types = args.type if args.type else ["single"]

    # Build volume configs
    volumes_data: list[dict[str, Any]] = []
    for config in configs:
        volume: dict[str, Any] = {
            "path": str(config.subvolume),
            "source": "snapper",
            "snapper": {
                "config_name": config.name,
                "include_types": include_types,
                "min_age": args.min_age,
            },
        }

        # Add target if specified
        if args.target:
            # For snapper backups, the target should include a subdirectory
            # for the snapper config to keep backups organized
            target_path = args.target
            if not target_path.endswith(f"/{config.name}"):
                target_path = f"{target_path.rstrip('/')}/{config.name}"
            target = {"path": target_path}
            if args.target.startswith("ssh://") and args.ssh_sudo:
                target["ssh_sudo"] = True
            volume["targets"] = [target]

        volumes_data.append(volume)

    # JSON output
    if args.json:
        print(json.dumps({"volumes": volumes_data}, indent=2))
        return 0

    # Generate TOML
    toml_lines = _generate_snapper_toml(volumes_data, args.target)
    toml_content = "\n".join(toml_lines)

    # Append mode
    if args.append:
        return _append_to_config(args.append, toml_content)

    # Write to file
    if args.output:
        try:
            output_path = Path(args.output).expanduser()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(toml_content)
            print(f"Configuration written to: {output_path}")
            return 0
        except OSError as e:
            print(f"Error writing file: {e}")
            return 1

    # Print to stdout
    print(toml_content)
    return 0


def _generate_snapper_toml(
    volumes_data: list[dict[str, Any]], target: str | None
) -> list[str]:
    """Generate TOML lines for snapper volume configs.

    Args:
        volumes_data: List of volume configuration dictionaries
        target: Optional default target path

    Returns:
        List of TOML lines
    """
    lines = [
        "# Snapper volume configuration",
        "# Generated by: btrfs-backup-ng snapper generate-config",
        "#",
        "# These volumes use snapper as the snapshot source.",
        "# Snapshots are discovered from snapper rather than managed directly.",
        "# Backups use snapper's native directory layout: .snapshots/{num}/snapshot",
        "",
    ]

    for volume in volumes_data:
        lines.append("[[volumes]]")
        lines.append(f'path = "{volume["path"]}"')
        lines.append(f'source = "{volume["source"]}"')
        lines.append("")

        # Snapper section
        snapper = volume["snapper"]
        lines.append("[volumes.snapper]")
        lines.append(f'config_name = "{snapper["config_name"]}"')

        types_str = ", ".join(f'"{t}"' for t in snapper["include_types"])
        lines.append(f"include_types = [{types_str}]")
        lines.append(f'min_age = "{snapper["min_age"]}"')
        lines.append("")

        # Target section
        if "targets" in volume:
            for tgt in volume["targets"]:
                lines.append("[[volumes.targets]]")
                lines.append(f'path = "{tgt["path"]}"')
                if tgt.get("ssh_sudo"):
                    lines.append("ssh_sudo = true")
                lines.append("")
        elif target is None:
            # Add placeholder target
            lines.append("# [[volumes.targets]]")
            lines.append('# path = "/mnt/backup/{config_name}"')
            lines.append("# ssh_sudo = false")
            lines.append("")

    return lines


def _append_to_config(config_path: str, new_content: str) -> int:
    """Append snapper volume config to existing TOML file.

    Args:
        config_path: Path to existing config file
        new_content: TOML content to append

    Returns:
        Exit code
    """
    try:
        path = Path(config_path).expanduser()

        if not path.exists():
            print(f"Config file not found: {path}")
            print("Use -o/--output to create a new file instead.")
            return 1

        existing = path.read_text()

        # Check if file already has snapper volumes
        if 'source = "snapper"' in existing:
            print("Warning: Config already contains snapper volumes.")
            try:
                response = input("Append anyway? [y/N]: ").strip().lower()
                if response not in ("y", "yes"):
                    print("Aborted.")
                    return 1
            except (EOFError, KeyboardInterrupt):
                print("\nAborted.")
                return 1

        # Append with separator
        separator = "\n\n# --- Snapper volumes (auto-generated) ---\n\n"
        new_file_content = existing.rstrip() + separator + new_content

        path.write_text(new_file_content)
        print(f"Appended snapper config to: {path}")
        return 0

    except OSError as e:
        print(f"Error updating config file: {e}")
        return 1
