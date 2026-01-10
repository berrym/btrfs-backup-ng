"""Systemd integration utilities.

Provides functions for detecting, enabling, and disabling systemd
services and timers for btrfs-backup-ng and btrbk migration.
"""

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


# Common btrbk systemd unit locations
BTRBK_UNIT_NAMES = [
    "btrbk.timer",
    "btrbk.service",
    "btrbk@.timer",
    "btrbk@.service",
]

# btrfs-backup-ng unit names
BACKUP_NG_UNIT_NAMES = [
    "btrfs-backup-ng.timer",
    "btrfs-backup-ng.service",
]

# Systemd unit search paths
SYSTEMD_PATHS = [
    Path("/etc/systemd/system"),
    Path("/usr/lib/systemd/system"),
    Path("/lib/systemd/system"),
]


@dataclass
class SystemdUnitStatus:
    """Status of a systemd unit."""

    name: str
    exists: bool
    enabled: bool
    active: bool
    path: Path | None = None


def run_systemctl(
    *args: str,
    check: bool = False,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    """Run a systemctl command.

    Args:
        *args: Arguments to pass to systemctl
        check: If True, raise on non-zero exit
        capture: If True, capture stdout/stderr

    Returns:
        CompletedProcess result
    """
    cmd = ["systemctl", *args]
    try:
        return subprocess.run(
            cmd,
            check=check,
            capture_output=capture,
            text=True,
        )
    except FileNotFoundError:
        # systemctl not available
        return subprocess.CompletedProcess(
            cmd, returncode=1, stdout="", stderr="systemctl not found"
        )


def get_unit_status(unit_name: str) -> SystemdUnitStatus:
    """Get the status of a systemd unit.

    Args:
        unit_name: Name of the unit (e.g., "btrbk.timer")

    Returns:
        SystemdUnitStatus with current state
    """
    # Check if unit exists
    exists = False
    path = None

    for search_path in SYSTEMD_PATHS:
        unit_path = search_path / unit_name
        if unit_path.exists():
            exists = True
            path = unit_path
            break

    # Also check if systemd knows about it
    result = run_systemctl("cat", unit_name)
    if result.returncode == 0:
        exists = True

    # Check if enabled
    result = run_systemctl("is-enabled", unit_name)
    enabled = result.returncode == 0 and "enabled" in result.stdout

    # Check if active
    result = run_systemctl("is-active", unit_name)
    active = result.returncode == 0 and "active" in result.stdout

    return SystemdUnitStatus(
        name=unit_name,
        exists=exists,
        enabled=enabled,
        active=active,
        path=path,
    )


def find_btrbk_units() -> list[SystemdUnitStatus]:
    """Find all btrbk systemd units on the system.

    Returns:
        List of SystemdUnitStatus for found btrbk units
    """
    found = []
    for unit_name in BTRBK_UNIT_NAMES:
        status = get_unit_status(unit_name)
        if status.exists:
            found.append(status)

    # Also check for instance units (btrbk@*.timer)
    for search_path in SYSTEMD_PATHS:
        if not search_path.exists():
            continue
        for unit_file in search_path.glob("btrbk@*.timer"):
            status = get_unit_status(unit_file.name)
            if status.exists and status not in found:
                found.append(status)
        for unit_file in search_path.glob("btrbk@*.service"):
            status = get_unit_status(unit_file.name)
            if status.exists and status not in found:
                found.append(status)

    return found


def find_backup_ng_units() -> list[SystemdUnitStatus]:
    """Find btrfs-backup-ng systemd units.

    Returns:
        List of SystemdUnitStatus for found units
    """
    found = []
    for unit_name in BACKUP_NG_UNIT_NAMES:
        status = get_unit_status(unit_name)
        if status.exists:
            found.append(status)
    return found


def disable_unit(unit_name: str, stop: bool = True) -> tuple[bool, str]:
    """Disable a systemd unit.

    Args:
        unit_name: Name of the unit to disable
        stop: If True, also stop the unit

    Returns:
        Tuple of (success, message)
    """
    messages = []

    if stop:
        result = run_systemctl("stop", unit_name)
        if result.returncode == 0:
            messages.append(f"Stopped {unit_name}")
        else:
            # Not fatal if stop fails (might not be running)
            pass

    result = run_systemctl("disable", unit_name)
    if result.returncode == 0:
        messages.append(f"Disabled {unit_name}")
        return True, "; ".join(messages)
    else:
        return False, f"Failed to disable {unit_name}: {result.stderr}"


def enable_unit(unit_name: str, start: bool = False) -> tuple[bool, str]:
    """Enable a systemd unit.

    Args:
        unit_name: Name of the unit to enable
        start: If True, also start the unit

    Returns:
        Tuple of (success, message)
    """
    messages = []

    result = run_systemctl("enable", unit_name)
    if result.returncode != 0:
        return False, f"Failed to enable {unit_name}: {result.stderr}"

    messages.append(f"Enabled {unit_name}")

    if start:
        result = run_systemctl("start", unit_name)
        if result.returncode == 0:
            messages.append(f"Started {unit_name}")
        else:
            messages.append(f"Warning: Failed to start {unit_name}: {result.stderr}")

    return True, "; ".join(messages)


def migrate_from_btrbk(
    dry_run: bool = False,
) -> tuple[bool, list[str]]:
    """Migrate systemd integration from btrbk to btrfs-backup-ng.

    This will:
    1. Stop and disable btrbk timers/services
    2. Enable btrfs-backup-ng timer (if available)

    Args:
        dry_run: If True, only report what would be done

    Returns:
        Tuple of (success, list of messages)
    """
    messages = []
    success = True

    # Find btrbk units
    btrbk_units = find_btrbk_units()
    active_btrbk = [u for u in btrbk_units if u.enabled or u.active]

    if not active_btrbk:
        messages.append("No active btrbk systemd units found")
    else:
        messages.append(f"Found {len(active_btrbk)} active btrbk unit(s):")
        for unit in active_btrbk:
            status_parts = []
            if unit.enabled:
                status_parts.append("enabled")
            if unit.active:
                status_parts.append("active")
            messages.append(f"  - {unit.name} ({', '.join(status_parts)})")

        if dry_run:
            messages.append("Would disable btrbk units (dry-run)")
        else:
            for unit in active_btrbk:
                ok, msg = disable_unit(unit.name)
                if ok:
                    messages.append(f"  {msg}")
                else:
                    messages.append(f"  Error: {msg}")
                    success = False

    # Check for btrfs-backup-ng units
    backup_ng_units = find_backup_ng_units()

    if not backup_ng_units:
        messages.append("")
        messages.append("No btrfs-backup-ng systemd units found.")
        messages.append("To install systemd integration, run:")
        messages.append("  btrfs-backup-ng systemd install")
    else:
        timer_unit = next(
            (u for u in backup_ng_units if u.name.endswith(".timer")), None
        )
        if timer_unit and not timer_unit.enabled:
            if dry_run:
                messages.append(f"Would enable {timer_unit.name} (dry-run)")
            else:
                ok, msg = enable_unit(timer_unit.name)
                if ok:
                    messages.append(f"  {msg}")
                else:
                    messages.append(f"  Error: {msg}")
                    success = False

    return success, messages


def get_migration_summary() -> dict:
    """Get a summary of systemd migration status.

    Returns:
        Dictionary with migration information
    """
    btrbk_units = find_btrbk_units()
    backup_ng_units = find_backup_ng_units()

    return {
        "btrbk_units": [
            {
                "name": u.name,
                "enabled": u.enabled,
                "active": u.active,
                "path": str(u.path) if u.path else None,
            }
            for u in btrbk_units
        ],
        "backup_ng_units": [
            {
                "name": u.name,
                "enabled": u.enabled,
                "active": u.active,
                "path": str(u.path) if u.path else None,
            }
            for u in backup_ng_units
        ],
        "btrbk_active": any(u.enabled or u.active for u in btrbk_units),
        "backup_ng_active": any(u.enabled or u.active for u in backup_ng_units),
        "migration_needed": any(u.enabled or u.active for u in btrbk_units),
    }
