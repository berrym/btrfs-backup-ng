"""Install/Uninstall commands: Systemd timer management."""

import argparse
import logging
import os
import shutil
from pathlib import Path

from ..__logger__ import create_logger
from .common import get_log_level

logger = logging.getLogger(__name__)


# Systemd service template
SERVICE_TEMPLATE = """\
[Unit]
Description=btrfs-backup-ng automated backup
Documentation=https://github.com/berrym/btrfs-backup-ng
After=local-fs.target

[Service]
Type=oneshot
ExecStart={exec_start} run
Nice=19
IOSchedulingClass=idle

[Install]
WantedBy=multi-user.target
"""

# Systemd timer template
TIMER_TEMPLATE = """\
[Unit]
Description=btrfs-backup-ng automated backup timer
Documentation=https://github.com/berrym/btrfs-backup-ng

[Timer]
OnCalendar={oncalendar}
Persistent=true
RandomizedDelaySec=5m

[Install]
WantedBy=timers.target
"""

# Preset OnCalendar values
TIMER_PRESETS = {
    "hourly": "*:00",
    "daily": "*-*-* 02:00:00",
    "weekly": "Sun *-*-* 02:00:00",
}


def _config_visibility_warning(user_mode: bool) -> str | None:
    """Warn when the unit being installed will not see the config that exists.

    A SYSTEM unit runs as root with no SUDO_USER, so config discovery searches
    /root/.config and then /etc -- never the home of whoever ran `install`. Since
    `sudo btrfs-backup-ng install` DOES see the invoking user's config (discovery
    honours SUDO_USER), the install succeeds and the timer then fails with "No
    configuration file found" while the operator is looking straight at their
    config file.

    The unit deliberately does NOT get `--config /home/<user>/...` baked in. A
    root service reading its configuration from a user-writable path hands
    whoever can write that file control over what root backs up, and where to.
    Copying it to /etc, or running the timer as the user, are both safe; silently
    widening root's trust to a home directory is not.

    Returns operator-facing text, or None when the unit will find its config.
    """
    from ..config import find_config_file

    if user_mode:
        return None
    try:
        found = find_config_file(None)
    except Exception:  # noqa: BLE001 - a warning must not break the install
        return None
    if found is None:
        return (
            "No configuration file was found. The timer will fail until one "
            "exists at /etc/btrfs-backup-ng/config.toml (create it with "
            "`btrfs-backup-ng config init`)."
        )

    system_config = Path("/etc/btrfs-backup-ng/config.toml")
    if found == system_config:
        return None

    return (
        f"The configuration in use is {found}, but a SYSTEM timer runs as root "
        f"and only looks in /root/.config and /etc -- it will not read that "
        f"file, and will fail with 'No configuration file found'.\n"
        f"  Either:  sudo cp {found} {system_config}\n"
        f"  Or:      btrfs-backup-ng install --user   (runs the timer as you)\n"
        f"The unit is deliberately not pointed at a file in a home directory: a "
        f"root service must not take its configuration from a path other users "
        f"may be able to write."
    )


def _resolve_exec_start() -> str:
    """Return the absolute path to the btrfs-backup-ng executable for ExecStart.

    systemd requires an ABSOLUTE ExecStart path and does not search $PATH. The old
    template hardcoded ``/usr/bin/btrfs-backup-ng``, which fails for any install not
    there -- notably ``--user`` / pipx / uv / venv installs in ``~/.local/bin`` or a
    virtualenv (the service errors with "No such file or directory"). Resolve the
    real binary via PATH, falling back to the historical default if not found."""
    return shutil.which("btrfs-backup-ng") or "/usr/bin/btrfs-backup-ng"


def execute_install(args: argparse.Namespace) -> int:
    """Execute the install command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Determine OnCalendar value
    oncalendar = None

    if getattr(args, "oncalendar", None):
        oncalendar = args.oncalendar
    elif getattr(args, "timer", None):
        oncalendar = TIMER_PRESETS.get(args.timer)
        if not oncalendar:
            logger.error("Unknown timer preset: %s", args.timer)
            return 1

    if not oncalendar:
        print("Error: Specify --timer or --oncalendar")
        print("")
        print("Examples:")
        print("  btrfs-backup-ng install --timer=hourly")
        print("  btrfs-backup-ng install --timer=daily")
        print("  btrfs-backup-ng install --oncalendar='*:0/15'    # Every 15 minutes")
        print("  btrfs-backup-ng install --oncalendar='*:0/5'     # Every 5 minutes")
        return 1

    # Determine installation paths
    user_mode = getattr(args, "user", False)
    if user_mode:
        systemd_dir = Path.home() / ".config" / "systemd" / "user"
    else:
        systemd_dir = Path("/etc/systemd/system")

    if not user_mode and os.geteuid() != 0:
        print("Error: Root privileges required for system-wide installation")
        print("Use --user for user-level installation, or run with sudo")
        return 1

    # Create directory if needed
    systemd_dir.mkdir(parents=True, exist_ok=True)

    service_file = systemd_dir / "btrfs-backup-ng.service"
    timer_file = systemd_dir / "btrfs-backup-ng.timer"

    # Say now, at install time, if this unit will not be able to find the config
    # -- rather than letting the operator discover it from a failed timer later.
    visibility = _config_visibility_warning(user_mode)

    # Generate content
    service_content = SERVICE_TEMPLATE.format(exec_start=_resolve_exec_start())
    timer_content = TIMER_TEMPLATE.format(oncalendar=oncalendar)

    # Write files
    try:
        service_file.write_text(service_content)
        print(f"Created: {service_file}")

        timer_file.write_text(timer_content)
        print(f"Created: {timer_file}")

    except OSError as e:
        logger.error("Failed to write systemd files: %s", e)
        return 1

    if visibility:
        print("")
        print("WARNING: this timer will not find your configuration.")
        print("")
        for line in visibility.splitlines():
            print(f"  {line}")

    print("")
    print("To enable the timer:")
    if user_mode:
        print("  systemctl --user daemon-reload")
        print("  systemctl --user enable --now btrfs-backup-ng.timer")
    else:
        print("  systemctl daemon-reload")
        print("  systemctl enable --now btrfs-backup-ng.timer")

    return 0


def execute_uninstall(args: argparse.Namespace) -> int:
    """Execute the uninstall command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Check both user and system locations
    locations = [
        (Path.home() / ".config" / "systemd" / "user", True),
        (Path("/etc/systemd/system"), False),
    ]

    found = False
    for systemd_dir, is_user in locations:
        service_file = systemd_dir / "btrfs-backup-ng.service"
        timer_file = systemd_dir / "btrfs-backup-ng.timer"

        if service_file.exists() or timer_file.exists():
            found = True

            if not is_user and os.geteuid() != 0:
                print(f"Found system files in {systemd_dir}")
                print("Run with sudo to remove system-wide installation")
                continue

            mode = "--user" if is_user else ""

            print(f"Found installation in: {systemd_dir}")
            print("")
            print("To disable and remove:")
            print(f"  systemctl {mode} disable --now btrfs-backup-ng.timer".strip())

            try:
                if timer_file.exists():
                    timer_file.unlink()
                    print(f"Removed: {timer_file}")

                if service_file.exists():
                    service_file.unlink()
                    print(f"Removed: {service_file}")

                print(f"  systemctl {mode} daemon-reload".strip())

            except OSError as e:
                logger.error("Failed to remove files: %s", e)
                return 1

    if not found:
        print("No btrfs-backup-ng systemd files found")

    return 0
