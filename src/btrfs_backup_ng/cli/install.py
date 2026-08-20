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


#: Directories whose contents only root can change, by convention. Used only to
#: describe the fix in the warning text, never to decide it -- the decision is
#: made from the actual ownership and mode on disk.
_SYSTEM_BIN_HINT = "/usr/local/bin"


def _untrusted_ancestor(path: Path) -> str | None:
    """Return why one of ``path``'s directories is not root-controlled, or None.

    The mode and owner of the executable alone do not settle who controls it. A
    root-owned 0755 binary sitting in a directory an ordinary user can write is
    still theirs to choose: replacing a file is unlink plus create, which is a
    write to the DIRECTORY, not to the file. Checking the file and stopping
    there answers a question nobody asked.

    A sticky directory is exempt from the write test, since the sticky bit is
    exactly the rule that stops one user removing another user's files -- /tmp
    is 1777 and is not, by itself, a way to replace a root-owned binary.
    """
    for parent in path.parents:
        try:
            info = os.stat(parent)
        except OSError as err:
            # A directory that cannot be examined has not been cleared; saying
            # so is the point. Reporting nothing here would be the same silent
            # pass this function exists to remove.
            return f"its directory {parent} could not be examined ({err.strerror})"
        if info.st_uid != 0:
            return f"its directory {parent} is owned by uid {info.st_uid}"
        if info.st_mode & 0o022 and not info.st_mode & 0o1000:
            return (
                f"its directory {parent} is writable by others "
                f"(mode {info.st_mode & 0o777:04o}), so the executable can be "
                f"replaced without ever writing to it"
            )
    return None


def _exec_start_missing_warning(exec_start: str, user_mode: bool) -> str | None:
    """Warn when the unit would point at an executable that is not there.

    ``_resolve_exec_start`` falls back to the historical /usr/bin path when the
    binary is not on PATH, so an install can write a perfectly valid-looking
    unit aimed at nothing. The failure then surfaces at the first timer run, as
    a systemd status nobody is watching. The config warning already reports a
    missing config at install time; this is the same courtesy for the binary.

    Applies to user units too: a user timer pointed at a missing binary fails
    just as completely. This is correctness, not a trust boundary.
    """
    if Path(exec_start).exists():
        return None
    return (
        f"The timer would run {exec_start}, which does not exist.\n"
        f"Every run will fail with 'No such file or directory'.\n"
        f"  Install btrfs-backup-ng somewhere on PATH, then run "
        f"`btrfs-backup-ng install` again"
        + ("" if user_mode else f" -- for a system timer, into {_SYSTEM_BIN_HINT}")
    )


def _exec_start_trust_warning(exec_start: str, user_mode: bool) -> str | None:
    """Warn when a ROOT unit would run a binary ordinary users can replace.

    ``_config_visibility_warning`` already refuses to point a root service at a
    config in a home directory, on the grounds that a root service must not take
    its input from a path other users may write. The executable is the stronger
    version of the same trust: a `--user`/pipx/uv install resolves through PATH
    to something like ``~/.local/bin/btrfs-backup-ng``, and baking that into a
    SYSTEM unit means whoever can write that file chooses what root runs, nightly.

    A user unit runs as the user and is not affected.
    """
    if user_mode:
        return None

    path = Path(exec_start)
    try:
        info = path.stat()
    except FileNotFoundError:
        # Nothing to vouch for, and not silence either: the missing path is
        # reported by _exec_start_missing_warning, which is always consulted
        # alongside this one.
        return None
    except OSError as err:
        # The check did not run. Previously this returned None, which an
        # operator reads as "checked, fine" -- a failed check reported as a
        # clean result, which is worse than no check at all.
        return (
            f"The system timer would run {exec_start}, which could not be "
            f"examined ({err.strerror}), so it was NOT checked.\n"
            f"A SYSTEM unit runs as root, so this cannot confirm the binary is "
            f"one only root can change.\n"
            f"  Check it yourself:  ls -ld {exec_start} $(dirname {exec_start})"
        )

    home_dirs = (Path.home(), Path("/home"), Path("/root"))
    in_home = any(
        path == home or home in path.parents for home in home_dirs if str(home) != "/"
    )
    # Writable by group or other, or owned by someone other than root.
    loosely_owned = info.st_uid != 0 or bool(info.st_mode & 0o022)
    ancestor = _untrusted_ancestor(path)

    if not (in_home or loosely_owned or ancestor):
        return None

    detail = ancestor or f"owner: uid {info.st_uid}, mode: {info.st_mode & 0o777:04o}"
    return (
        f"The system timer would run {exec_start}, which is not a root-owned "
        f"system path ({detail}).\n"
        f"A SYSTEM unit runs as root, so whoever can write that file decides "
        f"what root executes on every run.\n"
        f"  Either:  install btrfs-backup-ng system-wide (e.g. into "
        f"{_SYSTEM_BIN_HINT}, owned by root)\n"
        f"  Or:      btrfs-backup-ng install --user   (runs the timer as you)"
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
    exec_start = _resolve_exec_start()
    missing = _exec_start_missing_warning(exec_start, user_mode)
    trust = _exec_start_trust_warning(exec_start, user_mode)
    service_content = SERVICE_TEMPLATE.format(exec_start=exec_start)
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

    if missing:
        print("")
        print("WARNING: this timer's executable does not exist.")
        print("")
        for line in missing.splitlines():
            print(f"  {line}")

    if trust:
        print("")
        print("WARNING: this timer would run a binary others may be able to replace.")
        print("")
        for line in trust.splitlines():
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
