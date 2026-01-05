"""Man pages installation command."""

import gzip
import shutil
import sys
from argparse import Namespace
from importlib.resources import files
from pathlib import Path


def get_manpages_dir() -> Path | None:
    """Get the directory containing man page files.

    Returns:
        Path to man directory, or None if not found
    """
    # Try to find man pages in the package
    try:
        # When installed as a package
        pkg_files = files("btrfs_backup_ng")
        man_path = Path(str(pkg_files)).parent.parent.parent / "man"
        if man_path.exists():
            return man_path
    except Exception:
        pass

    # Try relative to this file (development mode)
    dev_path = Path(__file__).parent.parent.parent.parent / "man"
    if dev_path.exists():
        return dev_path

    return None


def execute_manpages(args: Namespace) -> int:
    """Execute manpages command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    if args.manpages_action == "install":
        return install_manpages(args)
    elif args.manpages_action == "path":
        return show_manpages_path(args)
    else:
        print("No action specified. Use 'install' or 'path'.")
        print("  btrfs-backup-ng manpages install")
        print("  btrfs-backup-ng manpages path")
        return 1


def show_manpages_path(args: Namespace) -> int:
    """Show the path to man page files.

    Args:
        args: Parsed arguments

    Returns:
        Exit code
    """
    man_dir = get_manpages_dir()

    if man_dir is None:
        print("Error: Could not find man pages directory.", file=sys.stderr)
        print("Man pages may not be included in this installation.", file=sys.stderr)
        return 1

    print(f"Man pages are located at: {man_dir}")
    print()
    print("Available man pages:")
    for f in sorted(man_dir.glob("*.1")):
        print(f"  {f.name}")
    print()
    print("View without installing:")
    print(f"  man {man_dir}/btrfs-backup-ng.1")

    return 0


def install_manpages(args: Namespace) -> int:
    """Install man pages.

    Args:
        args: Parsed arguments with options

    Returns:
        Exit code
    """
    man_dir = get_manpages_dir()
    if man_dir is None:
        print("Error: Could not find man pages directory.", file=sys.stderr)
        return 1

    # Determine installation directory
    if args.prefix:
        dest_dir = Path(args.prefix) / "share" / "man" / "man1"
    elif args.system:
        dest_dir = Path("/usr/local/share/man/man1")
    else:
        dest_dir = Path.home() / ".local" / "share" / "man" / "man1"

    # Check permissions
    if not _can_write(dest_dir):
        print(f"Error: Cannot write to {dest_dir}", file=sys.stderr)
        if not args.system and not args.prefix:
            print(
                "Try running with sudo for system-wide installation:", file=sys.stderr
            )
            print("  sudo btrfs-backup-ng manpages install --system", file=sys.stderr)
        return 1

    # Create destination directory
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        print(f"Error: Cannot create directory {dest_dir}", file=sys.stderr)
        return 1

    # Find all man pages
    man_pages = list(man_dir.glob("*.1"))
    if not man_pages:
        print("Error: No man pages found.", file=sys.stderr)
        return 1

    # Install each man page (gzipped)
    installed = []
    for src in man_pages:
        dest = dest_dir / f"{src.name}.gz"
        try:
            # Read source and write gzipped
            with open(src, "rb") as f_in:
                with gzip.open(dest, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            installed.append(dest)
        except PermissionError:
            print(f"Error: Permission denied writing to {dest}", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error installing {src.name}: {e}", file=sys.stderr)
            return 1

    print(f"Installed {len(installed)} man pages to: {dest_dir}")
    for p in installed:
        print(f"  {p.name}")

    # Update man database if possible
    print()
    if args.system or args.prefix:
        print("Updating man database...")
        _update_mandb()
    else:
        print("To make man pages available, add to MANPATH or update mandb:")
        print(f'  export MANPATH="{dest_dir.parent}:$MANPATH"')
        print("  # Or add to ~/.bashrc / ~/.zshrc")

    print()
    print("View man pages with:")
    print("  man btrfs-backup-ng")
    print("  man btrfs-backup-ng-restore")
    print("  man btrfs-backup-ng-config")

    return 0


def _can_write(path: Path) -> bool:
    """Check if we can write to a path.

    Args:
        path: Path to check

    Returns:
        True if writable
    """
    import os

    # Check if we're root
    if os.geteuid() == 0:
        return True

    # Check if path exists and is writable
    if path.exists():
        return os.access(path, os.W_OK)

    # Check if parent exists and is writable
    parent = path.parent
    if parent.exists():
        return os.access(parent, os.W_OK)

    # Try to find first existing parent
    while not parent.exists() and parent != parent.parent:
        parent = parent.parent

    if parent.exists():
        return os.access(parent, os.W_OK)

    return False


def _update_mandb() -> None:
    """Try to update the man database."""
    import subprocess

    try:
        subprocess.run(
            ["mandb", "-q"],
            capture_output=True,
            timeout=30,
        )
    except (subprocess.SubprocessError, FileNotFoundError):
        # mandb not available or failed, not critical
        pass
