"""Shell completions installation command."""

import shutil
import sys
from argparse import Namespace
from importlib.resources import files
from pathlib import Path
from typing import TypedDict


class ShellConfig(TypedDict):
    """Type for shell completion configuration."""

    source: str
    system_dest: Path
    user_dest: Path


def get_completions_dir() -> Path | None:
    """Get the directory containing completion scripts.

    Returns:
        Path to completions directory, or None if not found
    """
    # Try to find completions in the package
    try:
        # When installed as a package
        pkg_files = files("btrfs_backup_ng")
        completions_path = Path(str(pkg_files)).parent.parent.parent / "completions"
        if completions_path.exists():
            return completions_path
    except Exception:
        pass

    # Try relative to this file (development mode)
    dev_path = Path(__file__).parent.parent.parent.parent / "completions"
    if dev_path.exists():
        return dev_path

    return None


def execute_completions(args: Namespace) -> int:
    """Execute completions command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    if args.completions_action == "install":
        return install_completions(args)
    elif args.completions_action == "path":
        return show_completions_path(args)
    else:
        print("No action specified. Use 'install' or 'path'.")
        print("  btrfs-backup-ng completions install --shell bash")
        print("  btrfs-backup-ng completions path")
        return 1


def show_completions_path(args: Namespace) -> int:
    """Show the path to completion scripts.

    Args:
        args: Parsed arguments

    Returns:
        Exit code
    """
    completions_dir = get_completions_dir()

    if completions_dir is None:
        print("Error: Could not find completions directory.", file=sys.stderr)
        print("Completions may not be included in this installation.", file=sys.stderr)
        return 1

    print(f"Completion scripts are located at: {completions_dir}")
    print()
    print("Available files:")
    for f in sorted(completions_dir.glob("btrfs-backup-ng.*")):
        print(f"  {f.name}")

    return 0


def install_completions(args: Namespace) -> int:
    """Install shell completion scripts.

    Args:
        args: Parsed arguments with shell type and options

    Returns:
        Exit code
    """
    shell = args.shell
    system_wide = args.system

    completions_dir = get_completions_dir()
    if completions_dir is None:
        print("Error: Could not find completions directory.", file=sys.stderr)
        return 1

    # Map shell to file and destinations
    shell_config: dict[str, ShellConfig] = {
        "bash": {
            "source": "btrfs-backup-ng.bash",
            "system_dest": Path("/etc/bash_completion.d/btrfs-backup-ng"),
            "user_dest": Path.home()
            / ".local/share/bash-completion/completions/btrfs-backup-ng",
        },
        "zsh": {
            "source": "btrfs-backup-ng.zsh",
            "system_dest": Path("/usr/share/zsh/site-functions/_btrfs-backup-ng"),
            "user_dest": Path.home() / ".zfunc/_btrfs-backup-ng",
        },
        "fish": {
            "source": "btrfs-backup-ng.fish",
            "system_dest": Path(
                "/usr/share/fish/vendor_completions.d/btrfs-backup-ng.fish"
            ),
            "user_dest": Path.home() / ".config/fish/completions/btrfs-backup-ng.fish",
        },
    }

    if shell not in shell_config:
        print(f"Error: Unknown shell '{shell}'", file=sys.stderr)
        print(f"Supported shells: {', '.join(shell_config.keys())}", file=sys.stderr)
        return 1

    config = shell_config[shell]
    source_file = completions_dir / config["source"]

    if not source_file.exists():
        print(f"Error: Completion file not found: {source_file}", file=sys.stderr)
        return 1

    dest = config["system_dest"] if system_wide else config["user_dest"]

    # Check permissions for system-wide install
    if system_wide and not _can_write_system(dest):
        print(f"Error: Cannot write to {dest}", file=sys.stderr)
        print(
            "Try running with sudo for system-wide installation, or use user install:",
            file=sys.stderr,
        )
        print(f"  btrfs-backup-ng completions install --shell {shell}", file=sys.stderr)
        return 1

    # Create parent directory if needed
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        print(f"Error: Cannot create directory {dest.parent}", file=sys.stderr)
        return 1

    # Copy the file
    try:
        shutil.copy2(source_file, dest)
        print(f"Installed {shell} completions to: {dest}")
    except PermissionError:
        print(f"Error: Permission denied writing to {dest}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error installing completions: {e}", file=sys.stderr)
        return 1

    # Shell-specific post-install instructions
    if shell == "bash":
        print()
        print("To activate completions:")
        print("  - Start a new shell, or")
        print(f"  - Run: source {dest}")
    elif shell == "zsh":
        print()
        print("To activate completions:")
        if not system_wide:
            print("  1. Add to ~/.zshrc (before compinit):")
            print("     fpath=(~/.zfunc $fpath)")
        print("  2. Run: autoload -Uz compinit && compinit")
        print("  3. Start a new shell")
    elif shell == "fish":
        print()
        print("Fish will automatically load the completions in new shells.")

    return 0


def _can_write_system(path: Path) -> bool:
    """Check if we can write to a system path.

    Args:
        path: Path to check

    Returns:
        True if writable
    """
    import os

    # Check if we're root
    if os.geteuid() == 0:
        return True

    # Check if parent exists and is writable
    parent = path.parent
    if parent.exists():
        return os.access(parent, os.W_OK)

    return False
