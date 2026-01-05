"""SSH and Sudo Diagnostic Tool for btrfs-backup-ng

This script helps diagnose common SSH and sudo configuration issues
that may prevent btrfs-backup-ng from successfully transferring backups.
"""

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_command(cmd: List[str], timeout: int = 30) -> Tuple[int, str, str]:
    """Run a command and return returncode, stdout, stderr."""
    logger.debug(f"Running command: {' '.join(cmd)}")
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            text=True,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired:
        return 1, "", f"Command timed out after {timeout} seconds: {' '.join(cmd)}"
    except Exception as e:
        return 1, "", f"Error running command: {e}"


def ssh_command_base(
    host: str, port: Optional[int] = None, identity_file: Optional[str] = None
) -> List[str]:
    """Build a base SSH command."""
    cmd = ["ssh"]

    # Add options
    cmd.extend(["-o", "BatchMode=yes"])  # Don't prompt for password
    cmd.extend(["-o", "ConnectTimeout=10"])

    # Add port if specified
    if port:
        cmd.extend(["-p", str(port)])

    # Add identity file if specified
    if identity_file:
        cmd.extend(["-i", identity_file])

    # Add host
    cmd.append(host)

    return cmd


def test_ssh_connection(
    host: str, port: Optional[int] = None, identity_file: Optional[str] = None
) -> bool:
    """Test basic SSH connectivity."""
    logger.debug(f"Testing SSH connectivity to {host}...")

    cmd = ssh_command_base(host, port, identity_file)
    cmd.append("echo SSH connection successful")

    returncode, stdout, stderr = run_command(cmd)

    if returncode == 0:
        logger.debug("SSH connection successful")
        return True
    else:
        logger.error(f"SSH connection failed: {stderr}")
        return False


def test_sudo_access(
    host: str, port: Optional[int] = None, identity_file: Optional[str] = None
) -> bool:
    """Test if passwordless sudo is available."""
    logger.debug(f"Testing passwordless sudo access on {host}...")

    cmd = ssh_command_base(host, port, identity_file)
    cmd.append("sudo -n true")

    returncode, stdout, stderr = run_command(cmd)

    if returncode == 0:
        logger.debug("Passwordless sudo available")
        return True
    else:
        logger.debug("Passwordless sudo not available")
        logger.debug(f"Error: {stderr}")
        return False


def test_btrfs_command(
    host: str, port: Optional[int] = None, identity_file: Optional[str] = None
) -> bool:
    """Test if btrfs command is available."""
    logger.debug(f"Testing btrfs command availability on {host}...")

    cmd = ssh_command_base(host, port, identity_file)
    cmd.append("command -v btrfs")

    returncode, stdout, stderr = run_command(cmd)

    if returncode == 0:
        logger.debug(f"btrfs command available: {stdout.strip()}")
        return True
    else:
        logger.error("btrfs command not found")
        return False


def test_sudo_btrfs(
    host: str, port: Optional[int] = None, identity_file: Optional[str] = None
) -> bool:
    """Test if btrfs command can be run with sudo."""
    logger.debug(f"Testing sudo btrfs on {host}...")

    cmd = ssh_command_base(host, port, identity_file)
    cmd.append("sudo -n btrfs --version")

    returncode, stdout, stderr = run_command(cmd)

    if returncode == 0:
        logger.debug(f"Passwordless sudo btrfs works: {stdout.strip()}")
        return True
    else:
        logger.debug("Cannot run btrfs with passwordless sudo")
        logger.debug(f"Error: {stderr}")
        return False


def test_write_permissions(
    host: str,
    path: str,
    port: Optional[int] = None,
    identity_file: Optional[str] = None,
) -> bool:
    """Test if specified path is writable."""
    logger.debug(f"Testing write permissions for {path} on {host}...")

    # First check if path exists
    cmd = ssh_command_base(host, port, identity_file)
    cmd.append(f"test -e '{path}'")

    returncode, stdout, stderr = run_command(cmd)

    if returncode != 0:
        logger.debug(f"Path does not exist: {path}")

        # Check if parent directory exists and is writable
        parent_path = str(Path(path).parent)
        cmd = ssh_command_base(host, port, identity_file)
        cmd.append(f"test -d '{parent_path}' && test -w '{parent_path}'")

        returncode, stdout, stderr = run_command(cmd)

        if returncode == 0:
            logger.debug(f"Parent directory {parent_path} exists and is writable")
            return True
        else:
            logger.debug(
                f"Parent directory {parent_path} does not exist or is not writable"
            )
            return False

    # Check if path is writable
    cmd = ssh_command_base(host, port, identity_file)
    cmd.append(f"test -w '{path}'")

    returncode, stdout, stderr = run_command(cmd)

    if returncode == 0:
        logger.debug(f"Path is writable: {path}")
        return True
    else:
        logger.debug(f"Path is not writable: {path}")

        # Check if it's writable with sudo
        cmd = ssh_command_base(host, port, identity_file)
        cmd.append(f"sudo -n test -w '{path}'")

        returncode, stdout, stderr = run_command(cmd)

        if returncode == 0:
            logger.debug(f"Path is writable with sudo: {path}")
            return True
        else:
            logger.debug(f"Path is not writable even with sudo: {path}")
            return False


def test_btrfs_filesystem(
    host: str,
    path: str,
    port: Optional[int] = None,
    identity_file: Optional[str] = None,
) -> bool:
    """Test if the path is on a btrfs filesystem."""
    logger.debug(f"Testing if {path} is on a btrfs filesystem...")

    cmd = ssh_command_base(host, port, identity_file)
    cmd.append(
        f"stat -f -c '%T' '{path}' 2>/dev/null || stat -f -c '%T' '{str(Path(path).parent)}' 2>/dev/null"
    )

    returncode, stdout, stderr = run_command(cmd)

    if returncode == 0:
        fs_type = stdout.strip()
        if fs_type == "btrfs":
            logger.debug(f"Path is on a btrfs filesystem: {path}")
            return True
        else:
            logger.debug(f"Path is on a {fs_type} filesystem, not btrfs: {path}")
            return False
    else:
        logger.debug(f"Could not determine filesystem type: {stderr}")
        return False


def test_btrfs_receive(
    host: str,
    path: str,
    port: Optional[int] = None,
    identity_file: Optional[str] = None,
) -> bool:
    """Test if btrfs receive command works in the specified path."""
    logger.debug(f"Testing 'btrfs receive' capability on {host} for path {path}...")

    # First check filesystem type
    if not test_btrfs_filesystem(host, path, port, identity_file):
        return False

    # Create a small test subvolume locally
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = os.path.join(tmpdir, "testfile")
        with open(test_file, "w") as f:
            f.write("test data for btrfs send/receive\n")

        # Check if we can run btrfs commands locally
        returncode, stdout, stderr = run_command(["btrfs", "--version"])
        if returncode != 0:
            logger.debug("btrfs command not available locally")
            logger.debug("Cannot test btrfs send/receive without local btrfs support")
            return False

        # Skip actual send/receive test for now as it's complex to do safely
        # Just check if remote btrfs receive would work in principle
        cmd = ssh_command_base(host, port, identity_file)
        if path.endswith("/"):
            test_path = f"{path}test_receive"
        else:
            test_path = f"{path}.test_receive"

        cmd.append(
            f"sudo -n btrfs subvolume create '{test_path}' && sudo -n btrfs subvolume delete '{test_path}'"
        )

        returncode, stdout, stderr = run_command(cmd, timeout=60)

        if returncode == 0:
            logger.debug(
                "Successfully tested btrfs subvolume create/delete on remote host"
            )
            return True
        else:
            logger.debug("Failed to create/delete test subvolume on remote host")
            logger.debug(f"Error: {stderr}")
            return False


def create_sudoers_fix_instructions(host: str) -> None:
    """Provide instructions for fixing sudoers configuration."""
    logger.info("\n" + "=" * 70)
    logger.info("INSTRUCTIONS FOR FIXING SUDO ACCESS FOR BTRFS COMMANDS")
    logger.info("=" * 70)
    logger.info("")
    logger.info(
        "To enable passwordless sudo for btrfs commands, run these commands on the remote host:"
    )
    logger.info("")
    logger.info("1. Connect to the remote host:")
    logger.info(f"   ssh {host}")
    logger.info("")
    logger.info("2. Create a sudoers file for btrfs commands:")
    logger.info(
        "   sudo sh -c 'echo \"# Allow passwordless sudo for btrfs commands\" > /etc/sudoers.d/btrfs-backup'"
    )
    logger.info(
        "   sudo sh -c 'echo \"%sudo ALL=(ALL) NOPASSWD: /usr/bin/btrfs\" >> /etc/sudoers.d/btrfs-backup'"
    )
    logger.info("")
    logger.info("3. Set proper permissions:")
    logger.info("   sudo chmod 440 /etc/sudoers.d/btrfs-backup")
    logger.info("")
    logger.info("4. Test that it works:")
    logger.info("   sudo -n btrfs --version")
    logger.info("")
    logger.info("=" * 70)


def main():
    """Main function to run diagnostic tests."""
    parser = argparse.ArgumentParser(
        description="Diagnose SSH and sudo issues for btrfs-backup-ng"
    )
    parser.add_argument("host", help="SSH host to test (user@hostname)")
    parser.add_argument("path", help="Path to test for btrfs operations")
    parser.add_argument("-p", "--port", type=int, help="SSH port to use")
    parser.add_argument("-i", "--identity-file", help="SSH identity file")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("Starting SSH and sudo diagnostics for btrfs-backup-ng")
    logger.debug(f"Testing host: {args.host}")
    logger.debug(f"Testing path: {args.path}")

    # Run tests
    ssh_ok = test_ssh_connection(args.host, args.port, args.identity_file)
    if not ssh_ok:
        logger.error("SSH connection failed, cannot continue with tests")
        return 1

    btrfs_ok = test_btrfs_command(args.host, args.port, args.identity_file)
    sudo_ok = test_sudo_access(args.host, args.port, args.identity_file)
    sudo_btrfs_ok = test_sudo_btrfs(args.host, args.port, args.identity_file)
    write_ok = test_write_permissions(
        args.host, args.path, args.port, args.identity_file
    )
    btrfs_fs_ok = test_btrfs_filesystem(
        args.host, args.path, args.port, args.identity_file
    )

    # Print summary
    logger.info("-" * 50)
    logger.info("DIAGNOSTIC SUMMARY")
    logger.info("-" * 50)
    logger.info(f"SSH Connection:          {'PASS' if ssh_ok else 'FAIL'}")
    logger.info(f"btrfs Command:           {'PASS' if btrfs_ok else 'FAIL'}")
    logger.info(f"Passwordless Sudo:       {'PASS' if sudo_ok else 'FAIL'}")
    logger.info(f"Sudo btrfs Command:      {'PASS' if sudo_btrfs_ok else 'FAIL'}")
    logger.info(f"Write Permissions:       {'PASS' if write_ok else 'FAIL'}")
    logger.info(f"btrfs Filesystem:        {'PASS' if btrfs_fs_ok else 'FAIL'}")
    logger.info("-" * 50)

    # Overall assessment
    if all([ssh_ok, btrfs_ok, sudo_btrfs_ok, write_ok, btrfs_fs_ok]):
        logger.info("All tests passed! btrfs-backup-ng should work correctly.")
        return 0
    else:
        logger.warning("Some tests failed. btrfs-backup-ng may not work correctly.")

        # Give specific recommendations based on what failed
        if not sudo_ok or not sudo_btrfs_ok:
            create_sudoers_fix_instructions(args.host)

        if not write_ok:
            logger.info("\nFIX WRITE PERMISSIONS:")
            logger.info(f"Ensure that the SSH user has write permission to {args.path}")
            logger.info(
                "or that sudo is configured properly to allow writing to this location."
            )

        if not btrfs_fs_ok:
            logger.info("\nFIX FILESYSTEM TYPE:")
            logger.info(f"The path {args.path} must be on a btrfs filesystem.")
            logger.info("btrfs-backup-ng cannot work with other filesystem types.")

        return 1


if __name__ == "__main__":
    sys.exit(main())
