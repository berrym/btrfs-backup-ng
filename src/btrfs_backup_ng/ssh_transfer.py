"""
Direct SSH transfer script for btrfs-backup-ng.

DEPRECATION NOTICE:
This module is deprecated and will be removed in a future version.
The direct SSH transfer functionality has been integrated directly into the SSHEndpoint class
in btrfs_backup_ng.endpoint.ssh module.

Please update your code to use SSHEndpoint._try_direct_transfer() instead of this module.
Example:
    from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
    endpoint = SSHEndpoint(hostname="remote-host", config={"path": "/path/to/dest"})
    success = endpoint._try_direct_transfer(
        source_path="/path/to/source",
        dest_path="/path/to/dest",
        snapshot_name="snapshot_name",
        parent_path=None,  # Optional parent for incremental
    )
"""

import os
import subprocess
import time
import warnings
from typing import Optional, Dict, Tuple


# Warning about deprecation
warnings.warn(
    "The ssh_transfer module is deprecated and will be removed in a future version. "
    "Please use SSHEndpoint._try_direct_transfer() instead.",
    DeprecationWarning,
    stacklevel=2,
)


def _find_buffer_program() -> Tuple[str, str]:
    """Find a suitable buffer program for improving transfer reliability."""
    # Check for mbuffer first - best option for large transfers
    try:
        mbuffer_path = (
            subprocess.check_output(["which", "mbuffer"], stderr=subprocess.DEVNULL)
            .decode()
            .strip()
        )
        if mbuffer_path:
            # Default to 128M buffer size
            return "mbuffer", f"{mbuffer_path} -q -m 128M"
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    # Fallback to pv if available
    try:
        pv_path = (
            subprocess.check_output(["which", "pv"], stderr=subprocess.DEVNULL)
            .decode()
            .strip()
        )
        if pv_path:
            return "pv", f"{pv_path} -q"
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    # No buffer program found
    return "", ""


def _run_diagnostics(
    host: str,
    path: str,
    user: Optional[str] = None,
    identity_file: Optional[str] = None,
    use_sudo: bool = False,
) -> Dict[str, bool]:
    """Run diagnostics to ensure the remote system is ready for BTRFS transfers."""
    result = {
        "ssh_connection": False,
        "btrfs_command": False,
        "write_permissions": False,
        "btrfs_filesystem": False,
    }

    # Build basic SSH command
    ssh_cmd = ["ssh"]
    if identity_file:
        ssh_cmd.extend(["-i", identity_file])
    if user:
        ssh_target = f"{user}@{host}"
    else:
        ssh_target = host
    ssh_cmd.append(ssh_target)

    # Test SSH connection
    try:
        conn_test = subprocess.run(
            ssh_cmd + ["echo", "SSH connection successful"],
            capture_output=True,
            timeout=10,
        )
        result["ssh_connection"] = conn_test.returncode == 0
        if not result["ssh_connection"]:
            print(f"SSH connection failed: {conn_test.stderr.decode()}")
            return result
    except Exception as e:
        print(f"SSH connection test failed: {e}")
        return result

    # Test BTRFS command availability
    btrfs_cmd = "sudo -n btrfs --version" if use_sudo else "btrfs --version"
    try:
        btrfs_test = subprocess.run(
            ssh_cmd + [btrfs_cmd], capture_output=True, timeout=10
        )
        result["btrfs_command"] = btrfs_test.returncode == 0
        if not result["btrfs_command"]:
            print(f"BTRFS command not available: {btrfs_test.stderr.decode()}")
    except Exception as e:
        print(f"BTRFS command test failed: {e}")

    # Test write permissions to destination path
    write_test_cmd = "touch" if not use_sudo else "sudo -n touch"
    test_file = f"{path}/.btrfs_backup_write_test"
    try:
        write_test = subprocess.run(
            ssh_cmd + [f"{write_test_cmd} {test_file} && echo success || echo failed"],
            capture_output=True,
            timeout=10,
        )
        result["write_permissions"] = "success" in write_test.stdout.decode()
        # Cleanup test file
        subprocess.run(ssh_cmd + [f"rm -f {test_file}"], capture_output=True, timeout=5)
    except Exception as e:
        print(f"Write permission test failed: {e}")

    # Verify the filesystem is BTRFS
    fs_test_cmd = "df -T" if not use_sudo else "sudo -n df -T"
    try:
        fs_test = subprocess.run(
            ssh_cmd + [f"{fs_test_cmd} {path} | grep -i btrfs"],
            capture_output=True,
            timeout=10,
        )
        result["btrfs_filesystem"] = fs_test.returncode == 0
        if not result["btrfs_filesystem"]:
            print("Destination is not a BTRFS filesystem")
    except Exception as e:
        print(f"Filesystem type test failed: {e}")

    return result


def direct_ssh_transfer(
    source_path: str,
    host: str,
    dest_path: str,
    snapshot_name: str,
    parent_path: Optional[str] = None,
    user: Optional[str] = None,
    identity_file: Optional[str] = None,
    use_sudo: bool = False,
) -> bool:
    """
    Direct SSH transfer for btrfs-backup-ng, using robust logic and logging.

    Args:
        source_path: Path to the source snapshot
        host: Remote host to transfer to
        dest_path: Destination path on remote host
        snapshot_name: Name of the snapshot (basename of source_path)
        parent_path: Optional parent snapshot for incremental transfer
        user: Optional SSH username
        identity_file: Optional SSH identity file
        use_sudo: Whether to use sudo on the remote host

    Returns:
        bool: True if transfer was successful, False otherwise
    """
    print(f"Starting direct SSH transfer to {host}:{dest_path}")

    # Check if source path exists
    if not os.path.exists(source_path):
        print(f"Source path does not exist: {source_path}")
        return False

    # Run diagnostics
    diagnostics = _run_diagnostics(
        host=host,
        path=dest_path,
        user=user,
        identity_file=identity_file,
        use_sudo=use_sudo,
    )

    if not all(diagnostics.values()):
        print("Pre-transfer diagnostics failed. Cannot proceed with transfer.")
        for key, value in diagnostics.items():
            print(f"  - {key}: {'PASS' if value else 'FAIL'}")
        return False

    # Find buffer program for progress display and reliability
    buffer_name, buffer_cmd = _find_buffer_program()

    # Build the transfer command
    if parent_path and os.path.exists(parent_path):
        print(f"Using incremental transfer with parent: {parent_path}")
        send_cmd = ["sudo", "btrfs", "send", "-p", parent_path, source_path]
    else:
        print("Using full transfer")
        send_cmd = ["sudo", "btrfs", "send", source_path]

    # Build SSH command
    ssh_cmd = ["ssh"]
    if identity_file:
        ssh_cmd.extend(["-i", identity_file])
    if user:
        ssh_target = f"{user}@{host}"
    else:
        ssh_target = host
    ssh_cmd.append(ssh_target)

    # Build receive command
    receive_cmd = "btrfs receive"
    if use_sudo:
        receive_cmd = "sudo -S btrfs receive"

    try:
        # Start send process
        print(f"Starting transfer from {source_path}...")
        send_process = subprocess.Popen(
            send_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0
        )

        # Setup buffer process if available
        buffer_process = None
        if buffer_cmd:
            print(f"Using {buffer_name} to improve transfer reliability")
            buffer_args = buffer_cmd.split()
            buffer_process = subprocess.Popen(
                buffer_args,
                stdin=send_process.stdout,
                stdout=subprocess.PIPE,
                bufsize=0,
            )
            if send_process.stdout:
                send_process.stdout.close()  # Allow send_process to receive SIGPIPE
            stdin_pipe = buffer_process.stdout
        else:
            stdin_pipe = send_process.stdout

        # Build full SSH receive command
        full_receive_cmd = ssh_cmd + [f"{receive_cmd} {dest_path}"]

        print(f"Starting receive process on {host}:{dest_path}")
        receive_process = subprocess.Popen(
            full_receive_cmd,
            stdin=stdin_pipe,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )

        # Wait for processes to complete
        start_time = time.time()
        send_result = send_process.wait()
        if buffer_process:
            buffer_process.wait()
        receive_result = receive_process.wait()
        elapsed_time = time.time() - start_time

        # Check results
        if send_result != 0:
            stderr_text = ""
            if send_process.stderr:
                stderr_text = send_process.stderr.read().decode()
            print(f"Send process failed with exit code {send_result}: {stderr_text}")
            return False

        if receive_result != 0:
            stderr_text = ""
            if receive_process.stderr:
                stderr_text = receive_process.stderr.read().decode()
            print(
                f"Receive process failed with exit code {receive_result}: {stderr_text}"
            )
            return False

        print(f"Transfer completed in {elapsed_time:.2f} seconds")

        # Verify the transfer
        print("Verifying snapshot was created on remote host...")
        verify_cmd = ssh_cmd + [
            f"test -d {dest_path}/{snapshot_name} && echo 'VERIFIED' || echo 'NOT_FOUND'"
        ]
        verify_result = subprocess.run(verify_cmd, capture_output=True, text=True)
        if "VERIFIED" in verify_result.stdout:
            print("Transfer verification successful")
            return True
        else:
            print("Transfer verification failed - snapshot not found on remote host")
            return False

    except Exception as e:
        print(f"Error during transfer: {e}")
        return False


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Direct SSH transfer for BTRFS snapshots"
    )
    parser.add_argument("source", help="Source snapshot path")
    parser.add_argument("destination", help="Destination in the format user@host:/path")
    parser.add_argument("--parent", help="Parent snapshot for incremental transfer")
    parser.add_argument("--identity-file", "-i", help="SSH identity file")
    parser.add_argument("--sudo", action="store_true", help="Use sudo on remote host")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Parse destination
    if "@" in args.destination:
        user, host_path = args.destination.split("@", 1)
    else:
        user = None
        host_path = args.destination

    if ":" in host_path:
        host, path = host_path.split(":", 1)
    else:
        print("Error: Destination must be in format [user@]host:/path")
        return 1

    snapshot_name = os.path.basename(args.source)

    # Perform the transfer
    success = direct_ssh_transfer(
        source_path=args.source,
        host=host,
        dest_path=path,
        snapshot_name=snapshot_name,
        parent_path=args.parent,
        user=user,
        identity_file=args.identity_file,
        use_sudo=args.sudo,
    )

    return 0 if success else 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
