#!/usr/bin/env python3

"""
Direct SSH transfer script for btrfs-backup-ng.

This module provides a direct, reliable way to transfer BTRFS snapshots over SSH
by using a more straightforward approach than the complex process-based method.

Key features:
- Verifies remote filesystem is BTRFS before attempting transfers
- Tests SSH connectivity with a simple test file
- Uses mbuffer or pv if available to improve transfer reliability
- Provides detailed error reporting and verification
"""

import logging
import os
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

# Configure logging
logger = logging.getLogger("btrfs-backup-ng")

class SSHTransferError(Exception):
    """Exception raised when SSH transfer fails."""
    pass

def check_command_exists(command: str) -> bool:
    """Check if a command exists in the PATH."""
    try:
        subprocess.run(
            ["which", command], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            check=False
        )
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        return False

def test_remote_filesystem(
    host: str, 
    path: str, 
    user: Optional[str] = None,
    identity_file: Optional[str] = None,
    use_sudo: bool = False
) -> bool:
    """
    Test if the remote filesystem is BTRFS.
    
    Args:
        host: Remote host
        path: Remote path
        user: SSH username
        identity_file: SSH identity file
        use_sudo: Whether to use sudo for remote commands
        
    Returns:
        True if the remote filesystem is BTRFS, False otherwise
    """
    user_str = f"{user}@" if user else ""
    identity_arg = f"-i {identity_file}" if identity_file else ""
    sudo_cmd = "sudo" if use_sudo else ""
    
    # Create the test command
    test_cmd = f"ssh {identity_arg} {user_str}{host} '{sudo_cmd} stat -f -c %T {path}'"
    
    logger.debug(f"Testing remote filesystem: {test_cmd}")
    
    try:
        result = subprocess.run(
            test_cmd,
            shell=True,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        fs_type = result.stdout.strip()
        logger.debug(f"Remote filesystem type: {fs_type}")
        
        if result.returncode != 0:
            logger.error(f"Failed to check remote filesystem: {result.stderr}")
            return False
            
        # Check if filesystem is btrfs
        if fs_type.lower() == "btrfs":
            logger.info("Remote filesystem is BTRFS")
            return True
        else:
            logger.error(f"Remote filesystem is not BTRFS (found: {fs_type})")
            logger.error("The destination must be on a BTRFS filesystem")
            return False
    except Exception as e:
        logger.error(f"Error testing remote filesystem: {e}")
        return False

def test_ssh_connectivity(
    host: str, 
    path: str, 
    user: Optional[str] = None,
    identity_file: Optional[str] = None,
    use_sudo: bool = False
) -> bool:
    """
    Test SSH connectivity by creating and verifying a test file.
    
    Args:
        host: Remote host
        path: Remote path
        user: SSH username
        identity_file: SSH identity file
        use_sudo: Whether to use sudo for remote commands
        
    Returns:
        True if the test is successful, False otherwise
    """
    user_str = f"{user}@" if user else ""
    identity_arg = f"-i {identity_file}" if identity_file else ""
    sudo_cmd = "sudo" if use_sudo else ""
    test_content = f"BTRFS backup test file {time.time()}"
    test_file = f"{path}/.btrfs-backup-test-{int(time.time())}"
    
    # Create test file locally
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write(test_content)
        tmp_path = tmp.name
    
    try:
        # Copy the test file to the remote host
        scp_cmd = f"scp {identity_arg} {tmp_path} {user_str}{host}:{test_file}"
        logger.debug(f"Testing SSH connectivity: {scp_cmd}")
        
        scp_result = subprocess.run(
            scp_cmd,
            shell=True,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if scp_result.returncode != 0:
            logger.error(f"Failed to copy test file: {scp_result.stderr}")
            return False
            
        # Verify the test file on the remote host
        verify_cmd = f"ssh {identity_arg} {user_str}{host} 'cat {test_file}'"
        verify_result = subprocess.run(
            verify_cmd,
            shell=True,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if verify_result.returncode != 0:
            logger.error(f"Failed to verify test file: {verify_result.stderr}")
            return False
            
        if verify_result.stdout.strip() != test_content:
            logger.error(f"Test file content mismatch")
            return False
            
        # Clean up the test file
        cleanup_cmd = f"ssh {identity_arg} {user_str}{host} 'rm {test_file}'"
        subprocess.run(
            cleanup_cmd,
            shell=True,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        logger.info("SSH connectivity test successful")
        return True
    except Exception as e:
        logger.error(f"Error testing SSH connectivity: {e}")
        return False
    finally:
        # Clean up local temp file
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

def find_buffer_program() -> Tuple[Optional[str], Optional[str]]:
    """
    Find a suitable buffer program (mbuffer or pv) to use for transfers.
    
    Returns:
        Tuple of (program_name, command_string) or (None, None) if not found
    """
    mbuffer_size = "128M"  # Default buffer size
    
    # Check for mbuffer first (preferred)
    if check_command_exists("mbuffer"):
        logger.debug("Found mbuffer - using it for transfers")
        return "mbuffer", f"mbuffer -s {mbuffer_size} -q"
    
    # Check for pv as fallback
    if check_command_exists("pv"):
        logger.debug("Found pv - using it for transfers")
        return "pv", "pv -q"
    
    # No buffer program found
    logger.debug("No buffer program found - transfers may be less reliable")
    return None, None

def verify_snapshot_exists(
    host: str, 
    path: str, 
    snapshot_name: str,
    user: Optional[str] = None,
    identity_file: Optional[str] = None,
    use_sudo: bool = False
) -> bool:
    """
    Verify a snapshot exists on the remote host.
    
    Args:
        host: Remote host
        path: Remote path
        snapshot_name: Name of the snapshot to verify
        user: SSH username
        identity_file: SSH identity file
        use_sudo: Whether to use sudo for remote commands
        
    Returns:
        True if the snapshot exists, False otherwise
    """
    user_str = f"{user}@" if user else ""
    identity_arg = f"-i {identity_file}" if identity_file else ""
    sudo_cmd = "sudo" if use_sudo else ""
    
    # Try direct subvolume list first
    list_cmd = f"ssh {identity_arg} {user_str}{host} '{sudo_cmd} btrfs subvolume list -o {path}'"
    
    logger.debug(f"Verifying snapshot existence: {list_cmd}")
    
    try:
        list_result = subprocess.run(
            list_cmd,
            shell=True,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if list_result.returncode != 0:
            logger.warning(f"Failed to list subvolumes: {list_result.stderr}")
            # Fall back to simple path check
            check_cmd = f"ssh {identity_arg} {user_str}{host} '{sudo_cmd} test -d {path}/{snapshot_name} && echo EXISTS'"
            check_result = subprocess.run(
                check_cmd,
                shell=True,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            if "EXISTS" in check_result.stdout:
                logger.info(f"Snapshot exists at path: {path}/{snapshot_name}")
                return True
            else:
                logger.error(f"Snapshot not found at path: {path}/{snapshot_name}")
                return False
        
        # Check if the snapshot name appears in the subvolume list
        if snapshot_name in list_result.stdout:
            logger.info(f"Snapshot found in subvolume list: {snapshot_name}")
            return True
        else:
            logger.error(f"Snapshot not found in subvolume list")
            logger.debug(f"Subvolume list output: {list_result.stdout}")
            return False
    except Exception as e:
        logger.error(f"Error verifying snapshot: {e}")
        return False

def direct_ssh_transfer(
    source_path: str,
    host: str,
    dest_path: str,
    snapshot_name: str,
    parent_path: Optional[str] = None,
    user: Optional[str] = None,
    identity_file: Optional[str] = None,
    use_sudo: bool = False
) -> bool:
    """
    Transfer a BTRFS snapshot directly over SSH.
    
    Args:
        source_path: Local source path (complete snapshot path)
        host: Remote host
        dest_path: Remote destination path (directory)
        snapshot_name: Name of the snapshot (for verification)
        parent_path: Optional parent snapshot path for incremental transfer
        user: SSH username
        identity_file: SSH identity file
        use_sudo: Whether to use sudo for remote commands
        
    Returns:
        True if the transfer was successful, False otherwise
    """
    # Prepare command components
    user_str = f"{user}@" if user else ""
    identity_arg = f"-i {identity_file}" if identity_file else ""
    sudo_cmd = "sudo" if use_sudo else ""
    
    # Check if source path exists
    if not os.path.exists(source_path):
        logger.error(f"Source path does not exist: {source_path}")
        return False
    
    # Test SSH connectivity
    logger.info(f"Testing SSH connectivity to {host}...")
    if not test_ssh_connectivity(host, dest_path, user, identity_file, use_sudo):
        logger.error(f"SSH connectivity test failed")
        return False
    
    # Test remote filesystem
    logger.info(f"Testing remote filesystem at {dest_path}...")
    if not test_remote_filesystem(host, dest_path, user, identity_file, use_sudo):
        logger.error(f"Remote filesystem test failed")
        return False
    
    # Find buffer program
    buffer_name, buffer_cmd = find_buffer_program()
    
    # Construct the transfer command
    if parent_path and os.path.exists(parent_path):
        logger.info(f"Using incremental transfer with parent: {parent_path}")
        send_cmd = f"sudo btrfs send -p {parent_path} {source_path}"
    else:
        logger.info(f"Using full transfer")
        send_cmd = f"sudo btrfs send {source_path}"
    
    receive_cmd = f"ssh {identity_arg} {user_str}{host} '{sudo_cmd} btrfs receive {dest_path}'"
    
    # Add buffer if available
    if buffer_cmd:
        full_cmd = f"{send_cmd} | {buffer_cmd} | {receive_cmd}"
        logger.info(f"Using {buffer_name} to improve transfer reliability")
    else:
        full_cmd = f"{send_cmd} | {receive_cmd}"
    
    # Log the full command
    logger.debug(f"Executing transfer command: {full_cmd}")
    
    # Execute the transfer
    logger.info(f"Starting transfer from {source_path} to {host}:{dest_path}...")
    try:
        start_time = time.time()
        transfer_result = subprocess.run(
            full_cmd,
            shell=True,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        elapsed = time.time() - start_time
        
        if transfer_result.returncode != 0:
            logger.error(f"Transfer failed with exit code {transfer_result.returncode}")
            logger.error(f"Error output: {transfer_result.stderr}")
            return False
        
        logger.info(f"Transfer command completed in {elapsed:.2f} seconds")
        
        # Verify the transfer
        logger.info(f"Verifying snapshot was created on remote host...")
        if verify_snapshot_exists(host, dest_path, snapshot_name, user, identity_file, use_sudo):
            logger.info(f"Transfer verification successful - snapshot exists on remote host")
            return True
        else:
            logger.error(f"Transfer verification failed - snapshot not found on remote host")
            logger.error(f"This may indicate the transfer failed silently")
            return False
    except Exception as e:
        logger.error(f"Error during transfer: {e}")
        return False

def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Direct SSH transfer for BTRFS snapshots")
    parser.add_argument("source", help="Source snapshot path")
    parser.add_argument("destination", help="Destination in the format user@host:/path")
    parser.add_argument("--parent", help="Parent snapshot for incremental transfer")
    parser.add_argument("--identity-file", "-i", help="SSH identity file")
    parser.add_argument("--sudo", action="store_true", help="Use sudo on remote host")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )
    
    # Parse destination
    if ":" not in args.destination:
        parser.error("Destination must be in the format user@host:/path or host:/path")
    
    host_part, path = args.destination.split(":", 1)
    
    if "@" in host_part:
        user, host = host_part.split("@", 1)
    else:
        user = None
        host = host_part
    
    # Get snapshot name for verification
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
        use_sudo=args.sudo
    )
    
    if success:
        logger.info("Transfer completed successfully")
        return 0
    else:
        logger.error("Transfer failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())