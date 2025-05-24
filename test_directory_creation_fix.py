#!/usr/bin/env python3
"""
Test script to verify that directory creation commands now use cached sudo passwords.

This test focuses specifically on the directory creation context issue that was identified
where mkdir commands were not inheriting the authenticated sudo session.
"""

import os
import sys
import subprocess
import logging

# Add the source directory to Python path
sys.path.insert(0, "/home/mberry/Lab/python/btrfs-backup-ng/src")

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

def setup_logging():
    """Set up detailed logging to see the sudo command construction."""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)s (%(name)s) %(message)s',
        handlers=[logging.StreamHandler()]
    )

def test_directory_creation_with_sudo():
    """Test that directory creation commands get proper sudo flags with password support."""
    print("=" * 80)
    print("Testing Directory Creation Sudo Command Building")
    print("=" * 80)
    
    # Create SSH endpoint with sudo enabled
    config = {
        "ssh_sudo": True,
        "username": "mberry",
        "port": 22,
        "passwordless": False
    }
    
    endpoint = SSHEndpoint(
        hostname="192.168.5.85",
        config=config,
        ssh_sudo=True
    )
    
    # Test mkdir command building
    print("\n1. Testing mkdir command sudo construction...")
    mkdir_cmd = ["mkdir", "-p", "/home/mberry/test_dir"]
    built_cmd = endpoint._build_remote_command(mkdir_cmd)
    print(f"Original command: {mkdir_cmd}")
    print(f"Built command: {built_cmd}")
    
    # Verify that mkdir gets sudo -S (password support)
    if "sudo" in built_cmd and "-S" in built_cmd:
        print("✅ SUCCESS: mkdir command now includes sudo with password support (-S flag)")
    else:
        print("❌ FAILURE: mkdir command does not have proper sudo with password support")
        return False
    
    # Test other directory operations
    print("\n2. Testing other directory operations...")
    test_commands = [
        ["touch", "/tmp/test_file"],
        ["rm", "-f", "/tmp/test_file"], 
        ["test", "-d", "/tmp"]
    ]
    
    for cmd in test_commands:
        built_cmd = endpoint._build_remote_command(cmd)
        print(f"Command: {cmd[0]} -> Built: {built_cmd}")
        if "sudo" in built_cmd and "-S" in built_cmd:
            print(f"✅ {cmd[0]} command has sudo with password support")
        else:
            print(f"❌ {cmd[0]} command lacks proper sudo support")
            return False
    
    # Test btrfs command (should still work)
    print("\n3. Testing btrfs command (existing functionality)...")
    btrfs_cmd = ["btrfs", "subvolume", "list", "/"]
    built_cmd = endpoint._build_remote_command(btrfs_cmd)
    print(f"BTRFS command: {btrfs_cmd} -> Built: {built_cmd}")
    
    if "sudo" in built_cmd:
        print("✅ BTRFS command still has sudo support")
    else:
        print("❌ BTRFS command lost sudo support")
        return False
    
    print("\n" + "=" * 80)
    print("✅ ALL TESTS PASSED: Directory creation commands now support password authentication")
    print("=" * 80)
    return True

def test_environment_variable_mode():
    """Test the BTRFS_BACKUP_PASSWORDLESS_ONLY environment variable mode."""
    print("\n" + "=" * 80)
    print("Testing BTRFS_BACKUP_PASSWORDLESS_ONLY Environment Variable")
    print("=" * 80)
    
    # Set environment variable for passwordless-only mode
    os.environ["BTRFS_BACKUP_PASSWORDLESS_ONLY"] = "1"
    
    try:
        config = {
            "ssh_sudo": True,
            "username": "mberry", 
            "port": 22,
            "passwordless": False
        }
        
        endpoint = SSHEndpoint(
            hostname="192.168.5.85",
            config=config,
            ssh_sudo=True
        )
        
        # Test mkdir in passwordless mode
        mkdir_cmd = ["mkdir", "-p", "/home/mberry/test_dir"]
        built_cmd = endpoint._build_remote_command(mkdir_cmd)
        print(f"Passwordless mode mkdir: {mkdir_cmd} -> {built_cmd}")
        
        if "sudo" in built_cmd and "-n" in built_cmd and "-S" not in built_cmd:
            print("✅ SUCCESS: mkdir uses passwordless sudo (-n flag) when BTRFS_BACKUP_PASSWORDLESS_ONLY=1")
        else:
            print("❌ FAILURE: mkdir doesn't respect passwordless-only mode")
            return False
            
        print("✅ Environment variable test passed")
        return True
        
    finally:
        # Clean up environment variable
        del os.environ["BTRFS_BACKUP_PASSWORDLESS_ONLY"]

if __name__ == "__main__":
    setup_logging()
    
    success = True
    success &= test_directory_creation_with_sudo()
    success &= test_environment_variable_mode()
    
    if success:
        print("\n🎉 All directory creation fix tests passed!")
        print("The cached sudo password should now work for directory creation operations.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)
