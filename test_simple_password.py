#!/usr/bin/env python3
"""
Simple test to verify password handling improvement.
"""

import os
import sys

# Add the source directory to Python path
sys.path.insert(0, '/home/mberry/Lab/python/btrfs-backup-ng/src')

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

def test_password_handling():
    """Test the improved password handling logic."""
    print("Testing improved sudo password handling...")
    print(f"TTY available: {sys.stdin.isatty()}")
    
    # Create SSH endpoint
    config = {
        'hostname': '192.168.5.85',
        'username': 'mberry', 
        'path': '/home/mberry/snapshots/test',
        'ssh_sudo': True,
        'passwordless': False
    }
    
    endpoint = SSHEndpoint(hostname='192.168.5.85', config=config)
    
    # Test without environment variable
    print("\n--- Test 1: No BTRFS_BACKUP_SUDO_PASSWORD ---")
    password = endpoint._get_sudo_password()
    print(f"Result: {password}")
    
    # Test with environment variable
    print("\n--- Test 2: With BTRFS_BACKUP_SUDO_PASSWORD ---")
    os.environ['BTRFS_BACKUP_SUDO_PASSWORD'] = 'test_password'
    password = endpoint._get_sudo_password()
    print(f"Result: {'[PRESENT]' if password else 'None'}")
    
    # Clean up
    del os.environ['BTRFS_BACKUP_SUDO_PASSWORD']
    
    print("\nTest completed successfully!")

if __name__ == '__main__':
    test_password_handling()
