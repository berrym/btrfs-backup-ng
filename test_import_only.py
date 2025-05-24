#!/usr/bin/env python3
"""
Very simple test to verify the basic functionality works
"""

import sys
import os

# Add the source directory to Python path
sys.path.insert(0, '/home/mberry/Lab/python/btrfs-backup-ng/src')

print("Starting simple import test...")

try:
    print("Importing SSHEndpoint...")
    from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
    print("✓ Import successful")
    
    print("Creating endpoint...")
    endpoint = SSHEndpoint("test@localhost", port=22)
    print("✓ Endpoint created")
    
    print("Testing environment variable support...")
    os.environ['BTRFS_BACKUP_SUDO_PASSWORD'] = 'test123'
    
    # This should work without hanging since environment variable is set
    password = endpoint._get_sudo_password()
    
    if password == 'test123':
        print("✓ Environment variable password retrieval works")
    else:
        print(f"✗ Expected 'test123', got: {password}")
    
    del os.environ['BTRFS_BACKUP_SUDO_PASSWORD']
    print("✓ Test completed successfully")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
