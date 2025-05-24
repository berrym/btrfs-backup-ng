#!/usr/bin/env python3
"""
Test the complete sudo password fix in a non-interactive environment.
This simulates running btrfs-backup-ng from a subprocess or automation.
"""

import os
import sys
import subprocess
import tempfile
import time
from pathlib import Path

def test_with_environment_variable():
    """Test using BTRFS_BACKUP_SUDO_PASSWORD environment variable."""
    print("Testing with BTRFS_BACKUP_SUDO_PASSWORD environment variable...")
    
    # Create a simple test script that uses our SSH endpoint
    test_script = '''
import sys
import os
sys.path.insert(0, "/home/mberry/Lab/python/btrfs-backup-ng/src")

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

try:
    # Create SSH endpoint
    endpoint = SSHEndpoint("mberry@192.168.5.85", port=22)
    
    # Test password retrieval in non-TTY environment
    print("Testing password retrieval...")
    
    # This should use the environment variable
    password = endpoint._get_sudo_password()
    if password:
        print("✓ Password retrieved successfully from environment")
    else:
        print("✗ No password retrieved")
        
    print("Test completed successfully")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
'''
    
    # Write test script to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(test_script)
        script_path = f.name
    
    try:
        # Set environment variable with a dummy password
        env = os.environ.copy()
        env['BTRFS_BACKUP_SUDO_PASSWORD'] = 'dummy_password_for_testing'
        
        # Run in subprocess without TTY
        print("Running test in subprocess without TTY...")
        result = subprocess.run([
            sys.executable, script_path
        ], env=env, capture_output=True, text=True, timeout=10)
        
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        print(f"Return code: {result.returncode}")
        
    finally:
        # Clean up
        os.unlink(script_path)

def test_without_environment_variable():
    """Test behavior when no environment variable is set."""
    print("\nTesting without BTRFS_BACKUP_SUDO_PASSWORD environment variable...")
    
    # Create a simple test script that uses our SSH endpoint
    test_script = '''
import sys
import os
sys.path.insert(0, "/home/mberry/Lab/python/btrfs-backup-ng/src")

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

try:
    # Create SSH endpoint
    endpoint = SSHEndpoint("mberry@192.168.5.85", port=22)
    
    # Test password retrieval in non-TTY environment
    print("Testing password retrieval without environment variable...")
    
    # This should fail gracefully with helpful error message
    try:
        password = endpoint._get_sudo_password()
        print("Unexpected: Password retrieved when none should be available")
    except RuntimeError as e:
        print(f"✓ Got expected error: {e}")
    except Exception as e:
        print(f"✗ Got unexpected error: {e}")
        
    print("Test completed")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
'''
    
    # Write test script to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(test_script)
        script_path = f.name
    
    try:
        # Run in subprocess without TTY and without environment variable
        print("Running test in subprocess without TTY...")
        result = subprocess.run([
            sys.executable, script_path
        ], capture_output=True, text=True, timeout=10)
        
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        print(f"Return code: {result.returncode}")
        
    finally:
        # Clean up
        os.unlink(script_path)

if __name__ == "__main__":
    print("Testing Complete Sudo Password Fix")
    print("=" * 50)
    
    test_with_environment_variable()
    test_without_environment_variable()
    
    print("\nAll tests completed!")
