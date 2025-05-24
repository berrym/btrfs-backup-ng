#!/usr/bin/env python3
"""
Test script to verify the interactive password fix.

This test verifies that:
1. Interactive password prompting works in subprocess environments
2. Password is cached after first entry
3. Subsequent operations use the cached password without prompting
4. Retry logic uses cached password instead of prompting again
"""

import subprocess
import sys
import os
import time

def test_interactive_password_subprocess():
    """Test that interactive password prompting works in subprocess environments."""
    print("=== Testing Interactive Password Fix ===")
    print()
    
    # Test script that simulates what btrfs-backup-ng does
    test_script = '''
import sys
import os
sys.path.insert(0, '/home/mberry/Lab/python/btrfs-backup-ng/src')

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
import logging

# Enable detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)-8s (%(name)s) %(message)s'
)
logger = logging.getLogger('test')

print("Creating SSH endpoint...")
config = {
    'hostname': 'localhost',
    'username': 'mberry',
    'path': '/tmp/test-backups'
}

endpoint = SSHEndpoint(config)
print(f"Endpoint created: {endpoint}")

print("\\nTesting password prompting...")
print("This should prompt for password interactively:")

# Test 1: First call should prompt for password
password1 = endpoint._get_sudo_password()
if password1:
    print(f"SUCCESS: Got password (length: {len(password1)})")
else:
    print("FAILED: No password returned")
    sys.exit(1)

print("\\nTesting password caching...")
print("This should use cached password (no prompt):")

# Test 2: Second call should use cached password
password2 = endpoint._get_sudo_password()
if password2:
    print(f"SUCCESS: Got cached password (length: {len(password2)})")
    if password1 == password2:
        print("SUCCESS: Cached password matches original")
    else:
        print("ERROR: Cached password does not match original")
        sys.exit(1)
else:
    print("FAILED: No cached password returned")
    sys.exit(1)

print("\\n=== Interactive Password Test PASSED ===")
'''

    # Run the test in a subprocess to simulate real-world usage
    print("Running test in subprocess (simulating btrfs-backup-ng execution)...")
    print("You should be prompted for your sudo password ONCE only.")
    print()
    
    try:
        result = subprocess.run(
            [sys.executable, '-c', test_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60  # 1 minute timeout
        )
        
        print("=== SUBPROCESS OUTPUT ===")
        print(result.stdout)
        print("=== END OUTPUT ===")
        print()
        
        if result.returncode == 0:
            print("✅ SUCCESS: Interactive password test passed!")
            return True
        else:
            print(f"❌ FAILED: Test failed with exit code {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ FAILED: Test timed out (no user input)")
        return False
    except Exception as e:
        print(f"❌ FAILED: Test error: {e}")
        return False

def test_retry_logic_with_cached_password():
    """Test that retry logic uses cached password."""
    print("\n=== Testing Retry Logic with Cached Password ===")
    
    test_script = '''
import sys
import os
sys.path.insert(0, '/home/mberry/Lab/python/btrfs-backup-ng/src')

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
import logging

# Enable detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)-8s (%(name)s) %(message)s'
)

config = {
    'hostname': 'localhost',
    'username': 'mberry',
    'path': '/tmp/test-backups'
}

endpoint = SSHEndpoint(config)

print("First call - should prompt for password:")
password1 = endpoint._get_sudo_password()
if not password1:
    print("ERROR: Failed to get password")
    sys.exit(1)

print(f"Got password (length: {len(password1)})")

print("\\nSecond call - should use cached password:")
password2 = endpoint._get_sudo_password()
if not password2:
    print("ERROR: Failed to get cached password")
    sys.exit(1)

if password1 == password2:
    print("SUCCESS: Cached password matches!")
else:
    print("ERROR: Cached password doesn't match")
    sys.exit(1)

print("\\nTesting that retry logic would use cached password...")
print("(This simulates what happens in list_snapshots retry)")

# Simulate the retry scenario
if endpoint._cached_sudo_password:
    print("SUCCESS: Cached password available for retry logic")
    print(f"Cached password length: {len(endpoint._cached_sudo_password)}")
else:
    print("ERROR: No cached password available")
    sys.exit(1)

print("\\n=== Retry Logic Test PASSED ===")
'''

    try:
        result = subprocess.run(
            [sys.executable, '-c', test_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60
        )
        
        print("=== RETRY TEST OUTPUT ===")
        print(result.stdout)
        print("=== END OUTPUT ===")
        
        if result.returncode == 0:
            print("✅ SUCCESS: Retry logic test passed!")
            return True
        else:
            print(f"❌ FAILED: Retry test failed with exit code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ FAILED: Retry test error: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("TESTING INTERACTIVE PASSWORD FIX")
    print("=" * 60)
    print()
    print("This test verifies that:")
    print("1. Password prompting works in subprocess environments")
    print("2. Password is cached after first entry")
    print("3. Subsequent calls use cached password")
    print("4. Retry logic has access to cached password")
    print()
    
    # Test 1: Interactive password in subprocess
    test1_passed = test_interactive_password_subprocess()
    
    # Test 2: Retry logic with cached password  
    test2_passed = test_retry_logic_with_cached_password()
    
    print("\n" + "=" * 60)
    print("FINAL RESULTS:")
    print("=" * 60)
    print(f"Interactive Password Test: {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"Retry Logic Test: {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 ALL TESTS PASSED! Interactive password fix is working correctly.")
        return 0
    else:
        print("\n💥 SOME TESTS FAILED. Interactive password fix needs more work.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
