#!/usr/bin/env python3
"""
Debug script to understand the sudo retry issue in btrfs-backup-ng.

This script simulates the exact scenario where multiple sudo password prompts occur
and helps identify where the retry logic is causing issues.
"""

import os
import sys
sys.path.insert(0, '/home/mberry/Lab/python/btrfs-backup-ng/src')

def simulate_list_snapshots_retry():
    """Simulate the list_snapshots retry logic that's causing multiple password prompts."""
    
    print("=== DEBUGGING SUDO RETRY ISSUE ===")
    print()
    print("The issue occurs in the list_snapshots method when:")
    print("1. First command: sudo -n (passwordless) fails")  
    print("2. Retry logic creates: sudo -S (password required)")
    print("3. Each retry asks for password again")
    print()
    
    # Show the problematic retry logic
    print("=== PROBLEMATIC CODE IN list_snapshots() ===")
    print()
    code_snippet = '''
# From list_snapshots method around line 1274:
if use_sudo and (
    "a password is required" in stderr or "sudo:" in stderr
):
    logger.warning(f"Passwordless sudo failed: {stderr}")
    # Try password-based sudo, but do NOT let _build_remote_command add another sudo
    cmd_pw = ["sudo", "-S", "btrfs", "subvolume", "list", "-o", path]
    logger.info("Retrying remote snapshot listing with password-based sudo...")
    orig_ssh_sudo = self.config.get("ssh_sudo", False)
    self.config["ssh_sudo"] = False  # <-- Temporarily disable ssh_sudo
    try:
        result_pw = self._exec_remote_command(
            cmd_pw,  # <-- Manual sudo -S command
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    finally:
        self.config["ssh_sudo"] = orig_ssh_sudo
'''
    print(code_snippet)
    print()
    
    print("=== THE PROBLEM ===")
    print()
    print("The retry logic creates its own 'sudo -S' commands that:")
    print("1. Go through _exec_remote_command normally")
    print("2. Trigger our TTY allocation logic")
    print("3. Call _get_sudo_password() each time")
    print("4. If password caching fails, user gets prompted multiple times")
    print()
    
    print("=== POTENTIAL CAUSES ===")
    print()
    print("1. Password caching not working properly")
    print("2. Multiple different operations triggering retry logic")
    print("3. Each operation creating separate SSHEndpoint instances")
    print("4. Password cache not being shared between operations")
    print()
    
    print("=== SOLUTION APPROACHES ===")
    print()
    print("Option 1: Fix the retry logic to use cached passwords")
    print("Option 2: Prevent retry if password is already cached")  
    print("Option 3: Use environment variable for persistent password")
    print("Option 4: Better coordination between retry logic and password caching")
    print()

def test_password_caching_behavior():
    """Test if password caching is working properly."""
    
    print("=== TESTING PASSWORD CACHING ===")
    print()
    
    # Test environment variable approach
    test_password = "test-password-123"
    print(f"Setting test password in environment: BTRFS_BACKUP_SUDO_PASSWORD")
    os.environ['BTRFS_BACKUP_SUDO_PASSWORD'] = test_password
    
    try:
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
        
        # Create endpoint with ssh_sudo enabled
        config = {
            'ssh_sudo': True,
            'passwordless': False,
            'username': 'testuser'
        }
        
        endpoint = SSHEndpoint('localhost', config)
        
        print(f"Created SSHEndpoint with config: {config}")
        print()
        
        # Test password retrieval multiple times
        print("Testing password retrieval (should use cached value):")
        for i in range(3):
            password = endpoint._get_sudo_password()
            if password:
                print(f"  Attempt {i+1}: Retrieved password of length {len(password)}")
            else:
                print(f"  Attempt {i+1}: No password retrieved")
        
        print()
        print("If caching works properly, all attempts should return the same password")
        print("without prompting the user.")
        
    except Exception as e:
        print(f"Error testing password caching: {e}")
    finally:
        # Clean up
        if 'BTRFS_BACKUP_SUDO_PASSWORD' in os.environ:
            del os.environ['BTRFS_BACKUP_SUDO_PASSWORD']

def suggest_fixes():
    """Suggest specific fixes for the sudo retry issue."""
    
    print("=== SUGGESTED FIXES ===")
    print()
    
    print("Fix 1: Improve the retry logic to check for cached password first")
    print("---")
    fix1_code = '''
# In list_snapshots method, before creating cmd_pw:
if use_sudo and ("a password is required" in stderr or "sudo:" in stderr):
    # Check if we have a cached password before retrying
    cached_password = self._get_sudo_password()
    if not cached_password:
        logger.error("Passwordless sudo failed and no password available for retry")
        # Handle this case - maybe skip retry or fail gracefully
        continue
    
    logger.warning(f"Passwordless sudo failed: {stderr}")
    # Now proceed with retry knowing we have a password...
'''
    print(fix1_code)
    print()
    
    print("Fix 2: Use a global password cache across all SSHEndpoint instances")
    print("---")
    print("Store the password in a class variable or module-level cache")
    print("so multiple endpoint instances can share the same password.")
    print()
    
    print("Fix 3: Better logging to understand when/why retries happen")
    print("---")
    print("Add detailed logging to track:")
    print("- When retry logic is triggered")
    print("- Whether password cache is hit or miss")
    print("- How many retry attempts are made")
    print()

if __name__ == "__main__":
    simulate_list_snapshots_retry()
    print()
    test_password_caching_behavior()
    print()
    suggest_fixes()
