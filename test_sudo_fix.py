#!/usr/bin/env python3
"""Test script to verify the sudo password fix."""

import os
import sys
import logging
from pathlib import Path

# Add the src directory to the path so we can import the module
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
from btrfs_backup_ng.__logger__ import logger

# Set up logging to see the debug messages
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s')

def test_sudo_password_logic():
    """Test the sudo password logic without actually executing SSH commands."""
    print("=== Testing sudo password logic ===")
    
    # Create a test SSH endpoint
    config = {
        'hostname': 'test-host',
        'username': 'test-user',
        'path': '/test/path',
        'ssh_sudo': True,
        'passwordless': False
    }
    
    endpoint = SSHEndpoint(
        hostname='test-host',
        config=config,
        ssh_sudo=True,
        passwordless=False
    )
    
    print(f"Created endpoint with cached password: {endpoint._cached_sudo_password}")
    
    # Test 1: Check if password caching works
    print("\n--- Test 1: Environment variable password ---")
    os.environ['BTRFS_BACKUP_SUDO_PASSWORD'] = 'test-password-from-env'
    password1 = endpoint._get_sudo_password()
    print(f"Password from env: '{password1}'")
    print(f"Cached password after env: '{endpoint._cached_sudo_password}'")
    
    # Test 2: Check if cached password is reused
    print("\n--- Test 2: Cached password reuse ---")
    password2 = endpoint._get_sudo_password()
    print(f"Password from cache: '{password2}'")
    print(f"Should be same as previous: {password1 == password2}")
    
    # Clean up env var
    del os.environ['BTRFS_BACKUP_SUDO_PASSWORD']
    
    # Test 3: Test TTY detection
    print("\n--- Test 3: TTY detection ---")
    endpoint._cached_sudo_password = None  # Reset cache
    print(f"sys.stdin.isatty(): {sys.stdin.isatty()}")
    
    # Test 4: Test the TTY logic fix - simulate a command with sudo -S
    print("\n--- Test 4: TTY allocation logic ---")
    
    # Simulate the command processing logic
    remote_cmd = ['sudo', '-S', 'btrfs', 'receive', '/test/path']
    using_sudo_with_stdin = any(arg == "-S" for arg in remote_cmd)
    print(f"Command: {remote_cmd}")
    print(f"Uses sudo -S: {using_sudo_with_stdin}")
    
    # Simulate the TTY logic
    needs_tty = False
    cmd_str = " ".join(map(str, remote_cmd))
    ssh_sudo = config.get("ssh_sudo", False)
    passwordless = config.get("passwordless", False)
    
    if ssh_sudo and not passwordless:
        if "sudo" in cmd_str and "-n" not in cmd_str and not using_sudo_with_stdin:
            needs_tty = True
    
    print(f"ssh_sudo: {ssh_sudo}")
    print(f"passwordless: {passwordless}")
    print(f"Command string: '{cmd_str}'")
    print(f"needs_tty (should be False for sudo -S): {needs_tty}")
    
    # Test 5: Test with regular sudo (not -S)
    print("\n--- Test 5: Regular sudo (should need TTY) ---")
    regular_sudo_cmd = ['sudo', 'btrfs', 'receive', '/test/path']
    using_sudo_with_stdin_regular = any(arg == "-S" for arg in regular_sudo_cmd)
    cmd_str_regular = " ".join(map(str, regular_sudo_cmd))
    needs_tty_regular = False
    
    if ssh_sudo and not passwordless:
        if "sudo" in cmd_str_regular and "-n" not in cmd_str_regular and not using_sudo_with_stdin_regular:
            needs_tty_regular = True
    
    print(f"Regular sudo command: {regular_sudo_cmd}")
    print(f"Uses sudo -S: {using_sudo_with_stdin_regular}")
    print(f"needs_tty (should be True for regular sudo): {needs_tty_regular}")

if __name__ == "__main__":
    test_sudo_password_logic()
