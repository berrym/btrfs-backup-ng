#!/usr/bin/env python3
"""
Final comprehensive test to verify directory creation authentication fix.
This test validates that our fix for directory creation (mkdir) commands
now correctly inherits sudo password authentication.
"""

import sys
import os

# Add the source directory to Python path
sys.path.insert(0, '/home/mberry/Lab/python/btrfs-backup-ng/src')

def test_directory_creation_fix_comprehensive():
    """Comprehensive test of the directory creation authentication fix."""
    print("=== COMPREHENSIVE DIRECTORY CREATION FIX TEST ===")
    print()
    
    try:
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
        
        # Test with our fixed implementation
        print("1. Testing directory creation commands get password support:")
        print()
        
        # Create a test endpoint
        config = {
            'ssh_sudo': True,
            'passwordless': False,
            'username': 'testuser'
        }
        
        endpoint = SSHEndpoint('test.host', config)
        
        # Test the fixed _build_remote_command method
        test_cases = [
            {
                'cmd': ['mkdir', '-p', '/tmp/test'],
                'expected_sudo_type': 'sudo -S',
                'description': 'mkdir should get password support'
            },
            {
                'cmd': ['touch', '/tmp/testfile'],
                'expected_sudo_type': 'sudo -S',
                'description': 'touch should get password support'
            },
            {
                'cmd': ['rm', '-f', '/tmp/testfile'],
                'expected_sudo_type': 'sudo -S',
                'description': 'rm should get password support'
            },
            {
                'cmd': ['test', '-d', '/tmp/test'],
                'expected_sudo_type': 'sudo -S',
                'description': 'test should get password support'
            },
            {
                'cmd': ['btrfs', 'receive', '/tmp'],
                'expected_sudo_type': 'sudo -S -E',
                'description': 'btrfs should get password support with -E'
            }
        ]
        
        all_passed = True
        
        for test_case in test_cases:
            try:
                # Use the fixed _build_remote_command method
                result_cmd = endpoint._build_remote_command(test_case['cmd'])
                
                # Check if the command has the expected sudo flags
                result_str = ' '.join(result_cmd)
                expected_in_result = test_case['expected_sudo_type'] in result_str
                
                status = "✓ PASS" if expected_in_result else "✗ FAIL"
                print(f"{status} {test_case['description']}")
                print(f"    Command: {' '.join(test_case['cmd'])}")
                print(f"    Result:  {' '.join(result_cmd)}")
                print(f"    Expected sudo type: {test_case['expected_sudo_type']}")
                print(f"    Found in result: {expected_in_result}")
                print()
                
                if not expected_in_result:
                    all_passed = False
                    
            except Exception as e:
                print(f"✗ FAIL {test_case['description']} - Exception: {e}")
                print()
                all_passed = False
        
        # Test with passwordless mode
        print("2. Testing passwordless mode still works:")
        print()
        
        # Set environment variable for passwordless mode
        os.environ['BTRFS_BACKUP_PASSWORDLESS_ONLY'] = '1'
        
        passwordless_test_cases = [
            {
                'cmd': ['mkdir', '-p', '/tmp/test'],
                'expected_sudo_type': 'sudo -n',
                'description': 'mkdir should use passwordless sudo when env var set'
            },
            {
                'cmd': ['btrfs', 'receive', '/tmp'],
                'expected_sudo_type': 'sudo -n -E',
                'description': 'btrfs should use passwordless sudo when env var set'
            }
        ]
        
        for test_case in passwordless_test_cases:
            try:
                # Create new endpoint to pick up environment variable
                endpoint_passwordless = SSHEndpoint('test.host', config)
                result_cmd = endpoint_passwordless._build_remote_command(test_case['cmd'])
                
                result_str = ' '.join(result_cmd)
                expected_in_result = test_case['expected_sudo_type'] in result_str
                
                status = "✓ PASS" if expected_in_result else "✗ FAIL"
                print(f"{status} {test_case['description']}")
                print(f"    Command: {' '.join(test_case['cmd'])}")
                print(f"    Result:  {' '.join(result_cmd)}")
                print(f"    Expected sudo type: {test_case['expected_sudo_type']}")
                print(f"    Found in result: {expected_in_result}")
                print()
                
                if not expected_in_result:
                    all_passed = False
                    
            except Exception as e:
                print(f"✗ FAIL {test_case['description']} - Exception: {e}")
                print()
                all_passed = False
        
        # Clean up environment variable
        if 'BTRFS_BACKUP_PASSWORDLESS_ONLY' in os.environ:
            del os.environ['BTRFS_BACKUP_PASSWORDLESS_ONLY']
        
        return all_passed
        
    except Exception as e:
        print(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run the comprehensive directory creation fix test."""
    print("Testing Directory Creation Authentication Fix")
    print("=" * 50)
    print()
    
    success = test_directory_creation_fix_comprehensive()
    
    print("=" * 50)
    if success:
        print("✓ ALL TESTS PASSED")
        print("Directory creation authentication fix is working correctly!")
        print()
        print("SUMMARY:")
        print("- mkdir commands now get password-capable sudo (-S)")
        print("- touch, rm, test commands now get password support")
        print("- btrfs commands still get password support with -E flag")
        print("- Passwordless mode still works when environment variable is set")
        print("- Commands will inherit cached sudo session from password authentication")
    else:
        print("✗ SOME TESTS FAILED")
        print("Directory creation authentication fix needs additional work.")
    print()
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
