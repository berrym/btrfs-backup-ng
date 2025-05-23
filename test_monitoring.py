#!/usr/bin/env python3
"""
Test script for the enhanced monitoring system in btrfs-backup-ng.
This script creates a simple SSH endpoint and tests the monitoring functionality.
"""

import os
import sys
import logging

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
from btrfs_backup_ng.__logger__ import logger

def test_monitoring():
    """Test the monitoring system functionality."""
    print("🧪 Testing btrfs-backup-ng enhanced monitoring system...")
    
    # Test basic SSH endpoint creation
    try:
        print("📡 Creating SSH endpoint...")
        
        # Create a minimal configuration for testing
        config = {
            'path': '/tmp/btrfs-test',
            'ssh_sudo': False,
            'passwordless': True
        }
        
        # Create SSH endpoint (using localhost for testing)
        endpoint = SSHEndpoint(hostname='localhost', config=config)
        print(f"✅ SSH endpoint created: {endpoint}")
        
        # Test the monitoring methods exist
        print("🔍 Checking monitoring methods...")
        
        if hasattr(endpoint, '_monitor_transfer_progress'):
            print("✅ _monitor_transfer_progress method found")
        else:
            print("❌ _monitor_transfer_progress method missing")
            
        if hasattr(endpoint, '_log_transfer_status'):
            print("✅ _log_transfer_status method found")
        else:
            print("❌ _log_transfer_status method missing")
            
        if hasattr(endpoint, '_log_process_error'):
            print("✅ _log_process_error method found")
        else:
            print("❌ _log_process_error method missing")
            
        if hasattr(endpoint, '_find_buffer_program'):
            print("✅ _find_buffer_program method found")
        else:
            print("❌ _find_buffer_program method missing")
            
        # Test buffer program detection
        print("🔧 Testing buffer program detection...")
        buffer_name, buffer_cmd = endpoint._find_buffer_program()
        if buffer_name:
            print(f"✅ Found buffer program: {buffer_name} -> {buffer_cmd}")
        else:
            print("ℹ️  No buffer program found (pv/mbuffer not available)")
            
        print("🎉 Basic monitoring system test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error during monitoring test: {e}")
        logger.error(f"Monitoring test failed: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    # Set up logging for testing
    logging.basicConfig(level=logging.INFO)
    
    success = test_monitoring()
    sys.exit(0 if success else 1)
