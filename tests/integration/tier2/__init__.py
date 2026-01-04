"""Tier 2 integration tests requiring real btrfs operations.

These tests use loopback btrfs filesystems to test actual snapshot,
send, and receive operations. They require:

- btrfs-progs installed
- Root/sudo privileges for mount operations
- Linux kernel with btrfs support

Run with: sudo pytest -m tier2
Skip with: pytest -m "not tier2" (default behavior)
"""
