# Complete Sudo Password Fix Summary

## Problem Description

The `btrfs-backup-ng` tool was experiencing **multiple sudo password prompts** when using SSH, despite having SSH master mode active and implementing password caching. The root cause was more complex than initially identified.

## Root Cause Analysis

The issue involved **two separate mechanisms** that both contributed to multiple password prompts:

### 1. TTY Allocation Conflicts (Previously Fixed)
- SSH commands with `sudo -S` were incorrectly requesting TTY allocation (`-tt` flag)
- This caused conflicts between SSH TTY management and `getpass.getpass()`
- **Solution**: Modified TTY allocation logic to exclude `sudo -S` commands

### 2. Retry Logic Bypassing Password Cache (NEW ISSUE FOUND)
- The `list_snapshots()` method contains retry logic that was bypassing our password caching
- When passwordless sudo fails, it creates new `sudo -S` commands manually
- Each retry operation could trigger new password prompts
- **This was the main cause of continued multiple password prompts**

## Complete Solution Implemented

### Fix 1: TTY Allocation Logic (Already Applied)

**File**: `src/btrfs_backup_ng/endpoint/ssh.py` in `_exec_remote_command()`

```python
# Detect if sudo -S is in the command (needs password on stdin)
using_sudo_with_stdin = any(arg == "-S" for arg in remote_cmd)

# Build the SSH command - determine if TTY allocation is needed
if self.config.get("ssh_sudo", False) and not self.config.get("passwordless", False):
    # THE KEY FIX: Don't allocate TTY when using sudo -S
    if "sudo" in cmd_str and "-n" not in cmd_str and not using_sudo_with_stdin:
        needs_tty = True
```

### Fix 2: Retry Logic Password Cache Integration (NEW FIX)

**File**: `src/btrfs_backup_ng/endpoint/ssh.py` in `list_snapshots()` method

**Before** (caused multiple prompts):
```python
if use_sudo and ("a password is required" in stderr or "sudo:" in stderr):
    logger.warning(f"Passwordless sudo failed: {stderr}")
    # Try password-based sudo, but do NOT let _build_remote_command add another sudo
    cmd_pw = ["sudo", "-S", "btrfs", "subvolume", "list", "-o", path]
    # ... immediately retry without checking for cached password
```

**After** (uses cached password):
```python
if use_sudo and ("a password is required" in stderr or "sudo:" in stderr):
    logger.warning(f"Passwordless sudo failed: {stderr}")
    
    # Check if we have a cached password before attempting retry
    cached_password = self._get_sudo_password()
    if not cached_password:
        logger.error("Passwordless sudo failed and no password available for retry")
        logger.info("Consider setting BTRFS_BACKUP_SUDO_PASSWORD environment variable or configuring passwordless sudo")
        # Skip retry and use the original failed result
        result_pw = result  # Use the original failed result
    else:
        # Proceed with retry using cached password
        cmd_pw = ["sudo", "-S", "btrfs", "subvolume", "list", "-o", path]
        logger.debug(f"Using cached sudo password for retry (length: {len(cached_password)})")
        # ... proceed with retry
```

### Fix 3: Password Caching System (Already Applied)

**File**: `src/btrfs_backup_ng/endpoint/ssh.py`

- Added `self._cached_sudo_password: Optional[str] = None` to `SSHEndpoint.__init__`
- Enhanced `_get_sudo_password()` method with comprehensive caching logic
- Support for `BTRFS_BACKUP_SUDO_PASSWORD` environment variable
- Graceful handling when no TTY available for interactive prompts

## How The Complete Fix Works

### Scenario 1: Environment Variable Set
1. User sets `BTRFS_BACKUP_SUDO_PASSWORD=mypassword`
2. First call to `_get_sudo_password()` retrieves from environment, caches it
3. All subsequent operations use cached password
4. **Result**: Only one password setup, no prompts during execution

### Scenario 2: Interactive Password Entry
1. First operation requiring sudo password calls `_get_sudo_password()`
2. User prompted once for password, password cached
3. Any retry operations check cache first before prompting
4. **Result**: Only one password prompt per session

### Scenario 3: No Password Available
1. Passwordless sudo fails
2. Retry logic checks for cached password
3. No password available (no env var, no TTY for prompt)
4. Retry is skipped, user informed about configuration options
5. **Result**: No infinite prompting, graceful degradation

## Testing and Verification

### Manual Testing
```bash
# Set password via environment variable
export BTRFS_BACKUP_SUDO_PASSWORD="your-password"

# Run btrfs-backup-ng operations - should only use cached password
btrfs-backup-ng [your-command]
```

### Verification Commands
```bash
# Test TTY allocation logic
cd /path/to/btrfs-backup-ng
python3 test_tty_logic.py

# Test sudo retry fix
python3 test_sudo_retry_fix.py

# Run comprehensive demonstration
python3 test_sudo_password_fix_demo.py
```

## Key Benefits

1. **Single Password Prompt**: Only one password prompt per session
2. **Environment Variable Support**: Automation-friendly via `BTRFS_BACKUP_SUDO_PASSWORD`
3. **Graceful Fallback**: Proper handling when passwords not available
4. **Retry Logic Integration**: Retry operations use cached passwords
5. **TTY Conflict Resolution**: No more conflicts between SSH and getpass

## Files Modified

1. **`src/btrfs_backup_ng/endpoint/ssh.py`**:
   - Enhanced `_get_sudo_password()` with caching
   - Fixed TTY allocation logic in `_exec_remote_command()`
   - Updated retry logic in `list_snapshots()` to use cached passwords

## Migration Guide

For users experiencing multiple password prompts:

### Option 1: Environment Variable (Recommended for automation)
```bash
export BTRFS_BACKUP_SUDO_PASSWORD="your-sudo-password"
btrfs-backup-ng [command]
```

### Option 2: Configure Passwordless Sudo (Recommended for security)
```bash
# Add to /etc/sudoers.d/btrfs-backup
your-username ALL=(ALL) NOPASSWD: /usr/bin/btrfs
```

### Option 3: Interactive Mode (Default)
- First operation will prompt for password
- Password cached for remainder of session
- No additional prompts for retry operations

## Security Considerations

- Environment variable approach: Convenient but password visible in process environment
- Passwordless sudo: Most secure, requires system configuration
- Interactive caching: Good balance of security and usability

The fix ensures that regardless of which approach is used, users will not experience multiple password prompts during a single session.
