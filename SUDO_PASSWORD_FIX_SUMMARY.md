# Sudo Password Fix Summary

## Problem Description

The `btrfs-backup-ng` tool was experiencing multiple sudo password prompts when using SSH, with the following issues:

1. **Multiple Password Prompts**: The sudo password was requested for every `sudo -S` command, even within the same session
2. **Password Echoing**: Password input was being echoed to the terminal instead of being hidden
3. **TTY Conflicts**: SSH master mode was active, but TTY allocation conflicts between SSH and `getpass.getpass()` were causing malfunction

## Root Cause Analysis

The issue was caused by a complex interaction between:

1. **TTY Allocation**: SSH commands with `force_tty=True` were adding `-tt` flag 
2. **BatchMode**: SSH was using `BatchMode=yes` which disables interactive authentication
3. **Sudo Password Input**: `sudo -S` commands were using stdin for password input
4. **getpass Conflicts**: `getpass.getpass()` was trying to read from TTY while SSH was also using TTY

## Solution Implemented

### 1. Sudo Password Caching

**File**: `src/btrfs_backup_ng/endpoint/ssh.py`

- Added `self._cached_sudo_password: Optional[str] = None` to `SSHEndpoint.__init__`
- Modified `_get_sudo_password()` to cache passwords from:
  - Environment variable `BTRFS_BACKUP_SUDO_PASSWORD`
  - Interactive `getpass.getpass()` prompts
- Ensured cached password is reused for subsequent `sudo -S` commands

### 2. TTY Allocation Logic Fix

**File**: `src/btrfs_backup_ng/endpoint/ssh.py` in `_exec_remote_command()`

**Before**:
```python
if self.config.get("ssh_sudo", False) and not self.config.get("passwordless", False):
    if "sudo" in cmd_str and "-n" not in cmd_str:
        needs_tty = True
```

**After**:
```python
if self.config.get("ssh_sudo", False) and not self.config.get("passwordless", False):
    # THE KEY FIX: Don't allocate TTY when using sudo -S
    if "sudo" in cmd_str and "-n" not in cmd_str and not using_sudo_with_stdin:
        needs_tty = True
```

This prevents TTY allocation when `sudo -S` is used, eliminating conflicts with stdin password input.

## Testing Results

The fix was verified with the following test cases:

| Command Type | Uses sudo -S | Needs TTY | Expected | Result |
|--------------|--------------|-----------|----------|---------|
| `sudo -S btrfs receive` | True | False | False | ✓ PASS |
| `sudo btrfs receive` | False | True | True | ✓ PASS |
| `sudo -n btrfs receive` | False | False | False | ✓ PASS |

## Expected Behavior After Fix

1. **Single Password Prompt**: Only one sudo password prompt per `btrfs-backup-ng` session
2. **Hidden Password Input**: Password input is properly hidden from terminal output
3. **No TTY Conflicts**: SSH and password prompts coordinate properly
4. **SSH Master Compatibility**: Works correctly with SSH master mode connection reuse

## Usage Recommendations

### Environment Variable (Recommended for automation)
```bash
export BTRFS_BACKUP_SUDO_PASSWORD="your-sudo-password"
btrfs-backup-ng send ssh://user@host:/path/to/dest
```

### Interactive Mode
```bash
# Will prompt once for sudo password and cache it for the session
btrfs-backup-ng send ssh://user@host:/path/to/dest
```

### Passwordless Sudo (Best for production)
```bash
# Configure passwordless sudo for btrfs commands
echo "user ALL=(ALL) NOPASSWD: /bin/btrfs" | sudo tee /etc/sudoers.d/btrfs-backup
```

## Files Modified

1. **`src/btrfs_backup_ng/endpoint/ssh.py`**:
   - Added sudo password caching mechanism
   - Fixed TTY allocation logic for `sudo -S` commands
   - Enhanced logging for debugging

## Verification Commands

To verify the fix is working correctly:

```bash
# Test TTY allocation logic
cd /path/to/btrfs-backup-ng
python3 test_tty_logic.py

# Run comprehensive demonstration
python3 test_sudo_password_fix_demo.py
```

The fix ensures that `sudo -S` commands (which supply password via stdin) do not request TTY allocation, preventing the conflict that was causing password echoing and multiple prompts.
