# Double Password Prompt Fix - Implementation Summary

## Problem Analysis
The user reported that btrfs-backup-ng was prompting for passwords twice during execution:
1. First prompt: SSH password authentication 
2. Second prompt: Sudo password for remote operations

This happened because `_run_diagnostics` was being called multiple times during a single transfer operation, causing redundant sudo testing and password prompts.

## Root Cause
Looking at the logs and code analysis, `_run_diagnostics` was being called:
1. **During initialization** (line 327) - Optional initial diagnostics
2. **During transfer preparation** (line 1872) - "Testing SSH connectivity and filesystem..."
3. **During verification** (line 2146) - Pre-transfer diagnostics  
4. **On errors** (lines 1572, 1603) - After authentication failures

Each call performed sudo tests that could prompt for passwords, leading to multiple authentication requests.

## Solution Implemented

### 1. Diagnostics Caching System
Added intelligent caching to prevent redundant diagnostic testing:

```python
# Cache structure with type hints
self._diagnostics_cache: Dict[str, Tuple[Dict[str, bool], float]] = {}
self._diagnostics_cache_timeout = 300  # 5 minutes

# Cache key format: "hostname:path"
cache_key = f"{self.hostname}:{path}"
```

### 2. Enhanced _run_diagnostics Method
- **Cache checking**: Returns cached results if within timeout (5 minutes)
- **Force refresh parameter**: Allows bypassing cache when needed after errors
- **Intelligent logging**: Clearly indicates when using cache vs running fresh tests

```python
def _run_diagnostics(self, path: str = "/", force_refresh: bool = False) -> Dict[str, bool]:
    # Check cache first unless force_refresh=True
    if not force_refresh and cache_key in self._diagnostics_cache:
        cached_result, cache_time = self._diagnostics_cache[cache_key]
        if current_time - cache_time < self._diagnostics_cache_timeout:
            # Return cached result
    
    # Run fresh diagnostics and cache the results
```

### 3. Strategic Cache Usage
- **Normal operations**: Use cached results to avoid redundant testing
- **Error scenarios**: Use `force_refresh=True` to get fresh diagnostics after authentication failures
- **Different paths**: Each hostname:path combination has its own cache entry

### 4. Improved Logging
Updated log messages to be clearer about what's happening:
- "Verifying SSH connectivity and filesystem readiness..." (instead of "Testing...")
- Debug messages indicate cache hits vs fresh runs

## Implementation Details

### Files Modified
- `/home/mberry/Lab/python/btrfs-backup-ng/src/btrfs_backup_ng/endpoint/ssh.py`

### Key Changes
1. **Added cache infrastructure** in `__init__`:
   ```python
   self._diagnostics_cache: Dict[str, Tuple[Dict[str, bool], float]] = {}
   self._diagnostics_cache_timeout = 300  # 5 minutes
   ```

2. **Enhanced `_run_diagnostics` method**:
   - Added `force_refresh` parameter
   - Implemented cache checking and storage
   - Improved logging for cache behavior

3. **Updated error handling calls**:
   - Lines 1574, 1605: Use `force_refresh=True` after authentication errors
   - Normal pre-transfer checks use cached results

### Authentication Retry Coverage
This builds on the previous work where we ensured all authentication-sensitive operations use `_exec_remote_command_with_retry`:

- `_btrfs_receive`: mkdir commands with sudo
- `_verify_btrfs_availability`: sudo btrfs testing  
- `_run_diagnostics`: passwordless sudo testing, sudo btrfs testing, write permission testing
- `_verify_snapshot_exists`: fallback verification commands with sudo

## Expected Behavior After Fix

### First Transfer
1. **Initial diagnostics**: Run once during connection setup (may prompt for passwords)
2. **Pre-transfer verification**: Use cached results (no additional prompts)
3. **During transfer**: Use cached authentication context
4. **Error scenarios**: Only prompt again if authentication actually fails

### Subsequent Transfers (within 5 minutes)
1. **All diagnostic checks**: Use cached results
2. **No redundant password prompts**: Unless authentication context expires or fails
3. **Faster operation**: Reduced overhead from diagnostic testing

### Cache Behavior
- **Cache duration**: 5 minutes per hostname:path combination
- **Cache invalidation**: Automatic timeout or force refresh on errors
- **Memory efficient**: Only stores boolean test results and timestamps

## Testing Verification

Created and ran test to verify caching behavior:
- ✅ First call executes all diagnostic commands
- ✅ Second call uses cache (0ms execution time)  
- ✅ Force refresh bypasses cache correctly
- ✅ Different paths get separate cache entries

## Expected User Experience

**Before Fix:**
```
18:50:39 - mberry@192.168.5.85's password: [user enters password]
18:50:50 - Sudo password for mberry@192.168.5.85: [user enters password again]
```

**After Fix:**
```
18:50:39 - mberry@192.168.5.85's password: [user enters password once]
18:50:40 - Verifying SSH connectivity and filesystem readiness... [uses cache]
18:50:40 - Transfer proceeding... [no additional prompts]
```

The fix should eliminate redundant password prompts while maintaining all security and verification functionality.
