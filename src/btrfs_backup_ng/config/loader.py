"""TOML configuration loading and validation.

Handles config file discovery, parsing, and validation with helpful error messages.
"""

import os
import pwd
import tomllib
from pathlib import Path
from typing import Any

from .schema import (
    Config,
    EmailNotificationConfig,
    GlobalConfig,
    NotificationConfig,
    RetentionConfig,
    SnapperSourceConfig,
    TargetConfig,
    VolumeConfig,
    WebhookNotificationConfig,
)


class ConfigError(Exception):
    """Configuration loading or validation error."""

    pass


def get_user_home() -> Path:
    """Get the appropriate user home directory.

    When running under sudo, returns the original user's home directory
    instead of root's. This ensures config files are saved to the
    correct XDG location.

    Returns:
        Path to user's home directory
    """
    sudo_user = os.environ.get("SUDO_USER")
    if sudo_user and os.geteuid() == 0:
        # Running as root via sudo - use original user's home
        try:
            return Path(pwd.getpwnam(sudo_user).pw_dir)
        except KeyError:
            pass  # User not found, fall back to default
    return Path.home()


def get_user_config_dir() -> Path:
    """Get the appropriate user config directory for btrfs-backup-ng.

    Follows XDG Base Directory Specification, using $XDG_CONFIG_HOME
    or ~/.config as the base. When running under sudo, uses the
    original user's config directory.

    Returns:
        Path to btrfs-backup-ng config directory
    """
    # Check XDG_CONFIG_HOME first (but not if running as root via sudo)
    sudo_user = os.environ.get("SUDO_USER")
    if not (sudo_user and os.geteuid() == 0):
        xdg_config = os.environ.get("XDG_CONFIG_HOME")
        if xdg_config:
            return Path(xdg_config) / "btrfs-backup-ng"

    # Fall back to ~/.config
    return get_user_home() / ".config" / "btrfs-backup-ng"


def get_default_config_path() -> Path:
    """Get the default path for saving configuration files.

    Returns:
        Path to default config.toml location
    """
    return get_user_config_dir() / "config.toml"


def _get_config_search_paths() -> list[Path]:
    """Get config file search paths in priority order.

    When running under sudo, prioritizes the original user's config
    before falling back to root's config and system-wide config.
    """
    paths = []

    # Check if running under sudo
    sudo_user = os.environ.get("SUDO_USER")
    if sudo_user and os.geteuid() == 0:
        # Running as root via sudo - check original user's config first
        try:
            sudo_user_home = pwd.getpwnam(sudo_user).pw_dir
            paths.append(
                Path(sudo_user_home) / ".config" / "btrfs-backup-ng" / "config.toml"
            )
        except KeyError:
            pass  # User not found, skip

    # Current user's config (root's if running as root)
    paths.append(Path.home() / ".config" / "btrfs-backup-ng" / "config.toml")

    # System-wide config
    paths.append(Path("/etc/btrfs-backup-ng/config.toml"))

    return paths


def find_config_file(explicit_path: str | None = None) -> Path | None:
    """Find configuration file.

    Args:
        explicit_path: Explicitly specified config path (highest priority)

    Returns:
        Path to config file, or None if not found

    When running under sudo, checks the original user's config directory
    before falling back to root's config and system-wide config.
    """
    if explicit_path:
        path = Path(explicit_path)
        if path.exists():
            return path
        raise ConfigError(f"Config file not found: {explicit_path}")

    for path in _get_config_search_paths():
        if path.exists():
            return path

    return None


def _parse_retention(data: dict[str, Any]) -> RetentionConfig:
    """Parse retention configuration from dict."""
    min_value = data.get("min", "1d")
    # Validate the min duration at the config boundary so an invalid value fails LOUD here
    # (ConfigError) instead of silently reaching the destructive prune path -- matching the
    # fail-closed validation already done for compress/encrypt/source. Function-local import
    # avoids a config<->retention import cycle.
    from ..retention import parse_duration

    try:
        parse_duration(str(min_value))
    except ValueError as e:
        raise ConfigError(
            f"Invalid retention 'min' duration: {min_value!r} ({e})"
        ) from e
    return RetentionConfig(
        min=min_value,
        hourly=data.get("hourly", 24),
        daily=data.get("daily", 7),
        weekly=data.get("weekly", 4),
        monthly=data.get("monthly", 12),
        yearly=data.get("yearly", 0),
    )


VALID_HOST_KEY_POLICIES = ("accept-new", "strict")


def _validate_host_key_policy(value: Any, path: str) -> str:
    """Validate the ssh_host_key_policy value, failing closed on anything unrecognized."""
    if value not in VALID_HOST_KEY_POLICIES:
        raise ConfigError(
            f"Invalid ssh_host_key_policy {value!r} for target {path}. "
            f"Valid: {list(VALID_HOST_KEY_POLICIES)}"
        )
    return value


def _parse_target(data: dict[str, Any]) -> TargetConfig:
    """Parse target configuration from dict."""
    if "path" not in data:
        raise ConfigError("Target missing required 'path' field")

    # Normalise here, at the producer, so every consumer sees one string.
    # endpoint.choose_endpoint does not strip: a stray space in a quoted TOML
    # path made ' ssh://user@host:/mnt/usb' build a LOCAL endpoint writing to
    # '<cwd>/ ssh:/user@host:/mnt/usb', and '/mnt/usb ' resolve to a directory
    # on the root filesystem rather than the mount point. Any consumer that
    # stripped on its own would then disagree with the endpoint actually built,
    # which is how a safety check ends up guarding a path nothing writes to.
    path = str(data["path"]).strip() if isinstance(data["path"], str) else data["path"]
    is_raw = str(path).startswith(("raw://", "raw+ssh://"))

    # Validate compression against what the target's transport ACTUALLY supports.
    # Raw endpoints and the btrfs/ssh transfer pipeline support different sets
    # (raw adds xz/lzo/bzip2/pbzip2; the transfer path uses lzop). Source the sets
    # from the authoritative maps so this can never drift again -- a hardcoded list
    # previously rejected xz/lzo/bzip2/pbzip2, which raw targets run and btrbk
    # migration emits, so a migrated config failed to load.
    from ..core.transfer import COMPRESSION_PROGRAMS
    from ..endpoint.raw_metadata import COMPRESSION_CONFIG

    compress = data.get("compress", "none")
    if is_raw:
        valid_compress = {"none", *COMPRESSION_CONFIG.keys()}
    else:
        valid_compress = {"none", *COMPRESSION_PROGRAMS.keys()}
    if compress not in valid_compress:
        raise ConfigError(
            f"Invalid compression '{compress}' for target {path}. "
            f"Valid: {sorted(valid_compress)}"
        )

    # Encryption is a raw-target-only feature. Validate at load time and FAIL
    # CLOSED: a requested encryption that cannot be honored must refuse to run
    # rather than silently writing plaintext to (often offsite) destinations.
    encrypt = data.get("encrypt", "none")
    gpg_recipient = data.get("gpg_recipient")
    valid_encrypt = {"none", "gpg", "openssl_enc"}
    if encrypt not in valid_encrypt:
        raise ConfigError(
            f"Invalid encryption '{encrypt}' for target {path}. "
            f"Valid: {sorted(valid_encrypt)}"
        )
    if encrypt != "none" and not is_raw:
        raise ConfigError(
            f"Encryption (encrypt={encrypt!r}) is only supported on raw targets "
            f"(raw:// / raw+ssh://), not {path}"
        )
    if encrypt == "gpg" and not gpg_recipient:
        raise ConfigError(f"gpg_recipient is required when encrypt=gpg (target {path})")

    # Host-key policy is a SECURITY selector: fail CLOSED on an unrecognized value rather
    # than silently falling back to a default (a typo'd "strict" must not degrade to
    # accept-new). Only the two safe modes are accepted -- there is no "off"/"no". R12b.
    ssh_host_key_policy = _validate_host_key_policy(
        data.get("ssh_host_key_policy", "accept-new"), path
    )

    return TargetConfig(
        path=path,
        ssh_sudo=data.get("ssh_sudo", False),
        ssh_port=data.get("ssh_port", 22),
        ssh_key=data.get("ssh_key"),
        ssh_auth_sock=data.get("ssh_auth_sock"),
        ssh_password_auth=data.get("ssh_password_auth", True),
        ssh_host_key_policy=ssh_host_key_policy,
        compress=compress,
        rate_limit=data.get("rate_limit"),
        require_mount=data.get("require_mount", False),
        encrypt=encrypt,
        gpg_recipient=gpg_recipient,
        gpg_keyring=data.get("gpg_keyring"),
        openssl_cipher=data.get("openssl_cipher"),
    )


def _parse_snapper_source(data: dict[str, Any]) -> SnapperSourceConfig:
    """Parse snapper source configuration from dict."""
    return SnapperSourceConfig(
        config_name=data.get("config_name", "auto"),
        include_types=data.get("include_types", ["single", "pre", "post"]),
        exclude_cleanup=data.get("exclude_cleanup", []),
        min_age=data.get("min_age", "1h"),
    )


def _parse_volume(data: dict[str, Any], global_config: GlobalConfig) -> VolumeConfig:
    """Parse volume configuration from dict."""
    if "path" not in data:
        raise ConfigError("Volume missing required 'path' field")

    targets = [_parse_target(t) for t in data.get("targets", [])]

    retention = None
    if "retention" in data:
        retention = _parse_retention(data["retention"])

    # Parse source type and snapper config
    source = data.get("source", "native")
    if source not in ("native", "snapper"):
        raise ConfigError(f"Invalid source type: {source}. Valid: native, snapper")

    snapper = None
    if "snapper" in data:
        snapper = _parse_snapper_source(data["snapper"])
    elif source == "snapper":
        # Auto-create snapper config with defaults if source is snapper
        snapper = SnapperSourceConfig()

    return VolumeConfig(
        path=data["path"],
        # Absent -> None so the schema auto-derives from the path; an explicit
        # value (including "") is passed through and honored. None is the
        # documented sentinel here, hence the arg-type ignore.
        snapshot_prefix=data.get("snapshot_prefix"),  # type: ignore[arg-type]
        snapshot_dir=data.get("snapshot_dir", global_config.snapshot_dir),
        targets=targets,
        retention=retention,
        enabled=data.get("enabled", True),
        source=source,
        snapper=snapper,
    )


def _parse_email_notification(data: dict[str, Any]) -> EmailNotificationConfig:
    """Parse email notification configuration from dict."""
    return EmailNotificationConfig(
        enabled=data.get("enabled", False),
        smtp_host=data.get("smtp_host", "localhost"),
        smtp_port=data.get("smtp_port", 25),
        smtp_user=data.get("smtp_user"),
        smtp_password=data.get("smtp_password"),
        smtp_tls=data.get("smtp_tls", "none"),
        from_addr=data.get("from_addr", "btrfs-backup-ng@localhost"),
        to_addrs=data.get("to_addrs", []),
        on_success=data.get("on_success", False),
        on_failure=data.get("on_failure", True),
    )


def _parse_webhook_notification(data: dict[str, Any]) -> WebhookNotificationConfig:
    """Parse webhook notification configuration from dict."""
    return WebhookNotificationConfig(
        enabled=data.get("enabled", False),
        url=data.get("url"),
        method=data.get("method", "POST"),
        headers=data.get("headers", {}),
        on_success=data.get("on_success", False),
        on_failure=data.get("on_failure", True),
        timeout=data.get("timeout", 30),
    )


def _parse_notifications(data: dict[str, Any]) -> NotificationConfig:
    """Parse notification configuration from dict."""
    email = EmailNotificationConfig()
    webhook = WebhookNotificationConfig()

    if "email" in data:
        email = _parse_email_notification(data["email"])
    if "webhook" in data:
        webhook = _parse_webhook_notification(data["webhook"])

    return NotificationConfig(email=email, webhook=webhook)


def _parse_global(data: dict[str, Any]) -> GlobalConfig:
    """Parse global configuration from dict."""
    retention = RetentionConfig()
    if "retention" in data:
        retention = _parse_retention(data["retention"])

    notifications = NotificationConfig()
    if "notifications" in data:
        notifications = _parse_notifications(data["notifications"])

    return GlobalConfig(
        snapshot_dir=data.get("snapshot_dir", ".snapshots"),
        timestamp_format=data.get("timestamp_format", "%Y%m%d-%H%M%S"),
        incremental=data.get("incremental", True),
        log_file=data.get("log_file"),
        transaction_log=data.get("transaction_log"),
        retention=retention,
        notifications=notifications,
        parallel_volumes=data.get("parallel_volumes", 2),
        parallel_targets=data.get("parallel_targets", 3),
        quiet=data.get("quiet", False),
        verbose=data.get("verbose", False),
    )


# Known TOML keys per section. Mirror the fields each `_parse_*` function above
# reads -- kept adjacent so adding a field there is an obvious prompt to add it
# here too. A key not in these sets is silently dropped by the parsers, so a
# typo (`retenion`) or a value placed under the wrong table would quietly void a
# safety knob (retention/encrypt/ssh_host_key_policy). We surface it as a
# warning; see `_collect_unknown_key_warnings`.
_KNOWN_TOPLEVEL_KEYS = {"global", "volumes"}
_KNOWN_GLOBAL_KEYS = {
    "snapshot_dir",
    "timestamp_format",
    "incremental",
    "log_file",
    "transaction_log",
    "retention",
    "notifications",
    "parallel_volumes",
    "parallel_targets",
    "quiet",
    "verbose",
}
_KNOWN_RETENTION_KEYS = {"min", "hourly", "daily", "weekly", "monthly", "yearly"}
_KNOWN_NOTIFICATIONS_KEYS = {"email", "webhook"}
_KNOWN_EMAIL_KEYS = {
    "enabled",
    "smtp_host",
    "smtp_port",
    "smtp_user",
    "smtp_password",
    "smtp_tls",
    "from_addr",
    "to_addrs",
    "on_success",
    "on_failure",
}
_KNOWN_WEBHOOK_KEYS = {
    "enabled",
    "url",
    "method",
    "headers",
    "on_success",
    "on_failure",
    "timeout",
}
_KNOWN_VOLUME_KEYS = {
    "path",
    "targets",
    "retention",
    "source",
    "snapper",
    "snapshot_prefix",
    "snapshot_dir",
    "enabled",
}
_KNOWN_TARGET_KEYS = {
    "path",
    "ssh_sudo",
    "ssh_port",
    "ssh_key",
    "ssh_auth_sock",
    "ssh_password_auth",
    "ssh_host_key_policy",
    "compress",
    "rate_limit",
    "require_mount",
    "encrypt",
    "gpg_recipient",
    "gpg_keyring",
    "openssl_cipher",
}
_KNOWN_SNAPPER_KEYS = {"config_name", "include_types", "exclude_cleanup", "min_age"}


def _unknown_keys_in(section_label: str, data: Any, known: set[str]) -> list[str]:
    """Return a warning per key in ``data`` that is not in ``known``.

    A no-op when ``data`` is not a dict (a mistyped table -- e.g. a scalar where
    a table is expected -- is reported with a clearer message by the parser)."""
    if not isinstance(data, dict):
        return []
    return [
        f"Unknown config key '{key}' in [{section_label}] (ignored)"
        for key in sorted(set(data) - known)
    ]


def _collect_unknown_key_warnings(data: dict[str, Any]) -> list[str]:
    """Walk the raw TOML dict and warn on keys no parser recognizes.

    Centralized here -- walking the raw dict rather than checking inside each
    ``_parse_*`` -- so parser signatures stay unchanged and every section is
    covered in one place. Section labels mirror the TOML table paths so the
    operator can locate the offending line."""
    warnings: list[str] = []
    if not isinstance(data, dict):
        return warnings

    warnings += _unknown_keys_in("top level", data, _KNOWN_TOPLEVEL_KEYS)

    global_data = data.get("global", {})
    if isinstance(global_data, dict):
        warnings += _unknown_keys_in("global", global_data, _KNOWN_GLOBAL_KEYS)
        warnings += _unknown_keys_in(
            "global.retention", global_data.get("retention", {}), _KNOWN_RETENTION_KEYS
        )
        notif = global_data.get("notifications", {})
        if isinstance(notif, dict):
            warnings += _unknown_keys_in(
                "global.notifications", notif, _KNOWN_NOTIFICATIONS_KEYS
            )
            warnings += _unknown_keys_in(
                "global.notifications.email",
                notif.get("email", {}),
                _KNOWN_EMAIL_KEYS,
            )
            warnings += _unknown_keys_in(
                "global.notifications.webhook",
                notif.get("webhook", {}),
                _KNOWN_WEBHOOK_KEYS,
            )

    volumes = data.get("volumes", [])
    if isinstance(volumes, list):
        for i, vol in enumerate(volumes):
            if not isinstance(vol, dict):
                continue
            label = f"volumes[{i}]"
            warnings += _unknown_keys_in(label, vol, _KNOWN_VOLUME_KEYS)
            warnings += _unknown_keys_in(
                f"{label}.retention", vol.get("retention", {}), _KNOWN_RETENTION_KEYS
            )
            warnings += _unknown_keys_in(
                f"{label}.snapper", vol.get("snapper", {}), _KNOWN_SNAPPER_KEYS
            )
            targets = vol.get("targets", [])
            if isinstance(targets, list):
                for j, tgt in enumerate(targets):
                    warnings += _unknown_keys_in(
                        f"{label}.targets[{j}]", tgt, _KNOWN_TARGET_KEYS
                    )

    return warnings


def _validate_config(config: Config) -> list[str]:
    """Validate configuration and return list of warnings."""
    warnings = []

    if not config.volumes:
        warnings.append("No volumes configured")

    for i, volume in enumerate(config.volumes):
        if not volume.targets:
            warnings.append(f"Volume '{volume.path}' has no targets configured")

        # Check for duplicate targets
        target_paths = [t.path for t in volume.targets]
        if len(target_paths) != len(set(target_paths)):
            warnings.append(f"Volume '{volume.path}' has duplicate target paths")

        # Validate SSH URLs
        for target in volume.targets:
            if target.path.startswith("ssh://"):
                if ":" not in target.path[6:]:
                    warnings.append(
                        f"SSH target '{target.path}' may be missing path separator ':'"
                    )

            # raw+ssh with ssh_sudo is fully supported, and it is also the
            # combination operators most often configure by analogy with ssh://
            # and then cannot use. A raw target stores plain files, so the remote
            # runs mkdir/find/cat/stat/mv/rm -- never btrfs -- and the sudoers
            # recipe written for ssh:// (NOPASSWD: /usr/bin/btrfs) authorises
            # none of them. Say so once, at load, rather than at 3am.
            if target.path.startswith("raw+ssh://") and target.ssh_sudo:
                warnings.append(
                    f"Target '{target.path}' uses ssh_sudo: a raw+ssh target "
                    "stores plain files, so the remote needs passwordless sudo "
                    "for the FILE tools (mkdir, find, cat, stat, mv, rm), not "
                    "for btrfs. Backups written this way are root-owned and need "
                    "ssh_sudo to read back. Owning the destination "
                    "(chown/setfacl) and leaving ssh_sudo off is simpler and "
                    "grants less; both are supported."
                )

        # Validate snapper configuration
        if volume.is_snapper_source():
            if volume.snapper is None:
                warnings.append(
                    f"Volume '{volume.path}' has source='snapper' but no snapper config"
                )
            else:
                # Validate include_types
                valid_types = {"single", "pre", "post"}
                for snap_type in volume.snapper.include_types:
                    if snap_type not in valid_types:
                        warnings.append(
                            f"Volume '{volume.path}' has invalid snapper type: {snap_type}"
                        )

    # Check for duplicate volume paths
    volume_paths = [v.path for v in config.volumes]
    if len(volume_paths) != len(set(volume_paths)):
        warnings.append("Duplicate volume paths detected")

    return warnings


def load_config(path: Path | str) -> tuple[Config, list[str]]:
    """Load and validate configuration from TOML file.

    Args:
        path: Path to configuration file

    Returns:
        Tuple of (Config object, list of warnings)

    Raises:
        ConfigError: If config is invalid or cannot be parsed
    """
    path = Path(path)

    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"Invalid TOML syntax: {e}")
    except OSError as e:
        raise ConfigError(f"Cannot read config file: {e}")

    # Parse global config
    global_config = _parse_global(data.get("global", {}))

    # Parse volumes
    volumes = []
    for vol_data in data.get("volumes", []):
        volumes.append(_parse_volume(vol_data, global_config))

    config = Config(global_config=global_config, volumes=volumes)

    # Validate and collect warnings. Unknown-key warnings first: a typo'd or
    # misplaced key is a structural mistake worth seeing before the semantic
    # checks, and it explains why an expected setting appears not to take effect.
    warnings = _collect_unknown_key_warnings(data) + _validate_config(config)

    return config, warnings


def generate_example_config() -> str:
    """Generate example configuration file content."""
    return """# btrfs-backup-ng configuration
# See documentation for full options

[global]
snapshot_dir = ".snapshots"
timestamp_format = "%Y%m%d-%H%M%S"
incremental = true
# log_file = "/var/log/btrfs-backup-ng.log"
# transaction_log = "/var/log/btrfs-backup-ng-transactions.jsonl"

# Parallelism settings
parallel_volumes = 2
parallel_targets = 3

[global.retention]
min = "1d"          # Keep all snapshots for at least 1 day
hourly = 24         # Then keep 24 hourly snapshots
daily = 7           # Then keep 7 daily snapshots
weekly = 4          # Then keep 4 weekly snapshots
monthly = 12        # Then keep 12 monthly snapshots
yearly = 0          # Don't keep yearly (0 = disabled)

# Email notifications (optional)
# [global.notifications.email]
# enabled = true
# smtp_host = "smtp.example.com"
# smtp_port = 587
# smtp_tls = "starttls"          # "ssl", "starttls", or "none"
# smtp_user = "alerts@example.com"
# smtp_password = "secret"
# from_addr = "btrfs-backup-ng@example.com"
# to_addrs = ["admin@example.com", "ops@example.com"]
# on_success = false             # Only notify on failure by default
# on_failure = true

# Webhook notifications (optional)
# [global.notifications.webhook]
# enabled = true
# url = "https://hooks.slack.com/services/xxx/yyy/zzz"
# method = "POST"
# on_success = false
# on_failure = true
# timeout = 30
# [global.notifications.webhook.headers]
# Authorization = "Bearer token123"

# Home directory backup
[[volumes]]
path = "/home"
snapshot_prefix = "home-"

[[volumes.targets]]
path = "/mnt/backup/home"

# Example external drive target with mount verification
# [[volumes.targets]]
# path = "/mnt/usb-backup/home"
# require_mount = true          # Fail if drive is not mounted (safety check)

# Example SSH target
# [[volumes.targets]]
# path = "ssh://backup@server:/backups/home"
# ssh_sudo = true

# System logs backup with custom retention
# [[volumes]]
# path = "/var/log"
# snapshot_prefix = "logs-"
#
# [volumes.retention]
# daily = 14
# weekly = 8
#
# [[volumes.targets]]
# path = "ssh://backup@server:/backups/logs"

# Snapper-managed root filesystem backup
# Use this when snapper is managing local snapshots and you want
# btrfs-backup-ng to back them up to remote targets
# [[volumes]]
# path = "/"
# source = "snapper"              # Use snapper as snapshot source
# snapshot_prefix = "root-"
#
# [volumes.snapper]
# config_name = "root"            # Snapper config name, or "auto" to detect
# include_types = ["single"]      # Only backup timeline/manual snapshots
# exclude_cleanup = []            # Optionally exclude by cleanup algorithm
# min_age = "1h"                  # Wait 1 hour before backing up
#
# [[volumes.targets]]
# path = "ssh://backup@server:/backups/root"
# ssh_sudo = true
"""
