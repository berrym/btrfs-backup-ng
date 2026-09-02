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
    keep = data.get("keep", 0)
    if not isinstance(keep, int) or isinstance(keep, bool) or keep < 0:
        raise ConfigError(
            f"Invalid retention 'keep': {keep!r}. It is the NUMBER of most-recent "
            f"snapshots to keep, so it must be a non-negative whole number "
            f"(0 means use the time buckets instead)."
        )

    return RetentionConfig(
        min=min_value,
        hourly=data.get("hourly", 24),
        daily=data.get("daily", 7),
        weekly=data.get("weekly", 4),
        monthly=data.get("monthly", 12),
        yearly=data.get("yearly", 0),
        keep=keep,
        # Which keys this block actually wrote, so a narrower scope inherits the
        # rest from the scope above instead of resetting them to the defaults
        # above. The defaults still stand for the GLOBAL scope, which has nothing
        # to inherit from.
        explicit=frozenset(k for k in RetentionConfig.RETENTION_KEYS if k in data),
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

    require_mount = _parse_require_mount(data.get("require_mount", False))

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
        require_mount=require_mount,
        optional=_parse_bool_option(data.get("optional", False), "optional"),
        retention=(
            _parse_retention(data["retention"]) if "retention" in data else None
        ),
        encrypt=encrypt,
        gpg_recipient=gpg_recipient,
        gpg_keyring=data.get("gpg_keyring"),
        openssl_cipher=data.get("openssl_cipher"),
    )


_TRUTHY_SPELLINGS = {"true", "yes", "on", "1"}
_FALSY_SPELLINGS = {"false", "no", "off", "0"}


def _parse_bool_option(value: Any, name: str) -> bool:
    """A strict boolean for a per-target switch.

    Quoted spellings are accepted with the value named back, because a quoted
    "false" reads as true to plain truthiness -- the mistake that made
    `require_mount = "false"` silently ENABLE the check. Anything with no sensible
    reading is refused rather than guessed at.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in _TRUTHY_SPELLINGS:
            return True
        if lowered in _FALSY_SPELLINGS:
            return False
    raise ConfigError(f"Invalid {name}: {value!r}. Write true or false, unquoted.")


def _parse_require_mount(value: Any) -> bool | str:
    """Normalise ``require_mount`` to a bool or a mount point.

    Most malformed values are COERCED here and warned about separately (see
    ``_collect_require_mount_warnings``), rather than rejected. The reason is
    blast radius: a ConfigError aborts the whole file -- every volume, every
    target, and `list`, `status` and `doctor` along with `run` -- while the mount
    gate already refuses an unusable value per target, fail-closed, with a
    message naming it. Erroring here adds no safety for those values and takes
    down configs that worked on 0.9.6.

    Two classes still raise, because coercing them would be a guess with a
    dangerous wrong answer:

    - An EMPTY or whitespace-only string. Both 0.9.6 and the gate read it as
      falsy, so it silently turns the check OFF -- exactly the failure
      require_mount exists to prevent -- and it almost always means a variable
      that expanded to nothing rather than a deliberate "no".
    - A type that carries no interpretation at all (list, table). No plausible
      working config contains one.
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        # 1/0 and 1.0/0.0 were truthiness on 0.9.6 and mean exactly that here.
        return bool(value)

    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            raise ConfigError(
                "require_mount is an empty string, which would turn the mount "
                "check OFF without saying so -- and that is the accident the "
                "option exists to prevent. Use true/false, or the mount point "
                'the target lives under (for example "/mnt/backup"). If a '
                "variable was meant to expand here, it did not."
            )
        lowered = candidate.lower()
        if lowered in _TRUTHY_SPELLINGS:
            return True
        if lowered in _FALSY_SPELLINGS:
            return False
        # Anything else is treated as a mount point. A relative or vacuous one
        # is refused by the gate, per target, with the value quoted.
        return candidate

    raise ConfigError(
        f"require_mount must be true, false, or a mount point path; got "
        f"{type(value).__name__} ({value!r})."
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

    source_retention = None
    if "source_retention" in data:
        source_retention = _parse_retention(data["source_retention"])

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
        source_retention=source_retention,
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
        transfer_timeout=data.get("transfer_timeout", 0),
        transfer_stall_timeout=data.get("transfer_stall_timeout", 900),
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
    "transfer_timeout",
    "transfer_stall_timeout",
    "parallel_volumes",
    "parallel_targets",
    "quiet",
    "verbose",
}
_KNOWN_RETENTION_KEYS = {
    "min",
    "hourly",
    "daily",
    "weekly",
    "monthly",
    "yearly",
    "keep",
}
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
    "source_retention",
    "source",
    "snapper",
    "snapshot_prefix",
    "snapshot_dir",
    "enabled",
}
_KNOWN_TARGET_KEYS = {
    "path",
    "retention",
    "ssh_sudo",
    "ssh_port",
    "ssh_key",
    "ssh_auth_sock",
    "ssh_password_auth",
    "ssh_host_key_policy",
    "compress",
    "rate_limit",
    "require_mount",
    "optional",
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


_BUCKET_KEYS = ("hourly", "daily", "weekly", "monthly", "yearly")


def _collect_retention_warnings(data: dict[str, Any]) -> list[str]:
    """Warn when a retention table sets BOTH a keep count and time buckets.

    Walks the RAW dict because the parser fills bucket defaults, so after parsing
    every policy looks as though daily/weekly/monthly were set. Only the raw
    table shows what the operator actually wrote.

    `keep` replaces the buckets rather than combining with them -- a count is
    neither obviously a floor nor obviously a ceiling, and guessing wrong either
    wastes space or deletes history -- so writing both is a contradiction worth
    naming rather than silently resolving.
    """
    warnings: list[str] = []
    if not isinstance(data, dict):
        return warnings

    def check(table: Any, where: str) -> None:
        if not isinstance(table, dict) or not table.get("keep"):
            return
        also = [k for k in _BUCKET_KEYS if k in table]
        if also:
            warnings.append(
                f"retention in [{where}] sets keep = {table['keep']!r} and also "
                f"{', '.join(also)}. keep replaces the time buckets, so "
                f"{', '.join(also)} {'is' if len(also) == 1 else 'are'} ignored "
                f"here. Remove one or the other to say which you meant."
            )

    global_data = data.get("global", {})
    if isinstance(global_data, dict):
        check(global_data.get("retention"), "global.retention")

    for v_index, volume in enumerate(data.get("volumes", []) or []):
        if not isinstance(volume, dict):
            continue
        check(volume.get("retention"), f"volumes[{v_index}].retention")
        check(volume.get("source_retention"), f"volumes[{v_index}].source_retention")
        for t_index, target in enumerate(volume.get("targets", []) or []):
            if isinstance(target, dict):
                check(
                    target.get("retention"),
                    f"volumes[{v_index}].targets[{t_index}].retention",
                )
    return warnings


def _collect_require_mount_warnings(data: dict[str, Any]) -> list[str]:
    """Report every ``require_mount`` value that was not written as intended.

    Walks the RAW dict, like ``_collect_unknown_key_warnings``, because by the
    time the config is built the value has been coerced and the original
    spelling -- the thing the operator needs to see -- is gone.

    These are warnings rather than errors deliberately. The mount gate refuses an
    unusable value per target, fail-closed, quoting it; a ConfigError here would
    add no safety and would stop the whole file loading, taking `list`, `status`
    and `doctor` down with `run`. But a coercion nobody is told about is the
    "recognised and silently ignored" shape this project keeps removing, so each
    one is named, with what it was read as.
    """
    warnings: list[str] = []
    if not isinstance(data, dict):
        return warnings

    for v_index, volume in enumerate(data.get("volumes", []) or []):
        if not isinstance(volume, dict):
            continue
        for t_index, target in enumerate(volume.get("targets", []) or []):
            if not isinstance(target, dict) or "require_mount" not in target:
                continue
            where = f"volumes[{v_index}].targets[{t_index}]"
            raw = target["require_mount"]
            if isinstance(raw, bool):
                continue

            if isinstance(raw, (int, float)):
                warnings.append(
                    f"require_mount = {raw!r} in [{where}] is not true/false. "
                    f"Read as {bool(raw)}"
                    + ("" if raw else " -- the mount check is OFF for this target")
                    + ". Write true or false, or the mount point the target "
                    "lives under."
                )
                continue

            if isinstance(raw, str):
                lowered = raw.strip().lower()
                if lowered in _TRUTHY_SPELLINGS:
                    warnings.append(
                        f"require_mount = {raw!r} in [{where}] is quoted. Read as "
                        f"true. Write it unquoted."
                    )
                elif lowered in _FALSY_SPELLINGS:
                    warnings.append(
                        f"require_mount = {raw!r} in [{where}] is quoted. Read as "
                        f"false, so the mount check is OFF for this target. Write "
                        f"it unquoted."
                    )
                elif not raw.strip().startswith("/"):
                    warnings.append(
                        f"require_mount = {raw!r} in [{where}] is not an absolute "
                        f"path, so it cannot name a mount point. This target will "
                        f"refuse to run until it is fixed."
                    )
                elif _target_is_remote(target.get("path")):
                    # A mount point is checked against THIS machine's mount
                    # table, which says nothing about a remote filesystem, so
                    # the gate exempts remote targets and the value does
                    # nothing. Warned rather than refused: README documents
                    # require_mount as having no effect on ssh:// targets, and
                    # the sibling precedent in this file (compress not applied
                    # to a same-machine target) warns for the same shape.
                    warnings.append(
                        f"require_mount = {raw!r} in [{where}] names a mount "
                        f"point, which is checked on THIS machine. The target is "
                        f"remote, so it has no effect there and nothing is being "
                        f"guarded."
                    )
                elif Path(raw.strip()).resolve() == Path("/"):
                    warnings.append(
                        f"require_mount = {raw!r} in [{where}] resolves to / , "
                        f"which is always mounted, so the check would pass "
                        f"unconditionally. This target will refuse to run until "
                        f"it names the mount the target actually lives under."
                    )
                else:
                    # Containment is decidable here with no I/O, and getting it
                    # wrong means this target aborts on every run. Reported at
                    # load so `config validate` says so, rather than only at
                    # backup time -- but as a warning, because the gate already
                    # refuses the target fail-closed and an error would stop the
                    # whole file loading.
                    warnings += _containment_warning(target.get("path"), raw, where)
    return warnings


def _target_is_remote(path: Any) -> bool:
    """Whether a target path is a remote scheme, per the one scheme authority."""
    from ..core.target import parse_target

    try:
        return bool(parse_target(str(path)).is_remote)
    except Exception:  # noqa: BLE001 - an unclassifiable path is not remote here
        return False


def _containment_warning(path: Any, require_mount: str, where: str) -> list[str]:
    """Warn when the target does not live under the mount point it names."""
    from ..core.target import parse_target

    try:
        scheme = parse_target(str(path))
        if not scheme.supports_mount_check or not scheme.path:
            return []
        target_dir = Path(scheme.path).resolve()
        expected = Path(require_mount.strip()).resolve()
    except Exception:  # noqa: BLE001 - undeterminable is the gate's problem
        return []

    if target_dir.is_relative_to(expected):
        return []
    return [
        f"require_mount = {require_mount!r} in [{where}] names a mount the "
        f"target is not inside, so it would confirm a drive this target is not "
        f"written to. This target will refuse to run until one of them changes."
    ]


def _validate_config(config: Config) -> list[str]:
    """Validate configuration and return list of warnings."""
    # Imported here rather than at module scope: core/__init__ pulls in
    # core.execution, which imports this package, so a top-level import is a
    # cycle. core.target itself has no such dependency.
    from ..core.target import TargetKind, parse_target

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

            # `compress` is accepted for every target and then dropped for a
            # destination whose endpoint does not implement it: core.operations
            # refuses to compress a stream only to decompress it on the same
            # machine, which is right, but nothing said so and the config still
            # read as though the backup were compressed. Collected and quietly
            # ignored is the shape of defect this project keeps finding, so it is
            # named at load. Non-fatal: the config is valid and the backup runs.
            compress = getattr(target, "compress", "none")
            if compress and compress != "none":
                scheme = parse_target(target.path)
                if (
                    not scheme.supports_compress
                    and scheme.kind is not TargetKind.UNSUPPORTED
                ):
                    warnings.append(
                        f"Target '{target.path}' sets compress = '{compress}', "
                        "which will not be applied: this destination is on the "
                        "same machine, so the stream would be compressed and "
                        "immediately decompressed again for no saving. The "
                        "backup still runs, uncompressed. Compression applies to "
                        "ssh:// (compressed over the wire) and to raw:// and "
                        "raw+ssh:// (compressed at rest)."
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
    warnings = (
        _collect_unknown_key_warnings(data)
        + _collect_require_mount_warnings(data)
        + _collect_retention_warnings(data)
        + _validate_config(config)
    )

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
# require_mount = "/mnt/usb-backup"   # Fail if the drive is not mounted.
# optional = true                     # Allowed to be absent: skip instead of
#                                     # failing, and let the source still prune.
#                                     # The target is a SUBDIRECTORY of the mount
#                                     # point, so the mount point is named here.
#                                     # `true` would require the target itself to
#                                     # be a mount point and would always abort.

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
