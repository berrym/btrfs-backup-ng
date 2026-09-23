"""Config command: Configuration management."""

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from .. import __util__
from ..__util__ import toml_str
from ..config.loader import generate_example_config, get_default_config_path
from .common import _MEMORY_BACKED_FILESYSTEMS, get_log_level
from .wizard_utils import (
    console,
    display_btrbk_detected,
    display_btrbk_import,
    display_config_preview,
    display_next_steps,
    display_section_header,
    display_snapper_configs,
    display_wizard_header,
    find_btrbk_config,
    prompt,
    prompt_bool,
    prompt_choice,
    prompt_int,
    prompt_selection,
    prompt_snapshot_prefix,
)

logger = logging.getLogger(__name__)


def _prompt(message: str, default: str = "") -> str:
    """Prompt user for input with optional default value.

    DEPRECATED: Use wizard_utils.prompt() instead.
    Kept for backwards compatibility during migration.
    """
    if default:
        prompt_text = f"{message} [{default}]: "
    else:
        prompt_text = f"{message}: "

    try:
        value = input(prompt_text).strip()
        return value if value else default
    except (EOFError, KeyboardInterrupt):
        print()
        raise KeyboardInterrupt


def _prompt_bool(message: str, default: bool = True) -> bool:
    """Prompt user for yes/no input."""
    default_str = "Y/n" if default else "y/N"
    prompt_text = f"{message} [{default_str}]: "

    try:
        value = input(prompt_text).strip().lower()
        if not value:
            return default
        return value in ("y", "yes", "true", "1")
    except (EOFError, KeyboardInterrupt):
        print()
        raise KeyboardInterrupt


def _prompt_choice(message: str, choices: list[str], default: str = "") -> str:
    """Prompt user to select from choices."""
    print(f"\n{message}")
    for i, choice in enumerate(choices, 1):
        marker = " *" if choice == default else ""
        print(f"  {i}. {choice}{marker}")

    while True:
        prompt_text = "Enter number or value"
        if default:
            prompt_text += f" [{default}]"
        prompt_text += ": "

        try:
            value = input(prompt_text).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            raise KeyboardInterrupt

        if not value and default:
            return default

        # Try as number
        try:
            idx = int(value) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
        except ValueError:
            pass

        # Try as direct value
        if value in choices:
            return value

        print(f"  Invalid choice. Enter 1-{len(choices)} or a value from the list.")


def _prompt_int(
    message: str, default: int, min_val: int = 0, max_val: int = 100
) -> int:
    """Prompt user for integer input."""
    prompt_text = f"{message} [{default}]: "

    while True:
        try:
            value = input(prompt_text).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            raise KeyboardInterrupt

        if not value:
            return default

        try:
            int_val = int(value)
            if min_val <= int_val <= max_val:
                return int_val
            print(f"  Value must be between {min_val} and {max_val}.")
        except ValueError:
            print("  Please enter a valid number.")


# One canonical implementation, in __util__, shared with the btrbk importer --
# which emitted its own unescaped f-strings and silently corrupted any path
# containing a backslash. Kept under the original private name so this module's
# call sites are unchanged.
_toml_str = toml_str


def _output_would_clobber(output: str | None, *, force: bool) -> bool:
    """Whether writing to ``output`` would destroy a file that is already there.

    ``config import -o`` had no existence check of any kind, and ``config init
    -o`` had one only when running interactively, so both silently replaced an
    existing configuration -- including the one the operator is currently backing
    up with. A configuration file is not a scratch artifact; it is the thing that
    says which subvolumes matter and how long their history is kept.
    """
    if not output or force:
        return False
    return os.path.exists(output)


def _unloadable_reason(content: str) -> str | None:
    """Return why ``content`` is not a configuration this tool can load back.

    Returns None when it loads. Validated through ``load_config`` itself rather
    than a ``tomllib.loads`` stand-in, so the check answers the only question
    that matters -- will the file we are about to write work -- instead of a
    narrower one that happens to be easier to ask.

    The importer used to emit a conversion, print "Configuration written to:"
    and exit 0 for content the very next ``config validate`` rejected. The
    wizard was worse: its parse attempt sat inside ``except Exception: pass``
    and only gated a summary table, so an unparseable conversion was written
    over the operator's real configuration and the wizard still returned 0.
    """
    handle = tempfile.NamedTemporaryFile(
        "w", suffix=".toml", prefix="bbng-verify-", delete=False, encoding="utf-8"
    )
    probe = Path(handle.name)
    try:
        handle.write(content)
        handle.close()
        load_config(probe)
    except ConfigError as e:
        return str(e)
    except OSError as e:
        return f"could not verify the generated configuration: {e}"
    finally:
        handle.close()
        probe.unlink(missing_ok=True)
    return None


def _prompt_target_encryption(target_path: str) -> dict[str, Any]:
    """Prompt for a raw target's encryption settings.

    Encryption is a RAW-target-only feature (the config loader rejects ``encrypt``
    on ssh:// or local btrfs targets), so this is a no-op for anything that is not
    ``raw://`` / ``raw+ssh://`` -- the wizard never asks an irrelevant question.
    When encryption is chosen it enforces the same rule the loader does: ``gpg``
    REQUIRES a ``gpg_recipient``. ``gpg_keyring`` and ``openssl_cipher`` are
    optional (blank keeps the defaults).

    Returns the encryption keys to merge into the target dict (empty when the
    target is not raw, or encryption is declined).
    """
    if not target_path.startswith(("raw://", "raw+ssh://")):
        return {}

    method = prompt_choice(
        "  Encrypt this raw target?",
        ["none", "gpg", "openssl_enc"],
        "none",
    )
    if method == "none":
        return {}

    result: dict[str, Any] = {"encrypt": method}
    if method == "gpg":
        # The loader fails closed if encrypt=gpg has no recipient; require it here
        # so the wizard never emits a config that won't load.
        recipient = ""
        while not recipient.strip():
            recipient = prompt("  GPG recipient (key ID or email) [required]", "")
            if not recipient.strip():
                console.print(
                    "  [red]A GPG recipient is required for gpg encryption.[/red]"
                )
        result["gpg_recipient"] = recipient.strip()
        keyring = prompt("  GPG keyring file (optional, blank = default keyring)", "")
        if keyring.strip():
            result["gpg_keyring"] = keyring.strip()
    else:  # openssl_enc
        cipher = prompt("  OpenSSL cipher (optional, blank = aes-256-cbc)", "")
        if cipher.strip():
            result["openssl_cipher"] = cipher.strip()
    return result


def _derive_require_mount(target_path: str) -> bool | str:
    """Turn "yes, check the mount" into a value that can actually pass.

    The wizard offers this check for /mnt/... paths, which are overwhelmingly
    SUBDIRECTORIES of a mount point. `require_mount = true` requires the target
    ITSELF to be a mount point, so for those it produced a config that aborts
    even with the drive correctly connected.

    Only the mount table is consulted, and only when it can actually answer. An
    earlier version fell back to the target's PARENT when nothing was mounted,
    which fabricated mount points that can never exist -- a target of
    /mnt/backup yielded "/mnt", and /run/media/<user>/<drive> yielded
    "/run/media/<user>" -- so the wizard emitted configs that abort forever, with
    the drive connected, for a reason the operator never chose. Guessing is worse
    than declining to guess: `true` is the documented default, and when it is
    wrong the runtime error now names the mounted drive and the exact line to
    write instead.

    The scheme comes from `parse_target`, not from `Path.is_absolute()`. A
    raw:// URI is not an absolute path, so the earlier check bailed to `true`
    for precisely the scheme whose targets are usually subdirectories.
    """
    from ..core.target import parse_target

    scheme = parse_target(target_path)
    if scheme.is_remote or not scheme.supports_mount_check:
        # Nothing local to check, or the path cannot be determined; `true` is the
        # documented default and the gate handles both cases on its own.
        return True

    path = Path(scheme.path)
    if not path.is_absolute():
        return True

    def _mounted(candidate: Path) -> bool:
        # The wizard did not read the mount table before this; an unreadable
        # /proc/mounts must not take the whole config wizard down with it.
        try:
            return __util__.is_mounted(candidate)
        except OSError:
            return False

    if _mounted(path):
        return True

    for parent in path.parents:
        if parent == Path("/"):
            break
        if not _mounted(parent):
            continue
        # Never name a filesystem held in memory: /run is tmpfs and always
        # mounted, so it would produce a check that always passes -- the very
        # thing assert_target_mounted refuses.
        info = None
        try:
            info = __util__.get_mount_info(parent)
        except OSError:
            pass
        if (info or {}).get("fs_type") in _MEMORY_BACKED_FILESYSTEMS:
            break
        return str(parent)

    return True


def _generate_config_from_wizard(config_data: dict[str, Any]) -> str:
    """Generate TOML config content from wizard data.

    Every string VALUE is emitted through ``_toml_str`` so a backslash, double
    quote, or control character in any user-entered field (paths, log files, email
    address/password, webhook URL, snapper config name, snapshot prefix, ...)
    serializes to valid, lossless TOML instead of an unparseable or silently
    corrupted config (e.g. a ``\\t`` in a path becoming a literal tab). Numeric and
    boolean fields are not quoted.
    """
    lines = [
        "# btrfs-backup-ng configuration",
        "# Generated by config init wizard",
        "",
        "[global]",
        f"snapshot_dir = {_toml_str(config_data['snapshot_dir'])}",
        f"timestamp_format = {_toml_str(config_data['timestamp_format'])}",
        f"incremental = {str(config_data['incremental']).lower()}",
    ]

    if config_data.get("log_file"):
        lines.append(f"log_file = {_toml_str(config_data['log_file'])}")

    if config_data.get("transaction_log"):
        lines.append(f"transaction_log = {_toml_str(config_data['transaction_log'])}")

    lines.extend(
        [
            "",
            "# Parallelism settings",
            f"parallel_volumes = {config_data['parallel_volumes']}",
            f"parallel_targets = {config_data['parallel_targets']}",
        ]
    )

    # Retention settings
    retention = config_data.get("retention", {})
    lines.extend(
        [
            "",
            "[global.retention]",
            f"min = {_toml_str(retention.get('min', '1d'))}",
            f"hourly = {retention.get('hourly', 24)}",
            f"daily = {retention.get('daily', 7)}",
            f"weekly = {retention.get('weekly', 4)}",
            f"monthly = {retention.get('monthly', 12)}",
            f"yearly = {retention.get('yearly', 0)}",
        ]
    )

    # Email notifications
    email = config_data.get("email")
    if email and email.get("enabled"):
        lines.extend(
            [
                "",
                "[global.notifications.email]",
                "enabled = true",
                f"smtp_host = {_toml_str(email.get('smtp_host', 'smtp.example.com'))}",
                f"smtp_port = {email.get('smtp_port', 587)}",
                f"smtp_tls = {_toml_str(email.get('smtp_tls', 'starttls'))}",
            ]
        )
        if email.get("smtp_user"):
            lines.append(f"smtp_user = {_toml_str(email['smtp_user'])}")
        if email.get("smtp_password"):
            lines.append(f"smtp_password = {_toml_str(email['smtp_password'])}")
        if email.get("from_addr"):
            lines.append(f"from_addr = {_toml_str(email['from_addr'])}")
        if email.get("to_addrs"):
            addrs = ", ".join(_toml_str(a) for a in email["to_addrs"])
            lines.append(f"to_addrs = [{addrs}]")
        lines.append(f"on_success = {str(email.get('on_success', False)).lower()}")
        lines.append(f"on_failure = {str(email.get('on_failure', True)).lower()}")

    # Webhook notifications
    webhook = config_data.get("webhook")
    if webhook and webhook.get("enabled"):
        lines.extend(
            [
                "",
                "[global.notifications.webhook]",
                "enabled = true",
                f"url = {_toml_str(webhook.get('url', ''))}",
                f"method = {_toml_str(webhook.get('method', 'POST'))}",
                f"on_success = {str(webhook.get('on_success', False)).lower()}",
                f"on_failure = {str(webhook.get('on_failure', True)).lower()}",
            ]
        )

    # Volumes
    for volume in config_data.get("volumes", []):
        lines.extend(
            [
                "",
                "[[volumes]]",
                f"path = {_toml_str(volume['path'])}",
            ]
        )

        # Handle snapper-sourced volumes
        if volume.get("source") == "snapper":
            lines.append('source = "snapper"')
            lines.append("")

            # Snapper configuration
            snapper = volume.get("snapper", {})
            lines.append("[volumes.snapper]")
            lines.append(
                f"config_name = {_toml_str(snapper.get('config_name', 'auto'))}"
            )

            include_types = snapper.get("include_types", ["single"])
            types_str = ", ".join(_toml_str(t) for t in include_types)
            lines.append(f"include_types = [{types_str}]")
            lines.append(f"min_age = {_toml_str(snapper.get('min_age', '1h'))}")
        else:
            # Native volumes use snapshot_prefix. Only emit it when explicitly
            # set, so an unset prefix round-trips to auto-derive on load; an
            # explicit "" is preserved and honored.
            if volume.get("snapshot_prefix") is not None:
                lines.append(
                    f"snapshot_prefix = {_toml_str(volume['snapshot_prefix'])}"
                )

        for target in volume.get("targets", []):
            lines.extend(
                [
                    "",
                    "[[volumes.targets]]",
                    f"path = {_toml_str(target['path'])}",
                ]
            )
            if target.get("ssh_sudo"):
                lines.append("ssh_sudo = true")
            # require_mount is bool OR a mount point path. Emitting `true` for
            # any truthy value silently rewrote a configured mount point as a
            # boolean on every round-trip through this writer, turning the
            # many-boxes-into-one-drive setup back into a check that cannot pass.
            require_mount = target.get("require_mount")
            if isinstance(require_mount, str):
                lines.append(f"require_mount = {_toml_str(require_mount)}")
            elif require_mount:
                lines.append("require_mount = true")
            # Raw-target encryption. Enforce the raw-only invariant at emit time
            # (matching the loader and _prompt_target_encryption) so the serializer
            # is self-consistent regardless of how the target dict was populated,
            # and only emit encrypt=gpg together with its required recipient.
            is_raw = str(target.get("path", "")).startswith(("raw://", "raw+ssh://"))
            encrypt = target.get("encrypt")
            if is_raw and encrypt and encrypt != "none":
                if encrypt == "gpg" and not target.get("gpg_recipient"):
                    # gpg requires a recipient (loader fails closed); skip an invalid
                    # block rather than emit an unloadable config.
                    pass
                else:
                    lines.append(f"encrypt = {_toml_str(encrypt)}")
                    if target.get("gpg_recipient"):
                        lines.append(
                            f"gpg_recipient = {_toml_str(target['gpg_recipient'])}"
                        )
                    if target.get("gpg_keyring"):
                        lines.append(
                            f"gpg_keyring = {_toml_str(target['gpg_keyring'])}"
                        )
                    if target.get("openssl_cipher"):
                        lines.append(
                            f"openssl_cipher = {_toml_str(target['openssl_cipher'])}"
                        )

    lines.append("")
    return "\n".join(lines)


# What the wizard ASKS about. Its answers are authoritative even where it
# writes nothing: a blank log_file means "no log file", declining email means
# "no email", and an old value must not come back because the answer was to
# leave it out. Everything else in an existing configuration -- the options
# the wizard never asks about -- is carried over when it saves over one.
_WIZARD_GLOBAL_KEYS = frozenset(
    {
        "snapshot_dir",
        "timestamp_format",
        "incremental",
        "log_file",
        "transaction_log",
        "parallel_volumes",
        "parallel_targets",
        # Asked as a whole: carrying a `keep = N` beside the bucket counts the
        # operator just gave would silently override them.
        "retention",
    }
)
_WIZARD_NOTIFICATION_KEYS = frozenset({"email", "webhook"})
_WIZARD_VOLUME_KEYS = frozenset({"path", "snapshot_prefix", "source", "snapper"})
_WIZARD_TARGET_KEYS = frozenset(
    {
        "path",
        "ssh_sudo",
        "require_mount",
        "encrypt",
        "gpg_recipient",
        "gpg_keyring",
        "openssl_cipher",
    }
)


class CarryOver:
    """What saving the wizard's configuration over an existing one keeps,
    replaces with the wizard's answers, and removes."""

    def __init__(self) -> None:
        self.kept: list[str] = []
        self.replaced: list[str] = []
        self.removed: list[str] = []

    @staticmethod
    def _describe(table: dict) -> str:
        return ", ".join(f"{k} = {_describe_value(v)}" for k, v in table.items())


def _describe_value(value: Any) -> str:
    if isinstance(value, dict):
        return "{...}"
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return f"[{len(value)} table(s)]"
    return str(value)


def _carry_keys(
    old: dict, new: dict, asked: frozenset, where: str, report: CarryOver
) -> None:
    """Copy into ``new`` every key of ``old`` the wizard does not ask about;
    record an asked-about key the wizard's answer replaced or removed."""
    for key, value in old.items():
        if key in asked:
            if key not in new:
                report.replaced.append(f"{where}{key} = {_describe_value(value)}")
            elif new[key] != value and isinstance(value, dict):
                gone = {k: v for k, v in value.items() if k not in new[key]}
                if gone:
                    report.replaced.append(f"{where}{key}: {CarryOver._describe(gone)}")
            continue
        if key not in new:
            new[key] = value
            report.kept.append(f"{where}{key} = {_describe_value(value)}")


def carry_over_existing(
    new_content: str, existing_content: str
) -> tuple[str, CarryOver]:
    """The wizard's configuration with what it does not ask about carried over
    from the configuration it is about to replace.

    Volumes are matched by path and targets by path within their volume. A
    matched one keeps every option the wizard does not ask about; the
    wizard's answers win for everything it does. A volume or target in the
    existing file that the new one does not have is reported as removed with
    its options, since there is nothing to carry them onto. The result is
    written with ``__util__.dump_toml``; comments in the existing file are
    not kept.
    """
    import tomllib

    old = tomllib.loads(existing_content)
    new = tomllib.loads(new_content)
    report = CarryOver()

    old_global = dict(old.get("global", {}))
    new_global = new.setdefault("global", {})
    old_notifications = old_global.pop("notifications", None)
    _carry_keys(old_global, new_global, _WIZARD_GLOBAL_KEYS, "global.", report)
    if isinstance(old_notifications, dict):
        new_notifications = new_global.setdefault("notifications", {})
        _carry_keys(
            old_notifications,
            new_notifications,
            _WIZARD_NOTIFICATION_KEYS,
            "global.notifications.",
            report,
        )
        if not new_notifications:
            del new_global["notifications"]

    for key, value in old.items():
        if key not in ("global", "volumes") and key not in new:
            new[key] = value
            report.kept.append(f"{key} = {_describe_value(value)}")

    new_volumes = {v.get("path"): v for v in new.get("volumes", [])}
    for old_volume in old.get("volumes", []):
        vpath = old_volume.get("path")
        volume = new_volumes.get(vpath)
        if volume is None:
            count = len(old_volume.get("targets", []))
            report.removed.append(
                f"volume {vpath} and its {count} target(s): it is not in the new "
                f"configuration"
            )
            continue
        old_targets = old_volume.get("targets", [])
        _carry_keys(
            {k: v for k, v in old_volume.items() if k != "targets"},
            volume,
            _WIZARD_VOLUME_KEYS,
            f"volume {vpath}: ",
            report,
        )
        new_targets = {t.get("path"): t for t in volume.get("targets", [])}
        for old_target in old_targets:
            tpath = old_target.get("path")
            target = new_targets.get(tpath)
            if target is None:
                extra = {k: v for k, v in old_target.items() if k != "path"}
                report.removed.append(
                    f"target {tpath} of volume {vpath}: it is not in the new "
                    f"configuration"
                    + (f" ({CarryOver._describe(extra)})" if extra else "")
                )
                continue
            _carry_keys(
                old_target,
                target,
                _WIZARD_TARGET_KEYS,
                f"target {tpath}: ",
                report,
            )

    header = (
        "# btrfs-backup-ng configuration\n"
        "# Written by the config wizard. Options it does not ask about were kept\n"
        "# from the configuration this file replaced.\n"
    )
    return __util__.dump_toml(new, header=header), report


def _show_carry_over(report: CarryOver) -> None:
    """Say, before anything is written, what the save keeps and what it drops."""
    if report.kept:
        console.print()
        console.print(
            "[bold]Kept from the existing configuration[/bold] "
            "(the wizard does not ask about these):"
        )
        for line in report.kept:
            console.print(f"  {line}")
    if report.replaced:
        console.print()
        console.print("[bold]Replaced by your answers:[/bold]")
        for line in report.replaced:
            console.print(f"  {line}")
    if report.removed:
        console.print()
        console.print("[bold yellow]Removed:[/bold yellow]")
        for line in report.removed:
            console.print(f"  {line}")


def _content_for_overwrite(content: str, path: Path) -> str | None:
    """The configuration to write over ``path``: the wizard's, with what it
    does not ask about carried over from ``path``, after saying what that
    keeps and removes. None when the existing file cannot be read as TOML --
    it is then written as the wizard produced it, and the caller's overwrite
    prompt is the operator's decision, as before."""
    try:
        existing = path.read_text(encoding="utf-8")
    except OSError as e:
        console.print(
            f"[yellow]Could not read {path} to keep its options: {e}[/yellow]"
        )
        return None
    try:
        merged, report = carry_over_existing(content, existing)
    except Exception as e:  # noqa: BLE001 - reported; the operator still decides
        console.print(
            f"[yellow]{path} is not a configuration this wizard can read ({e}); "
            f"nothing can be kept from it, and saving replaces it entirely.[/yellow]"
        )
        return None
    reason = _unloadable_reason(merged)
    if reason is not None:
        console.print(
            f"[yellow]Keeping the existing options would produce a configuration "
            f"that does not load ({reason}); saving writes the wizard's own "
            f"instead, and replaces them.[/yellow]"
        )
        return None
    _show_carry_over(report)
    return merged


def _confirm_overwrite(content: str, path: Path, question: str) -> str | None:
    """Ask before the wizard's configuration replaces ``path``; return what
    to write, or None when the operator declines.

    What is written is the wizard's configuration with the options it does
    not ask about carried over from ``path`` (``_content_for_overwrite``),
    and the question is asked only after the operator has been told what the
    save keeps and what it removes. Every save of a wizard configuration over
    an existing file goes through here.
    """
    content = _content_for_overwrite(content, path) or content
    if not prompt_bool(question, False):
        return None
    return content


def _run_interactive_wizard() -> str:
    """Run interactive configuration wizard and return TOML content."""
    display_wizard_header(
        "btrfs-backup-ng Configuration Wizard",
        "This wizard will help you create a configuration file.\n"
        "Press Ctrl+C at any time to cancel.",
    )

    config_data: dict[str, Any] = {}

    # Global settings
    display_section_header("Global Settings")

    config_data["snapshot_dir"] = prompt("Snapshot directory name", ".snapshots")

    config_data["timestamp_format"] = prompt("Timestamp format", "%Y%m%d-%H%M%S")

    config_data["incremental"] = prompt_bool(
        "Use incremental transfers by default?", True
    )

    config_data["log_file"] = prompt("Log file path (leave empty to disable)", "")

    config_data["transaction_log"] = prompt(
        "Transaction log path (leave empty to disable)", ""
    )

    # Parallelism
    console.print()
    display_section_header("Parallelism Settings")

    config_data["parallel_volumes"] = prompt_int("Max parallel volumes", 2, 1, 16)

    config_data["parallel_targets"] = prompt_int(
        "Max parallel targets per volume", 3, 1, 16
    )

    # Retention policy
    console.print()
    display_section_header("Retention Policy")
    console.print()
    console.print("Configure how long to keep snapshots. Set to 0 to disable.")

    retention: dict[str, str | int] = {}
    retention["min"] = prompt("Minimum retention period", "1d")
    retention["hourly"] = prompt_int("Hourly snapshots to keep", 24, 0, 1000)
    retention["daily"] = prompt_int("Daily snapshots to keep", 7, 0, 1000)
    retention["weekly"] = prompt_int("Weekly snapshots to keep", 4, 0, 1000)
    retention["monthly"] = prompt_int("Monthly snapshots to keep", 12, 0, 1000)
    retention["yearly"] = prompt_int("Yearly snapshots to keep", 0, 0, 1000)
    config_data["retention"] = retention

    # Notifications
    console.print()
    display_section_header("Notifications")

    if prompt_bool("Configure email notifications?", False):
        email: dict[str, Any] = {"enabled": True}
        email["smtp_host"] = prompt("SMTP host", "smtp.example.com")
        email["smtp_port"] = prompt_int("SMTP port", 587, 1, 65535)
        email["smtp_tls"] = prompt_choice(
            "SMTP security", ["starttls", "ssl", "none"], "starttls"
        )
        email["smtp_user"] = prompt("SMTP username (leave empty if none)", "")
        if email["smtp_user"]:
            email["smtp_password"] = prompt("SMTP password", "")
        email["from_addr"] = prompt("From address", "")
        to_addrs_str = prompt("To addresses (comma-separated)", "")
        if to_addrs_str:
            email["to_addrs"] = [
                a.strip() for a in to_addrs_str.split(",") if a.strip()
            ]
        email["on_success"] = prompt_bool("Notify on success?", False)
        email["on_failure"] = prompt_bool("Notify on failure?", True)
        config_data["email"] = email

    if prompt_bool("Configure webhook notifications?", False):
        webhook: dict[str, Any] = {"enabled": True}
        webhook["url"] = prompt("Webhook URL", "")
        webhook["method"] = prompt_choice("HTTP method", ["POST", "GET", "PUT"], "POST")
        webhook["on_success"] = prompt_bool("Notify on success?", False)
        webhook["on_failure"] = prompt_bool("Notify on failure?", True)
        config_data["webhook"] = webhook

    # Volumes
    console.print()
    display_section_header("Volumes to Backup")

    volumes: list[dict[str, Any]] = []
    add_volume = True

    while add_volume:
        console.print()
        console.print(f"  [bold]Volume #{len(volumes) + 1}[/bold]")

        volume_path = prompt("Volume path (e.g., /home)", "")
        if not volume_path:
            if not volumes:
                console.print("  [yellow]At least one volume is required.[/yellow]")
                continue
            break

        # Generate default prefix from path (with trailing dash for readable snapshot names)
        default_prefix = (Path(volume_path).name or "root") + "-"
        snapshot_prefix = prompt_snapshot_prefix(default_prefix)

        volume: dict[str, Any] = {
            "path": volume_path,
            "snapshot_prefix": snapshot_prefix,
            "targets": [],
        }

        # Targets for this volume
        console.print()
        console.print("  [bold]Add backup targets for this volume:[/bold]")

        add_target = True
        while add_target:
            target_path = prompt("  Target path (local or ssh://user@host:/path)", "")
            if not target_path:
                if not volume["targets"]:
                    console.print(
                        "  [yellow]At least one target is required per volume.[/yellow]"
                    )
                    continue
                break

            target: dict[str, Any] = {"path": target_path}

            if target_path.startswith("ssh://"):
                target["ssh_sudo"] = prompt_bool("  Use sudo on remote host?", False)
            elif target_path.startswith("/mnt/") or "usb" in target_path.lower():
                if prompt_bool("  Require mount check (for external drives)?", True):
                    target["require_mount"] = _derive_require_mount(target_path)

            # Encryption is offered only for raw:// / raw+ssh:// targets.
            target.update(_prompt_target_encryption(target_path))

            volume["targets"].append(target)
            console.print(f"  [green]Added target:[/green] {target_path}")
            add_target = prompt_bool("  Add another target?", False)

        volumes.append(volume)
        console.print(f"  [green]Added volume:[/green] {volume_path}")
        add_volume = prompt_bool("Add another volume?", False)

    config_data["volumes"] = volumes

    # Generate the config
    console.print()
    display_section_header("Configuration Complete")

    return _generate_config_from_wizard(config_data)


def execute_config(args: argparse.Namespace) -> int:
    """Execute the config command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    action = getattr(args, "config_action", None)

    if action == "validate":
        return _validate_config(args)
    elif action == "init":
        return _init_config(args)
    elif action == "import":
        return _import_config(args)
    elif action == "detect":
        return _detect_subvolumes(args)
    elif action == "migrate-systemd":
        return _migrate_systemd(args)
    else:
        print(
            "Usage: btrfs-backup-ng config "
            "<validate|init|import|detect|migrate-systemd>"
        )
        return 1


def _volume_readiness_problems(volume: Any) -> list[str]:
    """Local, cheap reasons this volume could not actually be backed up.

    Deliberately limited to what can be answered on THIS machine without opening
    a connection: does the source exist, is it a directory, is it on btrfs.
    Remote targets are not probed -- that is `doctor`'s job, which is allowed to
    reach the network; `validate` stays fast and offline.

    Returns a list of human-readable problems; empty means "nothing local says
    this cannot work".
    """
    from pathlib import Path

    from .. import __util__

    problems: list[str] = []
    path = Path(str(volume.path))
    if not path.exists():
        problems.append(f"source path does not exist: {path}")
        return problems
    if not path.is_dir():
        problems.append(f"source path is not a directory: {path}")
        return problems
    try:
        if not __util__.is_btrfs(path):
            problems.append(f"source path is not on a btrfs filesystem: {path}")
    except Exception as e:  # noqa: BLE001 - a probe failure is not a verdict
        logger.debug("Could not determine filesystem for %s: %s", path, e)
    return problems


def _validate_config(args: argparse.Namespace) -> int:
    """Validate configuration file."""
    try:
        config_path = find_config_file(getattr(args, "config", None))
        if config_path is None:
            print("No configuration file found.")
            print("Searched locations:")
            print("  ~/.config/btrfs-backup-ng/config.toml")
            print("  /etc/btrfs-backup-ng/config.toml")
            return 1

        print(f"Validating: {config_path}")
        config, warnings = load_config(config_path)

        if warnings:
            print("")
            print("Warnings:")
            for warning in warnings:
                print(f"  - {warning}")

        # Structure is only half the question. A config can parse perfectly and
        # still be unable to back anything up, and "Configuration is valid." was
        # printed for one whose every path was fictional -- measured. The word
        # "valid" is what an operator checks before trusting this tool, so it now
        # covers whether the sources could actually be read.
        enabled = config.get_enabled_volumes()
        if not enabled:
            # A config that backs nothing up is not a usable config, and the
            # readiness loop below cannot say so: it iterates the enabled
            # volumes, so zero of them means zero problems found and the
            # all-clear printed. Reached by an ordinary typo -- `[[volume]]` for
            # `[[volumes]]` parses as an unknown top-level key, the loader warns
            # "No volumes configured", and `validate` answered "All enabled
            # volumes look usable from this machine." and exited 0.
            print("")
            print("This configuration backs nothing up: it has no enabled volumes.")
            if warnings:
                print("The warnings above are the likely reason.")
            print(
                "Check the section names -- a volume is [[volumes]] and a target "
                "is [[volumes.targets]]; a misspelled section parses as an "
                "unknown key and is ignored."
            )
            return 1

        readiness: list[tuple[str, list[str]]] = []
        for volume in enabled:
            problems = _volume_readiness_problems(volume)
            if problems:
                readiness.append((str(volume.path), problems))

        print("")
        print("Configuration syntax and structure: valid.")
        print(f"  Volumes: {len(config.volumes)}")
        print(f"  Enabled: {len(enabled)}")

        total_targets = sum(len(v.targets) for v in config.volumes)
        print(f"  Targets: {total_targets}")

        if readiness:
            print("")
            print("These volumes cannot be backed up from this machine:")
            for path, problems in readiness:
                for problem in problems:
                    print(f"  - {path}: {problem}")
            print("")
            print(
                "The file itself is fine. Checked locally only -- remote targets "
                "are not probed here; run `doctor` for that."
            )
            print("(Exit status 2: the file is valid, this machine cannot run it.)")
            # Distinct from 1. Both situations are "not ready to run", but they
            # need different actions and a caller cannot tell them apart from a
            # single code: 1 means the FILE is wrong and must be edited, 2 means
            # the file is right and this machine is not the one that hosts these
            # volumes -- the normal case when editing a NAS's config on a laptop,
            # or validating in CI. Collapsing them made a correct config fail its
            # own check with nothing wrong.
            return 2

        print("")
        print("All enabled volumes look usable from this machine.")
        return 0

    except ConfigError as e:
        print(f"Configuration error: {e}")
        return 1


def _init_config(args: argparse.Namespace) -> int:
    """Generate example configuration."""
    interactive = getattr(args, "interactive", False)
    output = getattr(args, "output", None)

    if interactive:
        # Check if stdout is a TTY for interactive mode
        if not sys.stdin.isatty():
            print("Error: Interactive mode requires a terminal (TTY)")
            return 1

        try:
            content = _run_interactive_wizard()
        except KeyboardInterrupt:
            print("\nConfiguration cancelled.")
            return 1
    else:
        content = generate_example_config()

    if output:
        if _output_would_clobber(output, force=getattr(args, "force", False)):
            if interactive:
                confirmed = _confirm_overwrite(
                    content, Path(output), f"\nFile {output} exists. Overwrite?"
                )
                if confirmed is None:
                    console.print("[yellow]Aborted.[/yellow]")
                    return 1
                content = confirmed
            else:
                print(
                    f"Error: {output} already exists. "
                    "Re-run with --force to replace it.",
                    file=sys.stderr,
                )
                return 1

        try:
            # Create parent directory if needed
            Path(output).parent.mkdir(parents=True, exist_ok=True)
            with open(output, "w", encoding="utf-8") as f:
                f.write(content)
            if interactive:
                console.print()
                console.print(f"[green]Configuration written to:[/green] {output}")
                console.print()
                display_next_steps(
                    [
                        f"Review: btrfs-backup-ng config validate --config {output}",
                    ]
                )
            else:
                print(f"Example configuration written to: {output}")
        except OSError as e:
            console.print(f"[red]Error writing file:[/red] {e}")
            return 1
    elif interactive:
        # Interactive mode without explicit output - offer to save
        default_path = str(get_default_config_path())

        # Show config preview
        console.print()
        display_config_preview(content)
        console.print()

        save_choice = prompt_choice(
            "What would you like to do with this configuration?",
            ["save", "print", "cancel"],
            "save",
        )

        if save_choice == "cancel":
            console.print("[yellow]Configuration cancelled.[/yellow]")
            return 0

        if save_choice == "print":
            console.print()
            console.print(content)
            return 0

        # Save to file
        save_path = prompt("Save configuration to", default_path)
        save_file = Path(save_path)

        try:
            save_file.parent.mkdir(parents=True, exist_ok=True)

            if save_file.exists():
                confirmed = _confirm_overwrite(
                    content, save_file, f"File {save_path} exists. Overwrite?"
                )
                if confirmed is None:
                    console.print("[yellow]Aborted.[/yellow]")
                    return 1
                content = confirmed

            save_file.write_text(content, encoding="utf-8")
            console.print()
            console.print(f"[green]Configuration saved to:[/green] {save_path}")
            console.print()
            display_next_steps(
                [
                    f"Review:  btrfs-backup-ng config validate -c {save_path}",
                    f"Test:    sudo btrfs-backup-ng run --dry-run -c {save_path}",
                    f"Install: sudo btrfs-backup-ng install -c {save_path}",
                ]
            )
        except OSError as e:
            console.print(f"[red]Error saving configuration:[/red] {e}")
            return 1
    else:
        print(content)

    return 0


def _import_config(args: argparse.Namespace) -> int:
    """Import btrbk configuration."""
    from ..btrbk_import import import_btrbk_config

    btrbk_file = getattr(args, "btrbk_config", None)
    if not btrbk_file:
        print("Error: btrbk configuration file path required")
        return 1

    output = getattr(args, "output", None)
    if _output_would_clobber(output, force=getattr(args, "force", False)):
        print(
            f"Error: {output} already exists. Re-run with --force to replace it.",
            file=sys.stderr,
        )
        return 1

    try:
        toml_content, warnings = import_btrbk_config(btrbk_file)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        print(f"Error parsing btrbk config: {e}")
        return 1

    # Show warnings
    if warnings:
        print("# Conversion warnings:", file=sys.stderr)
        for warning in warnings:
            print(f"#   {warning}", file=sys.stderr)
        print("", file=sys.stderr)

    reason = _unloadable_reason(toml_content)
    if reason is not None:
        print(
            "# Refusing to write: the converted configuration cannot be loaded back",
            file=sys.stderr,
        )
        print(f"#   {reason}", file=sys.stderr)
        print(
            "# The conversion is printed below so nothing is lost. Please report "
            "this with the btrbk config it came from.",
            file=sys.stderr,
        )
        print(toml_content)
        return 1

    # Output TOML
    if output:
        try:
            with open(output, "w", encoding="utf-8") as f:
                f.write(toml_content)
            print(f"Configuration written to: {output}", file=sys.stderr)
            print("Review the file and adjust as needed.", file=sys.stderr)
        except OSError as e:
            print(f"Error writing file: {e}")
            return 1
    else:
        print(toml_content)

    if warnings:
        print("", file=sys.stderr)
        print(f"Conversion complete with {len(warnings)} warning(s).", file=sys.stderr)
        print(
            "Review the warnings above and adjust the configuration.", file=sys.stderr
        )

    return 0


def _detect_subvolumes(args: argparse.Namespace) -> int:
    """Detect btrfs subvolumes on the system.

    Args:
        args: Parsed command line arguments with:
            - json: Output in JSON format
            - wizard: Launch interactive wizard with detected volumes

    Returns:
        Exit code (0 for success, 1 for error)
    """
    import json as json_module

    from ..detection import (
        DetectionError,
        PermissionDeniedError,
        detect_subvolumes,
    )

    json_output = getattr(args, "json", False)
    wizard = getattr(args, "wizard", False)

    # Try full detection first
    try:
        result = detect_subvolumes(allow_partial=False)
    except PermissionDeniedError:
        # Offer fallback for non-root users
        if json_output:
            # In JSON mode, just do partial detection
            result = detect_subvolumes(allow_partial=True)
        else:
            console.print(
                "[yellow]For complete subvolume detection, "
                "root privileges are required.[/yellow]"
            )
            console.print()
            console.print("  Run: [cyan]sudo btrfs-backup-ng config detect[/cyan]")
            console.print()

            if sys.stdin.isatty():
                try:
                    if prompt_bool("Continue with limited detection?", False):
                        result = detect_subvolumes(allow_partial=True)
                    else:
                        return 1
                except KeyboardInterrupt:
                    console.print("\n[yellow]Cancelled.[/yellow]")
                    return 1
            else:
                # Non-interactive, just fail
                return 1
    except DetectionError as e:
        console.print(f"[red]Detection error:[/red] {e}")
        return 1

    # Handle no btrfs filesystems
    if not result.filesystems:
        if json_output:
            print(json_module.dumps(result.to_dict(), indent=2))
        else:
            print("No btrfs filesystems found on this system.")
            print()
            print("btrfs-backup-ng requires btrfs subvolumes to back up.")
            print("You can still create a configuration manually with:")
            print("  btrfs-backup-ng config init")
        return 1

    # JSON output mode
    if json_output:
        print(json_module.dumps(result.to_dict(), indent=2))
        return 0

    # Wizard mode
    if wizard:
        if not sys.stdin.isatty():
            print("Error: Wizard mode requires a terminal (TTY)")
            return 1
        try:
            return _run_detection_wizard(result)
        except KeyboardInterrupt:
            console.print("\n[yellow]Configuration cancelled.[/yellow]")
            return 1

    # Default: Display detection results
    _display_detection_results(result)
    return 0


def _migrate_systemd(args: argparse.Namespace) -> int:
    """Migrate systemd integration from btrbk to btrfs-backup-ng.

    Args:
        args: Parsed command line arguments with:
            - dry_run: If True, only show what would be done

    Returns:
        Exit code (0 for success, 1 for error)
    """
    from ..systemd_utils import get_migration_summary, migrate_from_btrbk

    dry_run = getattr(args, "dry_run", False)

    console.print()
    display_section_header("Systemd Migration")

    summary = get_migration_summary()

    # Show current state
    if summary["btrbk_units"]:
        console.print("btrbk systemd units:")
        for unit in summary["btrbk_units"]:
            status = []
            if unit["enabled"]:
                status.append("enabled")
            if unit["active"]:
                status.append("active")
            status_str = f" ({', '.join(status)})" if status else " (inactive)"
            console.print(f"  [cyan]{unit['name']}[/cyan]{status_str}")
    else:
        console.print("[dim]No btrbk systemd units found.[/dim]")

    console.print()

    if summary["backup_ng_units"]:
        console.print("btrfs-backup-ng systemd units:")
        for unit in summary["backup_ng_units"]:
            status = []
            if unit["enabled"]:
                status.append("enabled")
            if unit["active"]:
                status.append("active")
            status_str = f" ({', '.join(status)})" if status else " (inactive)"
            console.print(f"  [cyan]{unit['name']}[/cyan]{status_str}")
    else:
        console.print("[dim]No btrfs-backup-ng systemd units found.[/dim]")
        console.print("[dim]Install with: btrfs-backup-ng systemd install[/dim]")

    console.print()

    if not summary["migration_needed"]:
        console.print("[green]No migration needed.[/green]")
        console.print("btrbk systemd units are not active.")
        return 0

    if dry_run:
        console.print("[yellow]Dry run mode - no changes will be made.[/yellow]")
        console.print()

    success, messages = migrate_from_btrbk(dry_run=dry_run)

    for msg in messages:
        if msg.startswith("  Error"):
            console.print(f"[red]{msg}[/red]")
        elif msg.startswith("  "):
            console.print(f"[green]{msg}[/green]")
        else:
            console.print(msg)

    console.print()

    if dry_run:
        console.print("[dim]Run without --dry-run to apply changes.[/dim]")
        return 0

    if success:
        console.print("[green]Systemd migration complete![/green]")
        return 0
    else:
        console.print(
            "[yellow]Migration completed with errors. "
            "Check the messages above.[/yellow]"
        )
        return 1


def _display_detection_results(result) -> None:
    """Display detection results in a human-readable format.

    Args:
        result: DetectionResult from detect_subvolumes()
    """
    from ..detection import SubvolumeClass

    print()
    print("=" * 60)
    print("  Btrfs Subvolume Detection Results")
    print("=" * 60)

    if result.is_partial:
        print()
        print(f"  Note: {result.error_message}")
        print("  Results may be incomplete.")

    print()
    print(f"Found {len(result.filesystems)} btrfs filesystem(s)")
    print(f"Found {len(result.subvolumes)} subvolume(s)")
    print()

    # Recommended for backup
    recommended = [s for s in result.suggestions if s.is_recommended]
    if recommended:
        print("-" * 40)
        print("  Recommended for backup:")
        print("-" * 40)
        for i, suggestion in enumerate(recommended, 1):
            sv = suggestion.subvolume
            classification = sv.classification.value.replace("_", " ")
            print(f"  [{i}] {sv.display_path}")
            print(f"      Type: {classification}")
            print(f"      Suggested prefix: {suggestion.suggested_prefix}")
            print()

    # Optional
    optional = [s for s in result.suggestions if not s.is_recommended]
    if optional:
        print("-" * 40)
        print("  Optional (lower priority):")
        print("-" * 40)
        for suggestion in optional:
            sv = suggestion.subvolume
            classification = sv.classification.value.replace("_", " ")
            print(f"  - {sv.display_path} ({classification})")
        print()

    # Excluded
    excluded = result.excluded_subvolumes
    if excluded:
        print("-" * 40)
        print("  Excluded (snapshots/system):")
        print("-" * 40)
        # Group by classification
        snapshots = [
            sv for sv in excluded if sv.classification == SubvolumeClass.SNAPSHOT
        ]
        internal = [
            sv for sv in excluded if sv.classification == SubvolumeClass.INTERNAL
        ]

        if snapshots:
            if len(snapshots) <= 5:
                for sv in snapshots:
                    print(f"  - {sv.path} (snapshot)")
            else:
                print(f"  - {len(snapshots)} snapshot subvolumes")

        if internal:
            for sv in internal:
                print(f"  - {sv.path} (system internal)")
        print()

    # Next steps
    print("-" * 40)
    print("  Next steps:")
    print("-" * 40)
    print("  To create a configuration based on these results:")
    print("    btrfs-backup-ng config detect --wizard")
    print()
    print("  Or create a configuration manually:")
    print("    btrfs-backup-ng config init --interactive")
    print()


def _save_wizard_config(content: str) -> int:
    """Save wizard-generated configuration to file.

    Args:
        content: TOML configuration content

    Returns:
        Exit code
    """
    default_path = str(get_default_config_path())

    # Show config preview
    console.print()
    display_config_preview(content)
    console.print()

    save_choice = prompt_choice(
        "What would you like to do with this configuration?",
        ["save", "print", "cancel"],
        "save",
    )

    if save_choice == "cancel":
        console.print("[yellow]Configuration cancelled.[/yellow]")
        return 0

    if save_choice == "print":
        console.print()
        console.print(content)
        return 0

    reason = _unloadable_reason(content)
    if reason is not None:
        console.print()
        console.print(
            "[red]Refusing to save:[/red] the generated configuration cannot be "
            f"loaded back ({reason})"
        )
        console.print("[yellow]Nothing was written.[/yellow]")
        return 1

    # Save to file
    save_path = prompt("Save configuration to", default_path)
    save_file = Path(save_path)

    try:
        save_file.parent.mkdir(parents=True, exist_ok=True)

        if save_file.exists():
            confirmed = _confirm_overwrite(
                content, save_file, f"File {save_path} exists. Overwrite?"
            )
            if confirmed is None:
                console.print("[yellow]Aborted.[/yellow]")
                return 0
            content = confirmed

        save_file.write_text(content, encoding="utf-8")
        console.print()
        console.print(f"[green]Configuration saved to:[/green] {save_path}")
        console.print()
        display_next_steps(
            [
                f"Review:  btrfs-backup-ng config validate -c {save_path}",
                f"Test:    sudo btrfs-backup-ng run --dry-run -c {save_path}",
                f"Install: sudo btrfs-backup-ng install -c {save_path}",
            ]
        )
    except OSError as e:
        console.print(f"[red]Error saving configuration:[/red] {e}")
        return 1

    return 0


def _run_btrbk_import_wizard(btrbk_path: Path) -> int:
    """Run wizard to import and review btrbk configuration.

    Args:
        btrbk_path: Path to btrbk configuration file

    Returns:
        Exit code
    """
    from ..btrbk_import import import_btrbk_config

    display_wizard_header(
        "btrbk Configuration Import",
        f"Importing from: {btrbk_path}\n"
        "Review and customize the imported configuration.",
    )

    try:
        toml_content, warnings = import_btrbk_config(str(btrbk_path))
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        return 1
    except Exception as e:
        console.print(f"[red]Error parsing btrbk config:[/red] {e}")
        return 1

    # Parse the imported config to display in table format
    try:
        import tomllib

        parsed = tomllib.loads(toml_content)
        volumes = parsed.get("volumes", [])

        # Display imported volumes
        display_btrbk_import(volumes, warnings)
    except Exception:
        # If parsing fails, just show raw content
        if warnings:
            console.print("[yellow]Import warnings:[/yellow]")
            for w in warnings:
                console.print(f"  [yellow]![/yellow] {w}")
            console.print()

    # Ask if user wants to edit
    console.print()
    if prompt_bool("Would you like to review/edit before saving?", True):
        console.print()
        console.print("[dim]The configuration will be shown in preview.[/dim]")
        console.print("[dim]After saving, you can edit the file directly.[/dim]")
        console.print()

    # Save the config first
    result = _save_wizard_config(toml_content)

    if result != 0:
        return result

    # Offer systemd migration
    console.print()
    _offer_systemd_migration()

    return 0


def _offer_systemd_migration() -> None:
    """Offer to migrate systemd integration from btrbk to btrfs-backup-ng."""
    try:
        from ..systemd_utils import get_migration_summary, migrate_from_btrbk
    except ImportError:
        return

    summary = get_migration_summary()

    if not summary["btrbk_active"]:
        return

    console.print()
    display_section_header("Systemd Migration")

    console.print("Active btrbk systemd units detected:")
    for unit in summary["btrbk_units"]:
        if unit["enabled"] or unit["active"]:
            status = []
            if unit["enabled"]:
                status.append("enabled")
            if unit["active"]:
                status.append("active")
            console.print(f"  [cyan]{unit['name']}[/cyan] ({', '.join(status)})")

    console.print()
    console.print(
        "To complete the migration, btrbk's systemd timer should be disabled\n"
        "to prevent both tools from running backups simultaneously."
    )
    console.print()

    if prompt_bool("Disable btrbk systemd timer and enable btrfs-backup-ng?", True):
        console.print()
        success, messages = migrate_from_btrbk(dry_run=False)
        for msg in messages:
            if msg.startswith("  Error"):
                console.print(f"[red]{msg}[/red]")
            elif msg.startswith("  "):
                console.print(f"[green]{msg}[/green]")
            else:
                console.print(msg)

        if success:
            console.print()
            console.print("[green]Systemd migration complete![/green]")
        else:
            console.print()
            console.print(
                "[yellow]Some errors occurred. You may need to manually "
                "disable btrbk timer.[/yellow]"
            )
            console.print("  Run: sudo systemctl disable --now btrbk.timer")
    else:
        console.print()
        console.print("[dim]Skipping systemd migration.[/dim]")
        console.print("[dim]You can migrate later with:[/dim]")
        console.print("  btrfs-backup-ng config migrate-systemd")


def _run_detection_wizard(result) -> int:
    """Run interactive wizard with pre-populated detection results.

    Args:
        result: DetectionResult from detect_subvolumes()

    Returns:
        Exit code
    """
    # Check for btrbk config first - offer migration
    btrbk_path = find_btrbk_config()
    if btrbk_path:
        choice = display_btrbk_detected(btrbk_path)

        if choice == "import":
            # Run btrbk import wizard
            return _run_btrbk_import_wizard(btrbk_path)
        elif choice == "manual":
            # Fall through to manual interactive wizard
            console.print()
            console.print("[dim]Switching to manual configuration...[/dim]")
            console.print()
            content = _run_interactive_wizard()
            return _save_wizard_config(content)
        # else: "detect" - continue with auto-detection below

    # Try to detect snapper configurations
    snapper_configs = []
    snapper_path_map: dict = {}
    try:
        from ..snapper import SnapperScanner
        from ..snapper.scanner import SnapperNotFoundError

        try:
            scanner = SnapperScanner()
            snapper_configs = scanner.list_configs()
        except SnapperNotFoundError:
            pass  # Snapper not installed
    except ImportError:
        pass  # Snapper module not available

    # If we have snapper configs, use them to improve classification
    if snapper_configs:
        from ..detection import reclassify_with_snapper

        snapper_path_map = reclassify_with_snapper(result, snapper_configs)

    display_wizard_header(
        "Detection-based Configuration Wizard",
        "Create a backup configuration based on detected btrfs subvolumes.\n"
        "Press Ctrl+C at any time to cancel.",
    )

    if result.is_partial:
        console.print(f"[yellow]Note:[/yellow] {result.error_message}")
        console.print("[yellow]Results may be incomplete.[/yellow]")
        console.print()

    # Show snapper detection results
    if snapper_configs:
        display_snapper_configs(snapper_configs)
        console.print("  Snapper volumes will be offered for backup below.")
        console.print()

    # Build list of selectable volumes (exclude snapshots and internal)
    selectable = []
    for suggestion in result.suggestions:
        sv = suggestion.subvolume
        # Check if this volume is managed by snapper
        # Try display_path first, then mount_point as fallback
        snapper_cfg = snapper_path_map.get(sv.display_path)
        if snapper_cfg is None and sv.mount_point:
            snapper_cfg = snapper_path_map.get(sv.mount_point)
        selectable.append((suggestion, sv, snapper_cfg))

    if not selectable:
        console.print("[red]No subvolumes suitable for backup were detected.[/red]")
        console.print("You may need to run with sudo for complete detection.")
        return 1

    # Build items for table selection
    table_items = []
    recommended_indices = []
    for i, (suggestion, sv, snapper_cfg) in enumerate(selectable):
        classification = sv.classification.value.replace("_", " ")

        # Build snapper column with indicator for virtual subvolumes
        # (id=0 means detected via snapper, not directly from btrfs)
        if snapper_cfg:
            if sv.id == 0:
                # Virtual subvolume detected via snapper config
                snapper_name = f"{snapper_cfg.name} (rollback)"
            else:
                snapper_name = snapper_cfg.name
        else:
            snapper_name = ""

        table_items.append(
            {
                "path": sv.display_path,
                "type": classification,
                "snapper": snapper_name,
            }
        )
        if suggestion.is_recommended:
            recommended_indices.append(i)

    # Check if any subvolumes are in rollback mode (virtual, id=0)
    has_rollback_volumes = any(sv.id == 0 for _, sv, _ in selectable)

    # Build footer notes
    footer_notes = []
    if has_rollback_volumes:
        footer_notes.append(
            "[dim](rollback)[/dim] = detected via snapper (system booted from snapshot)"
        )

    # Use Rich table selection
    selected_indices = prompt_selection(
        title="Detected Subvolumes",
        items=table_items,
        columns=[
            ("path", "Path"),
            ("type", "Type"),
            ("snapper", "Snapper Config"),
        ],
        default_indices=recommended_indices,
        recommended_indices=recommended_indices,
        footer_notes=footer_notes if footer_notes else None,
    )

    # Build selected volumes list
    selected_volumes = []
    for idx in selected_indices:
        suggestion, _, snapper_cfg = selectable[idx]
        selected_volumes.append((suggestion, snapper_cfg))

    if not selected_volumes:
        console.print("[yellow]No volumes selected. Exiting.[/yellow]")
        return 1

    console.print()
    console.print(
        f"[green]Selected {len(selected_volumes)} volume(s) for backup.[/green]"
    )
    console.print()

    # Step 2: Configure each volume
    config_data: dict[str, Any] = {
        "snapshot_dir": ".snapshots",
        "timestamp_format": "%Y%m%d-%H%M%S",
        "incremental": True,
        "log_file": "",
        "transaction_log": "",
        "parallel_volumes": 2,
        "parallel_targets": 3,
        "retention": {
            "min": "1d",
            "hourly": 24,
            "daily": 7,
            "weekly": 4,
            "monthly": 12,
            "yearly": 0,
        },
        "volumes": [],
    }

    for suggestion, snapper_cfg in selected_volumes:
        sv = suggestion.subvolume
        if snapper_cfg:
            display_section_header(
                f"Volume: {sv.display_path} (snapper: {snapper_cfg.name})"
            )
        else:
            display_section_header(f"Volume: {sv.display_path}")

        volume: dict[str, Any] = {
            "path": sv.display_path,
            "targets": [],
        }

        # Configure based on whether this is a snapper volume
        if snapper_cfg:
            # Snapper-sourced volume
            console.print("  This volume is managed by snapper.")
            use_snapper = prompt_bool("  Use snapper as snapshot source?", True)

            if use_snapper:
                volume["source"] = "snapper"
                volume["snapper"] = {
                    "config_name": snapper_cfg.name,
                    "include_types": ["single"],  # Default to single, skip pre/post
                    "min_age": "1h",
                }
                console.print(
                    f"  [green]Configured to use snapper config '{snapper_cfg.name}'[/green]"
                )
            else:
                # User wants native management instead
                volume["snapshot_prefix"] = prompt_snapshot_prefix(
                    suggestion.suggested_prefix
                )
        else:
            # Native volume
            volume["snapshot_prefix"] = prompt_snapshot_prefix(
                suggestion.suggested_prefix
            )

        # Add targets
        console.print()
        console.print("  [bold]Add backup target(s) for this volume:[/bold]")
        add_target = True
        while add_target:
            target_path = prompt(
                "  Target path (local path or ssh://user@host:/path)", ""
            )
            if not target_path:
                if not volume["targets"]:
                    console.print(
                        "  [yellow]At least one target is required per volume.[/yellow]"
                    )
                    continue
                break

            target: dict[str, Any] = {"path": target_path}

            if target_path.startswith("ssh://"):
                target["ssh_sudo"] = prompt_bool("  Use sudo on remote host?", False)
            elif target_path.startswith("/mnt/") or "usb" in target_path.lower():
                if prompt_bool("  Require mount check (for external drives)?", True):
                    target["require_mount"] = _derive_require_mount(target_path)

            # Encryption is offered only for raw:// / raw+ssh:// targets.
            target.update(_prompt_target_encryption(target_path))

            volume["targets"].append(target)
            console.print(f"  [green]Added target:[/green] {target_path}")
            add_target = prompt_bool("  Add another target?", False)

        config_data["volumes"].append(volume)
        console.print()

    # Step 3: Global settings (optional)
    display_section_header("Global Settings")

    if prompt_bool("Configure global settings (retention, notifications)?", False):
        # Retention
        console.print()
        console.print("  [bold]Retention Policy:[/bold]")
        retention = config_data["retention"]
        retention["min"] = prompt("  Minimum retention period", "1d")
        retention["hourly"] = prompt_int("  Hourly snapshots to keep", 24, 0, 1000)
        retention["daily"] = prompt_int("  Daily snapshots to keep", 7, 0, 1000)
        retention["weekly"] = prompt_int("  Weekly snapshots to keep", 4, 0, 1000)
        retention["monthly"] = prompt_int("  Monthly snapshots to keep", 12, 0, 1000)
        retention["yearly"] = prompt_int("  Yearly snapshots to keep", 0, 0, 1000)

        # Email notifications
        console.print()
        if prompt_bool("  Configure email notifications?", False):
            email: dict[str, Any] = {"enabled": True}
            email["smtp_host"] = prompt("  SMTP host", "smtp.example.com")
            email["smtp_port"] = prompt_int("  SMTP port", 587, 1, 65535)
            email["smtp_tls"] = prompt_choice(
                "  SMTP security", ["starttls", "ssl", "none"], "starttls"
            )
            email["smtp_user"] = prompt("  SMTP username (leave empty if none)", "")
            if email["smtp_user"]:
                email["smtp_password"] = prompt("  SMTP password", "")
            email["from_addr"] = prompt("  From address", "")
            to_addrs_str = prompt("  To addresses (comma-separated)", "")
            if to_addrs_str:
                email["to_addrs"] = [
                    a.strip() for a in to_addrs_str.split(",") if a.strip()
                ]
            email["on_success"] = prompt_bool("  Notify on success?", False)
            email["on_failure"] = prompt_bool("  Notify on failure?", True)
            config_data["email"] = email
    else:
        console.print(
            "  [dim]Using default settings (can be changed later in config file).[/dim]"
        )

    console.print()

    # Step 4: Generate config
    new_config = _generate_config_from_wizard(config_data)

    # Step 5: Check for existing config and show diff if needed
    existing_config_path = find_config_file(None)
    existing_content = None

    if existing_config_path:
        try:
            existing_content = Path(existing_config_path).read_text()
        except OSError:
            existing_content = None

    if existing_content:
        display_section_header("Existing Configuration Found")
        console.print(f"  Found: [cyan]{existing_config_path}[/cyan]")
        console.print()

        # Offer diff view
        view_diff = prompt_bool("View changes compared to existing config?", True)
        if view_diff:
            diff_format = prompt_choice(
                "Diff format",
                ["summary", "text"],
                "summary",
            )
            console.print()
            # Against what saving over that file would write: the options the
            # wizard does not ask about are kept, so they are not changes.
            try:
                compared, _report = carry_over_existing(new_config, existing_content)
            except Exception:  # noqa: BLE001 - an unreadable file compares as-is
                compared = new_config
            if diff_format == "summary":
                _show_config_diff_summary(existing_content, compared, config_data)
            else:
                _show_config_diff_text(existing_content, compared)
            console.print()

    # Step 6: Save options
    display_section_header("Save Configuration")

    # Show config preview
    display_config_preview(new_config)
    console.print()

    save_choice = prompt_choice(
        "What would you like to do?",
        ["save", "print", "cancel"],
        "save",
    )

    if save_choice == "cancel":
        console.print()
        console.print("[yellow]Configuration cancelled.[/yellow]")
        return 0

    if save_choice == "print":
        console.print()
        console.print(new_config)
        return 0

    # Save to file
    if existing_config_path:
        default_path = str(existing_config_path)
        console.print()
        console.print(f"  Existing config: [cyan]{existing_config_path}[/cyan]")
        save_path = prompt(
            "Save to (enter new path to keep existing, or same to overwrite)",
            default_path,
        )
    else:
        default_path = str(get_default_config_path())
        save_path = prompt("Save configuration to", default_path)

    # Create directory if needed
    save_file = Path(save_path)
    try:
        save_file.parent.mkdir(parents=True, exist_ok=True)

        # Check for overwrite
        if save_file.exists():
            confirmed = _confirm_overwrite(
                new_config, save_file, f"Overwrite {save_path}?"
            )
            if confirmed is None:
                console.print("[yellow]Save cancelled.[/yellow]")
                return 0
            new_config = confirmed

        save_file.write_text(new_config, encoding="utf-8")
        console.print()
        console.print(f"[green]Configuration saved to:[/green] {save_path}")
        console.print()

        display_next_steps(
            [
                f"Review: btrfs-backup-ng config validate -c {save_path}",
                f"Test:   btrfs-backup-ng run --dry-run -c {save_path}",
                f"Install timer: btrfs-backup-ng install -c {save_path}",
            ]
        )

    except OSError as e:
        console.print(f"[red]Error saving configuration:[/red] {e}")
        return 1

    return 0


def _show_config_diff_summary(
    existing: str, new: str, config_data: dict[str, Any]
) -> None:
    """Show a human-friendly summary of config changes.

    Args:
        existing: Existing config content
        new: New config content
        config_data: Parsed config data from wizard
    """
    print("  Changes:")
    print()

    # Parse existing config to compare
    try:
        import tomllib

        existing_parsed = tomllib.loads(existing)
    except Exception:
        print("  (Could not parse existing config for comparison)")
        print("  New configuration will replace existing.")
        return

    # Compare volumes
    existing_volumes = {v.get("path"): v for v in existing_parsed.get("volumes", [])}
    new_volumes = {v["path"]: v for v in config_data.get("volumes", [])}

    # Added volumes
    for path in new_volumes:
        if path not in existing_volumes:
            prefix = new_volumes[path].get("snapshot_prefix", "")
            targets = len(new_volumes[path].get("targets", []))
            print(f"  + Add volume: {path}")
            print(f"      prefix: {prefix}, targets: {targets}")

    # Removed volumes
    for path in existing_volumes:
        if path not in new_volumes:
            print(f"  - Remove volume: {path}")

    # Modified volumes
    for path in new_volumes:
        if path in existing_volumes:
            old = existing_volumes[path]
            new_v = new_volumes[path]
            changes = []

            if old.get("snapshot_prefix") != new_v.get("snapshot_prefix"):
                changes.append(
                    f"prefix: {old.get('snapshot_prefix')} -> "
                    f"{new_v.get('snapshot_prefix')}"
                )

            old_targets = len(old.get("targets", []))
            new_targets = len(new_v.get("targets", []))
            if old_targets != new_targets:
                changes.append(f"targets: {old_targets} -> {new_targets}")

            if changes:
                print(f"  ~ Modify volume: {path}")
                for change in changes:
                    print(f"      {change}")

    # Compare retention
    old_retention = existing_parsed.get("global", {}).get("retention", {})
    new_retention = config_data.get("retention", {})

    retention_changes = []
    for key in ["min", "hourly", "daily", "weekly", "monthly", "yearly"]:
        old_val = old_retention.get(key)
        new_val = new_retention.get(key)
        if old_val != new_val and new_val is not None:
            retention_changes.append(f"{key}: {old_val} -> {new_val}")

    if retention_changes:
        print("  ~ Modify retention:")
        for change in retention_changes:
            print(f"      {change}")

    # Check for notification changes
    if config_data.get("email") and not existing_parsed.get("global", {}).get(
        "notifications", {}
    ).get("email"):
        print("  + Add email notifications")

    if config_data.get("webhook") and not existing_parsed.get("global", {}).get(
        "notifications", {}
    ).get("webhook"):
        print("  + Add webhook notifications")


def _show_config_diff_text(existing: str, new: str) -> None:
    """Show a text-based diff of config changes.

    Args:
        existing: Existing config content
        new: New config content
    """
    import difflib

    existing_lines = existing.splitlines(keepends=True)
    new_lines = new.splitlines(keepends=True)

    diff = difflib.unified_diff(
        existing_lines,
        new_lines,
        fromfile="existing config",
        tofile="new config",
        lineterm="",
    )

    diff_output = list(diff)
    if not diff_output:
        print("  No differences detected.")
        return

    print("  " + "-" * 38)
    for line in diff_output:
        line = line.rstrip("\n")
        if line.startswith("+") and not line.startswith("+++"):
            print(f"  {line}")
        elif line.startswith("-") and not line.startswith("---"):
            print(f"  {line}")
        elif line.startswith("@@"):
            print(f"  {line}")
        elif line.startswith("---") or line.startswith("+++"):
            print(f"  {line}")
    print("  " + "-" * 38)
