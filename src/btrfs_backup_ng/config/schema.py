"""Configuration schema definitions using dataclasses.

Defines the structure for TOML configuration with sensible defaults.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class EmailNotificationConfig:
    """Email notification configuration.

    Attributes:
        enabled: Whether email notifications are enabled
        smtp_host: SMTP server hostname
        smtp_port: SMTP server port (465 for SSL, 587 for STARTTLS, 25 for plain)
        smtp_user: SMTP authentication username (optional)
        smtp_password: SMTP authentication password (optional)
        smtp_tls: TLS mode: "ssl" (implicit), "starttls" (explicit), or "none"
        from_addr: Sender email address
        to_addrs: List of recipient email addresses
        on_success: Send notification on successful backup
        on_failure: Send notification on failed backup
    """

    enabled: bool = False
    smtp_host: str = "localhost"
    smtp_port: int = 25
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_tls: str = "none"
    from_addr: str = "btrfs-backup-ng@localhost"
    to_addrs: list[str] = field(default_factory=list)
    on_success: bool = False
    on_failure: bool = True


@dataclass
class WebhookNotificationConfig:
    """Webhook notification configuration.

    Attributes:
        enabled: Whether webhook notifications are enabled
        url: Webhook URL to POST to
        method: HTTP method (POST or PUT)
        headers: Additional headers to send
        on_success: Send notification on successful backup
        on_failure: Send notification on failed backup
        timeout: Request timeout in seconds
    """

    enabled: bool = False
    url: Optional[str] = None
    method: str = "POST"
    headers: dict[str, str] = field(default_factory=dict)
    on_success: bool = False
    on_failure: bool = True
    timeout: int = 30


@dataclass
class NotificationConfig:
    """Combined notification configuration.

    Attributes:
        email: Email notification settings
        webhook: Webhook notification settings
    """

    email: EmailNotificationConfig = field(default_factory=EmailNotificationConfig)
    webhook: WebhookNotificationConfig = field(
        default_factory=WebhookNotificationConfig
    )

    def is_enabled(self) -> bool:
        """Check if any notification method is enabled."""
        return self.email.enabled or self.webhook.enabled


@dataclass
class RetentionConfig:
    """Retention policy configuration.

    Attributes:
        min: Minimum retention period (e.g., "1d", "2h", "30m")
        hourly: Number of hourly snapshots to keep
        daily: Number of daily snapshots to keep
        weekly: Number of weekly snapshots to keep
        monthly: Number of monthly snapshots to keep
        yearly: Number of yearly snapshots to keep
    """

    min: str = "1d"
    hourly: int = 24
    daily: int = 7
    weekly: int = 4
    monthly: int = 12
    yearly: int = 0


@dataclass
class SnapperSourceConfig:
    """Snapper source configuration.

    When a volume uses snapper as its snapshot source, this configures
    how snapper snapshots are discovered and filtered.

    Attributes:
        config_name: Snapper config name (e.g., 'root', 'home') or 'auto' to detect
        include_types: Snapshot types to include ('single', 'pre', 'post')
        exclude_cleanup: Cleanup algorithms to exclude (e.g., 'number' for transient)
        min_age: Minimum age before backing up (e.g., '1h', '30m') to avoid
                 backing up incomplete pre/post pairs
    """

    config_name: str = "auto"
    include_types: list[str] = field(default_factory=lambda: ["single", "pre", "post"])
    exclude_cleanup: list[str] = field(default_factory=list)
    min_age: str = "0"


@dataclass
class TargetConfig:
    """Backup target configuration.

    Attributes:
        path: Target path (local path or ssh://user@host:/path)
        ssh_sudo: Whether to use sudo on remote SSH targets
        ssh_port: SSH port for remote targets
        ssh_key: Path to SSH private key
        ssh_password_auth: Allow password authentication fallback
        compress: Compression algorithm for transfers (none, gzip, zstd, lz4)
        rate_limit: Bandwidth limit for transfers (e.g., "10M", "1G", "500K")
        require_mount: Require path to be an active mount point (safety check for external drives)
    """

    path: str
    ssh_sudo: bool = False
    ssh_port: int = 22
    ssh_key: Optional[str] = None
    ssh_password_auth: bool = True
    compress: str = "none"
    rate_limit: Optional[str] = None
    require_mount: bool = False


@dataclass
class RawTargetConfig:
    """Raw file target configuration for non-btrfs destinations.

    Raw targets write btrfs send streams directly to files instead of using
    'btrfs receive'. This enables backups to non-btrfs filesystems (NFS, SMB,
    cloud storage) with optional compression and GPG encryption.

    Compatible with btrbk's "raw target" feature for migration.

    Attributes:
        path: Output directory for stream files (local path or ssh://user@host:/path)
        compress: Compression algorithm (gzip, zstd, lz4, xz, lzo, pigz, pbzip2, or none)
        encrypt: Encryption method (gpg or none)
        gpg_recipient: GPG key recipient (required when encrypt=gpg)
        gpg_keyring: Optional path to GPG keyring file
        ssh_sudo: Whether to use sudo on remote SSH targets
        ssh_port: SSH port for remote targets
        ssh_key: Path to SSH private key
    """

    path: str
    compress: str = "none"
    encrypt: str = "none"
    gpg_recipient: Optional[str] = None
    gpg_keyring: Optional[str] = None
    ssh_sudo: bool = False
    ssh_port: int = 22
    ssh_key: Optional[str] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.encrypt == "gpg" and not self.gpg_recipient:
            raise ValueError("gpg_recipient is required when encrypt=gpg")

        valid_compress = {
            "none",
            "gzip",
            "zstd",
            "lz4",
            "xz",
            "lzo",
            "pigz",
            "pbzip2",
            "bzip2",
        }
        if self.compress not in valid_compress:
            raise ValueError(
                f"Invalid compression: {self.compress}. Valid: {sorted(valid_compress)}"
            )

        valid_encrypt = {"none", "gpg"}
        if self.encrypt not in valid_encrypt:
            raise ValueError(
                f"Invalid encryption: {self.encrypt}. Valid: {sorted(valid_encrypt)}"
            )


@dataclass
class VolumeConfig:
    """Volume backup configuration.

    Attributes:
        path: Path to the btrfs subvolume to back up
        snapshot_prefix: Prefix for snapshot names
        snapshot_dir: Directory to store snapshots (relative to volume or absolute)
        targets: List of backup targets for this volume
        retention: Volume-specific retention policy (overrides global)
        enabled: Whether this volume is enabled for backup
        source: Snapshot source type: 'native' (btrfs-backup-ng managed) or 'snapper'
        snapper: Snapper-specific configuration when source='snapper'
    """

    path: str
    snapshot_prefix: str = ""
    snapshot_dir: str = ".snapshots"
    targets: list[TargetConfig] = field(default_factory=list)
    retention: Optional[RetentionConfig] = None
    enabled: bool = True
    source: str = "native"
    snapper: Optional[SnapperSourceConfig] = None

    def __post_init__(self):
        # Generate default prefix from path if not specified
        if not self.snapshot_prefix:
            # /home -> home-, /var/log -> var-log- (trailing dash for readable snapshot names)
            base = self.path.strip("/").replace("/", "-") or "root"
            self.snapshot_prefix = base + "-"

    def is_snapper_source(self) -> bool:
        """Check if this volume uses snapper as its snapshot source."""
        return self.source == "snapper"


@dataclass
class GlobalConfig:
    """Global configuration settings.

    Attributes:
        snapshot_dir: Default snapshot directory for all volumes
        timestamp_format: Format string for snapshot timestamps
        incremental: Whether to use incremental transfers by default
        log_file: Path to log file (None for no file logging)
        transaction_log: Path to JSON transaction log for auditing
        retention: Default retention policy
        notifications: Notification settings (email, webhook)
        parallel_volumes: Max concurrent volume backups
        parallel_targets: Max concurrent target transfers per volume
        quiet: Suppress non-essential output
        verbose: Enable verbose output
    """

    snapshot_dir: str = ".snapshots"
    timestamp_format: str = "%Y%m%d-%H%M%S"
    incremental: bool = True
    log_file: Optional[str] = None
    transaction_log: Optional[str] = None
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)
    parallel_volumes: int = 2
    parallel_targets: int = 3
    quiet: bool = False
    verbose: bool = False


@dataclass
class Config:
    """Root configuration object.

    Attributes:
        global_config: Global settings that apply to all volumes
        volumes: List of volume configurations
    """

    global_config: GlobalConfig = field(default_factory=GlobalConfig)
    volumes: list[VolumeConfig] = field(default_factory=list)

    def get_effective_retention(self, volume: VolumeConfig) -> RetentionConfig:
        """Get the effective retention policy for a volume.

        Volume-specific retention overrides global retention.
        """
        return volume.retention or self.global_config.retention

    def get_enabled_volumes(self) -> list[VolumeConfig]:
        """Get list of enabled volumes."""
        return [v for v in self.volumes if v.enabled]
