"""Notification system for backup events.

Supports email (SMTP) and webhook notifications for backup success/failure.
"""

import json
import logging
import smtplib
import ssl
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Literal, Optional

logger = logging.getLogger(__name__)


@dataclass
class NotificationEvent:
    """Represents a backup event to notify about.

    Attributes:
        event_type: Type of event (backup_complete, backup_failed, prune_complete, etc.)
        status: Overall status (success, failure, partial)
        timestamp: When the event occurred
        hostname: Machine hostname
        summary: Brief summary of what happened
        details: Detailed information (optional)
        volumes_processed: Number of volumes processed
        volumes_failed: Number of volumes that failed
        snapshots_created: Number of snapshots created
        transfers_completed: Number of transfers completed
        transfers_failed: Number of transfers that failed
        snapshots_pruned: Number of snapshots pruned
        duration_seconds: How long the operation took
        errors: List of error messages
    """

    event_type: str
    status: Literal["success", "failure", "partial"]
    timestamp: str
    hostname: str
    summary: str
    details: Optional[str] = None
    volumes_processed: int = 0
    volumes_failed: int = 0
    snapshots_created: int = 0
    transfers_completed: int = 0
    transfers_failed: int = 0
    snapshots_pruned: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    def to_text(self) -> str:
        """Format as plain text for email body."""
        lines = [
            "btrfs-backup-ng Notification",
            "=" * 40,
            "",
            f"Event: {self.event_type}",
            f"Status: {self.status.upper()}",
            f"Time: {self.timestamp}",
            f"Host: {self.hostname}",
            "",
            f"Summary: {self.summary}",
        ]

        if self.details:
            lines.extend(["", f"Details: {self.details}"])

        lines.extend(
            [
                "",
                "Statistics:",
                f"  Volumes processed: {self.volumes_processed}",
                f"  Volumes failed: {self.volumes_failed}",
            ]
        )

        if self.snapshots_created:
            lines.append(f"  Snapshots created: {self.snapshots_created}")
        if self.transfers_completed or self.transfers_failed:
            lines.append(f"  Transfers completed: {self.transfers_completed}")
            lines.append(f"  Transfers failed: {self.transfers_failed}")
        if self.snapshots_pruned:
            lines.append(f"  Snapshots pruned: {self.snapshots_pruned}")

        if self.duration_seconds:
            lines.append(f"  Duration: {self.duration_seconds:.1f}s")

        if self.errors:
            lines.extend(["", "Errors:"])
            for err in self.errors:
                lines.append(f"  - {err}")

        return "\n".join(lines)

    def to_html(self) -> str:
        """Format as HTML for rich email body."""
        status_color = {
            "success": "#28a745",
            "failure": "#dc3545",
            "partial": "#ffc107",
        }.get(self.status, "#6c757d")

        errors_html = ""
        if self.errors:
            error_items = "".join(f"<li>{err}</li>" for err in self.errors)
            errors_html = f"""
            <h3 style="color: #dc3545;">Errors</h3>
            <ul>{error_items}</ul>
            """

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .header {{ background: {status_color}; color: white; padding: 20px; border-radius: 8px 8px 0 0; }}
                .content {{ padding: 20px; }}
                .stats {{ background: #f8f9fa; padding: 15px; border-radius: 4px; margin: 15px 0; }}
                .stats table {{ width: 100%; border-collapse: collapse; }}
                .stats td {{ padding: 5px 10px; }}
                .stats td:first-child {{ font-weight: bold; width: 60%; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1 style="margin: 0;">btrfs-backup-ng</h1>
                    <p style="margin: 10px 0 0 0; opacity: 0.9;">{self.event_type} - {self.status.upper()}</p>
                </div>
                <div class="content">
                    <p><strong>Host:</strong> {self.hostname}</p>
                    <p><strong>Time:</strong> {self.timestamp}</p>
                    <h3>Summary</h3>
                    <p>{self.summary}</p>
                    {f"<p>{self.details}</p>" if self.details else ""}

                    <div class="stats">
                        <h3 style="margin-top: 0;">Statistics</h3>
                        <table>
                            <tr><td>Volumes processed</td><td>{self.volumes_processed}</td></tr>
                            <tr><td>Volumes failed</td><td>{self.volumes_failed}</td></tr>
                            {"<tr><td>Snapshots created</td><td>" + str(self.snapshots_created) + "</td></tr>" if self.snapshots_created else ""}
                            {"<tr><td>Transfers completed</td><td>" + str(self.transfers_completed) + "</td></tr>" if self.transfers_completed or self.transfers_failed else ""}
                            {"<tr><td>Transfers failed</td><td>" + str(self.transfers_failed) + "</td></tr>" if self.transfers_completed or self.transfers_failed else ""}
                            {"<tr><td>Snapshots pruned</td><td>" + str(self.snapshots_pruned) + "</td></tr>" if self.snapshots_pruned else ""}
                            <tr><td>Duration</td><td>{self.duration_seconds:.1f}s</td></tr>
                        </table>
                    </div>
                    {errors_html}
                </div>
            </div>
        </body>
        </html>
        """


@dataclass
class EmailConfig:
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
    smtp_tls: str = "none"  # "ssl", "starttls", or "none"
    from_addr: str = "btrfs-backup-ng@localhost"
    to_addrs: list[str] = None
    on_success: bool = False
    on_failure: bool = True

    def __post_init__(self):
        if self.to_addrs is None:
            self.to_addrs = []


@dataclass
class WebhookConfig:
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
    headers: dict[str, str] = None
    on_success: bool = False
    on_failure: bool = True
    timeout: int = 30

    def __post_init__(self):
        if self.headers is None:
            self.headers = {}


@dataclass
class NotificationConfig:
    """Combined notification configuration.

    Attributes:
        email: Email notification settings
        webhook: Webhook notification settings
    """

    email: EmailConfig = None
    webhook: WebhookConfig = None

    def __post_init__(self):
        if self.email is None:
            self.email = EmailConfig()
        if self.webhook is None:
            self.webhook = WebhookConfig()

    def is_enabled(self) -> bool:
        """Check if any notification method is enabled."""
        return self.email.enabled or self.webhook.enabled


def send_email(config: EmailConfig, event: NotificationEvent) -> bool:
    """Send email notification.

    Args:
        config: Email configuration
        event: Notification event to send

    Returns:
        True if email was sent successfully, False otherwise
    """
    if not config.enabled or not config.to_addrs:
        return False

    # Check if we should send based on status
    if event.status == "success" and not config.on_success:
        logger.debug("Skipping email notification for success (on_success=false)")
        return False
    if event.status in ("failure", "partial") and not config.on_failure:
        logger.debug("Skipping email notification for failure (on_failure=false)")
        return False

    try:
        # Create message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = (
            f"[btrfs-backup-ng] {event.event_type}: {event.status.upper()} on {event.hostname}"
        )
        msg["From"] = config.from_addr
        msg["To"] = ", ".join(config.to_addrs)

        # Attach both plain text and HTML versions
        msg.attach(MIMEText(event.to_text(), "plain"))
        msg.attach(MIMEText(event.to_html(), "html"))

        # Connect and send
        context = ssl.create_default_context()

        if config.smtp_tls == "ssl":
            # Implicit TLS (port 465)
            with smtplib.SMTP_SSL(
                config.smtp_host, config.smtp_port, context=context
            ) as server:
                if config.smtp_user and config.smtp_password:
                    server.login(config.smtp_user, config.smtp_password)
                server.sendmail(config.from_addr, config.to_addrs, msg.as_string())
        else:
            # Plain or STARTTLS
            with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
                if config.smtp_tls == "starttls":
                    server.starttls(context=context)
                if config.smtp_user and config.smtp_password:
                    server.login(config.smtp_user, config.smtp_password)
                server.sendmail(config.from_addr, config.to_addrs, msg.as_string())

        logger.info(f"Email notification sent to {', '.join(config.to_addrs)}")
        return True

    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")
        return False


def send_webhook(config: WebhookConfig, event: NotificationEvent) -> bool:
    """Send webhook notification.

    Args:
        config: Webhook configuration
        event: Notification event to send

    Returns:
        True if webhook was sent successfully, False otherwise
    """
    if not config.enabled or not config.url:
        return False

    # Check if we should send based on status
    if event.status == "success" and not config.on_success:
        logger.debug("Skipping webhook notification for success (on_success=false)")
        return False
    if event.status in ("failure", "partial") and not config.on_failure:
        logger.debug("Skipping webhook notification for failure (on_failure=false)")
        return False

    try:
        # Prepare payload
        payload = json.dumps(event.to_dict()).encode("utf-8")

        # Build headers
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "btrfs-backup-ng/1.0",
            **config.headers,
        }

        # Create request
        req = urllib.request.Request(
            config.url, data=payload, headers=headers, method=config.method
        )

        # Send request
        with urllib.request.urlopen(req, timeout=config.timeout) as response:
            status = response.getcode()
            if 200 <= status < 300:
                logger.info(
                    f"Webhook notification sent to {config.url} (status: {status})"
                )
                return True
            else:
                logger.warning(f"Webhook returned non-success status: {status}")
                return False

    except urllib.error.HTTPError as e:
        logger.error(f"Webhook HTTP error: {e.code} {e.reason}")
        return False
    except urllib.error.URLError as e:
        logger.error(f"Webhook URL error: {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Failed to send webhook notification: {e}")
        return False


def send_notifications(
    config: NotificationConfig, event: NotificationEvent
) -> dict[str, bool]:
    """Send all configured notifications.

    Args:
        config: Notification configuration
        event: Notification event to send

    Returns:
        Dictionary mapping notification type to success status
    """
    results = {}

    if config.email.enabled:
        results["email"] = send_email(config.email, event)

    if config.webhook.enabled:
        results["webhook"] = send_webhook(config.webhook, event)

    return results


def create_backup_event(
    status: Literal["success", "failure", "partial"],
    volumes_processed: int = 0,
    volumes_failed: int = 0,
    snapshots_created: int = 0,
    transfers_completed: int = 0,
    transfers_failed: int = 0,
    duration_seconds: float = 0.0,
    errors: list[str] = None,
) -> NotificationEvent:
    """Create a backup completion notification event.

    Args:
        status: Overall backup status
        volumes_processed: Number of volumes backed up
        volumes_failed: Number of volumes that failed
        snapshots_created: Number of snapshots created
        transfers_completed: Number of successful transfers
        transfers_failed: Number of failed transfers
        duration_seconds: Total backup duration
        errors: List of error messages

    Returns:
        NotificationEvent ready to send
    """
    import socket

    hostname = socket.gethostname()
    timestamp = datetime.now().isoformat()

    if status == "success":
        summary = f"Backup completed successfully: {volumes_processed} volumes, {transfers_completed} transfers"
    elif status == "partial":
        summary = f"Backup partially completed: {volumes_processed - volumes_failed}/{volumes_processed} volumes succeeded"
    else:
        summary = f"Backup failed: {volumes_failed} volumes failed"

    return NotificationEvent(
        event_type="backup_complete",
        status=status,
        timestamp=timestamp,
        hostname=hostname,
        summary=summary,
        volumes_processed=volumes_processed,
        volumes_failed=volumes_failed,
        snapshots_created=snapshots_created,
        transfers_completed=transfers_completed,
        transfers_failed=transfers_failed,
        duration_seconds=duration_seconds,
        errors=errors or [],
    )


def create_prune_event(
    status: Literal["success", "failure", "partial"],
    volumes_processed: int = 0,
    volumes_failed: int = 0,
    snapshots_pruned: int = 0,
    duration_seconds: float = 0.0,
    errors: list[str] = None,
) -> NotificationEvent:
    """Create a prune completion notification event.

    Args:
        status: Overall prune status
        volumes_processed: Number of volumes processed
        volumes_failed: Number of volumes that failed
        snapshots_pruned: Number of snapshots deleted
        duration_seconds: Total prune duration
        errors: List of error messages

    Returns:
        NotificationEvent ready to send
    """
    import socket

    hostname = socket.gethostname()
    timestamp = datetime.now().isoformat()

    if status == "success":
        summary = f"Prune completed successfully: {snapshots_pruned} snapshots removed from {volumes_processed} volumes"
    elif status == "partial":
        summary = f"Prune partially completed: {volumes_processed - volumes_failed}/{volumes_processed} volumes succeeded"
    else:
        summary = f"Prune failed: {volumes_failed} volumes failed"

    return NotificationEvent(
        event_type="prune_complete",
        status=status,
        timestamp=timestamp,
        hostname=hostname,
        summary=summary,
        volumes_processed=volumes_processed,
        volumes_failed=volumes_failed,
        snapshots_pruned=snapshots_pruned,
        duration_seconds=duration_seconds,
        errors=errors or [],
    )
