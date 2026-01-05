"""Tests for the notifications module."""

from unittest.mock import MagicMock, patch


from btrfs_backup_ng.notifications import (
    EmailConfig,
    NotificationConfig,
    NotificationEvent,
    WebhookConfig,
    create_backup_event,
    create_prune_event,
    send_email,
    send_notifications,
    send_webhook,
)


class TestNotificationEvent:
    """Tests for NotificationEvent dataclass."""

    def test_default_values(self):
        """Test NotificationEvent with minimal required fields."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test summary",
        )
        assert event.event_type == "backup_complete"
        assert event.status == "success"
        assert event.volumes_processed == 0
        assert event.errors == []

    def test_with_all_fields(self):
        """Test NotificationEvent with all fields populated."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
            details="Connection refused",
            volumes_processed=3,
            volumes_failed=1,
            snapshots_created=2,
            transfers_completed=2,
            transfers_failed=1,
            snapshots_pruned=5,
            duration_seconds=123.5,
            errors=["Error 1", "Error 2"],
        )
        assert event.volumes_failed == 1
        assert event.transfers_failed == 1
        assert len(event.errors) == 2

    def test_to_dict(self):
        """Test conversion to dictionary."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        d = event.to_dict()
        assert isinstance(d, dict)
        assert d["event_type"] == "backup_complete"
        assert d["status"] == "success"
        assert "timestamp" in d

    def test_to_text_minimal(self):
        """Test plain text formatting with minimal fields."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup completed",
        )
        text = event.to_text()
        assert "btrfs-backup-ng Notification" in text
        assert "backup_complete" in text
        assert "SUCCESS" in text
        assert "testhost" in text
        assert "Backup completed" in text

    def test_to_text_with_details(self):
        """Test plain text formatting with details."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup completed",
            details="All volumes backed up successfully",
        )
        text = event.to_text()
        assert "Details: All volumes backed up successfully" in text

    def test_to_text_with_snapshots(self):
        """Test plain text formatting with snapshot stats."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
            snapshots_created=5,
        )
        text = event.to_text()
        assert "Snapshots created: 5" in text

    def test_to_text_with_transfers(self):
        """Test plain text formatting with transfer stats."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
            transfers_completed=3,
            transfers_failed=1,
        )
        text = event.to_text()
        assert "Transfers completed: 3" in text
        assert "Transfers failed: 1" in text

    def test_to_text_with_pruned(self):
        """Test plain text formatting with prune stats."""
        event = NotificationEvent(
            event_type="prune_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
            snapshots_pruned=10,
        )
        text = event.to_text()
        assert "Snapshots pruned: 10" in text

    def test_to_text_with_duration(self):
        """Test plain text formatting with duration."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
            duration_seconds=65.5,
        )
        text = event.to_text()
        assert "Duration: 65.5s" in text

    def test_to_text_with_errors(self):
        """Test plain text formatting with errors."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
            errors=["Connection refused", "Permission denied"],
        )
        text = event.to_text()
        assert "Errors:" in text
        assert "- Connection refused" in text
        assert "- Permission denied" in text

    def test_to_html_success(self):
        """Test HTML formatting for success status."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup completed",
        )
        html = event.to_html()
        assert "<!DOCTYPE html>" in html
        assert "#28a745" in html  # success color
        assert "testhost" in html

    def test_to_html_failure(self):
        """Test HTML formatting for failure status."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
            errors=["Error message"],
        )
        html = event.to_html()
        assert "#dc3545" in html  # failure color
        assert "Error message" in html

    def test_to_html_partial(self):
        """Test HTML formatting for partial status."""
        event = NotificationEvent(
            event_type="backup_complete",
            status="partial",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Partial backup",
        )
        html = event.to_html()
        assert "#ffc107" in html  # partial/warning color


class TestEmailConfig:
    """Tests for EmailConfig dataclass."""

    def test_default_values(self):
        """Test EmailConfig default values."""
        config = EmailConfig()
        assert config.enabled is False
        assert config.smtp_host == "localhost"
        assert config.smtp_port == 25
        assert config.smtp_tls == "none"
        assert config.on_success is False
        assert config.on_failure is True

    def test_custom_values(self):
        """Test EmailConfig with custom values."""
        config = EmailConfig(
            enabled=True,
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_user="user",
            smtp_password="pass",
            smtp_tls="starttls",
            from_addr="backup@example.com",
            to_addrs=["admin@example.com"],
        )
        assert config.enabled is True
        assert config.smtp_port == 587
        assert config.smtp_tls == "starttls"


class TestWebhookConfig:
    """Tests for WebhookConfig dataclass."""

    def test_default_values(self):
        """Test WebhookConfig default values."""
        config = WebhookConfig()
        assert config.enabled is False
        assert config.url is None
        assert config.method == "POST"
        assert config.headers == {}
        assert config.timeout == 30

    def test_custom_values(self):
        """Test WebhookConfig with custom values."""
        config = WebhookConfig(
            enabled=True,
            url="https://hooks.example.com/backup",
            method="PUT",
            headers={"Authorization": "Bearer token"},
            timeout=60,
        )
        assert config.enabled is True
        assert config.method == "PUT"
        assert "Authorization" in config.headers


class TestNotificationConfig:
    """Tests for NotificationConfig dataclass."""

    def test_default_values(self):
        """Test NotificationConfig default values."""
        config = NotificationConfig()
        assert isinstance(config.email, EmailConfig)
        assert isinstance(config.webhook, WebhookConfig)

    def test_is_enabled_false_by_default(self):
        """Test is_enabled returns False when nothing enabled."""
        config = NotificationConfig()
        assert config.is_enabled() is False

    def test_is_enabled_with_email(self):
        """Test is_enabled returns True when email enabled."""
        config = NotificationConfig(email=EmailConfig(enabled=True))
        assert config.is_enabled() is True

    def test_is_enabled_with_webhook(self):
        """Test is_enabled returns True when webhook enabled."""
        config = NotificationConfig(webhook=WebhookConfig(enabled=True))
        assert config.is_enabled() is True


class TestSendEmail:
    """Tests for send_email function."""

    def test_disabled_returns_false(self):
        """Test send_email returns False when disabled."""
        config = EmailConfig(enabled=False)
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        assert send_email(config, event) is False

    def test_no_recipients_returns_false(self):
        """Test send_email returns False with no recipients."""
        config = EmailConfig(enabled=True, to_addrs=[])
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        assert send_email(config, event) is False

    def test_success_skipped_when_on_success_false(self):
        """Test success event skipped when on_success=False."""
        config = EmailConfig(
            enabled=True,
            to_addrs=["admin@example.com"],
            on_success=False,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        assert send_email(config, event) is False

    def test_failure_skipped_when_on_failure_false(self):
        """Test failure event skipped when on_failure=False."""
        config = EmailConfig(
            enabled=True,
            to_addrs=["admin@example.com"],
            on_failure=False,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        assert send_email(config, event) is False

    def test_partial_treated_as_failure(self):
        """Test partial status is treated as failure for on_failure check."""
        config = EmailConfig(
            enabled=True,
            to_addrs=["admin@example.com"],
            on_failure=False,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="partial",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        assert send_email(config, event) is False

    @patch("btrfs_backup_ng.notifications.smtplib.SMTP")
    def test_send_plain_smtp(self, mock_smtp):
        """Test sending email via plain SMTP."""
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

        config = EmailConfig(
            enabled=True,
            smtp_host="localhost",
            smtp_port=25,
            smtp_tls="none",
            from_addr="backup@example.com",
            to_addrs=["admin@example.com"],
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_email(config, event)
        assert result is True
        mock_smtp.assert_called_once_with("localhost", 25)
        mock_server.sendmail.assert_called_once()

    @patch("btrfs_backup_ng.notifications.smtplib.SMTP")
    def test_send_starttls_smtp(self, mock_smtp):
        """Test sending email via STARTTLS SMTP."""
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

        config = EmailConfig(
            enabled=True,
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_tls="starttls",
            smtp_user="user",
            smtp_password="pass",
            from_addr="backup@example.com",
            to_addrs=["admin@example.com"],
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_email(config, event)
        assert result is True
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user", "pass")

    @patch("btrfs_backup_ng.notifications.smtplib.SMTP_SSL")
    def test_send_ssl_smtp(self, mock_smtp_ssl):
        """Test sending email via SSL SMTP."""
        mock_server = MagicMock()
        mock_smtp_ssl.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_ssl.return_value.__exit__ = MagicMock(return_value=False)

        config = EmailConfig(
            enabled=True,
            smtp_host="smtp.example.com",
            smtp_port=465,
            smtp_tls="ssl",
            smtp_user="user",
            smtp_password="pass",
            from_addr="backup@example.com",
            to_addrs=["admin@example.com"],
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_email(config, event)
        assert result is True
        mock_smtp_ssl.assert_called_once()
        mock_server.login.assert_called_once_with("user", "pass")

    @patch("btrfs_backup_ng.notifications.smtplib.SMTP")
    def test_send_email_exception_returns_false(self, mock_smtp):
        """Test send_email returns False on exception."""
        mock_smtp.side_effect = Exception("Connection failed")

        config = EmailConfig(
            enabled=True,
            smtp_host="localhost",
            smtp_port=25,
            from_addr="backup@example.com",
            to_addrs=["admin@example.com"],
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_email(config, event)
        assert result is False


class TestSendWebhook:
    """Tests for send_webhook function."""

    def test_disabled_returns_false(self):
        """Test send_webhook returns False when disabled."""
        config = WebhookConfig(enabled=False)
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        assert send_webhook(config, event) is False

    def test_no_url_returns_false(self):
        """Test send_webhook returns False with no URL."""
        config = WebhookConfig(enabled=True, url=None)
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        assert send_webhook(config, event) is False

    def test_success_skipped_when_on_success_false(self):
        """Test success event skipped when on_success=False."""
        config = WebhookConfig(
            enabled=True,
            url="https://example.com/hook",
            on_success=False,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        assert send_webhook(config, event) is False

    def test_failure_skipped_when_on_failure_false(self):
        """Test failure event skipped when on_failure=False."""
        config = WebhookConfig(
            enabled=True,
            url="https://example.com/hook",
            on_failure=False,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        assert send_webhook(config, event) is False

    @patch("btrfs_backup_ng.notifications.urllib.request.urlopen")
    def test_send_webhook_success(self, mock_urlopen):
        """Test successful webhook send."""
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        config = WebhookConfig(
            enabled=True,
            url="https://example.com/hook",
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_webhook(config, event)
        assert result is True
        mock_urlopen.assert_called_once()

    @patch("btrfs_backup_ng.notifications.urllib.request.urlopen")
    def test_send_webhook_with_custom_headers(self, mock_urlopen):
        """Test webhook with custom headers."""
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        config = WebhookConfig(
            enabled=True,
            url="https://example.com/hook",
            headers={"Authorization": "Bearer token123"},
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_webhook(config, event)
        assert result is True

        # Check that the request was made with correct headers
        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        assert request.get_header("Authorization") == "Bearer token123"
        assert request.get_header("Content-type") == "application/json"

    @patch("btrfs_backup_ng.notifications.urllib.request.urlopen")
    def test_send_webhook_non_success_status(self, mock_urlopen):
        """Test webhook with non-2xx response."""
        mock_response = MagicMock()
        mock_response.getcode.return_value = 400
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        config = WebhookConfig(
            enabled=True,
            url="https://example.com/hook",
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_webhook(config, event)
        assert result is False

    @patch("btrfs_backup_ng.notifications.urllib.request.urlopen")
    def test_send_webhook_http_error(self, mock_urlopen):
        """Test webhook with HTTP error."""
        import urllib.error

        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="https://example.com",
            code=500,
            msg="Internal Server Error",
            hdrs={},
            fp=None,
        )

        config = WebhookConfig(
            enabled=True,
            url="https://example.com/hook",
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_webhook(config, event)
        assert result is False

    @patch("btrfs_backup_ng.notifications.urllib.request.urlopen")
    def test_send_webhook_url_error(self, mock_urlopen):
        """Test webhook with URL error."""
        import urllib.error

        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        config = WebhookConfig(
            enabled=True,
            url="https://example.com/hook",
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_webhook(config, event)
        assert result is False

    @patch("btrfs_backup_ng.notifications.urllib.request.urlopen")
    def test_send_webhook_generic_exception(self, mock_urlopen):
        """Test webhook with generic exception."""
        mock_urlopen.side_effect = Exception("Something went wrong")

        config = WebhookConfig(
            enabled=True,
            url="https://example.com/hook",
            on_failure=True,
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Backup failed",
        )

        result = send_webhook(config, event)
        assert result is False


class TestSendNotifications:
    """Tests for send_notifications function."""

    def test_no_notifications_enabled(self):
        """Test with no notifications enabled."""
        config = NotificationConfig()
        event = NotificationEvent(
            event_type="backup_complete",
            status="success",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        results = send_notifications(config, event)
        assert results == {}

    @patch("btrfs_backup_ng.notifications.send_email")
    def test_email_only(self, mock_send_email):
        """Test with only email enabled."""
        mock_send_email.return_value = True

        config = NotificationConfig(
            email=EmailConfig(enabled=True, to_addrs=["admin@example.com"])
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        results = send_notifications(config, event)
        assert results == {"email": True}
        mock_send_email.assert_called_once()

    @patch("btrfs_backup_ng.notifications.send_webhook")
    def test_webhook_only(self, mock_send_webhook):
        """Test with only webhook enabled."""
        mock_send_webhook.return_value = True

        config = NotificationConfig(
            webhook=WebhookConfig(enabled=True, url="https://example.com/hook")
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        results = send_notifications(config, event)
        assert results == {"webhook": True}
        mock_send_webhook.assert_called_once()

    @patch("btrfs_backup_ng.notifications.send_email")
    @patch("btrfs_backup_ng.notifications.send_webhook")
    def test_both_enabled(self, mock_send_webhook, mock_send_email):
        """Test with both email and webhook enabled."""
        mock_send_email.return_value = True
        mock_send_webhook.return_value = False

        config = NotificationConfig(
            email=EmailConfig(enabled=True, to_addrs=["admin@example.com"]),
            webhook=WebhookConfig(enabled=True, url="https://example.com/hook"),
        )
        event = NotificationEvent(
            event_type="backup_complete",
            status="failure",
            timestamp="2024-01-01T12:00:00",
            hostname="testhost",
            summary="Test",
        )
        results = send_notifications(config, event)
        assert results == {"email": True, "webhook": False}


class TestCreateBackupEvent:
    """Tests for create_backup_event function."""

    @patch("socket.gethostname")
    def test_success_event(self, mock_hostname):
        """Test creating a success backup event."""
        mock_hostname.return_value = "testhost"

        event = create_backup_event(
            status="success",
            volumes_processed=3,
            snapshots_created=3,
            transfers_completed=3,
            duration_seconds=120.5,
        )

        assert event.event_type == "backup_complete"
        assert event.status == "success"
        assert event.hostname == "testhost"
        assert "successfully" in event.summary
        assert event.volumes_processed == 3
        assert event.snapshots_created == 3
        assert event.transfers_completed == 3
        assert event.duration_seconds == 120.5

    @patch("socket.gethostname")
    def test_failure_event(self, mock_hostname):
        """Test creating a failure backup event."""
        mock_hostname.return_value = "testhost"

        event = create_backup_event(
            status="failure",
            volumes_processed=3,
            volumes_failed=3,
            errors=["Error 1", "Error 2"],
        )

        assert event.status == "failure"
        assert "failed" in event.summary
        assert event.volumes_failed == 3
        assert len(event.errors) == 2

    @patch("socket.gethostname")
    def test_partial_event(self, mock_hostname):
        """Test creating a partial backup event."""
        mock_hostname.return_value = "testhost"

        event = create_backup_event(
            status="partial",
            volumes_processed=3,
            volumes_failed=1,
        )

        assert event.status == "partial"
        assert "partially" in event.summary
        assert "2/3" in event.summary

    @patch("socket.gethostname")
    def test_default_errors(self, mock_hostname):
        """Test that errors defaults to empty list."""
        mock_hostname.return_value = "testhost"

        event = create_backup_event(status="success")
        assert event.errors == []


class TestCreatePruneEvent:
    """Tests for create_prune_event function."""

    @patch("socket.gethostname")
    def test_success_event(self, mock_hostname):
        """Test creating a success prune event."""
        mock_hostname.return_value = "testhost"

        event = create_prune_event(
            status="success",
            volumes_processed=2,
            snapshots_pruned=15,
            duration_seconds=30.0,
        )

        assert event.event_type == "prune_complete"
        assert event.status == "success"
        assert "successfully" in event.summary
        assert "15 snapshots" in event.summary
        assert event.snapshots_pruned == 15

    @patch("socket.gethostname")
    def test_failure_event(self, mock_hostname):
        """Test creating a failure prune event."""
        mock_hostname.return_value = "testhost"

        event = create_prune_event(
            status="failure",
            volumes_processed=2,
            volumes_failed=2,
            errors=["Permission denied"],
        )

        assert event.status == "failure"
        assert "failed" in event.summary
        assert len(event.errors) == 1

    @patch("socket.gethostname")
    def test_partial_event(self, mock_hostname):
        """Test creating a partial prune event."""
        mock_hostname.return_value = "testhost"

        event = create_prune_event(
            status="partial",
            volumes_processed=3,
            volumes_failed=1,
        )

        assert event.status == "partial"
        assert "partially" in event.summary
        assert "2/3" in event.summary

    @patch("socket.gethostname")
    def test_default_errors(self, mock_hostname):
        """Test that errors defaults to empty list."""
        mock_hostname.return_value = "testhost"

        event = create_prune_event(status="success")
        assert event.errors == []
