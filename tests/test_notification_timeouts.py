"""A notification must not be able to stop backups from happening.

Notifications are sent AFTER a backup completes, from inside `run`. smtplib
defaults both SMTP and SMTP_SSL to socket._GLOBAL_DEFAULT_TIMEOUT -- the
process-wide default, which is None, i.e. block forever. One unreachable or
silently-dropping SMTP host therefore left the `run` process blocked
indefinitely; under a systemd Type=oneshot unit there is no start timeout, so the
unit stayed in `activating` and every subsequent timer fire was skipped. A
successful backup, then silence.

The webhook transport has always passed `timeout=config.timeout`. Email now
matches it.
"""

from __future__ import annotations

import socket
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import notifications as nf


def _event():
    return nf.NotificationEvent(
        event_type="backup_complete",
        status="success",
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        hostname="host",
        summary="one volume backed up",
    )


def _config(**kw):
    base = {
        "enabled": True,
        "smtp_host": "smtp.invalid",
        "smtp_port": 25,
        "from_addr": "a@b",
        "to_addrs": ["c@d"],
        "on_success": True,
    }
    base.update(kw)
    return nf.EmailConfig(**base)


def test_the_stdlib_default_is_block_forever():
    """The premise. If this ever stops being true the rest is moot."""
    import inspect
    import smtplib

    for cls in (smtplib.SMTP, smtplib.SMTP_SSL):
        default = inspect.signature(cls.__init__).parameters["timeout"].default
        assert default is socket._GLOBAL_DEFAULT_TIMEOUT
    assert socket.getdefaulttimeout() is None


def test_email_has_a_timeout_field_matching_the_webhook():
    assert nf.EmailConfig().timeout == nf.WebhookConfig().timeout


@pytest.mark.parametrize(
    ("tls", "attr"), [("none", "SMTP"), ("starttls", "SMTP"), ("ssl", "SMTP_SSL")]
)
def test_every_tls_mode_passes_an_explicit_timeout(tls, attr):
    server = MagicMock()
    server.__enter__ = MagicMock(return_value=server)
    server.__exit__ = MagicMock(return_value=False)

    with patch.object(nf.smtplib, attr, return_value=server) as ctor:
        nf.send_email(_config(smtp_tls=tls, timeout=17), _event())

    assert ctor.called, f"{attr} was not used for smtp_tls={tls!r}"
    assert ctor.call_args.kwargs.get("timeout") == 17, (
        f"smtp_tls={tls!r} constructed {attr} without an explicit timeout; it "
        "would inherit the process default and block forever"
    )


@pytest.mark.parametrize("tls", ["none", "starttls", "ssl"])
def test_the_live_socket_is_bounded_too(tls):
    """smtplib's timeout covers the connect and each exchange it performs, but
    starttls replaces the socket. A server that accepts and then goes quiet
    mid-dialogue is exactly the case worth bounding."""
    server = MagicMock()
    server.__enter__ = MagicMock(return_value=server)
    server.__exit__ = MagicMock(return_value=False)
    attr = "SMTP_SSL" if tls == "ssl" else "SMTP"

    with patch.object(nf.smtplib, attr, return_value=server):
        nf.send_email(_config(smtp_tls=tls, timeout=17), _event())

    assert server.sock.settimeout.called
    assert all(c.args == (17,) for c in server.sock.settimeout.call_args_list)
    if tls == "starttls":
        assert server.sock.settimeout.call_count >= 2, (
            "the socket was not re-bounded after starttls replaced it"
        )


def test_a_server_without_a_socket_does_not_crash_the_notification():
    """A failed notification must not become a failed backup."""
    server = MagicMock()
    server.__enter__ = MagicMock(return_value=server)
    server.__exit__ = MagicMock(return_value=False)
    server.sock = None

    with patch.object(nf.smtplib, "SMTP", return_value=server):
        assert nf.send_email(_config(timeout=17), _event()) is True


def test_the_webhook_transport_still_passes_its_timeout():
    """The precedent this fix follows; it must not regress."""
    import inspect

    source = inspect.getsource(nf.send_webhook)
    assert "timeout=config.timeout" in source
