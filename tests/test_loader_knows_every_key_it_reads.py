"""The loader's "unknown key" warning names only keys it really ignores.

Two keys the loader READ were missing from its known-key sets, so a config
that set them got "Unknown config key ... (ignored)" -- a warning whose text
was untrue -- and one of them, the email notification's ``timeout``, was in
the schema (with a paragraph on why it exists) and never read at all.
"""

from __future__ import annotations

from btrfs_backup_ng.config.loader import load_config


def _load(tmp_path, body: str):
    path = tmp_path / "c.toml"
    path.write_text(body)
    return load_config(path)


def test_skip_remote_lock_on_a_target_is_a_known_key(tmp_path):
    config, warnings = _load(
        tmp_path,
        '[[volumes]]\npath = "/home"\n\n'
        '[[volumes.targets]]\npath = "ssh://h:/b"\nskip_remote_lock = true\n',
    )
    assert config.volumes[0].targets[0].skip_remote_lock is True
    assert not [w for w in warnings if "skip_remote_lock" in w], warnings


def test_the_email_timeout_is_read_and_known(tmp_path):
    config, warnings = _load(
        tmp_path,
        '[global.notifications.email]\nenabled = true\nsmtp_host = "h"\n'
        'to_addrs = ["a@b"]\ntimeout = 9\n\n'
        '[[volumes]]\npath = "/home"\n\n[[volumes.targets]]\npath = "/b"\n',
    )
    assert config.global_config.notifications.email.timeout == 9
    assert not [w for w in warnings if "timeout" in w], warnings


def test_the_email_timeout_keeps_its_default_when_absent(tmp_path):
    config, _ = _load(
        tmp_path,
        "[global.notifications.email]\nenabled = true\n\n"
        '[[volumes]]\npath = "/home"\n\n[[volumes.targets]]\npath = "/b"\n',
    )
    assert config.global_config.notifications.email.timeout == 30
