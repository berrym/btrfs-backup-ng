"""Every btrbk target form must survive import, pointing where it pointed.

Three ways a destination could be lost or moved on migration:

  * `target <type> <url>` -- btrbk's own documented form -- had its TYPE token
    read as the destination, so `target send-receive ssh://nas/backup` imported
    a target literally named "send-receive" and dropped the URL. The emitted
    config had nowhere to back up to.
  * A raw target already written as an ssh URL became `raw:///ssh://nas/backup`,
    a local directory that cannot exist: a REMOTE backup silently turned local.
  * Options written under one target were stored on the enclosing subvolume, so
    they also applied to that subvolume's other targets.

Migration is the one moment a user hands over a working setup and trusts the
result, so each form is checked end to end: parsed, converted, and the emitted
URI parsed back by this project's own target parser.
"""

from __future__ import annotations

import re

import pytest

from btrfs_backup_ng.btrbk_import import convert_to_toml, parse_btrbk_config
from btrfs_backup_ng.core.target import TargetKind, parse_target


def _emit(block: str) -> list[str]:
    config = parse_btrbk_config(f"volume /mnt/data\n  subvolume home\n{block}\n")
    toml, _warnings = convert_to_toml(config)
    paths = []
    for target_block in toml.split("[[volumes.targets]]")[1:]:
        match = re.search(r'path = "([^"]+)"', target_block)
        if match:
            paths.append(match.group(1))
    return paths


@pytest.mark.parametrize(
    ("block", "expected", "kind"),
    [
        ("  target /mnt/backup", "/mnt/backup", TargetKind.LOCAL),
        (
            "  target send-receive ssh://nas/backup",
            "ssh://nas/backup",
            TargetKind.SSH,
        ),
        ("  target raw /mnt/backup", "raw:///mnt/backup", TargetKind.RAW),
        ("  target raw ssh://nas/backup", "raw+ssh://nas:/backup", TargetKind.RAW_SSH),
        ("  target nas:/mnt/backup", "ssh://nas:/mnt/backup", TargetKind.SSH),
        ("  target ssh://nas/backup", "ssh://nas/backup", TargetKind.SSH),
        (
            "  target ssh://nas/backup\n    raw_target_compress zstd",
            "raw+ssh://nas:/backup",
            TargetKind.RAW_SSH,
        ),
        (
            "  target nas:/mnt/backup\n    raw_target_compress zstd",
            "raw+ssh://nas:/mnt/backup",
            TargetKind.RAW_SSH,
        ),
    ],
)
def test_the_destination_survives_import(block, expected, kind):
    emitted = _emit(block)
    assert emitted == [expected], emitted
    assert parse_target(expected).kind is kind


@pytest.mark.parametrize(
    "block",
    [
        "  target send-receive ssh://nas/backup",
        "  target raw /mnt/backup",
    ],
)
def test_a_declared_target_type_never_becomes_the_destination(block):
    """The regression itself: the type token read as a path."""
    emitted = _emit(block)
    assert emitted, "the destination was dropped entirely"
    for path in emitted:
        assert "send-receive" not in path
        assert path not in ("raw", "send-receive")


def test_a_remote_raw_target_does_not_become_a_local_directory():
    emitted = _emit("  target ssh://nas/backup\n    raw_target_compress zstd")
    assert emitted == ["raw+ssh://nas:/backup"], emitted
    scheme = parse_target(emitted[0])
    assert scheme.ssh_destination == "nas", (
        "the remote host was lost; this backup would be written locally"
    )


def test_an_option_under_one_target_does_not_reach_its_sibling():
    config = parse_btrbk_config(
        "volume /mnt/data\n"
        "  subvolume home\n"
        "    target /mnt/backup-a\n"
        "      stream_compress zstd\n"
        "    target /mnt/backup-b\n"
    )
    toml, _warnings = convert_to_toml(config)
    blocks = toml.split("[[volumes.targets]]")[1:]
    by_path = {}
    for block in blocks:
        path = re.search(r'path = "([^"]+)"', block)
        compress = re.search(r'compress = "([^"]+)"', block)
        if path:
            by_path[path.group(1)] = compress.group(1) if compress else None
    assert by_path.get("/mnt/backup-a") == "zstd"
    assert by_path.get("/mnt/backup-b") is None, (
        "compression leaked onto a target the user did not configure it for"
    )


class TestCompressionSettingsSurviveOrAreReported:
    """A migrated config must either keep the setting or say it did not.

    btrbk and this project do not agree on every name or every supported method,
    and the gaps were all silent: `lzo` was dropped although it is the `lzop`
    this project runs; `bzip3` was emitted verbatim into a config the loader
    then refused, so the migration looked fine and the first run died on its own
    output; and `stream_compress` on a raw target vanished with no mention.
    """

    def _convert(self, block):
        config = parse_btrbk_config(f"volume /mnt/data\n  subvolume home\n{block}\n")
        return convert_to_toml(config)

    def test_btrbk_lzo_becomes_the_lzop_this_project_runs(self):
        toml, _ = self._convert("  target /mnt/backup\n    stream_compress lzo")
        assert 'compress = "lzop"' in toml, toml

    def test_an_unsupported_raw_method_is_reported_and_left_out(self):
        toml, warnings = self._convert(
            "  target raw /mnt/backup\n      raw_target_compress bzip3"
        )
        assert 'compress = "bzip3"' not in toml
        assert any("bzip3" in w and "not supported" in w for w in warnings), warnings

    def test_what_the_importer_emits_can_actually_be_loaded(self, tmp_path):
        """The migration's output has to be a config this tool accepts."""
        from btrfs_backup_ng.config import load_config

        toml, _ = self._convert(
            "  target raw /mnt/backup\n      raw_target_compress bzip3"
        )
        path = tmp_path / "config.toml"
        path.write_text(toml, encoding="utf-8")
        load_config(path)  # must not raise

    def test_raw_only_methods_still_come_through(self):
        toml, _ = self._convert(
            "  target raw /mnt/backup\n      raw_target_compress xz"
        )
        assert 'compress = "xz"' in toml, toml

    def test_stream_compress_on_a_raw_target_is_not_dropped_silently(self):
        _toml, warnings = self._convert(
            "  target raw /mnt/backup\n"
            "      stream_compress zstd\n"
            "      raw_target_compress xz"
        )
        assert any("stream_compress" in w and "raw target" in w for w in warnings), (
            warnings
        )

    def test_a_compression_level_is_reported_rather_than_assumed(self):
        _toml, warnings = self._convert(
            "  target /mnt/backup\n"
            "    stream_compress zstd\n"
            "    stream_compress_level 15"
        )
        assert any("stream_compress_level" in w for w in warnings), warnings

    def test_ssh_compression_is_not_mentioned_for_a_local_target(self):
        """The advice is about ssh's own -C; a local target has no ssh."""
        _toml, warnings = self._convert("  target /mnt/backup\n    ssh_compression yes")
        assert not any("ssh_compression" in w for w in warnings), warnings

    def test_ssh_compression_is_mentioned_for_a_remote_target(self):
        _toml, warnings = self._convert(
            "  target nas:/mnt/backup\n    ssh_compression yes"
        )
        assert any("ssh_compression" in w for w in warnings), warnings


class TestConnectionOptionsSurviveImport:
    """A migrated config must connect the way btrbk connected.

    `ssh_identity`, `ssh_user`, `ssh_port` and `rate_limit` were recognised by
    the parser -- so they never looked unknown -- stored, and never read back.
    They vanished at EVERY scope with no warning, while the migration guide
    promised three of them by name. Someone reading that table believed their key
    and username had come across, and got authentication failures against a host
    btrbk had been backing up to correctly.
    """

    def _targets(self, text):
        config = parse_btrbk_config(text)
        toml, _warnings = convert_to_toml(config)
        blocks = []
        for block in toml.split("[[volumes.targets]]")[1:]:
            body = []
            for line in block.strip().splitlines():
                if line.startswith("[["):
                    break
                body.append(line.strip())
            blocks.append("\n".join(body))
        return blocks

    GLOBAL = (
        "ssh_identity /keys/id_ed25519\n"
        "ssh_user backup\n"
        "ssh_port 2222\n"
        "rate_limit 50M\n"
        "volume /mnt/pool\n"
        "  subvolume home\n"
        "    target send-receive ssh://nas.local/mnt/backup/home\n"
    )

    TARGET_SCOPE = (
        "volume /mnt/pool\n"
        "  subvolume home\n"
        "    target send-receive ssh://nas.local/mnt/backup/home\n"
        "      ssh_identity /keys/id_ed25519\n"
        "      ssh_user backup\n"
        "      ssh_port 2222\n"
        "      rate_limit 50M\n"
    )

    @pytest.mark.parametrize("scope", ["GLOBAL", "TARGET_SCOPE"])
    def test_all_four_are_carried_over(self, scope):
        block = self._targets(getattr(self, scope))[0]
        assert 'ssh_key = "/keys/id_ed25519"' in block, block
        assert "ssh_port = 2222" in block, block
        assert 'rate_limit = "50M"' in block, block
        assert "backup@nas.local" in block, block

    @pytest.mark.parametrize("scope", ["GLOBAL", "TARGET_SCOPE"])
    def test_the_emitted_uri_carries_the_user(self, scope):
        """btrbk keeps the remote user in its own option; this project puts it in
        the URL, so a dropped ssh_user means connecting as the wrong account."""
        block = self._targets(getattr(self, scope))[0]
        path = re.search(r'path = "([^"]+)"', block).group(1)
        assert parse_target(path).ssh_destination == "backup@nas.local", path

    def test_a_raw_target_gets_them_too(self):
        blocks = self._targets(
            "ssh_identity /keys/id_ed25519\n"
            "ssh_user backup\n"
            "volume /mnt/pool\n"
            "  subvolume data\n"
            "    target raw ssh://nas.local/mnt/backup/data\n"
            "      raw_target_compress xz\n"
        )
        assert 'ssh_key = "/keys/id_ed25519"' in blocks[0], blocks[0]
        path = re.search(r'path = "([^"]+)"', blocks[0]).group(1)
        assert parse_target(path).ssh_destination == "backup@nas.local", path

    def test_an_existing_user_in_the_url_is_not_overwritten(self):
        blocks = self._targets(
            "ssh_user backup\n"
            "volume /mnt/pool\n"
            "  subvolume home\n"
            "    target send-receive ssh://someone@nas.local/mnt/backup/home\n"
        )
        path = re.search(r'path = "([^"]+)"', blocks[0]).group(1)
        assert parse_target(path).ssh_destination == "someone@nas.local", path

    def test_a_local_target_gets_no_ssh_options(self):
        blocks = self._targets(
            "ssh_identity /keys/id_ed25519\n"
            "ssh_user backup\n"
            "volume /mnt/pool\n"
            "  subvolume home\n"
            "    target /mnt/backup/home\n"
        )
        assert "backup@" not in blocks[0], blocks[0]

    def test_a_non_numeric_port_is_reported_not_emitted(self):
        config = parse_btrbk_config(
            "ssh_port ssh-alt\n"
            "volume /mnt/pool\n"
            "  subvolume home\n"
            "    target send-receive ssh://nas.local/mnt/backup/home\n"
        )
        toml, warnings = convert_to_toml(config)
        assert "ssh_port" not in toml
        assert any("ssh_port" in w for w in warnings), warnings

    def test_rate_limit_no_is_not_emitted(self):
        blocks = self._targets(
            "rate_limit no\n"
            "volume /mnt/pool\n"
            "  subvolume home\n"
            "    target send-receive ssh://nas.local/mnt/backup/home\n"
        )
        assert "rate_limit" not in blocks[0], blocks[0]

    def test_what_the_importer_emits_still_loads(self, tmp_path):
        from btrfs_backup_ng.config import load_config

        config = parse_btrbk_config(self.GLOBAL)
        toml, _warnings = convert_to_toml(config)
        path = tmp_path / "config.toml"
        path.write_text(toml, encoding="utf-8")
        loaded, _warn = load_config(path)
        target = loaded.volumes[0].targets[0]
        assert target.ssh_key == "/keys/id_ed25519"
        assert target.ssh_port == 2222


class TestBtrbkOffValuesAreNotSettings:
    """btrbk writes `no` to disable an option; it is not a value.

    Taken literally the string is truthy and non-empty, so it flowed straight
    through: `ssh_user no` produced `ssh://no@host/...` and `ssh_key = "no"` -- a
    config that logs in as a user called "no" with a key file called "no". Worse,
    `raw_target_compress no` made `is_raw_target` true, turning every plain
    send-receive destination into a raw stream-file one: a different backup
    format entirely, chosen silently.
    """

    def _toml(self, text):
        return convert_to_toml(
            parse_btrbk_config(
                f"{text}volume /mnt/pool\n  subvolume home\n"
                f"    target send-receive ssh://nas.local/backup\n"
            )
        )[0]

    @pytest.mark.parametrize("off", ["no", "NO", "off", "false", "0"])
    def test_a_disabled_ssh_user_is_not_a_username(self, off):
        toml = self._toml(f"ssh_user {off}\n")
        assert "@nas.local" not in toml, toml
        assert f"{off}@" not in toml, toml

    @pytest.mark.parametrize("off", ["no", "off", "false"])
    def test_a_disabled_ssh_identity_is_not_a_key_path(self, off):
        assert "ssh_key" not in self._toml(f"ssh_identity {off}\n")

    @pytest.mark.parametrize("off", ["no", "off", "false"])
    def test_a_disabled_raw_option_does_not_make_the_target_raw(self, off):
        """The destination format must not change because an option was OFF."""
        toml = self._toml(f"raw_target_compress {off}\n")
        assert "raw+ssh://" not in toml, toml
        assert 'path = "ssh://nas.local/backup"' in toml, toml

    def test_a_disabled_raw_encrypt_does_not_make_the_target_raw(self):
        toml = self._toml("raw_target_encrypt no\n")
        assert "raw+ssh://" not in toml, toml

    def test_real_values_are_still_carried(self):
        """The guard must not swallow genuine settings."""
        toml = self._toml("ssh_user backup\nssh_identity /keys/id\n")
        assert 'path = "ssh://backup@nas.local/backup"' in toml, toml
        assert 'ssh_key = "/keys/id"' in toml, toml

    def test_a_genuinely_raw_target_is_still_raw(self):
        toml = convert_to_toml(
            parse_btrbk_config(
                "volume /mnt/pool\n  subvolume home\n"
                "    target raw ssh://nas.local/backup\n"
                "      raw_target_compress zstd\n"
            )
        )[0]
        assert "raw+ssh://" in toml, toml
        assert 'compress = "zstd"' in toml, toml
