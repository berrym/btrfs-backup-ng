"""``restore --status`` reports the snapper backups a location holds.

A snapper backup location is laid out as numbered slots
(``.snapshots/<n>/snapshot`` beside ``info.xml``), not as prefix-named
snapshots, so the prefix listing ``--status`` ends with is 0 there however
many backups the location holds -- and the command's own comment says
reporting 0 for a location holding backups reads as data loss. The status
now enumerates the snapper backups the way ``snapper restore --list`` does
and shows the pins a snapper restore holds on them.
"""

from __future__ import annotations

import argparse
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from btrfs_backup_ng.cli import restore as restore_cli
from btrfs_backup_ng.endpoint.local import LocalEndpoint

INFO_XML = """<?xml version="1.0"?>
<snapshot>
  <type>single</type>
  <num>{num}</num>
  <date>2026-09-{day:02d} 03:00:00</date>
  <description>timeline {num}</description>
  <cleanup>timeline</cleanup>
</snapshot>
"""


def _shown(day: int) -> str:
    """The date the status shows for a slot: snapper's UTC ``<date>`` in local
    time, as every reader of info.xml renders it."""
    from datetime import datetime, timezone

    utc = datetime(2026, 9, day, 3, 0, 0, tzinfo=timezone.utc)
    return utc.astimezone().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _snapper_location(tmp_path, *numbers: int):
    for n in numbers:
        slot = tmp_path / ".snapshots" / str(n)
        (slot / "snapshot").mkdir(parents=True)
        (slot / "info.xml").write_text(INFO_XML.format(num=n, day=n))
    return tmp_path


def _endpoint(path):
    return LocalEndpoint(
        config={"path": str(path), "fs_checks": "skip", "snap_prefix": ""}
    )


def _status(endpoint, capsys, **extra) -> tuple[int, str]:
    args = argparse.Namespace(
        source=str(endpoint.config["path"]), fs_checks="skip", prefix=None, **extra
    )
    with patch.object(restore_cli, "_prepare_backup_endpoint", lambda ar, s: endpoint):
        rc = restore_cli._execute_status(args)
    return rc, capsys.readouterr().out


class TestTheBackupsAreCounted:
    def test_a_snapper_location_is_not_reported_as_empty(self, tmp_path, capsys):
        endpoint = _endpoint(_snapper_location(tmp_path, 5, 7))
        rc, out = _status(endpoint, capsys)
        assert rc == 0
        assert "Snapper backups: 2" in out
        assert f"     5  single  {_shown(5)}  timeline 5" in out
        assert f"     7  single  {_shown(7)}  timeline 7" in out
        # The prefix count still prints -- it is true -- but it no longer
        # stands alone as the location's whole answer.
        assert "Available snapshots: 0" in out
        assert "snapper restore --list" in out

    def test_a_plain_location_says_nothing_about_snapper(self, tmp_path, capsys):
        (tmp_path / "home-20260901-120000").mkdir()
        endpoint = _endpoint(tmp_path)
        _rc, out = _status(endpoint, capsys)
        assert "Snapper backups" not in out

    def test_an_enumeration_that_fails_is_not_zero(self, tmp_path, capsys):
        endpoint = _endpoint(_snapper_location(tmp_path, 5))
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups",
            side_effect=RuntimeError("permission denied"),
        ):
            rc, out = _status(endpoint, capsys)
        assert rc == 0
        assert "could not be enumerated" in out
        assert "permission denied" in out
        assert "NOT a report of zero" in out
        assert "Snapper backups: 0" not in out


class TestThePinsAreShown:
    def test_a_pin_held_by_a_restore_is_shown_beside_its_backup(self, tmp_path, capsys):
        """A snapper restore pins the backup it reads under ``restore:<session>``
        with the engine's name for a slot, ``snapshot-<n>``. The pin is written
        through the endpoint's real lock store and read back by the status."""
        endpoint = _endpoint(_snapper_location(tmp_path, 5, 7))
        pinned = SimpleNamespace(
            locks=set(), parent_locks=set(), get_name=lambda: "snapshot-7"
        )
        endpoint.set_lock(pinned, "restore:abcd1234", True)
        rc, out = _status(endpoint, capsys)
        assert rc == 0
        line7 = next(line for line in out.splitlines() if "timeline 7" in line)
        assert "pinned: restore:abcd1234" in line7
        line5 = next(line for line in out.splitlines() if "timeline 5" in line)
        assert "pinned" not in line5
        assert "1 snapper backup(s) are pinned" in out
        # The lock section names the session too, as it does for any pin.
        assert "snapshot-7: session abcd1234" in out

    def test_a_raw_backup_is_pinned_by_its_stream_name(self):
        assert restore_cli._snapper_pin_key(
            {"number": 3, "backup_name": "root-3-x"}
        ) == ("root-3-x")
        assert restore_cli._snapper_pin_key({"number": 3}) == "snapshot-3"


class TestTheOptionsFollowTheRestoreCommand:
    @pytest.mark.parametrize(
        "flag,key,value",
        [
            ("ssh_sudo", "ssh_sudo", True),
            ("ssh_key", "ssh_key", "/k"),
            ("ssh_key", "ssh_identity_file", "/k"),
            ("ssh_auth_sock", "ssh_auth_sock", "/s"),
            ("ssh_host_key_policy", "ssh_host_key_policy", "strict"),
            ("skip_remote_lock", "skip_remote_lock", True),
            ("gpg_keyring", "gpg_keyring", "/r"),
            ("openssl_cipher", "openssl_cipher", "aes"),
        ],
    )
    def test_each_connection_option_reaches_the_enumeration(self, flag, key, value):
        """A location written with --ssh-sudo is read back with it, or the
        enumeration of a root-owned remote layout comes back empty."""
        args = argparse.Namespace(**{flag: value})
        assert restore_cli._snapper_endpoint_options(args)[key] == value

    def test_nothing_set_is_nothing_threaded(self):
        assert restore_cli._snapper_endpoint_options(argparse.Namespace()) == {}
