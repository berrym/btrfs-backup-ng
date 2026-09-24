"""A snapper slot is published with its info.xml, or not at all.

The layout writes the slot's info.xml into ``.incoming`` and then renames the
directory, so a published slot carries its metadata from the instant it
exists -- that is what the documentation says in both directions. The write
was soft-fail: on an error it logged a warning and the rename went ahead,
publishing a slot snapper does not list (it needs the info.xml), and a
restore that ended so reported success for a copy snapper could not see. Now
a failed write abandons the slot and fails the transfer, like any other
publish failure.
"""

from __future__ import annotations

import errno
import subprocess
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.core.layout import SnapperLayout
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.snapper import SnapperConfig, SnapperScanner


def _real_shell(ep, script):
    r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return r.returncode, r.stdout


@pytest.fixture(autouse=True)
def real_shell(monkeypatch):
    monkeypatch.setattr(ops, "_snapper_run_shell", _real_shell)


def _layout(base: Path) -> SnapperLayout:
    (base / ".snapshots").mkdir(parents=True, exist_ok=True)
    ep = LocalEndpoint(
        config={"path": str(base), "snap_prefix": "", "fs_checks": "skip"}
    )
    config = SnapperConfig(name="root", subvolume=base)
    return SnapperLayout(
        ep,
        next_number=lambda: SnapperScanner.get_next_snapshot_number(None, config),  # type: ignore[arg-type]
    )


def _refuse_writes(monkeypatch):
    def refused(path, data, **kw):
        raise OSError(errno.EACCES, "info.xml write refused")

    monkeypatch.setattr(__util__, "privileged_write_bytes", refused)


class TestTheRestoreDirection:
    def test_a_copy_whose_info_xml_cannot_be_written_is_not_published(
        self, tmp_path, monkeypatch, caplog
    ):
        layout = _layout(tmp_path / "local")
        layout.open_slot(1)
        (Path(layout.incoming_dir(1)) / "snapshot").mkdir()
        _refuse_writes(monkeypatch)
        with pytest.raises(__util__.SnapshotTransferError, match="info.xml"):
            layout.publish_fresh(lambda n: b"<snapshot><num>1</num></snapshot>")
        snapshots = tmp_path / "local" / ".snapshots"
        assert sorted(p.name for p in snapshots.iterdir()) == [], (
            "the slot was published, or its temp was left behind"
        )
        assert layout.open_number is None
        assert "Failed to place info.xml" in caplog.text


class TestTheBackupDirection:
    def test_a_backup_whose_info_xml_cannot_be_written_is_not_published(
        self, tmp_path, monkeypatch
    ):
        layout = _layout(tmp_path / "target")
        layout.open_slot(7)
        (Path(layout.incoming_dir(7)) / "snapshot").mkdir()
        _refuse_writes(monkeypatch)
        with pytest.raises(__util__.SnapshotTransferError, match="info.xml"):
            layout.publish(b"<snapshot><num>7</num></snapshot>")
        snapshots = tmp_path / "target" / ".snapshots"
        assert sorted(p.name for p in snapshots.iterdir()) == []

    def test_a_publish_with_no_info_xml_to_write_is_unaffected(self, tmp_path):
        layout = _layout(tmp_path / "target")
        layout.open_slot(3)
        (Path(layout.incoming_dir(3)) / "snapshot").mkdir()
        assert layout.publish(None) == 3
        assert (tmp_path / "target" / ".snapshots" / "3" / "snapshot").is_dir()

    def test_a_written_info_xml_publishes_with_the_slot(self, tmp_path):
        layout = _layout(tmp_path / "target")
        layout.open_slot(4)
        (Path(layout.incoming_dir(4)) / "snapshot").mkdir()
        assert layout.publish(b"<snapshot><num>4</num></snapshot>") == 4
        assert (tmp_path / "target" / ".snapshots" / "4" / "info.xml").read_bytes() == (
            b"<snapshot><num>4</num></snapshot>"
        )


def test_the_writer_reports_its_outcome(tmp_path, monkeypatch):
    ep = LocalEndpoint(
        config={"path": str(tmp_path), "snap_prefix": "", "fs_checks": "skip"}
    )
    slot = tmp_path / "1.incoming"
    slot.mkdir()
    assert ops._write_info_xml(ep, str(slot), b"<x/>") is True
    _refuse_writes(monkeypatch)
    assert ops._write_info_xml(ep, str(slot), b"<x/>") is False
