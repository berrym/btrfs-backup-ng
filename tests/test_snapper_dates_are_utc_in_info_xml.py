"""snapper writes ``<date>`` in UTC; this tool works in local time.

Measured on a snapper 0.13 host in EDT: ``snapper list`` and
``snapper --jsonout list`` show a snapshot at ``2026-09-23 20:00:09`` and its
``info.xml`` says ``<date>2026-09-24 00:00:09</date>``. Every other date this
tool handles is naive local time, so the info.xml text is converted at the
boundary, in both directions. Read as local it put every backup's date off by
the zone's offset: west of UTC a fresh backup was "dated in the future" and
quarantined from retention, east of UTC it left its minimum window early; and
a destination's backups (dated from info.xml) disagreed with the source's
snapshots (dated by ``snapper list``) about the very same snapshot, which is
the comparison ``run``'s snapper catch-up makes.

The assertions here are written against Python's own local conversion, so
they hold under any TZ the suite runs in (UTC in CI, a far offset before a
release) and are only interesting where the offset is not zero.
"""

from __future__ import annotations

import calendar
import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from btrfs_backup_ng.snapper import metadata as md
from btrfs_backup_ng.snapper.scanner import SnapperConfig, SnapperScanner

UTC_TEXT = "2026-09-24 00:00:09"
UTC_TUPLE = (2026, 9, 24, 0, 0, 9)


def _local_of(utc_tuple) -> datetime:
    """What a UTC wall time is in this process's local zone, by the standard
    library alone."""
    return datetime.fromtimestamp(calendar.timegm((*utc_tuple, 0, 0, 0)))


def _xml(date_text: str, num: int = 7) -> str:
    return (
        '<?xml version="1.0"?>\n<snapshot>\n  <type>single</type>\n'
        f"  <num>{num}</num>\n  <date>{date_text}</date>\n"
        "  <description>timeline</description>\n  <cleanup>timeline</cleanup>\n"
        "</snapshot>\n"
    )


class TestParsing:
    def test_the_date_is_read_as_utc_and_returned_as_local(self):
        parsed = md.parse_info_xml_string(_xml(UTC_TEXT))
        assert parsed.date.tzinfo is None
        assert parsed.date == _local_of(UTC_TUPLE)

    def test_a_file_reads_the_same_as_a_string(self, tmp_path):
        path = tmp_path / "info.xml"
        path.write_text(_xml(UTC_TEXT))
        assert md.parse_info_xml(path).date == _local_of(UTC_TUPLE)

    def test_the_conversion_is_a_real_shift_when_the_zone_has_an_offset(self):
        """The guard against a conversion that quietly does nothing: outside
        UTC the parsed date differs from the text; in UTC they are equal."""
        parsed = md.parse_info_xml_string(_xml(UTC_TEXT)).date
        naive = datetime(*UTC_TUPLE)
        offset = datetime.now(timezone.utc).astimezone().utcoffset()
        if offset and offset.total_seconds() != 0:
            assert parsed != naive
        else:
            assert parsed == naive


class TestGenerating:
    def test_a_local_date_is_written_as_utc(self):
        meta = md.SnapperMetadata(type="single", num=1, date=_local_of(UTC_TUPLE))
        assert f"<date>{UTC_TEXT}</date>" in md.generate_info_xml(meta)

    def test_generate_then_parse_is_the_identity(self):
        when = datetime(2026, 3, 8, 2, 30, 0)  # a spring-forward morning
        meta = md.SnapperMetadata(type="single", num=3, date=when)
        again = md.parse_info_xml_string(md.generate_info_xml(meta))
        assert again.date == when.astimezone().replace(tzinfo=None)


class TestTheSourceAgreesWithItself:
    def test_snapper_list_and_info_xml_date_the_same_snapshot_alike(self, tmp_path):
        """The command path dates a snapshot from ``snapper --jsonout list``
        (local time); the filesystem path from its info.xml (UTC). They must
        say the same thing, or a source snapshot and its backup disagree about
        when it was taken."""
        local = _local_of(UTC_TUPLE)
        snapshots_dir = tmp_path / ".snapshots"
        slot = snapshots_dir / "7"
        (slot / "snapshot").mkdir(parents=True)
        (slot / "info.xml").write_text(_xml(UTC_TEXT))
        config = SnapperConfig(name="root", subvolume=tmp_path)
        scanner = SnapperScanner.__new__(SnapperScanner)
        scanner.use_snapper_command = True
        via_fs = scanner._get_snapshots_via_filesystem(config)
        json_out = json.dumps(
            {
                "root": [
                    {
                        "number": 7,
                        "type": "single",
                        "date": local.strftime("%Y-%m-%d %H:%M:%S"),
                        "description": "timeline",
                        "cleanup": "timeline",
                        "userdata": None,
                    }
                ]
            }
        )
        with patch(
            "btrfs_backup_ng.snapper.scanner.subprocess.run",
            return_value=type(
                "R", (), {"returncode": 0, "stdout": json_out, "stderr": ""}
            )(),
        ):
            via_cmd = scanner._get_snapshots_via_command(config)
        assert [s.date for s in via_fs] == [local]
        assert [s.date for s in via_cmd] == [local]


class TestTheRawSidecar:
    def test_the_stored_xml_is_the_authority_for_the_date(self):
        """A sidecar written before the conversion existed stored the UTC text
        as ``snapper_date``; its ``original_info_xml`` is snapper's own and
        reads correctly."""
        sidecar = md.BackupMetadata(
            snapper_config="root",
            snapper_number=7,
            snapper_type="single",
            snapper_description="",
            snapper_cleanup="",
            snapper_pre_num=None,
            snapper_userdata={},
            snapper_date=UTC_TEXT,  # the old mistake, preserved on disk
            original_info_xml=_xml(UTC_TEXT),
        )
        assert sidecar.to_snapper_metadata().date == _local_of(UTC_TUPLE)

    def test_a_sidecar_without_xml_falls_back_to_its_string(self):
        sidecar = md.BackupMetadata(
            snapper_config="root",
            snapper_number=7,
            snapper_type="single",
            snapper_description="",
            snapper_cleanup="",
            snapper_pre_num=None,
            snapper_userdata={},
            snapper_date="2026-09-23 20:00:09",
            original_info_xml="",
        )
        assert sidecar.to_snapper_metadata().date == datetime(2026, 9, 23, 20, 0, 9)

    def test_a_new_sidecar_round_trips(self):
        meta = md.parse_info_xml_string(_xml(UTC_TEXT))
        sidecar = md.BackupMetadata.from_snapper_metadata("root", meta, _xml(UTC_TEXT))
        again = md.BackupMetadata.from_dict(json.loads(json.dumps(sidecar.__dict__)))
        assert again.to_snapper_metadata().date == meta.date


@pytest.mark.parametrize("tz", ["UTC", "Pacific/Chatham", "America/New_York"])
def test_the_helpers_invert_each_other_in_any_zone(tz, monkeypatch):
    """Direct check of the pair, with the zone forced: the parsed local time
    written back is the same UTC text."""
    import time

    monkeypatch.setenv("TZ", tz)
    time.tzset()
    try:
        local = md._utc_text_to_local(UTC_TEXT)
        assert md._local_to_utc_text(local) == UTC_TEXT
        assert local == _local_of(UTC_TUPLE)
    finally:
        monkeypatch.undo()
        time.tzset()
