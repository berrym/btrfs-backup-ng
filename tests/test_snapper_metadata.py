"""Tests for snapper metadata handling."""

import json
from datetime import datetime

import pytest

from btrfs_backup_ng.snapper.metadata import (
    BackupMetadata,
    SnapperMetadata,
    generate_info_xml,
    load_backup_metadata,
    parse_info_xml,
    parse_info_xml_string,
    save_backup_metadata,
)


class TestSnapperSnapshotBackupName:
    """SnapperSnapshot.get_backup_name honors the configured timestamp_format (#7)."""

    @staticmethod
    def _snapshot():
        from pathlib import Path
        from unittest.mock import MagicMock

        from btrfs_backup_ng.snapper.snapshot import SnapperSnapshot

        meta = MagicMock()
        meta.date = datetime(2024, 1, 15, 14, 30, 22)
        return SnapperSnapshot(
            config_name="root",
            number=7,
            metadata=meta,
            subvolume_path=Path("/snap"),
            info_xml_path=Path("/snap/info.xml"),
        )

    def test_default_format(self):
        assert self._snapshot().get_backup_name() == "root-7-20240115-143022"

    def test_honors_configured_format(self):
        assert (
            self._snapshot().get_backup_name("%Y%m%dT%H%M%S")
            == "root-7-20240115T143022"
        )


class TestCreateSnapperSnapshotWrapper:
    """The real wrapper is null-safe and honors the endpoint's timestamp_format.

    Regression guard for the #7 blocker: the wrapper dereferenced a None
    destination_endpoint, crashing every snapper backup. The snapper_cmd tests
    mock the wrapper out, so this exercises the REAL function.
    """

    @staticmethod
    def _snap(tmp_path):
        from unittest.mock import MagicMock

        from btrfs_backup_ng.snapper.snapshot import SnapperSnapshot

        meta = MagicMock()
        meta.date = datetime(2024, 1, 15, 14, 30, 22)
        sub = tmp_path / "snapshot"
        sub.mkdir()
        return SnapperSnapshot(
            config_name="root",
            number=7,
            metadata=meta,
            subvolume_path=sub,
            info_xml_path=sub.parent / "info.xml",
        )

    def test_none_endpoint_does_not_crash(self, tmp_path):
        from btrfs_backup_ng.core.operations import _create_snapper_snapshot_wrapper

        wrapper = _create_snapper_snapshot_wrapper(self._snap(tmp_path))
        assert wrapper.get_name() == "root-7-20240115-143022"

    def test_honors_endpoint_timestamp_format(self, tmp_path):
        from types import SimpleNamespace

        from btrfs_backup_ng.core.operations import _create_snapper_snapshot_wrapper

        ep = SimpleNamespace(config={"timestamp_format": "%Y%m%dT%H%M%S"})
        wrapper = _create_snapper_snapshot_wrapper(self._snap(tmp_path), ep)
        assert wrapper.get_name() == "root-7-20240115T143022"


class TestSnapperMetadata:
    """Tests for SnapperMetadata dataclass."""

    def test_create_basic_metadata(self):
        """Test creating basic metadata."""
        date = datetime(2025, 10, 1, 11, 42, 50)
        meta = SnapperMetadata(
            type="single",
            num=10368,
            date=date,
            description="timeline",
            cleanup="timeline",
        )
        assert meta.type == "single"
        assert meta.num == 10368
        assert meta.date == date
        assert meta.description == "timeline"
        assert meta.cleanup == "timeline"
        assert meta.pre_num is None
        assert meta.userdata == {}

    def test_create_post_metadata(self):
        """Test creating post snapshot metadata with pre_num."""
        date = datetime(2025, 8, 30, 14, 50, 55)
        meta = SnapperMetadata(
            type="post",
            num=9914,
            date=date,
            description="dnf remove neovim",
            cleanup="number",
            pre_num=9913,
        )
        assert meta.type == "post"
        assert meta.pre_num == 9913

    def test_to_dict(self):
        """Test conversion to dictionary."""
        date = datetime(2025, 10, 1, 11, 42, 50)
        meta = SnapperMetadata(
            type="single",
            num=100,
            date=date,
            description="test",
            cleanup="timeline",
            userdata={"key": "value"},
        )
        d = meta.to_dict()
        assert d["type"] == "single"
        assert d["num"] == 100
        assert d["date"] == "2025-10-01 11:42:50"
        assert d["description"] == "test"
        assert d["cleanup"] == "timeline"
        assert d["pre_num"] is None
        assert d["userdata"] == {"key": "value"}

    def test_from_dict(self):
        """Test creation from dictionary."""
        d = {
            "type": "single",
            "num": 100,
            "date": "2025-10-01 11:42:50",
            "description": "test",
            "cleanup": "timeline",
            "pre_num": None,
            "userdata": {},
        }
        meta = SnapperMetadata.from_dict(d)
        assert meta.type == "single"
        assert meta.num == 100
        assert meta.date == datetime(2025, 10, 1, 11, 42, 50)

    def test_roundtrip_dict(self):
        """Test roundtrip through dict."""
        date = datetime(2025, 10, 1, 11, 42, 50)
        original = SnapperMetadata(
            type="post",
            num=200,
            date=date,
            description="test desc",
            cleanup="number",
            pre_num=199,
            userdata={"foo": "bar"},
        )
        restored = SnapperMetadata.from_dict(original.to_dict())
        assert restored.type == original.type
        assert restored.num == original.num
        assert restored.date == original.date
        assert restored.description == original.description
        assert restored.cleanup == original.cleanup
        assert restored.pre_num == original.pre_num
        assert restored.userdata == original.userdata


class TestParseInfoXml:
    """Tests for parse_info_xml function."""

    def test_parse_single_snapshot(self, tmp_path):
        """Test parsing a single snapshot info.xml."""
        xml_content = """<?xml version="1.0"?>
<snapshot>
  <type>single</type>
  <num>10368</num>
  <date>2025-10-01 11:42:50</date>
  <description>timeline</description>
  <cleanup>timeline</cleanup>
</snapshot>"""
        xml_file = tmp_path / "info.xml"
        xml_file.write_text(xml_content)

        meta = parse_info_xml(xml_file)
        assert meta.type == "single"
        assert meta.num == 10368
        assert meta.date == datetime(2025, 10, 1, 11, 42, 50)
        assert meta.description == "timeline"
        assert meta.cleanup == "timeline"
        assert meta.pre_num is None

    def test_parse_post_snapshot(self, tmp_path):
        """Test parsing a post snapshot info.xml."""
        xml_content = """<?xml version="1.0"?>
<snapshot>
  <type>post</type>
  <num>9914</num>
  <date>2025-08-30 14:50:55</date>
  <pre_num>9913</pre_num>
  <description>dnf remove neovim</description>
  <cleanup>number</cleanup>
</snapshot>"""
        xml_file = tmp_path / "info.xml"
        xml_file.write_text(xml_content)

        meta = parse_info_xml(xml_file)
        assert meta.type == "post"
        assert meta.num == 9914
        assert meta.pre_num == 9913
        assert meta.description == "dnf remove neovim"

    def test_parse_with_userdata(self, tmp_path):
        """Test parsing info.xml with userdata."""
        xml_content = """<?xml version="1.0"?>
<snapshot>
  <type>single</type>
  <num>100</num>
  <date>2025-01-01 12:00:00</date>
  <description>manual snapshot</description>
  <userdata>
    <important>yes</important>
    <comment>user added</comment>
  </userdata>
</snapshot>"""
        xml_file = tmp_path / "info.xml"
        xml_file.write_text(xml_content)

        meta = parse_info_xml(xml_file)
        assert meta.userdata == {"important": "yes", "comment": "user added"}

    def test_parse_minimal_xml(self, tmp_path):
        """Test parsing minimal info.xml with only required fields."""
        xml_content = """<?xml version="1.0"?>
<snapshot>
  <type>single</type>
  <num>1</num>
  <date>2025-01-01 00:00:00</date>
</snapshot>"""
        xml_file = tmp_path / "info.xml"
        xml_file.write_text(xml_content)

        meta = parse_info_xml(xml_file)
        assert meta.type == "single"
        assert meta.num == 1
        assert meta.description == ""
        assert meta.cleanup == ""

    def test_parse_missing_file(self, tmp_path):
        """Test parsing non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            parse_info_xml(tmp_path / "nonexistent.xml")

    def test_parse_invalid_xml(self, tmp_path):
        """Test parsing invalid XML raises ValueError."""
        xml_file = tmp_path / "info.xml"
        xml_file.write_text("not valid xml <><>")
        with pytest.raises(ValueError, match="Failed to parse"):
            parse_info_xml(xml_file)

    def test_parse_missing_type(self, tmp_path):
        """Test parsing XML missing type element."""
        xml_content = """<?xml version="1.0"?>
<snapshot>
  <num>1</num>
  <date>2025-01-01 00:00:00</date>
</snapshot>"""
        xml_file = tmp_path / "info.xml"
        xml_file.write_text(xml_content)
        with pytest.raises(ValueError, match="Missing <type>"):
            parse_info_xml(xml_file)

    def test_parse_wrong_root(self, tmp_path):
        """Test parsing XML with wrong root element."""
        xml_content = """<?xml version="1.0"?>
<wrong>
  <type>single</type>
</wrong>"""
        xml_file = tmp_path / "info.xml"
        xml_file.write_text(xml_content)
        with pytest.raises(ValueError, match="Expected <snapshot>"):
            parse_info_xml(xml_file)


class TestGenerateInfoXml:
    """Tests for generate_info_xml function."""

    def test_generate_basic(self):
        """Test generating basic info.xml."""
        meta = SnapperMetadata(
            type="single",
            num=100,
            date=datetime(2025, 10, 1, 11, 42, 50),
            description="timeline",
            cleanup="timeline",
        )
        xml = generate_info_xml(meta)
        assert '<?xml version="1.0"?>' in xml
        assert "<snapshot>" in xml
        assert "<type>single</type>" in xml
        assert "<num>100</num>" in xml
        assert "<date>2025-10-01 11:42:50</date>" in xml
        assert "<description>timeline</description>" in xml
        assert "<cleanup>timeline</cleanup>" in xml
        assert "</snapshot>" in xml

    def test_generate_with_pre_num(self):
        """Test generating info.xml with pre_num."""
        meta = SnapperMetadata(
            type="post",
            num=200,
            date=datetime(2025, 10, 1, 12, 0, 0),
            description="package update",
            cleanup="number",
            pre_num=199,
        )
        xml = generate_info_xml(meta)
        assert "<type>post</type>" in xml
        assert "<pre_num>199</pre_num>" in xml

    def test_generate_with_userdata(self):
        """Test generating info.xml with userdata (snapper <key>/<value> pairs)."""
        meta = SnapperMetadata(
            type="single",
            num=100,
            date=datetime(2025, 10, 1, 12, 0, 0),
            userdata={"key1": "value1", "key2": "value2"},
        )
        xml = generate_info_xml(meta)
        assert "<userdata>" in xml
        assert "<key>key1</key>" in xml
        assert "<value>value1</value>" in xml
        assert "<key>key2</key>" in xml
        assert "<value>value2</value>" in xml
        assert "</userdata>" in xml

    def test_generate_escapes_special_chars(self):
        """Test that special XML characters are escaped."""
        meta = SnapperMetadata(
            type="single",
            num=100,
            date=datetime(2025, 10, 1, 12, 0, 0),
            description="test <with> & special",
        )
        xml = generate_info_xml(meta)
        assert "&lt;with&gt;" in xml
        assert "&amp;" in xml

    def test_roundtrip_xml(self, tmp_path):
        """Test roundtrip through XML generation and parsing."""
        original = SnapperMetadata(
            type="post",
            num=500,
            date=datetime(2025, 6, 15, 10, 30, 45),
            description="test snapshot",
            cleanup="timeline",
            pre_num=499,
            userdata={"tag": "important"},
        )
        xml = generate_info_xml(original)
        xml_file = tmp_path / "info.xml"
        xml_file.write_text(xml)

        restored = parse_info_xml(xml_file)
        assert restored.type == original.type
        assert restored.num == original.num
        assert restored.date == original.date
        assert restored.description == original.description
        assert restored.cleanup == original.cleanup
        assert restored.pre_num == original.pre_num
        assert restored.userdata == original.userdata


class TestBackupMetadata:
    """Tests for BackupMetadata class."""

    def test_from_snapper_metadata(self):
        """Test creating BackupMetadata from SnapperMetadata."""
        snapper_meta = SnapperMetadata(
            type="single",
            num=100,
            date=datetime(2025, 10, 1, 12, 0, 0),
            description="timeline",
            cleanup="timeline",
            userdata={"key": "val"},
        )
        original_xml = '<?xml version="1.0"?><snapshot>...</snapshot>'

        backup_meta = BackupMetadata.from_snapper_metadata(
            "root", snapper_meta, original_xml
        )
        assert backup_meta.snapper_config == "root"
        assert backup_meta.snapper_number == 100
        assert backup_meta.snapper_type == "single"
        assert backup_meta.snapper_description == "timeline"
        assert backup_meta.snapper_cleanup == "timeline"
        assert backup_meta.snapper_pre_num is None
        assert backup_meta.snapper_userdata == {"key": "val"}
        assert backup_meta.original_info_xml == original_xml

    def test_to_snapper_metadata(self):
        """Test converting BackupMetadata back to SnapperMetadata."""
        backup_meta = BackupMetadata(
            snapper_config="home",
            snapper_number=50,
            snapper_type="post",
            snapper_description="update",
            snapper_cleanup="number",
            snapper_pre_num=49,
            snapper_userdata={},
            snapper_date="2025-05-01 10:00:00",
            original_info_xml="",
        )
        snapper_meta = backup_meta.to_snapper_metadata()
        assert snapper_meta.type == "post"
        assert snapper_meta.num == 50
        assert snapper_meta.date == datetime(2025, 5, 1, 10, 0, 0)
        assert snapper_meta.pre_num == 49


class TestSaveLoadBackupMetadata:
    """Tests for save/load backup metadata functions."""

    def test_save_and_load(self, tmp_path):
        """Test saving and loading backup metadata."""
        meta = BackupMetadata(
            snapper_config="root",
            snapper_number=1000,
            snapper_type="single",
            snapper_description="timeline",
            snapper_cleanup="timeline",
            snapper_pre_num=None,
            snapper_userdata={"foo": "bar"},
            snapper_date="2025-10-01 12:00:00",
            original_info_xml='<?xml version="1.0"?>...',
        )
        meta_file = tmp_path / "test.snapper-meta.json"
        save_backup_metadata(meta_file, meta)

        loaded = load_backup_metadata(meta_file)
        assert loaded.snapper_config == meta.snapper_config
        assert loaded.snapper_number == meta.snapper_number
        assert loaded.snapper_type == meta.snapper_type
        assert loaded.snapper_description == meta.snapper_description
        assert loaded.snapper_cleanup == meta.snapper_cleanup
        assert loaded.snapper_pre_num == meta.snapper_pre_num
        assert loaded.snapper_userdata == meta.snapper_userdata
        assert loaded.snapper_date == meta.snapper_date
        assert loaded.original_info_xml == meta.original_info_xml

    def test_load_missing_file(self, tmp_path):
        """Test loading non-existent file."""
        with pytest.raises(FileNotFoundError):
            load_backup_metadata(tmp_path / "nonexistent.json")

    def test_load_invalid_json(self, tmp_path):
        """Test loading invalid JSON."""
        meta_file = tmp_path / "bad.json"
        meta_file.write_text("not valid json {{{")
        with pytest.raises(ValueError, match="Invalid metadata JSON"):
            load_backup_metadata(meta_file)

    def test_saved_file_is_valid_json(self, tmp_path):
        """Test that saved file is valid, readable JSON."""
        meta = BackupMetadata(
            snapper_config="test",
            snapper_number=1,
            snapper_type="single",
            snapper_description="",
            snapper_cleanup="",
            snapper_pre_num=None,
            snapper_userdata={},
            snapper_date="2025-01-01 00:00:00",
            original_info_xml="",
        )
        meta_file = tmp_path / "test.json"
        save_backup_metadata(meta_file, meta)

        # Verify it's valid JSON that can be loaded directly
        with open(meta_file) as f:
            data = json.load(f)
        assert data["snapper_config"] == "test"
        assert data["snapper_number"] == 1


class TestUserdataKeyValueFormat:
    """R11: snapper stores userdata as <key>/<value> pairs; parse+generate round-trip."""

    # Snapper's real 0.13.0 format: ONE <userdata> block per entry (siblings).
    SNAPPER_MULTI = (
        "<?xml version='1.0'?><snapshot>"
        "<type>single</type><num>7</num><date>2026-07-29 03:12:48</date>"
        "<userdata><key>reason</key><value>manual</value></userdata>"
        "<userdata><key>ticket</key><value>ABC-123</value></userdata>"
        "</snapshot>"
    )
    # An alternative single-block-multi-pair form we also accept.
    SNAPPER_SINGLE_BLOCK = (
        "<?xml version='1.0'?><snapshot>"
        "<type>single</type><num>7</num><date>2026-07-29 03:12:48</date>"
        "<userdata>"
        "<key>reason</key><value>manual</value>"
        "<key>ticket</key><value>ABC-123</value>"
        "</userdata></snapshot>"
    )

    def test_parse_string_multi_entry_userdata(self):
        """Snapper's sibling <userdata> blocks parse to a full {name: value} dict."""
        meta = parse_info_xml_string(self.SNAPPER_MULTI)
        assert meta.userdata == {"reason": "manual", "ticket": "ABC-123"}
        assert meta.type == "single"
        assert meta.num == 7

    def test_parse_single_block_multi_pair_also_supported(self):
        """A single <userdata> block with several key/value pairs also parses."""
        meta = parse_info_xml_string(self.SNAPPER_SINGLE_BLOCK)
        assert meta.userdata == {"reason": "manual", "ticket": "ABC-123"}

    def test_parse_file_multi_entry_userdata(self, tmp_path):
        """The file parser shares the fixed logic (not just the string variant)."""
        xml_file = tmp_path / "info.xml"
        xml_file.write_text(self.SNAPPER_MULTI)
        meta = parse_info_xml(xml_file)
        assert meta.userdata == {"reason": "manual", "ticket": "ABC-123"}

    def test_generate_emits_key_value_pairs(self):
        """generate emits snapper's <key>/<value> form, not <name>value</name>."""
        meta = SnapperMetadata(
            type="single",
            num=7,
            date=datetime(2026, 7, 29, 3, 12, 48),
            userdata={"reason": "manual", "ticket": "ABC-123"},
        )
        xml = generate_info_xml(meta)
        assert "<key>reason</key>" in xml
        assert "<value>manual</value>" in xml
        assert "<key>ticket</key>" in xml
        assert "<value>ABC-123</value>" in xml
        assert "<reason>manual</reason>" not in xml  # the old, wrong form

    def test_round_trip_preserves_multi_entry(self):
        """parse(generate(x)) == x for multi-entry userdata (the real regression)."""
        userdata = {"reason": "manual", "ticket": "ABC-123", "important": "yes"}
        meta = SnapperMetadata(
            type="pre",
            num=42,
            date=datetime(2026, 7, 29, 3, 12, 48),
            description="before upgrade",
            cleanup="number",
            userdata=userdata,
        )
        reparsed = parse_info_xml_string(generate_info_xml(meta))
        assert reparsed.userdata == userdata
        assert reparsed.type == "pre"
        assert reparsed.description == "before upgrade"
        assert reparsed.cleanup == "number"

    def test_legacy_name_value_form_still_parses(self):
        """A legacy <name>value</name> userdata child is still read (back-compat)."""
        legacy = (
            "<?xml version='1.0'?><snapshot>"
            "<type>single</type><num>1</num><date>2026-07-29 03:12:48</date>"
            "<userdata><reason>manual</reason></userdata></snapshot>"
        )
        meta = parse_info_xml_string(legacy)
        assert meta.userdata == {"reason": "manual"}

    def test_value_without_key_is_ignored(self):
        """A malformed <value> with no preceding <key> does not corrupt the dict."""
        malformed = (
            "<?xml version='1.0'?><snapshot>"
            "<type>single</type><num>1</num><date>2026-07-29 03:12:48</date>"
            "<userdata><value>orphan</value><key>k</key><value>v</value></userdata>"
            "</snapshot>"
        )
        meta = parse_info_xml_string(malformed)
        assert meta.userdata == {"k": "v"}

    def test_parse_string_rejects_malformed_xml(self):
        """A non-XML original_info_xml raises ValueError (restore falls back)."""
        with pytest.raises(ValueError):
            parse_info_xml_string("{not xml")


class TestRenumberInfoXml:
    """R11 #4: renumber preserves snapper's xml verbatim (incl. unmodeled <uid>)."""

    SNAP_WITH_UID = (
        "<?xml version='1.0'?>\n<snapshot>\n"
        "  <type>single</type>\n  <num>6052</num>\n"
        "  <date>2026-07-31 10:40:37</date>\n"
        "  <description>Fedora restore point</description>\n"
        "  <cleanup>number</cleanup>\n"
        "  <uid>1000</uid>\n"
        "  <userdata>\n    <key>reason</key>\n    <value>manual</value>\n  </userdata>\n"
        "  <userdata>\n    <key>ticket</key>\n    <value>OP-1</value>\n  </userdata>\n"
        "</snapshot>\n"
    )

    def test_changes_only_num_and_preserves_uid(self):
        from btrfs_backup_ng.snapper.metadata import renumber_info_xml

        out = renumber_info_xml(self.SNAP_WITH_UID, 99)
        assert "<num>99</num>" in out
        assert "<num>6052</num>" not in out
        # everything else verbatim
        assert "<uid>1000</uid>" in out  # the unmodeled element R11 must NOT drop
        assert "<key>reason</key>" in out and "<value>manual</value>" in out
        assert "<key>ticket</key>" in out and "<value>OP-1</value>" in out
        assert "Fedora restore point" in out
        assert "<cleanup>number</cleanup>" in out

    def test_reparses_to_same_metadata_with_new_num(self):
        from btrfs_backup_ng.snapper.metadata import (
            parse_info_xml_string,
            renumber_info_xml,
        )

        m = parse_info_xml_string(renumber_info_xml(self.SNAP_WITH_UID, 99))
        assert m.num == 99
        assert m.userdata == {"reason": "manual", "ticket": "OP-1"}

    def test_rejects_missing_num(self):
        from btrfs_backup_ng.snapper.metadata import renumber_info_xml

        with pytest.raises(ValueError):
            renumber_info_xml(
                "<?xml version='1.0'?><snapshot><type>single</type></snapshot>", 1
            )

    def test_rejects_malformed_xml(self):
        from btrfs_backup_ng.snapper.metadata import renumber_info_xml

        with pytest.raises(ValueError):
            renumber_info_xml("{not xml", 1)


class TestUserdataEscaping:
    """R11 #5: userdata keys/values with XML special chars round-trip safely."""

    def test_special_chars_round_trip(self):
        meta = SnapperMetadata(
            type="single",
            num=1,
            date=datetime(2026, 7, 31, 10, 40, 37),
            userdata={"a&b": "x<y>z", "note": "tom & jerry <fwd>"},
        )
        xml = generate_info_xml(meta)
        # raw xml must be escaped (not literal & < >)
        assert "&amp;" in xml and "&lt;" in xml and "&gt;" in xml
        assert "<value>x<y>z</value>" not in xml  # unescaped would be malformed
        # and it must parse back to the exact original dict
        reparsed = parse_info_xml_string(xml)
        assert reparsed.userdata == {"a&b": "x<y>z", "note": "tom & jerry <fwd>"}


class TestSaveBackupMetadataAtomic:
    """R11 #6 / pt1: sidecar is written atomically at 0600."""

    def test_saved_file_is_0600_and_no_temp_left(self, tmp_path):
        import os

        meta = BackupMetadata(
            snapper_config="root",
            snapper_number=1,
            snapper_type="single",
            snapper_description="d",
            snapper_cleanup="number",
            snapper_pre_num=None,
            snapper_userdata={},
            snapper_date="2026-07-31 10:40:37",
            original_info_xml="",
        )
        path = tmp_path / "root-1.snapper-meta.json"
        save_backup_metadata(path, meta)
        # tightened mode from pt1 -- a revert to open('w') would yield 0644
        assert oct(os.stat(path).st_mode & 0o777) == oct(0o600)
        # atomic write leaves no stray temp sibling in the dir
        assert [p.name for p in tmp_path.iterdir()] == ["root-1.snapper-meta.json"]
