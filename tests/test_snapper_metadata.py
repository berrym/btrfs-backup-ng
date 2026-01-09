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
    save_backup_metadata,
)


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
        """Test generating info.xml with userdata."""
        meta = SnapperMetadata(
            type="single",
            num=100,
            date=datetime(2025, 10, 1, 12, 0, 0),
            userdata={"key1": "value1", "key2": "value2"},
        )
        xml = generate_info_xml(meta)
        assert "<userdata>" in xml
        assert "<key1>value1</key1>" in xml
        assert "<key2>value2</key2>" in xml
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
