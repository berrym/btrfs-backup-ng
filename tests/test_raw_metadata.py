"""Tests for raw target metadata handling."""

import json
from datetime import datetime, timezone
from pathlib import Path


from btrfs_backup_ng.endpoint.raw_metadata import (
    COMPRESSION_CONFIG,
    RawSnapshot,
    discover_raw_snapshots,
    get_file_extension,
    parse_stream_filename,
)


class TestRawSnapshot:
    """Tests for RawSnapshot dataclass."""

    def test_basic_creation(self):
        """Test creating a basic RawSnapshot."""
        snapshot = RawSnapshot(
            name="root.20240115T120000",
            stream_path=Path("/backup/root.20240115T120000.btrfs"),
        )
        assert snapshot.name == "root.20240115T120000"
        assert snapshot.stream_path == Path("/backup/root.20240115T120000.btrfs")
        assert snapshot.uuid == ""
        assert snapshot.parent_uuid is None
        assert snapshot.parent_name is None
        assert snapshot.compress is None
        assert snapshot.encrypt is None

    def test_with_compression_and_encryption(self):
        """Test snapshot with compression and encryption."""
        snapshot = RawSnapshot(
            name="home.20240115T120000",
            stream_path=Path("/backup/home.20240115T120000.btrfs.zst.gpg"),
            uuid="abc123",
            parent_uuid="def456",
            compress="zstd",
            encrypt="gpg",
            gpg_recipient="backup@example.com",
        )
        assert snapshot.compress == "zstd"
        assert snapshot.encrypt == "gpg"
        assert snapshot.gpg_recipient == "backup@example.com"
        assert snapshot.is_incremental is True

    def test_metadata_path(self):
        """Test metadata path derivation."""
        snapshot = RawSnapshot(
            name="test",
            stream_path=Path("/backup/test.btrfs.zst"),
        )
        assert snapshot.metadata_path == Path("/backup/test.btrfs.zst.meta")

    def test_is_incremental(self):
        """Test incremental detection."""
        # Not incremental
        snap1 = RawSnapshot(name="test", stream_path=Path("/backup/test.btrfs"))
        assert snap1.is_incremental is False

        # Incremental by parent_uuid
        snap2 = RawSnapshot(
            name="test",
            stream_path=Path("/backup/test.btrfs"),
            parent_uuid="abc123",
        )
        assert snap2.is_incremental is True

        # Incremental by parent_name
        snap3 = RawSnapshot(
            name="test",
            stream_path=Path("/backup/test.btrfs"),
            parent_name="test.prev",
        )
        assert snap3.is_incremental is True

    def test_to_dict(self):
        """Test serialization to dictionary."""
        created = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        snapshot = RawSnapshot(
            name="root.20240115T120000",
            stream_path=Path("/backup/root.btrfs.zst"),
            uuid="abc123",
            parent_uuid="def456",
            parent_name="root.20240114T120000",
            created=created,
            size=1234567,
            compress="zstd",
            encrypt="gpg",
            gpg_recipient="backup@example.com",
        )

        data = snapshot.to_dict()

        assert data["version"] == 1
        assert data["name"] == "root.20240115T120000"
        assert data["uuid"] == "abc123"
        assert data["parent_uuid"] == "def456"
        assert data["parent_name"] == "root.20240114T120000"
        assert data["size"] == 1234567
        assert data["pipeline"]["compress"] == "zstd"
        assert data["pipeline"]["encrypt"] == "gpg"
        assert data["pipeline"]["gpg_recipient"] == "backup@example.com"
        assert "btrfs_backup_ng_version" in data

    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "version": 1,
            "name": "test.20240115T120000",
            "uuid": "abc123",
            "parent_uuid": "def456",
            "created": "2024-01-15T12:00:00+00:00",
            "size": 9876543,
            "pipeline": {
                "compress": "lz4",
                "encrypt": None,
                "gpg_recipient": None,
            },
        }

        snapshot = RawSnapshot.from_dict(data, Path("/backup/test.btrfs.lz4"))

        assert snapshot.name == "test.20240115T120000"
        assert snapshot.uuid == "abc123"
        assert snapshot.parent_uuid == "def456"
        assert snapshot.size == 9876543
        assert snapshot.compress == "lz4"
        assert snapshot.encrypt is None

    def test_save_and_load_metadata(self, tmp_path):
        """Test saving and loading metadata files."""
        stream_path = tmp_path / "test.btrfs.zst"
        stream_path.touch()

        snapshot = RawSnapshot(
            name="test.20240115T120000",
            stream_path=stream_path,
            uuid="abc123",
            size=1000,
            compress="zstd",
        )

        # Save metadata
        snapshot.save_metadata()
        assert snapshot.metadata_path.exists()

        # Verify JSON content
        with open(snapshot.metadata_path) as f:
            data = json.load(f)
        assert data["name"] == "test.20240115T120000"
        assert data["pipeline"]["compress"] == "zstd"

        # Load metadata
        loaded = RawSnapshot.load_metadata(snapshot.metadata_path)
        assert loaded.name == snapshot.name
        assert loaded.uuid == snapshot.uuid
        assert loaded.compress == snapshot.compress


class TestGetFileExtension:
    """Tests for get_file_extension function."""

    def test_no_compression_no_encryption(self):
        """Test plain btrfs stream."""
        assert get_file_extension(None, None) == ".btrfs"

    def test_with_compression_only(self):
        """Test with various compression algorithms."""
        assert get_file_extension("gzip", None) == ".btrfs.gz"
        assert get_file_extension("zstd", None) == ".btrfs.zst"
        assert get_file_extension("lz4", None) == ".btrfs.lz4"
        assert get_file_extension("xz", None) == ".btrfs.xz"
        assert get_file_extension("lzo", None) == ".btrfs.lzo"
        assert get_file_extension("bzip2", None) == ".btrfs.bz2"

    def test_with_encryption_only(self):
        """Test with GPG encryption."""
        assert get_file_extension(None, "gpg") == ".btrfs.gpg"

    def test_with_openssl_encryption_only(self):
        """Test with OpenSSL encryption."""
        assert get_file_extension(None, "openssl_enc") == ".btrfs.enc"

    def test_with_compression_and_encryption(self):
        """Test with both compression and encryption."""
        assert get_file_extension("zstd", "gpg") == ".btrfs.zst.gpg"
        assert get_file_extension("gzip", "gpg") == ".btrfs.gz.gpg"
        assert get_file_extension("lz4", "gpg") == ".btrfs.lz4.gpg"

    def test_with_compression_and_openssl_encryption(self):
        """Test with compression and OpenSSL encryption."""
        assert get_file_extension("zstd", "openssl_enc") == ".btrfs.zst.enc"
        assert get_file_extension("gzip", "openssl_enc") == ".btrfs.gz.enc"


class TestParseStreamFilename:
    """Tests for parse_stream_filename function."""

    def test_plain_stream(self):
        """Test parsing plain btrfs stream filename."""
        result = parse_stream_filename("root.20240115T120000.btrfs")
        assert result["name"] == "root.20240115T120000"
        assert result["compress"] is None
        assert result["encrypt"] is None

    def test_compressed_stream(self):
        """Test parsing compressed stream filenames."""
        result = parse_stream_filename("home.20240115T120000.btrfs.zst")
        assert result["name"] == "home.20240115T120000"
        assert result["compress"] == "zstd"
        assert result["encrypt"] is None

        result = parse_stream_filename("var.20240115T120000.btrfs.gz")
        assert result["name"] == "var.20240115T120000"
        assert result["compress"] == "gzip"

    def test_encrypted_stream(self):
        """Test parsing encrypted stream filename."""
        result = parse_stream_filename("root.20240115T120000.btrfs.gpg")
        assert result["name"] == "root.20240115T120000"
        assert result["compress"] is None
        assert result["encrypt"] == "gpg"

    def test_openssl_encrypted_stream(self):
        """Test parsing OpenSSL encrypted stream filename."""
        result = parse_stream_filename("root.20240115T120000.btrfs.enc")
        assert result["name"] == "root.20240115T120000"
        assert result["compress"] is None
        assert result["encrypt"] == "openssl_enc"

    def test_compressed_and_encrypted(self):
        """Test parsing compressed and encrypted stream."""
        result = parse_stream_filename("root.20240115T120000.btrfs.zst.gpg")
        assert result["name"] == "root.20240115T120000"
        assert result["compress"] == "zstd"
        assert result["encrypt"] == "gpg"

    def test_compressed_and_openssl_encrypted(self):
        """Test parsing compressed and OpenSSL encrypted stream."""
        result = parse_stream_filename("root.20240115T120000.btrfs.zst.enc")
        assert result["name"] == "root.20240115T120000"
        assert result["compress"] == "zstd"
        assert result["encrypt"] == "openssl_enc"


class TestDiscoverRawSnapshots:
    """Tests for discover_raw_snapshots function."""

    def test_empty_directory(self, tmp_path):
        """Test discovering snapshots in empty directory."""
        snapshots = discover_raw_snapshots(tmp_path)
        assert snapshots == []

    def test_nonexistent_directory(self, tmp_path):
        """Test discovering snapshots in nonexistent directory."""
        snapshots = discover_raw_snapshots(tmp_path / "nonexistent")
        assert snapshots == []

    def test_discover_from_metadata_files(self, tmp_path):
        """Test discovering snapshots from metadata files."""
        # Create stream and metadata files
        stream_path = tmp_path / "root.20240115T120000.btrfs.zst"
        stream_path.write_bytes(b"dummy stream data")

        meta_path = tmp_path / "root.20240115T120000.btrfs.zst.meta"
        meta_data = {
            "version": 1,
            "name": "root.20240115T120000",
            "uuid": "abc123",
            "created": "2024-01-15T12:00:00+00:00",
            "size": 1000,
            "pipeline": {"compress": "zstd", "encrypt": None},
        }
        meta_path.write_text(json.dumps(meta_data))

        snapshots = discover_raw_snapshots(tmp_path)
        assert len(snapshots) == 1
        assert snapshots[0].name == "root.20240115T120000"
        assert snapshots[0].compress == "zstd"

    def test_discover_from_filenames(self, tmp_path):
        """Test discovering snapshots from filenames when metadata is missing."""
        # Create stream files without metadata
        (tmp_path / "home.20240115T120000.btrfs.lz4").write_bytes(b"data1")
        (tmp_path / "home.20240116T120000.btrfs.lz4").write_bytes(b"data2")

        snapshots = discover_raw_snapshots(tmp_path)
        assert len(snapshots) == 2
        assert snapshots[0].name == "home.20240115T120000"
        assert snapshots[1].name == "home.20240116T120000"

    def test_prefix_filter(self, tmp_path):
        """Test filtering by prefix."""
        (tmp_path / "root.20240115T120000.btrfs").write_bytes(b"data1")
        (tmp_path / "home.20240115T120000.btrfs").write_bytes(b"data2")

        # Filter by root prefix
        snapshots = discover_raw_snapshots(tmp_path, prefix="root")
        assert len(snapshots) == 1
        assert snapshots[0].name == "root.20240115T120000"

        # Filter by home prefix
        snapshots = discover_raw_snapshots(tmp_path, prefix="home")
        assert len(snapshots) == 1
        assert snapshots[0].name == "home.20240115T120000"

    def test_sorted_by_creation_time(self, tmp_path):
        """Test that snapshots are sorted by creation time."""
        import time

        # Create files with different times
        (tmp_path / "root.20240115T100000.btrfs").write_bytes(b"data1")
        time.sleep(0.1)
        (tmp_path / "root.20240115T120000.btrfs").write_bytes(b"data2")
        time.sleep(0.1)
        (tmp_path / "root.20240115T110000.btrfs").write_bytes(b"data3")

        snapshots = discover_raw_snapshots(tmp_path)
        # Should be sorted by mtime (file creation order)
        assert len(snapshots) == 3


class TestCompressionConfig:
    """Tests for compression configuration."""

    def test_all_algorithms_have_config(self):
        """Test that all expected algorithms are configured."""
        expected = {"gzip", "pigz", "zstd", "lz4", "xz", "lzo", "pbzip2", "bzip2"}
        assert set(COMPRESSION_CONFIG.keys()) == expected

    def test_config_has_required_fields(self):
        """Test that each config has required fields."""
        for algo, config in COMPRESSION_CONFIG.items():
            assert "extension" in config, f"{algo} missing extension"
            assert "compress_cmd" in config, f"{algo} missing compress_cmd"
            assert "decompress_cmd" in config, f"{algo} missing decompress_cmd"
            assert isinstance(config["compress_cmd"], list)
            assert isinstance(config["decompress_cmd"], list)
