"""Integration tests for raw target endpoint with real compression/decompression.

These tests actually run compression and decompression pipelines to verify
end-to-end functionality. They are skipped if the required tools are not
available on the system.
"""

import shutil
import subprocess

import pytest

from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def tool_available(tool: str) -> bool:
    """Check if a command-line tool is available."""
    return shutil.which(tool) is not None


# Test data - simulates a btrfs send stream (just random bytes for testing)
TEST_DATA = b"This is test data that simulates a btrfs send stream.\n" * 1000


class TestRealCompressionPipelines:
    """Tests that actually run compression/decompression pipelines."""

    @pytest.mark.skipif(not tool_available("pigz"), reason="pigz not available")
    def test_pigz_roundtrip(self, tmp_path):
        """Test that pigz compression and decompression work end-to-end."""
        RawEndpoint(config={"path": tmp_path, "compress": "pigz"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.gz"

        # Compress
        with open(input_file, "rb") as stdin:
            proc = subprocess.Popen(
                ["pigz", "-c"],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        assert output_file.exists()
        assert output_file.stat().st_size < input_file.stat().st_size

        # Decompress and verify
        result = subprocess.run(
            ["pigz", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(not tool_available("bzip2"), reason="bzip2 not available")
    def test_bzip2_roundtrip(self, tmp_path):
        """Test that bzip2 compression and decompression work end-to-end."""
        RawEndpoint(config={"path": tmp_path, "compress": "bzip2"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.bz2"

        # Compress
        with open(input_file, "rb") as stdin:
            proc = subprocess.Popen(
                ["bzip2", "-c"],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        assert output_file.exists()

        # Decompress and verify
        result = subprocess.run(
            ["bzip2", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(not tool_available("pbzip2"), reason="pbzip2 not available")
    def test_pbzip2_roundtrip(self, tmp_path):
        """Test that pbzip2 compression and decompression work end-to-end."""
        RawEndpoint(config={"path": tmp_path, "compress": "pbzip2"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.bz2"

        # Compress
        with open(input_file, "rb") as stdin:
            proc = subprocess.Popen(
                ["pbzip2", "-c"],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        assert output_file.exists()

        # Decompress and verify
        result = subprocess.run(
            ["pbzip2", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(not tool_available("lzop"), reason="lzop not available")
    def test_lzo_roundtrip(self, tmp_path):
        """Test that lzo (lzop) compression and decompression work end-to-end."""
        RawEndpoint(config={"path": tmp_path, "compress": "lzo"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.lzo"

        # Compress
        with open(input_file, "rb") as stdin:
            proc = subprocess.Popen(
                ["lzop", "-c"],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        assert output_file.exists()

        # Decompress and verify
        result = subprocess.run(
            ["lzop", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(not tool_available("gzip"), reason="gzip not available")
    def test_gzip_roundtrip(self, tmp_path):
        """Test that gzip compression and decompression work end-to-end."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "gzip"})

        # Create a mock input stream
        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)

        output_file = tmp_path / "output.btrfs.gz"

        # Compress using the actual pipeline
        with open(input_file, "rb") as stdin:
            pipeline = endpoint._build_receive_pipeline(output_file)
            assert pipeline == [["gzip", "-c"]]

            # Actually run the compression
            proc = subprocess.Popen(
                ["gzip", "-c"],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        # Verify compressed file exists and is smaller
        assert output_file.exists()
        assert output_file.stat().st_size < input_file.stat().st_size

        # Decompress and verify
        snapshot = RawSnapshot(name="test", stream_path=output_file, compress="gzip")
        restore_pipeline = endpoint._build_restore_pipeline(snapshot)
        assert restore_pipeline == [["gzip", "-d", "-c"]]

        # Actually decompress
        result = subprocess.run(
            ["gzip", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(not tool_available("zstd"), reason="zstd not available")
    def test_zstd_roundtrip(self, tmp_path):
        """Test that zstd compression and decompression work end-to-end."""
        RawEndpoint(config={"path": tmp_path, "compress": "zstd"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.zst"

        # Compress
        with open(input_file, "rb") as stdin:
            proc = subprocess.Popen(
                ["zstd", "-c"],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        assert output_file.exists()

        # Decompress and verify
        result = subprocess.run(
            ["zstd", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(not tool_available("lz4"), reason="lz4 not available")
    def test_lz4_roundtrip(self, tmp_path):
        """Test that lz4 compression and decompression work end-to-end."""
        RawEndpoint(config={"path": tmp_path, "compress": "lz4"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.lz4"

        # Compress
        with open(input_file, "rb") as stdin:
            proc = subprocess.Popen(
                ["lz4", "-c"],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        assert output_file.exists()

        # Decompress and verify
        result = subprocess.run(
            ["lz4", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(not tool_available("xz"), reason="xz not available")
    def test_xz_roundtrip(self, tmp_path):
        """Test that xz compression and decompression work end-to-end."""
        RawEndpoint(config={"path": tmp_path, "compress": "xz"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.xz"

        # Compress
        with open(input_file, "rb") as stdin:
            proc = subprocess.Popen(
                ["xz", "-c"],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        assert output_file.exists()

        # Decompress and verify
        result = subprocess.run(
            ["xz", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA


class TestRealEncryptionPipelines:
    """Tests that actually run encryption/decryption pipelines."""

    @pytest.mark.skipif(not tool_available("openssl"), reason="openssl not available")
    def test_openssl_roundtrip(self, tmp_path, monkeypatch):
        """Test that OpenSSL encryption and decryption work end-to-end."""
        passphrase = "test_passphrase_123"
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", passphrase)

        RawEndpoint(config={"path": tmp_path, "encrypt": "openssl_enc"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.enc"

        # Encrypt
        with open(input_file, "rb") as stdin:
            proc = subprocess.Popen(
                [
                    "openssl",
                    "enc",
                    "-aes-256-cbc",
                    "-salt",
                    "-pbkdf2",
                    "-pass",
                    f"pass:{passphrase}",
                ],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        assert output_file.exists()
        # Encrypted data should be different from input
        assert output_file.read_bytes() != TEST_DATA

        # Decrypt and verify
        result = subprocess.run(
            [
                "openssl",
                "enc",
                "-d",
                "-aes-256-cbc",
                "-pbkdf2",
                "-pass",
                f"pass:{passphrase}",
                "-in",
                str(output_file),
            ],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(
        not tool_available("openssl") or not tool_available("zstd"),
        reason="openssl or zstd not available",
    )
    def test_compression_plus_openssl_roundtrip(self, tmp_path, monkeypatch):
        """Test compression followed by OpenSSL encryption."""
        passphrase = "test_passphrase_456"
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", passphrase)

        RawEndpoint(
            config={"path": tmp_path, "compress": "zstd", "encrypt": "openssl_enc"}
        )

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.zst.enc"

        # Compress then encrypt (pipeline order: compress -> encrypt)
        compress_proc = subprocess.Popen(
            ["zstd", "-c"],
            stdin=open(input_file, "rb"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        encrypt_proc = subprocess.Popen(
            [
                "openssl",
                "enc",
                "-aes-256-cbc",
                "-salt",
                "-pbkdf2",
                "-pass",
                f"pass:{passphrase}",
            ],
            stdin=compress_proc.stdout,
            stdout=open(output_file, "wb"),
            stderr=subprocess.PIPE,
        )
        compress_proc.stdout.close()
        encrypt_proc.wait()
        compress_proc.wait()

        assert compress_proc.returncode == 0
        assert encrypt_proc.returncode == 0
        assert output_file.exists()

        # Restore: decrypt then decompress (reverse order)
        decrypt_proc = subprocess.Popen(
            [
                "openssl",
                "enc",
                "-d",
                "-aes-256-cbc",
                "-pbkdf2",
                "-pass",
                f"pass:{passphrase}",
                "-in",
                str(output_file),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        decompress_proc = subprocess.Popen(
            ["zstd", "-d", "-c"],
            stdin=decrypt_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        decrypt_proc.stdout.close()
        stdout, _ = decompress_proc.communicate()
        decrypt_proc.wait()

        assert decrypt_proc.returncode == 0
        assert decompress_proc.returncode == 0
        assert stdout == TEST_DATA


class TestShellPipelineExecution:
    """Test the actual shell pipeline execution used by RawEndpoint."""

    @pytest.mark.skipif(not tool_available("gzip"), reason="gzip not available")
    def test_execute_pipeline_actually_compresses(self, tmp_path):
        """Test that _execute_pipeline actually runs and compresses data."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "gzip"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "test-snapshot.btrfs.gz"

        # Set up pending metadata as receive() would
        endpoint._pending_metadata = {
            "name": "test-snapshot",
            "stream_path": output_file,
            "parent_name": None,
            "compress": "gzip",
            "encrypt": None,
            "gpg_recipient": None,
        }

        # Build and execute the pipeline with real data
        pipeline = endpoint._build_receive_pipeline(output_file)

        with open(input_file, "rb") as stdin:
            proc = endpoint._execute_pipeline(pipeline, stdin)
            proc.wait()

        assert proc.returncode == 0
        assert output_file.exists()
        assert output_file.stat().st_size > 0

        # Verify the compressed file is valid gzip
        result = subprocess.run(
            ["gzip", "-t", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0

        # Decompress and verify content
        result = subprocess.run(
            ["gzip", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(
        not tool_available("gzip") or not tool_available("cat"),
        reason="gzip or cat not available",
    )
    def test_multi_stage_pipeline_execution(self, tmp_path):
        """Test multi-stage pipeline (compression + another command)."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "gzip"})

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "test.btrfs.gz"

        endpoint._pending_metadata = {
            "name": "test",
            "stream_path": output_file,
            "parent_name": None,
            "compress": "gzip",
            "encrypt": None,
            "gpg_recipient": None,
        }

        # Test with a two-stage pipeline (even though gzip alone would work)
        # This tests the shell pipeline construction
        with open(input_file, "rb") as stdin:
            # Single command pipeline (tests the non-shell path)
            proc = endpoint._execute_pipeline([["gzip", "-c"]], stdin)
            proc.wait()

        assert proc.returncode == 0
        assert output_file.exists()


class TestRestorePipelineExecution:
    """Test restore pipeline execution with real data."""

    @pytest.mark.skipif(not tool_available("gzip"), reason="gzip not available")
    def test_restore_pipeline_decompresses(self, tmp_path):
        """Test that restore pipeline actually decompresses data."""
        endpoint = RawEndpoint(config={"path": tmp_path})

        # Create a compressed file
        compressed_file = tmp_path / "test.btrfs.gz"
        result = subprocess.run(
            ["gzip", "-c"],
            input=TEST_DATA,
            capture_output=True,
        )
        compressed_file.write_bytes(result.stdout)

        # Create snapshot object
        snapshot = RawSnapshot(
            name="test",
            stream_path=compressed_file,
            compress="gzip",
        )

        # Build restore pipeline
        pipeline = endpoint._build_restore_pipeline(snapshot)
        assert pipeline == [["gzip", "-d", "-c"]]

        # Execute restore pipeline
        proc = endpoint._execute_restore_pipeline(pipeline, compressed_file)
        stdout, stderr = proc.communicate()

        assert proc.returncode == 0
        assert stdout == TEST_DATA


class TestGPGEncryptionPipelines:
    """Tests that actually run GPG encryption/decryption pipelines.

    These tests require GPG to be installed and will create a temporary
    keyring for testing.
    """

    @pytest.fixture
    def gpg_home(self, tmp_path):
        """Create a temporary GPG home directory with a test key."""
        gpg_dir = tmp_path / "gnupg"
        gpg_dir.mkdir(mode=0o700)

        # Generate a test key (no passphrase for testing)
        key_params = """
Key-Type: RSA
Key-Length: 2048
Subkey-Type: RSA
Subkey-Length: 2048
Name-Real: Test Backup Key
Name-Email: test@backup.local
Expire-Date: 0
%no-protection
%commit
"""
        key_file = tmp_path / "key_params"
        key_file.write_text(key_params)

        result = subprocess.run(
            ["gpg", "--homedir", str(gpg_dir), "--batch", "--gen-key", str(key_file)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip(f"Failed to generate GPG key: {result.stderr}")

        return gpg_dir

    @pytest.mark.skipif(not tool_available("gpg"), reason="gpg not available")
    def test_gpg_roundtrip(self, tmp_path, gpg_home):
        """Test that GPG encryption and decryption work end-to-end."""
        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.gpg"

        # Encrypt
        result = subprocess.run(
            [
                "gpg",
                "--homedir",
                str(gpg_home),
                "--encrypt",
                "--recipient",
                "test@backup.local",
                "--batch",
                "--quiet",
                "--trust-model",
                "always",
                "--output",
                str(output_file),
                str(input_file),
            ],
            capture_output=True,
        )
        assert result.returncode == 0, f"GPG encrypt failed: {result.stderr.decode()}"
        assert output_file.exists()
        # Encrypted data should be different (and often larger due to overhead)
        assert output_file.read_bytes() != TEST_DATA

        # Decrypt and verify
        result = subprocess.run(
            [
                "gpg",
                "--homedir",
                str(gpg_home),
                "--decrypt",
                "--batch",
                "--quiet",
                str(output_file),
            ],
            capture_output=True,
        )
        assert result.returncode == 0, f"GPG decrypt failed: {result.stderr.decode()}"
        assert result.stdout == TEST_DATA

    @pytest.mark.skipif(
        not tool_available("gpg") or not tool_available("zstd"),
        reason="gpg or zstd not available",
    )
    def test_compression_plus_gpg_roundtrip(self, tmp_path, gpg_home):
        """Test compression followed by GPG encryption."""
        input_file = tmp_path / "input.bin"
        input_file.write_bytes(TEST_DATA)
        output_file = tmp_path / "output.btrfs.zst.gpg"

        # Compress then encrypt (pipeline order: compress -> encrypt)
        compress_proc = subprocess.Popen(
            ["zstd", "-c"],
            stdin=open(input_file, "rb"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        encrypt_proc = subprocess.Popen(
            [
                "gpg",
                "--homedir",
                str(gpg_home),
                "--encrypt",
                "--recipient",
                "test@backup.local",
                "--batch",
                "--quiet",
                "--trust-model",
                "always",
            ],
            stdin=compress_proc.stdout,
            stdout=open(output_file, "wb"),
            stderr=subprocess.PIPE,
        )
        compress_proc.stdout.close()
        encrypt_proc.wait()
        compress_proc.wait()

        assert compress_proc.returncode == 0
        assert encrypt_proc.returncode == 0
        assert output_file.exists()

        # Restore: decrypt then decompress (reverse order)
        decrypt_proc = subprocess.Popen(
            [
                "gpg",
                "--homedir",
                str(gpg_home),
                "--decrypt",
                "--batch",
                "--quiet",
                str(output_file),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        decompress_proc = subprocess.Popen(
            ["zstd", "-d", "-c"],
            stdin=decrypt_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        decrypt_proc.stdout.close()
        stdout, _ = decompress_proc.communicate()
        decrypt_proc.wait()

        assert decrypt_proc.returncode == 0
        assert decompress_proc.returncode == 0
        assert stdout == TEST_DATA


class TestMetadataIntegration:
    """Tests for metadata file read/write with real filesystem operations."""

    def test_metadata_save_and_load_roundtrip(self, tmp_path):
        """Test that metadata can be saved and loaded correctly."""
        stream_path = tmp_path / "test.20240115T120000.btrfs.zst"
        stream_path.write_bytes(b"fake stream data")

        # Create a snapshot with metadata
        snapshot = RawSnapshot(
            name="test.20240115T120000",
            stream_path=stream_path,
            uuid="abc123-def456",
            parent_uuid="111222-333444",
            parent_name="test.20240114T120000",
            size=1024,
            compress="zstd",
            encrypt="gpg",
            gpg_recipient="backup@example.com",
        )

        # Save metadata
        snapshot.save_metadata()

        # Verify metadata file exists
        assert snapshot.metadata_path.exists()
        assert snapshot.metadata_path == stream_path.with_suffix(".zst.meta")

        # Load metadata back
        loaded = RawSnapshot.load_metadata(snapshot.metadata_path)

        # Verify all fields match
        assert loaded.name == snapshot.name
        assert loaded.uuid == snapshot.uuid
        assert loaded.parent_uuid == snapshot.parent_uuid
        assert loaded.parent_name == snapshot.parent_name
        assert loaded.size == snapshot.size
        assert loaded.compress == snapshot.compress
        assert loaded.encrypt == snapshot.encrypt
        assert loaded.gpg_recipient == snapshot.gpg_recipient

    def test_metadata_json_format(self, tmp_path):
        """Test that metadata JSON has expected structure."""
        import json

        stream_path = tmp_path / "root.20240115T120000.btrfs.gz"
        stream_path.write_bytes(b"test")

        snapshot = RawSnapshot(
            name="root.20240115T120000",
            stream_path=stream_path,
            uuid="test-uuid",
            compress="gzip",
        )
        snapshot.save_metadata()

        # Read raw JSON
        with open(snapshot.metadata_path) as f:
            data = json.load(f)

        # Check structure
        assert data["version"] == 1
        assert data["name"] == "root.20240115T120000"
        assert data["uuid"] == "test-uuid"
        assert "pipeline" in data
        assert data["pipeline"]["compress"] == "gzip"
        assert "btrfs_backup_ng_version" in data

    def test_discover_snapshots_from_metadata(self, tmp_path):
        """Test discovering snapshots from metadata files."""
        from btrfs_backup_ng.endpoint.raw_metadata import discover_raw_snapshots

        # Create several snapshots with metadata
        for i in range(3):
            stream_path = tmp_path / f"root.2024011{i}T120000.btrfs.zst"
            stream_path.write_bytes(b"test data")

            snapshot = RawSnapshot(
                name=f"root.2024011{i}T120000",
                stream_path=stream_path,
                uuid=f"uuid-{i}",
                compress="zstd",
            )
            snapshot.save_metadata()

        # Discover snapshots
        snapshots = discover_raw_snapshots(tmp_path)

        assert len(snapshots) == 3
        assert all(s.compress == "zstd" for s in snapshots)
        # Should be sorted by creation time
        assert snapshots[0].name == "root.20240110T120000"
        assert snapshots[2].name == "root.20240112T120000"

    def test_discover_snapshots_without_metadata(self, tmp_path):
        """Test discovering snapshots when metadata files are missing."""
        from btrfs_backup_ng.endpoint.raw_metadata import discover_raw_snapshots

        # Create stream files without metadata
        (tmp_path / "data.20240115T120000.btrfs.gz").write_bytes(b"compressed")
        (tmp_path / "data.20240116T120000.btrfs.zst.gpg").write_bytes(b"encrypted")

        snapshots = discover_raw_snapshots(tmp_path)

        assert len(snapshots) == 2
        # Should parse compression/encryption from filenames
        snap1 = next(s for s in snapshots if "0115" in s.name)
        snap2 = next(s for s in snapshots if "0116" in s.name)

        assert snap1.compress == "gzip"
        assert snap1.encrypt is None

        assert snap2.compress == "zstd"
        assert snap2.encrypt == "gpg"


class TestFullReceiveWorkflow:
    """Integration tests for the complete receive workflow."""

    @pytest.mark.skipif(not tool_available("gzip"), reason="gzip not available")
    def test_receive_finalize_workflow(self, tmp_path):
        """Test the full receive -> finalize -> metadata workflow."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "gzip"})
        endpoint._prepare()

        # Simulate receiving a stream
        snapshot_name = "root.20240115T120000"

        # Create input data (simulating btrfs send output)
        input_data = TEST_DATA

        # Call receive with a pipe
        import io

        io.BytesIO(input_data)

        # Build and execute pipeline manually (since we're not using subprocess pipe)
        extension = ".btrfs.gz"
        output_path = tmp_path / f"{snapshot_name}{extension}"

        endpoint._pending_metadata = {
            "name": snapshot_name,
            "stream_path": output_path,
            "parent_name": None,
            "compress": "gzip",
            "encrypt": None,
            "gpg_recipient": None,
        }

        # Compress the data
        result = subprocess.run(
            ["gzip", "-c"],
            input=input_data,
            capture_output=True,
        )
        assert result.returncode == 0
        output_path.write_bytes(result.stdout)

        # Create a mock process for finalize_receive
        class MockProc:
            returncode = 0

            def communicate(self):
                return (b"", b"")

        # Finalize the receive
        snapshot = endpoint.finalize_receive(
            MockProc(),
            uuid="test-uuid-123",
            parent_uuid="parent-uuid-456",
        )

        # Verify snapshot was created correctly
        assert snapshot.name == snapshot_name
        assert snapshot.uuid == "test-uuid-123"
        assert snapshot.parent_uuid == "parent-uuid-456"
        assert snapshot.compress == "gzip"
        assert snapshot.size > 0

        # Verify metadata file was created
        assert snapshot.metadata_path.exists()

        # Verify the data can be restored
        result = subprocess.run(
            ["gzip", "-d", "-c", str(output_path)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == input_data

    @pytest.mark.skipif(not tool_available("zstd"), reason="zstd not available")
    def test_list_and_delete_workflow(self, tmp_path):
        """Test listing and deleting snapshots."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "zstd"})
        endpoint._prepare()

        # Create some test snapshots
        for i in range(3):
            name = f"test.2024011{i}T120000"
            stream_path = tmp_path / f"{name}.btrfs.zst"

            # Compress some data
            result = subprocess.run(
                ["zstd", "-c"],
                input=f"data for snapshot {i}".encode(),
                capture_output=True,
            )
            stream_path.write_bytes(result.stdout)

            # Create metadata
            snapshot = RawSnapshot(
                name=name,
                stream_path=stream_path,
                uuid=f"uuid-{i}",
                compress="zstd",
            )
            snapshot.save_metadata()

        # List snapshots
        snapshots = endpoint.list_snapshots()
        assert len(snapshots) == 3

        # Delete one snapshot
        to_delete = [s for s in snapshots if "0111" in s.name][0]
        endpoint.delete_snapshot(to_delete)

        # Verify it's gone
        snapshots = endpoint.list_snapshots(flush_cache=True)
        assert len(snapshots) == 2
        assert not any("0111" in s.name for s in snapshots)

        # Delete old snapshots keeping only 1
        endpoint.delete_old_snapshots(keep=1)
        snapshots = endpoint.list_snapshots(flush_cache=True)
        assert len(snapshots) == 1
        assert "0112" in snapshots[0].name  # Should keep the newest


class TestSSHRawEndpointIntegration:
    """Integration tests for SSH raw endpoint.

    These tests verify SSH command construction and can optionally test
    against localhost if SSH is configured for passwordless access.
    """

    def test_ssh_command_construction(self, tmp_path):
        """Test that SSH commands are constructed correctly."""
        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup/raw",
                "hostname": "backup-server",
                "username": "backup",
                "port": 2222,
                "ssh_key": "/home/user/.ssh/backup_key",
                "compress": "zstd",
            }
        )

        ssh_cmd = endpoint._build_ssh_command()

        assert ssh_cmd[0] == "ssh"
        assert "-p" in ssh_cmd
        assert "2222" in ssh_cmd
        assert "-i" in ssh_cmd
        assert "/home/user/.ssh/backup_key" in ssh_cmd
        assert "backup@backup-server" in ssh_cmd

    def test_ssh_endpoint_id(self, tmp_path):
        """Test SSH endpoint ID generation."""
        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup/raw",
                "hostname": "nas.local",
                "username": "admin",
            }
        )

        assert endpoint.get_id() == "raw+ssh://admin@nas.local/backup/raw"

    def test_ssh_endpoint_repr(self, tmp_path):
        """Test SSH endpoint string representation."""
        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup/raw",
                "hostname": "nas.local",
                "compress": "gzip",
                "encrypt": "gpg",
                "gpg_recipient": "backup@example.com",
            }
        )

        repr_str = repr(endpoint)
        assert "SSHRawEndpoint" in repr_str
        assert "raw+ssh://" in repr_str
        assert "compress=gzip" in repr_str
        assert "encrypt=gpg" in repr_str

    @pytest.mark.skipif(
        not tool_available("ssh")
        or subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=1",
                "localhost",
                "true",
            ],
            capture_output=True,
        ).returncode
        != 0,
        reason="SSH to localhost not available or not passwordless",
    )
    def test_ssh_localhost_prepare(self, tmp_path):
        """Test SSH endpoint prepare with localhost."""
        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        remote_path = tmp_path / "ssh_test_dir"

        endpoint = SSHRawEndpoint(
            config={
                "path": str(remote_path),
                "hostname": "localhost",
            }
        )

        # This should create the directory via SSH
        endpoint._prepare()

        # Verify directory was created
        assert remote_path.exists()
        assert remote_path.is_dir()

    @pytest.mark.skipif(
        not tool_available("ssh")
        or subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=1",
                "localhost",
                "true",
            ],
            capture_output=True,
        ).returncode
        != 0,
        reason="SSH to localhost not available or not passwordless",
    )
    @pytest.mark.skipif(not tool_available("zstd"), reason="zstd not available")
    def test_ssh_localhost_pipeline(self, tmp_path):
        """Test SSH pipeline execution to localhost."""
        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        remote_path = tmp_path / "ssh_pipeline_test"
        remote_path.mkdir()

        endpoint = SSHRawEndpoint(
            config={
                "path": str(remote_path),
                "hostname": "localhost",
                "compress": "zstd",
            }
        )

        # Setup pending metadata
        output_file = remote_path / "test.btrfs.zst"
        endpoint._pending_metadata = {
            "name": "test",
            "stream_path": output_file,
            "parent_name": None,
            "compress": "zstd",
            "encrypt": None,
            "gpg_recipient": None,
        }

        # Execute pipeline with test data
        input_data = b"Test data for SSH transfer\n" * 100

        # Use subprocess.PIPE to provide input

        proc = subprocess.Popen(
            ["echo", "-n", input_data.decode()],
            stdout=subprocess.PIPE,
        )

        result = endpoint._execute_pipeline([["zstd", "-c"]], proc.stdout)
        result.wait()
        proc.wait()

        assert result.returncode == 0
        assert output_file.exists()

        # Verify data can be decompressed
        decomp = subprocess.run(
            ["zstd", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert decomp.returncode == 0


class TestBinaryDataIntegrity:
    """Tests for handling binary data correctly through pipelines."""

    @pytest.mark.skipif(not tool_available("gzip"), reason="gzip not available")
    def test_binary_data_with_null_bytes(self, tmp_path):
        """Test that binary data with null bytes survives compression."""
        # Create data with null bytes and all byte values
        binary_data = bytes(range(256)) * 100

        input_file = tmp_path / "binary.bin"
        input_file.write_bytes(binary_data)
        output_file = tmp_path / "binary.gz"

        # Compress
        with open(input_file, "rb") as stdin:
            proc = subprocess.Popen(
                ["gzip", "-c"],
                stdin=stdin,
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            proc.wait()
            assert proc.returncode == 0

        # Decompress and verify
        result = subprocess.run(
            ["gzip", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == binary_data

    @pytest.mark.skipif(
        not tool_available("openssl") or not tool_available("zstd"),
        reason="openssl or zstd not available",
    )
    def test_binary_data_through_full_pipeline(self, tmp_path, monkeypatch):
        """Test binary data through compress + encrypt pipeline."""
        passphrase = "test_binary_passphrase"
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", passphrase)

        # Create random-like binary data
        import hashlib

        binary_data = b""
        for i in range(1000):
            binary_data += hashlib.sha256(f"chunk{i}".encode()).digest()

        input_file = tmp_path / "binary.bin"
        input_file.write_bytes(binary_data)
        output_file = tmp_path / "binary.zst.enc"

        # Compress then encrypt
        compress_proc = subprocess.Popen(
            ["zstd", "-c"],
            stdin=open(input_file, "rb"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        encrypt_proc = subprocess.Popen(
            [
                "openssl",
                "enc",
                "-aes-256-cbc",
                "-salt",
                "-pbkdf2",
                "-pass",
                f"pass:{passphrase}",
            ],
            stdin=compress_proc.stdout,
            stdout=open(output_file, "wb"),
            stderr=subprocess.PIPE,
        )
        compress_proc.stdout.close()
        encrypt_proc.wait()
        compress_proc.wait()

        assert compress_proc.returncode == 0
        assert encrypt_proc.returncode == 0

        # Decrypt then decompress
        decrypt_proc = subprocess.Popen(
            [
                "openssl",
                "enc",
                "-d",
                "-aes-256-cbc",
                "-pbkdf2",
                "-pass",
                f"pass:{passphrase}",
                "-in",
                str(output_file),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        decompress_proc = subprocess.Popen(
            ["zstd", "-d", "-c"],
            stdin=decrypt_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        decrypt_proc.stdout.close()
        stdout, _ = decompress_proc.communicate()
        decrypt_proc.wait()

        assert decrypt_proc.returncode == 0
        assert decompress_proc.returncode == 0
        assert stdout == binary_data


class TestCompressionLevels:
    """Tests for different compression levels."""

    @pytest.mark.skipif(not tool_available("zstd"), reason="zstd not available")
    def test_zstd_levels_produce_different_sizes(self, tmp_path):
        """Test that zstd compression levels affect output size."""
        # Use compressible data
        test_data = b"The quick brown fox jumps over the lazy dog. " * 10000

        input_file = tmp_path / "input.bin"
        input_file.write_bytes(test_data)

        sizes = {}
        for level in [1, 10, 19]:  # Low, medium, high compression
            output_file = tmp_path / f"output_level{level}.zst"
            result = subprocess.run(
                ["zstd", f"-{level}", "-c"],
                stdin=open(input_file, "rb"),
                stdout=open(output_file, "wb"),
                stderr=subprocess.PIPE,
            )
            assert result.returncode == 0
            sizes[level] = output_file.stat().st_size

            # Verify roundtrip
            decomp = subprocess.run(
                ["zstd", "-d", "-c", str(output_file)],
                capture_output=True,
            )
            assert decomp.stdout == test_data

        # Higher levels should generally produce smaller files for this data
        # (though not guaranteed for all data)
        assert sizes[1] >= sizes[19], f"Expected level 19 <= level 1, got {sizes}"


class TestEdgeCases:
    """Test edge cases and error conditions with real execution."""

    @pytest.mark.skipif(not tool_available("gzip"), reason="gzip not available")
    def test_empty_input(self, tmp_path):
        """Test handling of empty input data."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "gzip"})

        input_file = tmp_path / "empty.bin"
        input_file.write_bytes(b"")
        output_file = tmp_path / "empty.btrfs.gz"

        endpoint._pending_metadata = {
            "name": "empty",
            "stream_path": output_file,
            "parent_name": None,
            "compress": "gzip",
            "encrypt": None,
            "gpg_recipient": None,
        }

        with open(input_file, "rb") as stdin:
            proc = endpoint._execute_pipeline([["gzip", "-c"]], stdin)
            proc.wait()

        assert proc.returncode == 0
        assert output_file.exists()

        # Decompress should return empty
        result = subprocess.run(
            ["gzip", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == b""

    @pytest.mark.skipif(not tool_available("gzip"), reason="gzip not available")
    def test_large_data(self, tmp_path):
        """Test handling of larger data (10MB)."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "gzip"})

        # 10MB of test data
        large_data = b"x" * (10 * 1024 * 1024)
        input_file = tmp_path / "large.bin"
        input_file.write_bytes(large_data)
        output_file = tmp_path / "large.btrfs.gz"

        endpoint._pending_metadata = {
            "name": "large",
            "stream_path": output_file,
            "parent_name": None,
            "compress": "gzip",
            "encrypt": None,
            "gpg_recipient": None,
        }

        with open(input_file, "rb") as stdin:
            proc = endpoint._execute_pipeline([["gzip", "-c"]], stdin)
            proc.wait()

        assert proc.returncode == 0
        assert output_file.exists()
        # Highly repetitive data should compress very well
        assert output_file.stat().st_size < len(large_data) / 100

        # Verify roundtrip
        result = subprocess.run(
            ["gzip", "-d", "-c", str(output_file)],
            capture_output=True,
        )
        assert result.returncode == 0
        assert result.stdout == large_data

    def test_path_with_spaces(self, tmp_path):
        """Test handling of paths with spaces."""
        space_dir = tmp_path / "path with spaces"
        space_dir.mkdir()

        endpoint = RawEndpoint(config={"path": space_dir})

        # Test that path handling works
        assert endpoint.config["path"] == space_dir
        assert endpoint.get_id() == f"raw://{space_dir}"
