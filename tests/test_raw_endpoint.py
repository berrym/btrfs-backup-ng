"""Tests for raw target endpoint."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import COMPRESSION_CONFIG, RawSnapshot


class TestRawEndpoint:
    """Tests for RawEndpoint class."""

    def test_basic_creation(self, tmp_path):
        """Test creating a basic RawEndpoint."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        assert endpoint.config["path"] == tmp_path
        assert endpoint.compress is None
        assert endpoint.encrypt is None

    def test_with_compression(self, tmp_path):
        """Test endpoint with compression."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "zstd"})
        assert endpoint.compress == "zstd"

    def test_with_encryption(self, tmp_path):
        """Test endpoint with GPG encryption."""
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "encrypt": "gpg",
                "gpg_recipient": "backup@example.com",
            }
        )
        assert endpoint.encrypt == "gpg"
        assert endpoint.gpg_recipient == "backup@example.com"

    def test_with_openssl_encryption(self, tmp_path, monkeypatch):
        """Test endpoint with OpenSSL encryption."""
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "test_passphrase")
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "encrypt": "openssl_enc",
            }
        )
        assert endpoint.encrypt == "openssl_enc"
        assert endpoint.openssl_cipher == "aes-256-cbc"

    def test_with_openssl_custom_cipher(self, tmp_path, monkeypatch):
        """Test endpoint with OpenSSL custom cipher."""
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "test_passphrase")
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "encrypt": "openssl_enc",
                "openssl_cipher": "aes-128-cbc",
            }
        )
        assert endpoint.openssl_cipher == "aes-128-cbc"

    def test_encryption_requires_recipient(self, tmp_path):
        """Test that GPG encryption requires a recipient."""
        with pytest.raises(ValueError, match="gpg_recipient is required"):
            RawEndpoint(config={"path": tmp_path, "encrypt": "gpg"})

    def test_invalid_encryption_method(self, tmp_path):
        """Test that invalid encryption method raises error."""
        with pytest.raises(ValueError, match="Unknown encryption method"):
            RawEndpoint(config={"path": tmp_path, "encrypt": "invalid"})

    def test_invalid_compression(self, tmp_path):
        """Test that invalid compression algorithm raises error."""
        with pytest.raises(ValueError, match="Unknown compression algorithm"):
            RawEndpoint(config={"path": tmp_path, "compress": "invalid"})

    def test_repr(self, tmp_path):
        """Test string representation."""
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "compress": "zstd",
                "encrypt": "gpg",
                "gpg_recipient": "test@example.com",
            }
        )
        repr_str = repr(endpoint)
        assert "RawEndpoint" in repr_str
        assert "raw://" in repr_str
        assert "compress=zstd" in repr_str
        assert "encrypt=gpg" in repr_str

    def test_get_id(self, tmp_path):
        """Test endpoint ID generation."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        assert endpoint.get_id().startswith("raw://")

    def test_prepare_creates_directory(self, tmp_path):
        """Test that prepare creates the target directory."""
        target_dir = tmp_path / "backups"
        endpoint = RawEndpoint(config={"path": target_dir})

        with patch.object(endpoint, "_check_tools", return_value=[]):
            endpoint.prepare()

        assert target_dir.exists()

    def test_check_tools_finds_missing(self, tmp_path):
        """Test tool availability checking."""
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "compress": "zstd",
                "encrypt": "gpg",
                "gpg_recipient": "test@example.com",
            }
        )

        with patch("shutil.which") as mock_which:
            mock_which.return_value = None  # All tools missing
            missing = endpoint._check_tools()

        assert "zstd" in missing
        assert "gpg" in missing

    def test_check_tools_all_present(self, tmp_path):
        """Test when all tools are present."""
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "compress": "gzip",
            }
        )

        with patch("shutil.which") as mock_which:
            mock_which.return_value = "/usr/bin/gzip"
            missing = endpoint._check_tools()

        assert missing == []


class TestRawEndpointPipeline:
    """Tests for pipeline construction."""

    def test_build_pipeline_no_processing(self, tmp_path):
        """Test pipeline with no compression or encryption."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        endpoint._pending_metadata = {"stream_path": tmp_path / "test.btrfs"}

        pipeline = endpoint._build_receive_pipeline(tmp_path / "test.btrfs")

        assert len(pipeline) == 1
        assert pipeline[0] == ["cat"]

    def test_build_pipeline_compression_only(self, tmp_path):
        """Test pipeline with compression only."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "zstd"})
        endpoint._pending_metadata = {"stream_path": tmp_path / "test.btrfs.zst"}

        pipeline = endpoint._build_receive_pipeline(tmp_path / "test.btrfs.zst")

        assert len(pipeline) == 1
        assert pipeline[0] == ["zstd", "-c"]

    def test_build_pipeline_encryption_only(self, tmp_path):
        """Test pipeline with encryption only."""
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "encrypt": "gpg",
                "gpg_recipient": "test@example.com",
            }
        )
        endpoint._pending_metadata = {"stream_path": tmp_path / "test.btrfs.gpg"}

        pipeline = endpoint._build_receive_pipeline(tmp_path / "test.btrfs.gpg")

        assert len(pipeline) == 1
        assert "gpg" in pipeline[0]
        assert "--encrypt" in pipeline[0]
        assert "--recipient" in pipeline[0]
        assert "test@example.com" in pipeline[0]

    def test_build_pipeline_with_keyring(self, tmp_path):
        """Test pipeline with GPG keyring specified."""
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "encrypt": "gpg",
                "gpg_recipient": "test@example.com",
                "gpg_keyring": "/path/to/keyring.gpg",
            }
        )
        endpoint._pending_metadata = {"stream_path": tmp_path / "test.btrfs.gpg"}

        pipeline = endpoint._build_receive_pipeline(tmp_path / "test.btrfs.gpg")

        assert "--keyring" in pipeline[0]
        assert "/path/to/keyring.gpg" in pipeline[0]

    def test_build_pipeline_compression_and_encryption(self, tmp_path):
        """Test pipeline with both compression and encryption."""
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "compress": "lz4",
                "encrypt": "gpg",
                "gpg_recipient": "test@example.com",
            }
        )
        endpoint._pending_metadata = {"stream_path": tmp_path / "test.btrfs.lz4.gpg"}

        pipeline = endpoint._build_receive_pipeline(tmp_path / "test.btrfs.lz4.gpg")

        assert len(pipeline) == 2
        assert pipeline[0] == ["lz4", "-c"]  # Compression first
        assert "gpg" in pipeline[1]  # Then encryption

    def test_build_pipeline_openssl_encryption(self, tmp_path, monkeypatch):
        """Test pipeline with OpenSSL encryption."""
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "test")
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "encrypt": "openssl_enc",
            }
        )
        endpoint._pending_metadata = {"stream_path": tmp_path / "test.btrfs.enc"}

        pipeline = endpoint._build_receive_pipeline(tmp_path / "test.btrfs.enc")

        assert len(pipeline) == 1
        assert "openssl" in pipeline[0]
        assert "enc" in pipeline[0]
        assert "-aes-256-cbc" in pipeline[0]
        assert "-pbkdf2" in pipeline[0]

    def test_build_pipeline_compression_and_openssl(self, tmp_path, monkeypatch):
        """Test pipeline with compression and OpenSSL encryption."""
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "test")
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "compress": "zstd",
                "encrypt": "openssl_enc",
            }
        )
        endpoint._pending_metadata = {"stream_path": tmp_path / "test.btrfs.zst.enc"}

        pipeline = endpoint._build_receive_pipeline(tmp_path / "test.btrfs.zst.enc")

        assert len(pipeline) == 2
        assert pipeline[0] == ["zstd", "-c"]  # Compression first
        assert "openssl" in pipeline[1]  # Then encryption


class TestRawEndpointRestorePipeline:
    """Tests for restore pipeline construction."""

    def test_build_restore_pipeline_no_processing(self, tmp_path):
        """Test restore pipeline with no processing."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        snapshot = RawSnapshot(
            name="test",
            stream_path=tmp_path / "test.btrfs",
        )

        pipeline = endpoint._build_restore_pipeline(snapshot)

        assert len(pipeline) == 1
        assert pipeline[0] == ["cat"]

    def test_build_restore_pipeline_compressed(self, tmp_path):
        """Test restore pipeline for compressed stream."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        snapshot = RawSnapshot(
            name="test",
            stream_path=tmp_path / "test.btrfs.zst",
            compress="zstd",
        )

        pipeline = endpoint._build_restore_pipeline(snapshot)

        assert len(pipeline) == 1
        assert pipeline[0] == ["zstd", "-d", "-c"]

    def test_build_restore_pipeline_encrypted(self, tmp_path):
        """Test restore pipeline for encrypted stream."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        snapshot = RawSnapshot(
            name="test",
            stream_path=tmp_path / "test.btrfs.gpg",
            encrypt="gpg",
        )

        pipeline = endpoint._build_restore_pipeline(snapshot)

        assert len(pipeline) == 1
        assert "gpg" in pipeline[0]
        assert "--decrypt" in pipeline[0]

    def test_build_restore_pipeline_compressed_and_encrypted(self, tmp_path):
        """Test restore pipeline for compressed and encrypted stream."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        snapshot = RawSnapshot(
            name="test",
            stream_path=tmp_path / "test.btrfs.zst.gpg",
            compress="zstd",
            encrypt="gpg",
        )

        pipeline = endpoint._build_restore_pipeline(snapshot)

        # Decrypt first, then decompress (reverse of encrypt then compress)
        assert len(pipeline) == 2
        assert "--decrypt" in pipeline[0]  # GPG decrypt first
        assert pipeline[1] == ["zstd", "-d", "-c"]  # Then decompress

    def test_build_restore_pipeline_openssl_encrypted(self, tmp_path, monkeypatch):
        """Test restore pipeline for OpenSSL encrypted stream."""
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "test")
        endpoint = RawEndpoint(config={"path": tmp_path, "encrypt": "openssl_enc"})
        snapshot = RawSnapshot(
            name="test",
            stream_path=tmp_path / "test.btrfs.enc",
            encrypt="openssl_enc",
        )

        pipeline = endpoint._build_restore_pipeline(snapshot)

        assert len(pipeline) == 1
        assert "openssl" in pipeline[0]
        assert "-d" in pipeline[0]  # Decrypt flag

    def test_build_restore_pipeline_compressed_and_openssl(self, tmp_path, monkeypatch):
        """Test restore pipeline for compressed and OpenSSL encrypted stream."""
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "test")
        endpoint = RawEndpoint(config={"path": tmp_path, "encrypt": "openssl_enc"})
        snapshot = RawSnapshot(
            name="test",
            stream_path=tmp_path / "test.btrfs.zst.enc",
            compress="zstd",
            encrypt="openssl_enc",
        )

        pipeline = endpoint._build_restore_pipeline(snapshot)

        # Decrypt first, then decompress
        assert len(pipeline) == 2
        assert "openssl" in pipeline[0]  # OpenSSL decrypt first
        assert "-d" in pipeline[0]
        assert pipeline[1] == ["zstd", "-d", "-c"]  # Then decompress


class TestRawEndpointSnapshotManagement:
    """Tests for snapshot listing and deletion."""

    def test_list_snapshots_empty(self, tmp_path):
        """Test listing snapshots in empty directory."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        snapshots = endpoint.list_snapshots()
        assert snapshots == []

    def test_list_snapshots_with_prefix(self, tmp_path):
        """Test listing snapshots with prefix filter."""
        # Create stream files
        (tmp_path / "root.20240115T120000.btrfs").write_bytes(b"data1")
        (tmp_path / "home.20240115T120000.btrfs").write_bytes(b"data2")

        endpoint = RawEndpoint(config={"path": tmp_path, "snap_prefix": "root"})
        snapshots = endpoint.list_snapshots()

        assert len(snapshots) == 1
        assert snapshots[0].name == "root.20240115T120000"

    def test_list_snapshots_caching(self, tmp_path):
        """Test that snapshot listing uses caching."""
        (tmp_path / "test.20240115T120000.btrfs").write_bytes(b"data")

        endpoint = RawEndpoint(config={"path": tmp_path})

        # First call populates cache
        snapshots1 = endpoint.list_snapshots()
        assert len(snapshots1) == 1

        # Add another file
        (tmp_path / "test.20240116T120000.btrfs").write_bytes(b"data2")

        # Second call uses cache (still 1 snapshot)
        snapshots2 = endpoint.list_snapshots()
        assert len(snapshots2) == 1

        # Flush cache to get updated list
        snapshots3 = endpoint.list_snapshots(flush_cache=True)
        assert len(snapshots3) == 2

    def test_delete_snapshot(self, tmp_path):
        """Test deleting a snapshot."""
        stream_path = tmp_path / "test.20240115T120000.btrfs"
        meta_path = tmp_path / "test.20240115T120000.btrfs.meta"
        stream_path.write_bytes(b"data")
        meta_path.write_text('{"name": "test"}')

        endpoint = RawEndpoint(config={"path": tmp_path})
        snapshot = RawSnapshot(name="test.20240115T120000", stream_path=stream_path)

        endpoint.delete_snapshot(snapshot)

        assert not stream_path.exists()
        assert not meta_path.exists()

    def test_delete_old_snapshots(self, tmp_path):
        """Test deleting old snapshots based on retention."""
        # Create multiple snapshots
        for i in range(5):
            path = tmp_path / f"test.2024011{i}T120000.btrfs"
            path.write_bytes(b"data")

        endpoint = RawEndpoint(config={"path": tmp_path})

        # Keep only 2 snapshots
        endpoint.delete_old_snapshots(keep=2)

        # Refresh cache and check
        remaining = endpoint.list_snapshots(flush_cache=True)
        assert len(remaining) == 2


class TestSSHRawEndpoint:
    """Tests for SSHRawEndpoint class."""

    def test_basic_creation(self):
        """Test creating an SSH raw endpoint."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "backup.example.com",
                "username": "backup",
            }
        )
        assert endpoint.hostname == "backup.example.com"
        assert endpoint.username == "backup"
        assert endpoint._is_remote is True

    def test_requires_hostname(self):
        """Test that hostname is required."""
        with pytest.raises(ValueError, match="hostname is required"):
            SSHRawEndpoint(config={"path": "/backup"})

    def test_repr(self):
        """Test string representation."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
                "username": "backup",
                "compress": "zstd",
            }
        )
        repr_str = repr(endpoint)
        assert "SSHRawEndpoint" in repr_str
        assert "raw+ssh://" in repr_str
        assert "backup@nas" in repr_str

    def test_get_id(self):
        """Test endpoint ID generation."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
                "username": "backup",
            }
        )
        assert endpoint.get_id() == "raw+ssh://backup@nas/backup"

    def test_build_ssh_command(self):
        """Test SSH command construction."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
                "username": "backup",
                "port": 2222,
                "ssh_key": "/home/user/.ssh/backup_key",
            }
        )

        cmd = endpoint._build_ssh_command()

        assert "ssh" in cmd
        assert "-p" in cmd
        assert "2222" in cmd
        assert "-i" in cmd
        assert "/home/user/.ssh/backup_key" in cmd
        assert "backup@nas" in cmd

    def test_build_ssh_command_default_port(self):
        """Test SSH command with default port."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
            }
        )

        cmd = endpoint._build_ssh_command()

        # Default port shouldn't be explicitly added
        assert "-p" not in cmd


class TestRawEndpointIntegration:
    """Integration tests for RawEndpoint."""

    def test_receive_requires_snapshot_name(self, tmp_path):
        """Test that receive requires a snapshot name."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        mock_stdin = MagicMock()

        with pytest.raises(ValueError, match="snapshot_name is required"):
            endpoint.receive(mock_stdin)

    def test_send_requires_existing_file(self, tmp_path):
        """Test that send requires the stream file to exist."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        snapshot = RawSnapshot(
            name="nonexistent",
            stream_path=tmp_path / "nonexistent.btrfs",
        )

        with pytest.raises(FileNotFoundError):
            endpoint.send(snapshot)


class TestRawEndpointReceive:
    """Tests for receive functionality."""

    def test_receive_sets_pending_metadata(self, tmp_path):
        """Test that receive sets pending metadata correctly."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        mock_stdin = MagicMock()

        with patch.object(endpoint, "_execute_pipeline") as mock_exec:
            mock_proc = MagicMock()
            mock_exec.return_value = mock_proc
            endpoint.receive(
                mock_stdin, snapshot_name="test-snap", parent_name="parent-snap"
            )

        assert endpoint._pending_metadata["name"] == "test-snap"
        assert endpoint._pending_metadata["parent_name"] == "parent-snap"
        assert endpoint._pending_metadata["compress"] is None
        assert endpoint._pending_metadata["encrypt"] is None

    def test_receive_with_compression(self, tmp_path):
        """Test receive with compression enabled."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": "gzip"})
        mock_stdin = MagicMock()

        with patch.object(endpoint, "_execute_pipeline") as mock_exec:
            mock_proc = MagicMock()
            mock_exec.return_value = mock_proc
            endpoint.receive(mock_stdin, snapshot_name="test-snap")

        assert endpoint._pending_metadata["compress"] == "gzip"
        # Check that output path has correct extension
        assert str(endpoint._pending_metadata["stream_path"]).endswith(".btrfs.gz")

    def test_receive_with_encryption(self, tmp_path):
        """Test receive with GPG encryption."""
        endpoint = RawEndpoint(
            config={
                "path": tmp_path,
                "encrypt": "gpg",
                "gpg_recipient": "test@example.com",
            }
        )
        mock_stdin = MagicMock()

        with patch.object(endpoint, "_execute_pipeline") as mock_exec:
            mock_proc = MagicMock()
            mock_exec.return_value = mock_proc
            endpoint.receive(mock_stdin, snapshot_name="test-snap")

        assert endpoint._pending_metadata["encrypt"] == "gpg"
        assert endpoint._pending_metadata["gpg_recipient"] == "test@example.com"
        assert str(endpoint._pending_metadata["stream_path"]).endswith(".btrfs.gpg")


class TestRawEndpointExecutePipeline:
    """Tests for pipeline execution."""

    def test_execute_pipeline_empty_raises(self, tmp_path):
        """Test that empty pipeline raises error."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        endpoint._pending_metadata = {"stream_path": tmp_path / "test.btrfs"}

        with pytest.raises(ValueError, match="Empty pipeline"):
            endpoint._execute_pipeline([], MagicMock())

    def test_execute_pipeline_single_command(self, tmp_path):
        """Test executing single-command pipeline."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        output_path = tmp_path / "test.btrfs"
        endpoint._pending_metadata = {"stream_path": output_path}

        mock_stdin = MagicMock()

        with patch("builtins.open", MagicMock()):
            with patch("subprocess.Popen") as mock_popen:
                mock_proc = MagicMock()
                mock_popen.return_value = mock_proc
                result = endpoint._execute_pipeline([["cat"]], mock_stdin)

        assert result == mock_proc

    def test_execute_pipeline_multi_command(self, tmp_path):
        """Test executing multi-command pipeline."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        output_path = tmp_path / "test.btrfs.gz"
        endpoint._pending_metadata = {"stream_path": output_path}

        mock_stdin = MagicMock()

        with patch("subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_popen.return_value = mock_proc
            endpoint._execute_pipeline([["gzip", "-c"], ["cat"]], mock_stdin)

        # Multi-command uses shell=True
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args
        assert call_args[1]["shell"] is True


class TestRawEndpointFinalizeReceive:
    """Tests for finalize_receive functionality."""

    def test_finalize_receive_success(self, tmp_path):
        """Test successful finalize_receive."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        stream_path = tmp_path / "test-snap.btrfs"
        stream_path.write_bytes(b"test data")

        endpoint._pending_metadata = {
            "name": "test-snap",
            "stream_path": stream_path,
            "parent_name": None,
            "compress": None,
            "encrypt": None,
            "gpg_recipient": None,
        }

        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (b"", b"")
        mock_proc.returncode = 0

        snapshot = endpoint.finalize_receive(
            mock_proc, uuid="abc123", parent_uuid="def456"
        )

        assert snapshot.name == "test-snap"
        assert snapshot.uuid == "abc123"
        assert snapshot.parent_uuid == "def456"
        assert snapshot.size == len(b"test data")
        # Check metadata file was created
        assert snapshot.metadata_path.exists()

    def test_finalize_receive_failure(self, tmp_path):
        """Test finalize_receive with pipeline failure."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        stream_path = tmp_path / "test-snap.btrfs"
        stream_path.write_bytes(b"")

        endpoint._pending_metadata = {
            "name": "test-snap",
            "stream_path": stream_path,
            "parent_name": None,
            "compress": None,
            "encrypt": None,
            "gpg_recipient": None,
        }

        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (b"", b"Pipeline error")
        mock_proc.returncode = 1

        with pytest.raises(RuntimeError, match="Raw receive pipeline failed"):
            endpoint.finalize_receive(mock_proc)

    def test_finalize_receive_updates_cache(self, tmp_path):
        """Test that finalize_receive updates the cache."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        stream_path = tmp_path / "test-snap.btrfs"
        stream_path.write_bytes(b"data")

        # Initialize cache
        endpoint._cached_snapshots = []

        endpoint._pending_metadata = {
            "name": "test-snap",
            "stream_path": stream_path,
            "parent_name": None,
            "compress": None,
            "encrypt": None,
            "gpg_recipient": None,
        }

        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (b"", b"")
        mock_proc.returncode = 0

        endpoint.finalize_receive(mock_proc)

        assert len(endpoint._cached_snapshots) == 1


class TestRawEndpointSend:
    """Tests for send (restore) functionality."""

    def test_send_existing_file(self, tmp_path):
        """Test sending an existing stream file."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        stream_path = tmp_path / "test.btrfs"
        stream_path.write_bytes(b"btrfs stream data")

        snapshot = RawSnapshot(name="test", stream_path=stream_path)

        with patch("subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_popen.return_value = mock_proc
            result = endpoint.send(snapshot)

        assert result == mock_proc


class TestRawEndpointExecuteRestorePipeline:
    """Tests for restore pipeline execution."""

    def test_execute_restore_single_command(self, tmp_path):
        """Test executing single-command restore pipeline."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        stream_path = tmp_path / "test.btrfs"
        stream_path.write_bytes(b"data")

        with patch("builtins.open", MagicMock()):
            with patch("subprocess.Popen") as mock_popen:
                mock_proc = MagicMock()
                mock_popen.return_value = mock_proc
                result = endpoint._execute_restore_pipeline([["cat"]], stream_path)

        assert result == mock_proc

    def test_execute_restore_multi_command(self, tmp_path):
        """Test executing multi-command restore pipeline."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        stream_path = tmp_path / "test.btrfs.gz"
        stream_path.write_bytes(b"compressed data")

        with patch("subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_popen.return_value = mock_proc
            result = endpoint._execute_restore_pipeline(
                [["gzip", "-d", "-c"]], stream_path
            )

        assert result == mock_proc


class TestRawEndpointDeleteSnapshots:
    """Additional tests for snapshot deletion."""

    def test_delete_snapshots_handles_errors(self, tmp_path):
        """Test that delete handles OSError gracefully."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        snapshot = RawSnapshot(
            name="test",
            stream_path=tmp_path / "nonexistent.btrfs",
        )

        # Should not raise, just log error
        endpoint.delete_snapshot(snapshot)

    def test_delete_old_snapshots_keep_zero(self, tmp_path):
        """Test delete_old_snapshots with keep=0 does nothing."""
        (tmp_path / "test.20240115T120000.btrfs").write_bytes(b"data")

        endpoint = RawEndpoint(config={"path": tmp_path})
        endpoint.delete_old_snapshots(keep=0)

        # Snapshot should still exist
        snapshots = endpoint.list_snapshots(flush_cache=True)
        assert len(snapshots) == 1

    def test_delete_updates_cache(self, tmp_path):
        """Test that delete updates the cache."""
        stream_path = tmp_path / "test.20240115T120000.btrfs"
        stream_path.write_bytes(b"data")

        endpoint = RawEndpoint(config={"path": tmp_path})

        # Populate cache
        snapshots = endpoint.list_snapshots()
        assert len(snapshots) == 1

        # Delete snapshot
        endpoint.delete_snapshot(snapshots[0])

        # Cache should be updated
        cached = endpoint._cached_snapshots
        assert len(cached) == 0


class TestRawEndpointGetSpaceInfo:
    """Tests for get_space_info."""

    def test_get_space_info(self, tmp_path):
        """Test getting space info for the target directory."""
        endpoint = RawEndpoint(config={"path": tmp_path})

        with patch("btrfs_backup_ng.core.space.get_space_info") as mock_get_space:
            mock_space = MagicMock()
            mock_get_space.return_value = mock_space
            result = endpoint.get_space_info()

        assert result == mock_space


class TestSSHRawEndpointMethods:
    """Additional tests for SSHRawEndpoint methods."""

    def test_prepare_creates_remote_directory(self):
        """Test that prepare creates remote directory via SSH."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup/data",
                "hostname": "nas",
                "username": "backup",
            }
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            with patch.object(endpoint, "_check_tools", return_value=[]):
                endpoint._prepare()

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "ssh" in call_args
        assert "backup@nas" in call_args
        assert "mkdir -p /backup/data" in call_args

    def test_prepare_with_sudo(self):
        """Test that prepare uses sudo when configured."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup/data",
                "hostname": "nas",
                "ssh_sudo": True,
            }
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            with patch.object(endpoint, "_check_tools", return_value=[]):
                endpoint._prepare()

        call_args = mock_run.call_args[0][0]
        assert "sudo mkdir" in call_args[-1]

    def test_prepare_failure(self):
        """Test prepare handles SSH failure."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup/data",
                "hostname": "nas",
            }
        )

        with patch("subprocess.run") as mock_run:
            import subprocess

            mock_run.side_effect = subprocess.CalledProcessError(
                1, "ssh", stderr=b"Connection refused"
            )
            with pytest.raises(subprocess.CalledProcessError):
                endpoint._prepare()

    def test_execute_pipeline_no_local_processing(self):
        """Test SSH pipeline with no local compression."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
                "username": "backup",
            }
        )
        endpoint._pending_metadata = {"stream_path": Path("/backup/test.btrfs")}
        mock_stdin = MagicMock()

        with patch("subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_popen.return_value = mock_proc
            result = endpoint._execute_pipeline([["cat"]], mock_stdin)

        assert result == mock_proc
        # Should pipe directly to SSH
        call_args = mock_popen.call_args[0][0]
        assert "ssh" in call_args

    def test_execute_pipeline_with_compression(self):
        """Test SSH pipeline with local compression."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
                "compress": "zstd",
            }
        )
        endpoint._pending_metadata = {"stream_path": Path("/backup/test.btrfs.zst")}
        mock_stdin = MagicMock()

        with patch("subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_popen.return_value = mock_proc
            endpoint._execute_pipeline([["zstd", "-c"]], mock_stdin)

        # Should use shell for pipeline
        call_args = mock_popen.call_args
        assert call_args[1]["shell"] is True

    def test_execute_pipeline_with_sudo(self):
        """Test SSH pipeline with sudo."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
                "ssh_sudo": True,
            }
        )
        endpoint._pending_metadata = {"stream_path": Path("/backup/test.btrfs")}
        mock_stdin = MagicMock()

        with patch("subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_popen.return_value = mock_proc
            endpoint._execute_pipeline([["cat"]], mock_stdin)

        call_args = mock_popen.call_args[0][0]
        # Command should include sudo
        assert "sudo" in str(call_args)

    def test_list_snapshots_empty(self):
        """Test listing snapshots on remote with no snapshots."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
            }
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            snapshots = endpoint.list_snapshots()

        assert snapshots == []

    def test_list_snapshots_with_metadata(self):
        """Test listing snapshots from remote metadata files."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
            }
        )

        meta_content = (
            '{"name": "test", "uuid": "abc123", "created": "2024-01-15T12:00:00"}'
        )

        with patch("subprocess.run") as mock_run:
            # First call: find metadata files
            # Second call: cat metadata file
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="/backup/test.btrfs.meta\n"),
                MagicMock(returncode=0, stdout=meta_content),
            ]
            snapshots = endpoint.list_snapshots()

        assert len(snapshots) == 1
        assert snapshots[0].name == "test"

    def test_list_snapshots_caching(self):
        """Test that SSH snapshot listing uses caching."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
            }
        )

        # Set cache manually
        cached_snapshot = RawSnapshot(
            name="cached",
            stream_path=Path("/backup/cached.btrfs"),
        )
        endpoint._cached_snapshots = [cached_snapshot]

        # Should return cached without SSH call
        with patch("subprocess.run") as mock_run:
            snapshots = endpoint.list_snapshots()
            mock_run.assert_not_called()

        assert len(snapshots) == 1
        assert snapshots[0].name == "cached"

    def test_delete_snapshots_remote(self):
        """Test deleting snapshots on remote host."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
            }
        )
        snapshot = RawSnapshot(
            name="test",
            stream_path=Path("/backup/test.btrfs"),
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            endpoint.delete_snapshots([snapshot])

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "rm -f" in call_args[-1]

    def test_delete_snapshots_failure(self):
        """Test handling delete failure on remote."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
            }
        )
        snapshot = RawSnapshot(
            name="test",
            stream_path=Path("/backup/test.btrfs"),
        )

        with patch("subprocess.run") as mock_run:
            import subprocess

            mock_run.side_effect = subprocess.CalledProcessError(
                1, "ssh", stderr=b"Permission denied"
            )
            # Should not raise, just log error
            endpoint.delete_snapshots([snapshot])

    def test_delete_updates_cache(self):
        """Test that remote delete updates cache."""
        endpoint = SSHRawEndpoint(
            config={
                "path": "/backup",
                "hostname": "nas",
            }
        )
        snapshot = RawSnapshot(
            name="test",
            stream_path=Path("/backup/test.btrfs"),
        )
        endpoint._cached_snapshots = [snapshot]

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            endpoint.delete_snapshots([snapshot])

        assert len(endpoint._cached_snapshots) == 0


class TestOpenSSLPassphrase:
    """Tests for OpenSSL passphrase handling."""

    def test_get_openssl_passphrase_primary_env(self, tmp_path, monkeypatch):
        """Test getting passphrase from primary env var."""
        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "primary_pass")
        endpoint = RawEndpoint(config={"path": tmp_path, "encrypt": "openssl_enc"})
        assert endpoint._get_openssl_passphrase() == "primary_pass"

    def test_get_openssl_passphrase_btrbk_fallback(self, tmp_path, monkeypatch):
        """Test getting passphrase from btrbk fallback env var."""
        monkeypatch.delenv("BTRFS_BACKUP_PASSPHRASE", raising=False)
        monkeypatch.setenv("BTRBK_PASSPHRASE", "btrbk_pass")
        endpoint = RawEndpoint(config={"path": tmp_path, "encrypt": "openssl_enc"})
        assert endpoint._get_openssl_passphrase() == "btrbk_pass"

    def test_get_openssl_passphrase_not_set(self, tmp_path, monkeypatch):
        """Test passphrase returns None when not set."""
        monkeypatch.delenv("BTRFS_BACKUP_PASSPHRASE", raising=False)
        monkeypatch.delenv("BTRBK_PASSPHRASE", raising=False)
        # Constructor logs warning but doesn't fail
        endpoint = RawEndpoint(config={"path": tmp_path, "encrypt": "openssl_enc"})
        assert endpoint._get_openssl_passphrase() is None


class TestAllCompressionMethods:
    """Test all supported compression methods."""

    @pytest.mark.parametrize(
        "compress,extension",
        [
            ("gzip", ".gz"),
            ("pigz", ".gz"),
            ("zstd", ".zst"),
            ("lz4", ".lz4"),
            ("xz", ".xz"),
            ("lzo", ".lzo"),
            ("bzip2", ".bz2"),
            ("pbzip2", ".bz2"),
        ],
    )
    def test_compression_pipeline(self, tmp_path, compress, extension):
        """Test pipeline construction for each compression method."""
        endpoint = RawEndpoint(config={"path": tmp_path, "compress": compress})
        endpoint._pending_metadata = {
            "stream_path": tmp_path / f"test.btrfs{extension}"
        }

        pipeline = endpoint._build_receive_pipeline(tmp_path / f"test.btrfs{extension}")

        assert len(pipeline) == 1
        config = COMPRESSION_CONFIG[compress]
        expected_cmd = config["compress_cmd"]
        assert pipeline[0] == list(expected_cmd)

    @pytest.mark.parametrize(
        "compress",
        ["gzip", "pigz", "zstd", "lz4", "xz", "lzo", "bzip2", "pbzip2"],
    )
    def test_decompression_pipeline(self, tmp_path, compress):
        """Test restore pipeline for each compression method."""
        endpoint = RawEndpoint(config={"path": tmp_path})
        snapshot = RawSnapshot(
            name="test",
            stream_path=tmp_path / "test.btrfs",
            compress=compress,
        )

        pipeline = endpoint._build_restore_pipeline(snapshot)

        assert len(pipeline) == 1
        config = COMPRESSION_CONFIG[compress]
        expected_cmd = config["decompress_cmd"]
        assert pipeline[0] == list(expected_cmd)
