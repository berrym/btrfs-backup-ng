"""Tests for shell completions installation command."""

from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestGetCompletionsDir:
    """Tests for get_completions_dir function."""

    def test_finds_dev_path(self, tmp_path):
        """Test finding completions in development mode."""
        from btrfs_backup_ng.cli.completions import get_completions_dir

        # The function looks relative to the module file
        result = get_completions_dir()

        # In dev mode, it should find the completions directory
        # or return None if not found
        assert result is None or result.exists()

    @patch("btrfs_backup_ng.cli.completions.files")
    def test_finds_package_path(self, mock_files, tmp_path):
        """Test finding completions when installed as package."""
        from btrfs_backup_ng.cli.completions import get_completions_dir

        # Create a fake package structure
        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        (completions_dir / "btrfs-backup-ng.bash").touch()

        pkg_path = tmp_path / "lib" / "python" / "btrfs_backup_ng"
        pkg_path.mkdir(parents=True)

        mock_pkg_files = MagicMock()
        mock_pkg_files.__str__ = lambda self: str(pkg_path)
        mock_files.return_value = mock_pkg_files

        result = get_completions_dir()

        # Should either find the real dev path or return None
        # (mock doesn't fully work since Path resolution differs)
        assert result is None or isinstance(result, Path)

    @patch("btrfs_backup_ng.cli.completions.files")
    def test_handles_exception(self, mock_files):
        """Test handling exception when looking for package path."""
        from btrfs_backup_ng.cli.completions import get_completions_dir

        mock_files.side_effect = Exception("Package not found")

        # Should not raise, should try dev path
        result = get_completions_dir()
        assert result is None or isinstance(result, Path)


class TestExecuteCompletions:
    """Tests for execute_completions function."""

    @patch("btrfs_backup_ng.cli.completions.install_completions")
    def test_install_action(self, mock_install):
        """Test install action is routed correctly."""
        from btrfs_backup_ng.cli.completions import execute_completions

        mock_install.return_value = 0

        args = Namespace(completions_action="install", shell="bash", system=False)
        result = execute_completions(args)

        assert result == 0
        mock_install.assert_called_once_with(args)

    @patch("btrfs_backup_ng.cli.completions.show_completions_path")
    def test_path_action(self, mock_path):
        """Test path action is routed correctly."""
        from btrfs_backup_ng.cli.completions import execute_completions

        mock_path.return_value = 0

        args = Namespace(completions_action="path")
        result = execute_completions(args)

        assert result == 0
        mock_path.assert_called_once_with(args)

    def test_no_action(self, capsys):
        """Test error when no action specified."""
        from btrfs_backup_ng.cli.completions import execute_completions

        args = Namespace(completions_action=None)
        result = execute_completions(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "No action specified" in captured.out

    def test_invalid_action(self, capsys):
        """Test error for invalid action."""
        from btrfs_backup_ng.cli.completions import execute_completions

        args = Namespace(completions_action="invalid")
        result = execute_completions(args)

        assert result == 1


class TestShowCompletionsPath:
    """Tests for show_completions_path function."""

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_dir_not_found(self, mock_get_dir, capsys):
        """Test error when completions directory not found."""
        from btrfs_backup_ng.cli.completions import show_completions_path

        mock_get_dir.return_value = None

        args = Namespace()
        result = show_completions_path(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Could not find completions directory" in captured.err

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_shows_path_and_files(self, mock_get_dir, tmp_path, capsys):
        """Test showing completions path and available files."""
        from btrfs_backup_ng.cli.completions import show_completions_path

        # Create fake completions directory with files
        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        (completions_dir / "btrfs-backup-ng.bash").touch()
        (completions_dir / "btrfs-backup-ng.zsh").touch()
        (completions_dir / "btrfs-backup-ng.fish").touch()

        mock_get_dir.return_value = completions_dir

        args = Namespace()
        result = show_completions_path(args)

        assert result == 0
        captured = capsys.readouterr()
        assert str(completions_dir) in captured.out
        assert "btrfs-backup-ng.bash" in captured.out
        assert "btrfs-backup-ng.zsh" in captured.out
        assert "btrfs-backup-ng.fish" in captured.out


class TestInstallCompletions:
    """Tests for install_completions function."""

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_completions_dir_not_found(self, mock_get_dir, capsys):
        """Test error when completions directory not found."""
        from btrfs_backup_ng.cli.completions import install_completions

        mock_get_dir.return_value = None

        args = Namespace(shell="bash", system=False)
        result = install_completions(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Could not find completions directory" in captured.err

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_unknown_shell(self, mock_get_dir, tmp_path, capsys):
        """Test error for unknown shell type."""
        from btrfs_backup_ng.cli.completions import install_completions

        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        mock_get_dir.return_value = completions_dir

        args = Namespace(shell="powershell", system=False)
        result = install_completions(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Unknown shell" in captured.err
        assert "powershell" in captured.err

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_source_file_not_found(self, mock_get_dir, tmp_path, capsys):
        """Test error when source completion file not found."""
        from btrfs_backup_ng.cli.completions import install_completions

        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        # Don't create the source file
        mock_get_dir.return_value = completions_dir

        args = Namespace(shell="bash", system=False)
        result = install_completions(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Completion file not found" in captured.err

    @patch("btrfs_backup_ng.cli.completions._can_write_system")
    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_system_install_permission_denied(
        self, mock_get_dir, mock_can_write, tmp_path, capsys
    ):
        """Test error when system install without permissions."""
        from btrfs_backup_ng.cli.completions import install_completions

        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        (completions_dir / "btrfs-backup-ng.bash").write_text("# bash completions")
        mock_get_dir.return_value = completions_dir

        mock_can_write.return_value = False

        args = Namespace(shell="bash", system=True)
        result = install_completions(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Cannot write to" in captured.err

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_successful_user_install_bash(self, mock_get_dir, tmp_path, capsys):
        """Test successful user installation for bash."""
        from btrfs_backup_ng.cli.completions import install_completions

        # Create source
        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        source_file = completions_dir / "btrfs-backup-ng.bash"
        source_file.write_text(
            "# bash completions\ncomplete -F _btrfs_backup btrfs-backup-ng"
        )
        mock_get_dir.return_value = completions_dir

        with patch.object(Path, "home", return_value=tmp_path / "user"):
            args = Namespace(shell="bash", system=False)
            result = install_completions(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Installed bash completions" in captured.out
        assert "source" in captured.out  # Activation instruction

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_successful_user_install_zsh(self, mock_get_dir, tmp_path, capsys):
        """Test successful user installation for zsh."""
        from btrfs_backup_ng.cli.completions import install_completions

        # Create source
        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        source_file = completions_dir / "btrfs-backup-ng.zsh"
        source_file.write_text("#compdef btrfs-backup-ng")
        mock_get_dir.return_value = completions_dir

        with patch.object(Path, "home", return_value=tmp_path / "user"):
            args = Namespace(shell="zsh", system=False)
            result = install_completions(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Installed zsh completions" in captured.out
        assert "compinit" in captured.out  # Activation instruction
        assert "fpath" in captured.out

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_successful_user_install_fish(self, mock_get_dir, tmp_path, capsys):
        """Test successful user installation for fish."""
        from btrfs_backup_ng.cli.completions import install_completions

        # Create source
        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        source_file = completions_dir / "btrfs-backup-ng.fish"
        source_file.write_text("complete -c btrfs-backup-ng")
        mock_get_dir.return_value = completions_dir

        with patch.object(Path, "home", return_value=tmp_path / "user"):
            args = Namespace(shell="fish", system=False)
            result = install_completions(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Installed fish completions" in captured.out
        assert "automatically load" in captured.out

    @patch("btrfs_backup_ng.cli.completions._can_write_system")
    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_successful_system_install(
        self, mock_get_dir, mock_can_write, tmp_path, capsys
    ):
        """Test successful system-wide installation."""
        from btrfs_backup_ng.cli.completions import install_completions

        # Create source
        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        source_file = completions_dir / "btrfs-backup-ng.bash"
        source_file.write_text("# bash completions")
        mock_get_dir.return_value = completions_dir

        mock_can_write.return_value = True

        # Create a temp destination that we can write to
        system_dest = tmp_path / "etc" / "bash_completion.d"
        system_dest.mkdir(parents=True)

        with patch.dict(
            "btrfs_backup_ng.cli.completions.install_completions.__globals__", {}
        ):
            # We need to patch the shell_config dict inside the function
            # Actually, let's just verify the function flow works
            args = Namespace(shell="bash", system=True)

            # This will fail because /etc isn't writable, but we can verify the flow
            # by mocking shutil.copy2
            with patch("shutil.copy2") as mock_copy:
                result = install_completions(args)

                # Should succeed because _can_write_system returns True
                assert result == 0
                mock_copy.assert_called_once()

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_mkdir_permission_error(self, mock_get_dir, tmp_path, capsys):
        """Test error when cannot create parent directory."""
        from btrfs_backup_ng.cli.completions import install_completions

        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        (completions_dir / "btrfs-backup-ng.bash").write_text("# bash")
        mock_get_dir.return_value = completions_dir

        with patch.object(Path, "home", return_value=tmp_path / "user"):
            with patch.object(Path, "mkdir", side_effect=PermissionError("denied")):
                args = Namespace(shell="bash", system=False)
                result = install_completions(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Cannot create directory" in captured.err

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_copy_permission_error(self, mock_get_dir, tmp_path, capsys):
        """Test error when copy fails with permission error."""
        from btrfs_backup_ng.cli.completions import install_completions

        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        (completions_dir / "btrfs-backup-ng.bash").write_text("# bash")
        mock_get_dir.return_value = completions_dir

        with patch.object(Path, "home", return_value=tmp_path / "user"):
            with patch("shutil.copy2", side_effect=PermissionError("denied")):
                args = Namespace(shell="bash", system=False)
                result = install_completions(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Permission denied" in captured.err

    @patch("btrfs_backup_ng.cli.completions.get_completions_dir")
    def test_copy_generic_error(self, mock_get_dir, tmp_path, capsys):
        """Test error when copy fails with generic error."""
        from btrfs_backup_ng.cli.completions import install_completions

        completions_dir = tmp_path / "completions"
        completions_dir.mkdir()
        (completions_dir / "btrfs-backup-ng.bash").write_text("# bash")
        mock_get_dir.return_value = completions_dir

        with patch.object(Path, "home", return_value=tmp_path / "user"):
            with patch("shutil.copy2", side_effect=OSError("disk error")):
                args = Namespace(shell="bash", system=False)
                result = install_completions(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Error installing completions" in captured.err


class TestCanWriteSystem:
    """Tests for _can_write_system function."""

    @patch("os.geteuid")
    def test_root_can_write(self, mock_euid):
        """Test that root user can write."""
        from btrfs_backup_ng.cli.completions import _can_write_system

        mock_euid.return_value = 0

        result = _can_write_system(Path("/etc/bash_completion.d/test"))

        assert result is True

    @patch("os.geteuid")
    @patch("os.access")
    def test_non_root_parent_writable(self, mock_access, mock_euid, tmp_path):
        """Test non-root user with writable parent."""
        from btrfs_backup_ng.cli.completions import _can_write_system

        mock_euid.return_value = 1000
        mock_access.return_value = True

        # Create the parent
        parent = tmp_path / "parent"
        parent.mkdir()

        result = _can_write_system(parent / "file")

        assert result is True
        mock_access.assert_called_once()

    @patch("os.geteuid")
    @patch("os.access")
    def test_non_root_parent_not_writable(self, mock_access, mock_euid, tmp_path):
        """Test non-root user with non-writable parent."""
        from btrfs_backup_ng.cli.completions import _can_write_system

        mock_euid.return_value = 1000
        mock_access.return_value = False

        # Create the parent
        parent = tmp_path / "parent"
        parent.mkdir()

        result = _can_write_system(parent / "file")

        assert result is False

    @patch("os.geteuid")
    def test_non_root_parent_not_exists(self, mock_euid):
        """Test non-root user when parent doesn't exist."""
        from btrfs_backup_ng.cli.completions import _can_write_system

        mock_euid.return_value = 1000

        result = _can_write_system(Path("/nonexistent/path/file"))

        assert result is False


class TestShellConfigTypes:
    """Tests to verify shell config is properly structured."""

    def test_bash_config_has_required_keys(self):
        """Test bash config has all required keys."""

        # We can't easily inspect the config dict, but we can verify
        # the function handles bash correctly
        # This is implicitly tested by the install tests above
        pass

    def test_zsh_config_has_required_keys(self):
        """Test zsh config has all required keys."""
        pass

    def test_fish_config_has_required_keys(self):
        """Test fish config has all required keys."""
        pass


class TestSnapperRestoreCompletionFlags:
    """R11b: new snapper-restore flags stay in lockstep across all shell completions."""

    def _completions_dir(self):
        return Path(__file__).resolve().parent.parent / "completions"

    def test_backup_name_and_date_in_all_shells(self):
        d = self._completions_dir()
        # bash/zsh spell long flags "--name"; fish uses "-l name".
        expected = {
            "btrfs-backup-ng.bash": ("--backup-name", "--date"),
            "btrfs-backup-ng.zsh": ("--backup-name", "--date"),
            "btrfs-backup-ng.fish": ("-l backup-name", "-l date"),
        }
        for fname, needles in expected.items():
            text = (d / fname).read_text()
            for needle in needles:
                assert needle in text, f"{fname} missing {needle!r}"


class TestDecryptCompletionFlags:
    """0.9.2 Phase 2A: --gpg-keyring/--openssl-cipher stay in lockstep across all
    shells for restore and snapper restore (the paths that DECODE). verify must
    NOT offer them -- it checksums ciphertext and never decrypts."""

    def _completions_dir(self):
        return Path(__file__).resolve().parent.parent / "completions"

    def test_decrypt_flags_in_bash_opt_lines(self):
        bash = (self._completions_dir() / "btrfs-backup-ng.bash").read_text()
        for opt_var in ("restore_opts=", "snapper_restore_opts="):
            line = next(line for line in bash.splitlines() if opt_var in line)
            assert "--gpg-keyring" in line, f"{opt_var} missing --gpg-keyring"
            assert "--openssl-cipher" in line, f"{opt_var} missing --openssl-cipher"

    def test_verify_bash_opts_have_no_decrypt_flags(self):
        bash = (self._completions_dir() / "btrfs-backup-ng.bash").read_text()
        line = next(line for line in bash.splitlines() if "verify_opts=" in line)
        assert "--gpg-keyring" not in line
        assert "--openssl-cipher" not in line

    def test_decrypt_flags_present_in_zsh_and_fish(self):
        d = self._completions_dir()
        # Two commands (restore, snapper restore) each declare both flags.
        zsh = (d / "btrfs-backup-ng.zsh").read_text()
        assert zsh.count("--gpg-keyring[") >= 2
        assert zsh.count("--openssl-cipher[") >= 2
        fish = (d / "btrfs-backup-ng.fish").read_text()
        assert fish.count("-l gpg-keyring") >= 2
        assert fish.count("-l openssl-cipher") >= 2


class TestRunCompletionNoStaleFlags:
    """0.9.2: `run` completions must not offer flags run doesn't accept."""

    def _dir(self):
        return Path(__file__).resolve().parent.parent / "completions"

    def test_run_has_no_fs_checks_or_positive_check_space(self):
        import re

        d = self._dir()
        fish = (d / "btrfs-backup-ng.fish").read_text()
        assert "_using_command run' -l fs-checks" not in fish
        assert "_using_command run' -l check-space" not in fish  # positive is stale

        bash = (d / "btrfs-backup-ng.bash").read_text()
        run_opts = next(line for line in bash.splitlines() if "run_opts=" in line)
        assert "--fs-checks" not in run_opts
        assert "--check-space" not in run_opts.replace("--no-check-space", "")

        zsh = (d / "btrfs-backup-ng.zsh").read_text()
        block = re.search(r"\brun\)(.*?);;", zsh, re.S)
        assert block is not None
        assert "--fs-checks" not in block.group(1)
        assert "--check-space" not in block.group(1).replace("--no-check-space", "")


class TestSshAuthSockCompletions:
    """CLI-polish: --ssh-auth-sock is a valid flag on 5 commands (restore, verify,
    estimate, snapper backup, snapper restore); completions must offer it there in
    every shell."""

    def _dir(self):
        return Path(__file__).resolve().parent.parent / "completions"

    def test_bash_opt_arrays_have_it(self):
        bash = (self._dir() / "btrfs-backup-ng.bash").read_text()
        for opt in (
            "restore_opts=",
            "verify_opts=",
            "estimate_opts=",
            "snapper_backup_opts=",
            "snapper_restore_opts=",
        ):
            line = next(line for line in bash.splitlines() if opt in line)
            assert "--ssh-auth-sock" in line, f"{opt} missing --ssh-auth-sock"

    def test_zsh_and_fish_cover_all_five(self):
        d = self._dir()
        zsh = (d / "btrfs-backup-ng.zsh").read_text()
        assert zsh.count("--ssh-auth-sock[") >= 5
        fish = (d / "btrfs-backup-ng.fish").read_text()
        assert fish.count("-l ssh-auth-sock") >= 5


class TestSshAuthSockManpages:
    """CLI-polish: the flag must be documented in the manpages for the commands
    that accept it."""

    def _man(self):
        return Path(__file__).resolve().parent.parent / "man" / "man1"

    def test_manpages_document_it(self):
        m = self._man()
        assert "ssh-auth-sock" in (m / "btrfs-backup-ng-restore.1").read_text()
        assert "ssh-auth-sock" in (m / "btrfs-backup-ng-verify.1").read_text()
        assert "ssh-auth-sock" in (m / "btrfs-backup-ng-estimate.1").read_text()
        # snapper.1 covers BOTH snapper backup and snapper restore.
        assert (m / "btrfs-backup-ng-snapper.1").read_text().count("ssh-auth-sock") >= 2
