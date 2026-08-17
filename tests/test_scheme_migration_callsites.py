"""The call sites migrated off ``startswith("ssh://")`` behave correctly.

``raw+ssh://`` does not start with ``ssh://``, so every site that dispatched on
that prefix treated a remote raw target as a local path. Two consequences are
pinned here, one per site, because both were user-visible and neither had a test:

* ``cli/restore.py`` gave a ``raw+ssh://`` source none of the ``--ssh-*``
  options, so its remote listing ran as whoever ssh logged in as and the
  identity file was threaded under a key ``SSHRawEndpoint`` does not read.
  Fixed here. This landed only after the endpoint stopped reporting a refused
  sudo as an empty target: threading the options earlier turned a working
  ``restore --list`` into a silent "No snapshots found" with exit 0, so the
  two tests below were strict xfails until that was corrected.
* ``cli/transfer.py`` handed ``Path()`` the whole URI when checking
  ``require_mount``. ``Path("raw:///mnt/usb")`` can never be a mount point, so
  the check aborted every raw transfer that asked for it -- the opposite of the
  external-drive safety the option exists to provide. FIXED here.

Both assert on what the code hands the layer beneath it, because that is where
the defect lived: the values looked plausible and were simply wrong.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import transfer
from btrfs_backup_ng.cli.restore import _prepare_backup_endpoint


def _restore_args(**overrides):
    defaults = {
        "prefix": "",
        "timestamp_format": None,
        "fs_checks": "skip",
        "no_fs_checks": True,
        "ssh_sudo": True,
        "ssh_key": "/keys/backup_ed25519",
        "ssh_auth_sock": None,
        "ssh_host_key_policy": None,
        "ssh_password_auth": True,
        "skip_verify": True,
        "gpg_keyring": None,
        "openssl_cipher": None,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestRestoreThreadsSshOptionsToRemoteSources:
    """A remote source must receive the SSH options, whichever scheme it uses."""

    @pytest.fixture(autouse=True)
    def _isolated_home(self, tmp_path, monkeypatch):
        # Constructing a remote endpoint touches ~/.ssh; keep it off the real one.
        monkeypatch.setenv("HOME", str(tmp_path))

    @staticmethod
    def _endpoint_for(source):
        with patch(
            "btrfs_backup_ng.endpoint.common.Endpoint.prepare", return_value=None
        ):
            return _prepare_backup_endpoint(_restore_args(), source)

    def test_rawssh_source_receives_ssh_sudo(self):
        """The bug: raw+ssh:// got no ssh_sudo, so the remote find ran unprivileged."""
        ep = self._endpoint_for("raw+ssh://backup@nas:/mnt/store")
        # ssh_sudo lands as an ATTRIBUTE: the base Endpoint rebuilds config from a
        # fixed key whitelist that drops SSH-specific keys, so asserting on
        # config alone would miss a working value.
        seen = getattr(ep, "ssh_sudo", None)
        if seen is None:
            seen = ep.config.get("ssh_sudo")
        assert seen is True, (
            "raw+ssh:// source did not receive ssh_sudo; its remote listing would "
            "run unprivileged and report no snapshots"
        )

    def test_rawssh_source_receives_the_identity_file_under_the_key_it_reads(self):
        """SSHRawEndpoint reads ssh_key and ignores ssh_identity_file.

        Threading the wrong name produces an ssh invocation with no -i flag and
        no error, so this asserts the endpoint actually ended up with the key.
        """
        ep = self._endpoint_for("raw+ssh://backup@nas:/mnt/store")
        seen = (
            getattr(ep, "ssh_key", None)
            or ep.config.get("ssh_key")
            or ep.config.get("ssh_identity_file")
        )
        assert seen == "/keys/backup_ed25519", (
            f"identity file did not reach the raw+ssh endpoint (saw {seen!r}); "
            "SSHRawEndpoint reads 'ssh_key', not 'ssh_identity_file'"
        )

    def test_plain_ssh_source_still_receives_its_options(self):
        """Guards against fixing raw+ssh by breaking the case that worked."""
        ep = self._endpoint_for("ssh://backup@nas:/mnt/store")
        assert ep.config.get("ssh_sudo") is True
        seen = (
            getattr(ep, "ssh_key", None)
            or ep.config.get("ssh_identity_file")
            or ep.config.get("ssh_key")
        )
        assert seen == "/keys/backup_ed25519"

    def test_local_source_gets_a_resolved_path_and_no_ssh_options(self, tmp_path):
        backups = tmp_path / "backups"
        backups.mkdir()
        ep = self._endpoint_for(str(backups))
        assert not ep.config.get("ssh_sudo")
        assert Path(ep.config["path"]) == backups.resolve()

    def test_local_raw_source_keeps_the_path_parsed_from_its_uri(self, tmp_path):
        """A raw source must not have its path overwritten with Path(uri).resolve()."""
        backups = tmp_path / "backups"
        backups.mkdir()
        ep = self._endpoint_for(f"raw://{backups}")
        assert Path(ep.config["path"]) == backups

    @pytest.mark.parametrize(
        "source", ["raw:///mnt/store", "raw+ssh://backup@nas:/mnt/store"]
    )
    def test_a_raw_source_is_never_handed_a_cwd_resolved_path(self, source):
        """Pin what is COMPUTED, not only what survives.

        choose_endpoint parses a raw URI itself and overwrites config["path"],
        so passing it `Path("raw:///mnt/store").resolve()` -- a nonsense path
        under the working directory -- is invisible today. It is still wrong to
        compute, and it would become visible the moment choose_endpoint stopped
        overriding. Asserting on the kwargs keeps the intent enforceable.
        """
        captured = {}

        def fake_choose(path, kwargs, source=False):
            captured.update(kwargs)
            return MagicMock(config={})

        with patch("btrfs_backup_ng.cli.restore.endpoint.choose_endpoint", fake_choose):
            _prepare_backup_endpoint(_restore_args(), source)

        assert "path" not in captured, (
            f"a raw source was handed path={captured.get('path')!r}, which "
            "resolves against the working directory"
        )


class TestRequireMountGate:
    """``require_mount`` must inspect the destination path, not the URI.

    Every test drives ``assert_target_mounted`` itself and records what reached
    ``is_mounted``, so a regression that resolves the URI again, or that skips
    the gate, fails here rather than on someone's external drive.
    """

    @staticmethod
    def _run(target_path, *, mounted=True):
        """Call the gate; return the paths handed to is_mounted."""
        seen = []

        def fake_is_mounted(path):
            seen.append(Path(path))
            return mounted

        with patch.object(__util__, "is_mounted", fake_is_mounted):
            transfer.assert_target_mounted(target_path)
        return seen

    def test_raw_target_checks_the_directory_not_the_uri(self, tmp_path):
        """Path("raw:///mnt/usb") is not a mount point and never can be."""
        usb = tmp_path / "usb"
        usb.mkdir()
        assert self._run(f"raw://{usb}") == [usb.resolve()]

    def test_local_target_is_checked_as_written(self, tmp_path):
        assert self._run(str(tmp_path)) == [tmp_path.resolve()]

    def test_unmounted_target_aborts(self, tmp_path):
        with pytest.raises(__util__.AbortError, match="is not mounted"):
            self._run(str(tmp_path), mounted=False)

    def test_unmounted_raw_target_aborts(self, tmp_path):
        """The case require_mount exists for: an external drive that is absent."""
        with pytest.raises(__util__.AbortError, match="is not mounted"):
            self._run(f"raw://{tmp_path}", mounted=False)

    @pytest.mark.parametrize(
        "uri", ["ssh://user@host:/backups", "raw+ssh://user@host/backups"]
    )
    def test_remote_targets_are_exempt(self, uri):
        """README scopes require_mount to local targets.

        A local mount table cannot answer for a remote filesystem, so consulting
        it would abort every remote transfer.
        """
        assert self._run(uri, mounted=False) == []

    @pytest.mark.parametrize(
        "uri",
        [
            "raw://mnt/usb/backups",  # two slashes: choose_endpoint writes to /usb/backups
            "shell://cat > /dev/null",
            "user@host:/backups",  # bare form: not constructible
        ],
    )
    def test_undeterminable_paths_fail_closed(self, uri):
        """A target whose path cannot be determined must abort, not proceed.

        This is the regression the first cut of the migration introduced. Gating
        on ``supports_mount_check`` alone made these skip the gate entirely,
        while choose_endpoint still built a working endpoint -- so a backup meant
        for an external drive would land on the root filesystem and the source
        would then be pruned as though it had succeeded. Path("") is
        PosixPath("."), which always exists, so an empty path must be refused
        explicitly rather than checked.
        """
        with pytest.raises(__util__.AbortError, match="cannot be determined"):
            self._run(uri, mounted=False)

    def test_execute_transfer_still_calls_the_gate(self):
        """The gate is only worth testing if the command still invokes it.

        Extracting it made it testable but also makes it droppable; the loop it
        sits in needs a real btrfs source with snapshots to reach, which is tier
        3 territory, so the call itself is pinned here.
        """
        import inspect

        source = inspect.getsource(transfer.execute_transfer)
        assert "assert_target_mounted(target.path)" in source
        assert "target.require_mount" in source
