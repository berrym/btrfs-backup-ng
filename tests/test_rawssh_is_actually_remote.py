"""A ``raw+ssh://`` endpoint must answer every question about the REMOTE host.

``SSHRawEndpoint`` overrides thirty-odd methods of ``RawEndpoint`` but inherits the
rest, and two of the inherited ones interrogated the LOCAL filesystem for a target
that lives on another machine:

* ``preflight_send`` asked ``snapshot.stream_path.exists()`` on the restoring host.
  ``core/restore.py`` calls it immediately before ``send``, so restoring from a
  raw+ssh target failed every time -- and where both hosts happened to use the same
  directory (``/backup`` on each, the arrangement the README suggests) it was worse
  than a failure, because the check passed against an unrelated local file.
* ``get_space_info`` ran ``os.statvfs`` here, so the pre-transfer space check and
  ``estimate`` reported the free space of the machine being backed UP.

Both defects share a shape the tests have to reproduce deliberately: a LOCAL file at
the remote's path makes the broken implementation look correct. So every case below
creates that local decoy and pins the remote's answer to the opposite, which is the
only arrangement that can tell the two implementations apart.
"""

from __future__ import annotations

import ast
import inspect
import subprocess
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _endpoint(path, **config):
    base = {"path": str(path), "hostname": "nas"}
    base.update(config)
    return SSHRawEndpoint(config=base)


def _snapshot(path):
    # checksum_value stays None so the integrity guard reports "unverifiable" and
    # returns: these cases are about WHICH HOST is asked, not about hashing.
    return RawSnapshot(name="root.20260101-120000", stream_path=Path(path))


class _Remote:
    """Stand-in for the far end. Records argv, answers by exit status."""

    def __init__(self, *, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.calls: list[list[str]] = []

    def __call__(self, cmd, *args, **kwargs):
        self.calls.append(list(cmd))
        result = subprocess.CompletedProcess(
            cmd, self.returncode, self.stdout, self.stderr
        )
        if kwargs.get("check") and self.returncode != 0:
            raise subprocess.CalledProcessError(
                self.returncode, cmd, self.stdout, self.stderr
            )
        return result

    @property
    def command_text(self) -> str:
        return " ".join(part for call in self.calls for part in call)


class TestPreflightSendAsksTheRemote:
    def test_a_stream_missing_on_the_remote_is_refused_even_though_it_exists_here(
        self, tmp_path
    ):
        """The exact restore-time arrangement: same path on both hosts, file here.

        The inherited implementation passes this -- against the local decoy -- and
        the restore then proceeds to stream a file the remote does not have.
        """
        stream = tmp_path / "root.20260101-120000.btrfs"
        stream.write_bytes(b"a local file that is NOT the backup")
        endpoint = _endpoint(tmp_path)
        remote = _Remote(returncode=1)  # remote `test -f` says no

        with patch.object(raw_mod.subprocess, "run", remote):
            with pytest.raises(FileNotFoundError) as excinfo:
                endpoint.preflight_send(_snapshot(stream))

        assert "nas" in str(excinfo.value), (
            "the error must name the host that is missing the stream, so an "
            "operator is not sent looking on the wrong machine"
        )
        assert "test -f" in remote.command_text, (
            "existence was not checked on the remote at all"
        )

    def test_a_stream_present_on_the_remote_passes_though_nothing_is_here(
        self, tmp_path
    ):
        """The inverse, and the one that made restore fail 100% of the time.

        Nothing is on this host -- correctly, the backup is on the remote -- and
        the local check rejected the restore before it began.
        """
        stream = tmp_path / "root.20260101-120000.btrfs"
        assert not stream.exists()
        endpoint = _endpoint(tmp_path)

        with patch.object(raw_mod.subprocess, "run", _Remote(returncode=0)):
            endpoint.preflight_send(_snapshot(stream))  # must not raise

    def test_send_performs_the_same_checks_it_delegates(self, tmp_path):
        """``send`` must not become the lenient path once the checks move out.

        ``core/restore.py`` preflights and then sends, but other callers reach
        ``send`` directly; a stream missing on the remote has to be refused
        either way.
        """
        stream = tmp_path / "root.20260101-120000.btrfs"
        stream.write_bytes(b"local decoy")
        endpoint = _endpoint(tmp_path)

        with patch.object(raw_mod.subprocess, "run", _Remote(returncode=1)):
            with pytest.raises(FileNotFoundError):
                endpoint.send(_snapshot(stream))


class TestTheIntegrityCheckIsNotPaidTwice:
    def test_a_restore_hashes_the_stream_once(self, tmp_path):
        """restore.py preflights and then calls send, which preflights again.

        Each verification is a full read of the stream -- across the network for
        raw+ssh -- so the second one is a duplicated transfer of the whole backup.
        """
        stream = tmp_path / "root.20260101-120000.btrfs"
        endpoint = _endpoint(tmp_path)
        snapshot = RawSnapshot(
            name="root.20260101-120000",
            stream_path=stream,
            checksum_value="d0",
            checksum_algorithm="sha256",
        )
        hashed: list[str] = []

        def _compute(snap):
            hashed.append(str(snap.stream_path))
            return "d0"

        with (
            patch.object(raw_mod.subprocess, "run", _Remote(returncode=0)),
            patch.object(
                SSHRawEndpoint, "compute_stream_checksum", staticmethod(_compute)
            ),
        ):
            endpoint.preflight_send(snapshot)  # as core/restore.py does
            endpoint.send(snapshot)  # which preflights again

        assert hashed == [str(stream)], (
            f"the stream was hashed {len(hashed)} times for one restore"
        )

    def test_a_corrupt_stream_is_still_refused(self, tmp_path):
        """Memoising the verdict must not memoise it before it is known good."""
        stream = tmp_path / "root.20260101-120000.btrfs"
        endpoint = _endpoint(tmp_path)
        snapshot = RawSnapshot(
            name="root.20260101-120000",
            stream_path=stream,
            checksum_value="sealed",
            checksum_algorithm="sha256",
        )

        with (
            patch.object(raw_mod.subprocess, "run", _Remote(returncode=0)),
            patch.object(
                SSHRawEndpoint,
                "compute_stream_checksum",
                staticmethod(lambda snap: "different"),
            ),
        ):
            for _ in range(2):
                with pytest.raises(Exception, match="CORRUPT"):
                    endpoint.preflight_send(snapshot)


class TestSpaceIsMeasuredOnTheRemote:
    # A row shaped like the far end's, not like this host's.
    DF = (
        "Filesystem 1024-blocks      Used Available Capacity Mounted on\n"
        "/dev/disk3s5   488000000 188000000 300000000      39% /backup\n"
    )

    def test_the_figures_come_from_the_remote_filesystem(self, tmp_path):
        endpoint = _endpoint(tmp_path)
        remote = _Remote(stdout=self.DF)

        with patch.object(raw_mod.subprocess, "run", remote):
            info = endpoint.get_space_info()

        assert info.total_bytes == 488000000 * 1024
        assert info.used_bytes == 188000000 * 1024
        assert info.available_bytes == 300000000 * 1024
        assert "df" in remote.command_text

    @pytest.mark.parametrize(
        ("label", "row"),
        [
            (
                "a mount point with spaces",
                "/dev/disk4s2  1000000 400000 600000  40% /Volumes/Backup Drive",
            ),
            (
                "a device with spaces",
                "//user@nas/Backup Drive 1000000 400000 600000 40% /Volumes/backup",
            ),
        ],
    )
    def test_a_row_whose_outer_fields_contain_spaces_is_parsed(
        self, tmp_path, label, row
    ):
        """Both ends of a df row are operator-named and both routinely hold spaces.

        A macOS raw target sits under /Volumes/<name>, and a NAS share mounts from
        //user@host/<share>; either shifts the columns, and counting fields from
        the left then reads Used where Available should be -- reporting a third of
        the free space that exists, or a capacity percentage as a byte count.
        """
        endpoint = _endpoint(tmp_path)
        table = (
            "Filesystem 1024-blocks Used Available Capacity Mounted on\n" + row + "\n"
        )

        with patch.object(raw_mod.subprocess, "run", _Remote(stdout=table)):
            info = endpoint.get_space_info()

        assert info.total_bytes == 1000000 * 1024, label
        assert info.used_bytes == 400000 * 1024, label
        assert info.available_bytes == 600000 * 1024, label

    def test_an_unreadable_remote_reports_nothing_rather_than_a_local_figure(
        self, tmp_path
    ):
        """Silence beats a confident answer measured on the wrong machine.

        The caller treats the raised error as "space unverified" and proceeds with
        a warning; a fabricated figure would either abort a sound transfer or
        green-light a doomed one.
        """
        endpoint = _endpoint(tmp_path)
        remote = _Remote(returncode=1, stderr="df: /backup: No such file or directory")

        with patch.object(raw_mod.subprocess, "run", remote):
            with pytest.raises(OSError) as excinfo:
                endpoint.get_space_info()

        assert "nas" in str(excinfo.value)

    def test_an_unparseable_table_is_not_read_as_an_empty_disk(self, tmp_path):
        """df exiting 0 with a row we cannot read must not become 0 bytes free."""
        endpoint = _endpoint(tmp_path)

        with patch.object(raw_mod.subprocess, "run", _Remote(stdout="something else")):
            with pytest.raises(OSError):
                endpoint.get_space_info()


class TestRemotenessIsSettledBeforeTheBaseInitialiserRuns:
    def test_is_remote_is_a_class_attribute(self):
        """Set in __init__ it arrived too late: the base initialiser had already
        normalised config["path"] down the LOCAL branch."""
        assert SSHRawEndpoint.__dict__.get("_is_remote") is True

    def test_a_relative_target_path_is_not_resolved_against_the_local_cwd(self):
        endpoint = SSHRawEndpoint(config={"path": "backups/nightly", "hostname": "nas"})
        assert str(endpoint.config["path"]) == "backups/nightly", (
            "a relative remote path was resolved against this host's working "
            "directory, so the streams would be written somewhere else entirely"
        )

    def test_the_target_path_stays_a_string(self):
        """As on SSHEndpoint. A local Path carries this host's semantics."""
        endpoint = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
        assert isinstance(endpoint.config["path"], str)


class TestNoInheritedMethodTouchesTheLocalFilesystem:
    """A guard against this whole class of gap returning.

    Reviewing an override list by eye is what let these two through; the
    relationship is checked mechanically instead, the way
    ``test_raw_stream_identity.py`` checks its own invariant by source
    inspection. Methods whose local I/O is CORRECT for a raw+ssh target are named
    below with the reason, so adding to the list is a deliberate act.
    """

    # Every entry is local-by-design, not an oversight.
    LOCAL_BY_DESIGN = {
        # Compression and encryption run on THIS host (secrets never go to an
        # untrusted remote), so the tools they need are looked up here.
        "_check_tools",
        "_preflight_restore_tools",
        # Reached only from base methods SSHRawEndpoint replaces.
        "receive",
        "_open_part_file",
        "_execute_restore_pipeline",
        "_fsync_dir",
        # `raw encrypt` refuses a raw+ssh target outright (cli/raw_cmd.py) so the
        # passphrase stays on this host; the operator mounts the target locally.
        "remediate_plaintext",
        "decrypt_matches_plaintext",
    }

    LOCAL_IO = {
        "exists",
        "is_file",
        "is_dir",
        "iterdir",
        "glob",
        "stat",
        "unlink",
        "mkdir",
        "rmdir",
        "read_bytes",
        "read_text",
        "write_bytes",
        "write_text",
        "statvfs",
        "listdir",
        "remove",
        "which",
        "rename",
        "replace",
    }

    def _inherited(self):
        """Every RawEndpoint method SSHRawEndpoint does not replace.

        staticmethods are unwrapped rather than skipped: ``_open_part_file`` is
        one, and being a staticmethod makes a local ``os.open`` no less local.
        """
        own = vars(SSHRawEndpoint)
        found = {}
        for name, value in vars(RawEndpoint).items():
            if name in own:
                continue
            func = getattr(value, "__func__", value)
            if inspect.isfunction(func):
                found[name] = func
        return found

    def test_the_exemption_list_has_no_stale_entries(self):
        """An exemption for a method that is no longer inherited hides the day it
        comes back."""
        inherited = self._inherited()
        stale = sorted(self.LOCAL_BY_DESIGN - set(inherited))
        assert not stale, f"exempted but not inherited: {stale}"

    def test_inherited_methods_do_not_reach_the_local_filesystem(self):
        offenders = []
        for name, func in sorted(self._inherited().items()):
            if name in self.LOCAL_BY_DESIGN:
                continue
            tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
            for node in ast.walk(tree):
                attr = None
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Attribute):
                        attr = node.func.attr
                    elif isinstance(node.func, ast.Name) and node.func.id == "open":
                        attr = "open"
                if attr in self.LOCAL_IO or attr == "open":
                    offenders.append(f"{name} calls {attr}()")
        assert not offenders, (
            "SSHRawEndpoint inherits a method that asks the LOCAL filesystem "
            "about a target on another machine: " + "; ".join(offenders)
        )
