"""The 34904c6 rule, extended to remote: a configured destination is never created.

Reproduced before the fix, with emitted commands captured: a missing remote
path made SSHEndpoint.send_receive run `mkdir -p` on the remote ("Destination
path doesn't exist, creating it"), and SSHRawEndpoint._prepare both created
the target outright AND had a login-user probe that itself began with
`mkdir -p` -- so even probing invented the directory. An unmounted remote NFS
share or secondary mount therefore took the backup onto the remote ROOT
filesystem, exactly the local failure the rule closed.

Both now refuse, naming the remedy (the exact ssh mkdir one-liner), and emit
no mkdir anywhere.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


def _ssh_endpoint():
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"path": "/remote/unmounted/btrfs", "username": "u"}
    ep.hostname = "nas"
    ep._last_transfer_error = None
    return ep


class TestSshDestinationIsNeverCreated:
    def test_a_missing_destination_refuses_and_names_the_remedy(self):
        """Mutation guards: an unconditional-True check proceeds to transfer;
        a restored mkdir emits the command this test forbids."""
        ep = _ssh_endpoint()
        emitted = []

        def fake_remote(cmd, *a, **k):
            emitted.append(list(cmd))
            r = MagicMock()
            r.returncode = 1
            r.stdout = b""
            r.stderr = b""
            return r

        ep._exec_remote_command = fake_remote

        assert ep._require_remote_destination("/remote/unmounted/btrfs") is False
        assert "does not exist" in ep._last_transfer_error
        assert "ssh u@nas 'mkdir -p /remote/unmounted/btrfs'" in (
            ep._last_transfer_error
        ), "a refusal that does not name the remedy relocates the confusion"
        assert "Nothing was created" in ep._last_transfer_error
        assert not any("mkdir" in str(part) for cmd in emitted for part in cmd), (
            f"a refusal emitted mkdir: {emitted}"
        )

    def test_an_existing_destination_passes_without_a_mkdir(self):
        ep = _ssh_endpoint()
        emitted = []

        def fake_remote(cmd, *a, **k):
            emitted.append(list(cmd))
            r = MagicMock()
            r.returncode = 0
            r.stdout = b""
            r.stderr = b""
            return r

        ep._exec_remote_command = fake_remote

        assert ep._require_remote_destination("/remote/present") is True
        assert ep._last_transfer_error is None
        assert not any("mkdir" in str(part) for cmd in emitted for part in cmd)

    def test_a_transport_failure_keeps_its_identity(self):
        """`test -d` answers with 1; ssh's own 255 is not a verdict about the
        directory. The refusal must carry the transport failure, never claim
        the destination does not exist. Mutation guard: collapsing the two
        return codes relabels a dead connection as a missing directory."""
        ep = _ssh_endpoint()

        def fake_remote(cmd, *a, **k):
            r = MagicMock()
            r.returncode = 255
            r.stdout = b""
            r.stderr = b"Connection refused"
            return r

        ep._exec_remote_command = fake_remote
        assert ep._require_remote_destination("/remote/x") is False
        assert "the probe exited 255" in ep._last_transfer_error
        assert "does not exist" not in ep._last_transfer_error

    def test_a_probe_failure_refuses_rather_than_raising(self):
        ep = _ssh_endpoint()

        def broken(cmd, *a, **k):
            raise OSError("connection dropped")

        ep._exec_remote_command = broken
        assert ep._require_remote_destination("/remote/x") is False
        assert "connection dropped" in ep._last_transfer_error

    def test_send_receive_stops_at_the_refusal(self):
        """The wiring: a missing destination returns False from send_receive
        BEFORE diagnostics or any transfer machinery runs."""
        ep = _ssh_endpoint()

        def fake_remote(cmd, *a, **k):
            r = MagicMock()
            r.returncode = 1
            r.stdout = b""
            r.stderr = b""
            return r

        ep._exec_remote_command = fake_remote
        ep._run_diagnostics = lambda *a, **k: pytest.fail(
            "send_receive proceeded past a missing destination"
        )
        snap = MagicMock()
        snap.get_path.return_value = "/src/home.x"
        snap.get_name.return_value = "home.x"

        assert ep.send_receive(snap) is False
        assert "does not exist" in ep._last_transfer_error


def _raw_run_factory(recorded, missing=True, stderr=b""):
    def fake_run(cmd, *a, **k):
        recorded.append(list(cmd))
        joined = " ".join(str(c) for c in cmd)
        r = MagicMock()
        r.stdout = b"RAWSSHOK\n"
        r.stderr = stderr
        if "test -d" in joined and missing:
            r.returncode = 1
            if k.get("check"):
                raise subprocess.CalledProcessError(1, cmd, output=b"", stderr=stderr)
        else:
            r.returncode = 0
        return r

    return fake_run


class TestRawSshTargetIsNeverCreated:
    def _endpoint(self, **extra):
        cfg = {"path": "/remote/unmounted/raw", "hostname": "nas", "username": "u"}
        cfg.update(extra)
        return SSHRawEndpoint(config=cfg)

    def test_a_missing_target_refuses_with_the_remedy_and_no_mkdir(self):
        ep = self._endpoint()
        recorded = []
        with patch(
            "btrfs_backup_ng.endpoint.raw.subprocess.run",
            _raw_run_factory(recorded, missing=True),
        ):
            with pytest.raises(__util__.AbortError) as ei:
                ep._prepare()
        msg = str(ei.value)
        assert "does not exist" in msg
        assert "ssh nas 'mkdir -p /remote/unmounted/raw'" in msg
        assert "Nothing was created" in msg
        assert not any("mkdir" in str(part) for cmd in recorded for part in cmd), (
            f"_prepare emitted mkdir: {recorded}"
        )

    def test_an_existing_target_prepares_without_a_mkdir(self):
        ep = self._endpoint()
        recorded = []
        with patch(
            "btrfs_backup_ng.endpoint.raw.subprocess.run",
            _raw_run_factory(recorded, missing=False),
        ):
            ep._prepare()
        assert not any("mkdir" in str(part) for cmd in recorded for part in cmd)

    def test_the_login_user_probe_no_longer_creates(self):
        """Under ssh_sudo the writability probe used to BEGIN with mkdir -p,
        so even asking the question invented the directory. It must probe
        existence first and create nothing."""
        ep = self._endpoint(ssh_sudo=True)
        recorded = []
        with patch(
            "btrfs_backup_ng.endpoint.raw.subprocess.run",
            _raw_run_factory(recorded, missing=False),
        ):
            ep._prepare()
        probes = [
            " ".join(str(part) for part in cmd)
            for cmd in recorded
            if ".bbng-probe." in " ".join(str(part) for part in cmd)
        ]
        assert probes, "the writability probe did not run"
        assert "test -d" in probes[0]
        assert "mkdir" not in probes[0]
        assert ep._file_ops_direct is True

    def test_a_sudo_denial_still_gets_the_sudoers_diagnosis(self):
        """The existing denial message (FILE tools, not btrfs) must survive
        the restructure: it now hangs off the elevated existence check."""
        ep = self._endpoint(ssh_sudo=True)
        ep._file_ops_direct = False  # skip the login-user probe
        recorded = []
        denial = b"sudo: a password is required"
        with patch(
            "btrfs_backup_ng.endpoint.raw.subprocess.run",
            _raw_run_factory(recorded, missing=True, stderr=denial),
        ):
            with pytest.raises(__util__.AbortError) as ei:
                ep._prepare()
        assert "passwordless sudo for" in str(ei.value)
        assert "FILE tools" in str(ei.value)


class TestTheTransferEngineNeverCreates:
    """The SIXTH site, the one every endpoint-layer audit missed: it lives in
    core/operations.py, its name reads like a check, and its body ran remote
    `mkdir -p` (locally, Path.mkdir) at send time -- rebuilding whatever the
    endpoint's prepare() had refused, behind a catch-all that logged "will
    try transfer anyway". Found by a tier3 cell: 5426 unit tests, tier2, and
    every endpoint mutation passed while a real transfer to a real host
    created the directory and exited 0."""

    def test_a_missing_local_destination_refuses_and_creates_nothing(self, tmp_path):
        from btrfs_backup_ng.core.operations import _ensure_destination_exists

        class Local:
            _is_remote = False
            config = {"path": str(tmp_path / "unmounted" / "backups")}

        with pytest.raises(__util__.SnapshotTransferError, match="does not exist"):
            _ensure_destination_exists(Local())
        assert not (tmp_path / "unmounted").exists(), "the engine created it"

    def test_a_file_at_the_local_destination_is_diagnosed(self, tmp_path):
        from btrfs_backup_ng.core.operations import _ensure_destination_exists

        stray = tmp_path / "backups"
        stray.write_text("not a directory")

        class Local:
            _is_remote = False
            config = {"path": str(stray)}

        with pytest.raises(__util__.SnapshotTransferError, match="not a directory"):
            _ensure_destination_exists(Local())

    def test_an_existing_local_destination_passes(self, tmp_path):
        from btrfs_backup_ng.core.operations import _ensure_destination_exists

        class Local:
            _is_remote = False
            config = {"path": str(tmp_path)}

        _ensure_destination_exists(Local())

    def test_a_remote_refusal_stops_the_send_with_the_endpoint_message(self):
        from btrfs_backup_ng.core.operations import _ensure_destination_exists

        class Remote:
            _is_remote = True
            config = {"path": "/remote/unmounted"}
            _last_transfer_error = None

            def _require_remote_destination(self, path):
                self._last_transfer_error = (
                    "Destination /remote/unmounted does not exist on u@nas"
                )
                return False

        with pytest.raises(
            __util__.SnapshotTransferError, match="does not exist on u@nas"
        ):
            _ensure_destination_exists(Remote())

    def test_a_verified_remote_destination_passes(self):
        from btrfs_backup_ng.core.operations import _ensure_destination_exists

        class Remote:
            _is_remote = True
            config = {"path": "/remote/present"}

            def _require_remote_destination(self, path):
                return True

        _ensure_destination_exists(Remote())

    def test_a_remote_endpoint_without_the_check_is_left_to_its_prepare(self):
        """raw+ssh carries no engine-level check by design: its _prepare
        already refused a missing target before the engine can run. The
        engine must not fall through to the LOCAL arm and judge a remote
        path against the local filesystem."""
        from btrfs_backup_ng.core.operations import _ensure_destination_exists

        class RawRemote:
            _is_remote = True
            config = {"path": "/remote/raw/that/does/not/exist/locally"}

        _ensure_destination_exists(RawRemote())
