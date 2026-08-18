"""Restoring a snapper backup that lives on an ``ssh://`` btrfs target.

`snapper restore --list ssh://...` learned to enumerate remote backups, but the
restore itself still ran the LOCAL branch: it built
``Path("ssh://user@host:/backups") / ".snapshots" / "2" / "snapshot"``, which
collapses to the nonexistent local path ``ssh:/user@host:/backups/...``, missed
the ``.exists()`` check, and reported

    Backup snapshot not found: ssh:/user@host:/backups/.snapshots/2/snapshot

So every remote backup listed perfectly and restored not at all, and the message
blamed a path nobody had typed. These tests pin the remote layout end to end:
the source is probed on the far side, the stream comes from a remote
``btrfs send``, and the fresh slot's info.xml is snapper's own, renumbered.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.restore import (
    RestoreError,
    _RemoteSubvolume,
    _resolve_remote_snapper_backup,
    restore_snapper_snapshot,
)

REMOTE = "ssh://backup@nas:/backups/home"
BASE = "/backups/home"

# A real snapper info.xml, including the multi-block userdata form snapper
# actually writes (one <userdata> element PER entry, not one holding many).
INFO_XML = """<?xml version="1.0"?>
<snapshot>
  <type>pre</type>
  <num>2</num>
  <date>2026-08-18 00:57:01</date>
  <uid>0</uid>
  <description>before upgrade</description>
  <cleanup>number</cleanup>
  <userdata>
    <key>reason</key>
    <value>manual</value>
  </userdata>
  <userdata>
    <key>requestor</key>
    <value>mberry</value>
  </userdata>
</snapshot>
"""


class FakeRemote:
    """An endpoint that answers remote probes from a table, and records them."""

    def __init__(self, present=(f"{BASE}/.snapshots/2/snapshot",), info_xml=INFO_XML):
        self.config = {"path": BASE}
        self.present = set(present)
        self.info_xml = info_xml
        self.calls: list[tuple[list[str], dict]] = []
        self.send = MagicMock(side_effect=self._send)
        self.sent: list = []

    def _send(self, snapshot, parent=None, clones=None):
        self.sent.append((snapshot, parent))
        proc = MagicMock()
        proc.stdout = MagicMock()
        proc.returncode = 0
        return proc

    def _exec_remote_command(self, command, **kwargs):
        self.calls.append((list(command), kwargs))
        if command[0] == "test":
            rc = 0 if command[-1] in self.present else 1
            return MagicMock(returncode=rc, stdout=b"", stderr=b"")
        if command[0] == "cat":
            if self.info_xml is None:
                return MagicMock(
                    returncode=1, stdout=b"", stderr=b"cat: No such file or directory"
                )
            return MagicMock(returncode=0, stdout=self.info_xml.encode(), stderr=b"")
        raise AssertionError(f"unexpected remote command: {command}")

    def issued(self):
        return [c for c, _ in self.calls]


class TestResolvingARemoteSnapperBackup:
    def test_it_probes_the_remote_slot_rather_than_a_local_path(self):
        ep = FakeRemote()
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=ep):
            endpoint, path, _ = _resolve_remote_snapper_backup(REMOTE, 2)
        assert endpoint is ep
        assert path == f"{BASE}/.snapshots/2/snapshot"
        assert ["test", "-d", f"{BASE}/.snapshots/2/snapshot"] in ep.issued()

    def test_the_resolved_path_never_carries_the_uri(self):
        """The old failure named `ssh:/backup@nas:/backups/...`. Never again."""
        ep = FakeRemote()
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=ep):
            _, path, _ = _resolve_remote_snapper_backup(REMOTE, 2)
        assert "ssh:" not in path
        assert path.startswith("/")

    def test_a_slot_that_cannot_be_read_raises(self):
        ep = FakeRemote(present=())
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=ep):
            with pytest.raises(RestoreError):
                _resolve_remote_snapper_backup(REMOTE, 2)

    def test_the_error_does_not_pretend_to_know_which_of_the_two_it_is(self):
        """`test -d` cannot separate "absent" from "cannot reach", so neither may
        the message: saying "not found" about a permission problem sends the
        operator to recreate a backup that is sitting right there."""
        ep = FakeRemote(present=())
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=ep):
            with pytest.raises(RestoreError) as excinfo:
                _resolve_remote_snapper_backup(REMOTE, 2)
        msg = str(excinfo.value)
        assert f"{BASE}/.snapshots/2/snapshot" in msg
        assert "absent" in msg and "cannot" in msg
        # And it must repeat the ssh:// elevation asymmetry, because --ssh-sudo is
        # the first thing an operator reaches for and it does not help here.
        assert "--ssh-sudo elevates only btrfs" in msg

    def test_the_info_xml_comes_back_from_the_remote(self):
        ep = FakeRemote()
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=ep):
            _, _, xml = _resolve_remote_snapper_backup(REMOTE, 2)
        assert xml is not None and "<description>before upgrade</description>" in xml
        assert ["cat", f"{BASE}/.snapshots/2/info.xml"] in ep.issued()

    def test_a_backup_without_info_xml_still_resolves(self):
        ep = FakeRemote(info_xml=None)
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=ep):
            _, path, xml = _resolve_remote_snapper_backup(REMOTE, 2)
        assert path == f"{BASE}/.snapshots/2/snapshot"
        assert xml is None

    def test_every_remote_call_requests_pipes(self):
        """SSHEndpoint._exec_remote_command captures nothing by default. A probe
        that forgets the pipes writes to the console and reads back empty."""
        ep = FakeRemote()
        with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=ep):
            _resolve_remote_snapper_backup(REMOTE, 2)
        assert ep.calls
        for command, kwargs in ep.calls:
            assert "stdout" in kwargs and "stderr" in kwargs, command


class TestTheRemoteSubvolumeAdapter:
    """The adapter must satisfy its REAL consumer, SSHEndpoint.send, not a mock."""

    def _endpoint(self, **config):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        base = {"path": BASE, "hostname": "nas", "username": "backup"}
        base.update(config)
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = base
        ep.hostname = base["hostname"]
        ep.ssh_manager = MagicMock()
        ep.ssh_manager.get_ssh_base_cmd.return_value = ["ssh", "backup@nas"]
        return ep

    def _sent_command(self, ep, snapshot, parent=None):
        with patch("btrfs_backup_ng.endpoint.ssh.subprocess.Popen") as popen:
            ep.send(snapshot, parent=parent)
        argv = popen.call_args[0][0]
        return argv[-1]

    def test_it_yields_a_remote_btrfs_send_of_that_exact_path(self):
        ep = self._endpoint()
        remote = self._sent_command(
            ep, _RemoteSubvolume(f"{BASE}/.snapshots/2/snapshot")
        )
        assert remote == f"btrfs send {BASE}/.snapshots/2/snapshot"

    def test_a_parent_becomes_dash_p(self):
        ep = self._endpoint()
        remote = self._sent_command(
            ep,
            _RemoteSubvolume(f"{BASE}/.snapshots/2/snapshot"),
            parent=_RemoteSubvolume(f"{BASE}/.snapshots/1/snapshot"),
        )
        assert remote == (
            f"btrfs send -p {BASE}/.snapshots/1/snapshot {BASE}/.snapshots/2/snapshot"
        )

    def test_ssh_sudo_does_elevate_the_remote_send(self):
        """The one remote command --ssh-sudo DOES elevate on an ssh:// target is
        btrfs -- which is exactly this one. The find/cat/test probes elsewhere in
        the restore are passed through unchanged; that asymmetry is the point."""
        ep = self._endpoint(ssh_sudo=True, passwordless=True)
        remote = self._sent_command(
            ep, _RemoteSubvolume(f"{BASE}/.snapshots/2/snapshot")
        )
        assert remote == f"sudo -n btrfs send {BASE}/.snapshots/2/snapshot"

    def test_a_path_with_spaces_cannot_be_split_by_the_remote_shell(self):
        ep = self._endpoint()
        remote = self._sent_command(ep, _RemoteSubvolume("/backups/my home/snapshot"))
        assert remote == "btrfs send '/backups/my home/snapshot'"


class TestRestoringFromAnSshTarget:
    """The whole function, ssh:// source, local snapper destination."""

    def _config(self, tmp_path):
        from btrfs_backup_ng.snapper import SnapperConfig

        return SnapperConfig(name="home", subvolume=tmp_path / "local")

    def _run(self, tmp_path, ep, *, number=2, parent=None, dry_run=False):
        config = self._config(tmp_path)
        recv = MagicMock()
        recv.communicate.return_value = (b"", b"")
        recv.returncode = 0
        with (
            patch("btrfs_backup_ng.snapper.SnapperScanner") as scanner,
            patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=ep),
            patch(
                "btrfs_backup_ng.core.restore.subprocess.Popen", return_value=recv
            ) as popen,
            patch(
                "btrfs_backup_ng.core.restore.progress_utils.is_interactive",
                return_value=False,
            ),
            patch("os.geteuid", return_value=0),
        ):
            scanner.return_value.get_config.return_value = config
            scanner.return_value.get_next_snapshot_number.return_value = 42
            result = restore_snapper_snapshot(
                backup_path=REMOTE,
                backup_number=number,
                snapper_config_name="home",
                parent_backup_number=parent,
                options={"show_progress": False},
                dry_run=dry_run,
            )
        return config, result, popen

    def test_an_unreadable_source_is_reported_by_its_real_remote_path(self, tmp_path):
        """The regression this commit exists for: the message used to name
        `ssh:/backup@nas:/backups/home/...`, a path that never existed."""
        ep = FakeRemote(present=())
        with pytest.raises(RestoreError) as excinfo:
            self._run(tmp_path, ep)
        msg = str(excinfo.value)
        assert f"{BASE}/.snapshots/2/snapshot" in msg
        assert "ssh:/backup@nas" not in msg

    def test_it_materializes_the_snapshot_from_a_remote_send(self, tmp_path):
        config, (next_num, path), popen = self._run(tmp_path, FakeRemote())
        assert next_num == 42
        assert path == config.snapshots_dir / "42" / "snapshot"

    def test_the_stream_comes_from_the_endpoint_not_a_local_btrfs_send(self, tmp_path):
        ep = FakeRemote()
        _, _, popen = self._run(tmp_path, ep)
        # The endpoint was asked to send the resolved remote subvolume ...
        assert len(ep.sent) == 1
        snapshot, parent = ep.sent[0]
        assert isinstance(snapshot, _RemoteSubvolume)
        assert snapshot.get_path() == f"{BASE}/.snapshots/2/snapshot"
        assert parent is None
        # ... and the only local process spawned was the receive. A local
        # `btrfs send` here would read a path that does not exist on this host.
        assert popen.call_count == 1
        assert popen.call_args[0][0][:2] == ["btrfs", "receive"]

    def test_info_xml_is_snappers_own_renumbered_verbatim(self, tmp_path):
        config, _, _ = self._run(tmp_path, FakeRemote())
        xml = (config.snapshots_dir / "42" / "info.xml").read_text()
        assert "<num>42</num>" in xml
        assert "<num>2</num>" not in xml
        # Everything else survives, including both userdata entries.
        assert "<type>pre</type>" in xml
        assert "<description>before upgrade</description>" in xml
        assert "<key>reason</key>" in xml and "<value>manual</value>" in xml
        assert "<key>requestor</key>" in xml and "<value>mberry</value>" in xml
        # <uid> is the element that separates RENUMBERING from regenerating:
        # SnapperMetadata does not model it, so a parse-and-regenerate round trip
        # silently drops it (measured). Asserting it here is what stops this test
        # from passing against the lower-fidelity implementation.
        assert "<uid>0</uid>" in xml

    def test_a_backup_without_info_xml_still_restores(self, tmp_path):
        config, (next_num, _), _ = self._run(tmp_path, FakeRemote(info_xml=None))
        xml = (config.snapshots_dir / "42" / "info.xml").read_text()
        assert "<num>42</num>" in xml
        assert next_num == 42

    def test_an_incremental_restore_probes_and_uses_the_remote_parent(self, tmp_path):
        ep = FakeRemote(
            present=(
                f"{BASE}/.snapshots/2/snapshot",
                f"{BASE}/.snapshots/1/snapshot",
            )
        )
        self._run(tmp_path, ep, parent=1)
        assert ["test", "-d", f"{BASE}/.snapshots/1/snapshot"] in ep.issued()
        _, parent = ep.sent[0]
        assert isinstance(parent, _RemoteSubvolume)
        assert parent.get_path() == f"{BASE}/.snapshots/1/snapshot"

    def test_an_absent_remote_parent_degrades_to_a_full_restore(self, tmp_path):
        """Same rule the local branch has always had: a missing parent must
        weaken the restore, never fail it."""
        ep = FakeRemote(present=(f"{BASE}/.snapshots/2/snapshot",))
        _, (next_num, _), _ = self._run(tmp_path, ep, parent=1)
        assert next_num == 42
        _, parent = ep.sent[0]
        assert parent is None

    def test_a_dry_run_resolves_the_source_but_transfers_nothing(self, tmp_path):
        ep = FakeRemote()
        _, (next_num, path), popen = self._run(tmp_path, ep, dry_run=True)
        assert next_num == 42
        assert str(path) == "/dev/null"
        assert ["test", "-d", f"{BASE}/.snapshots/2/snapshot"] in ep.issued()
        ep.send.assert_not_called()
        popen.assert_not_called()
