"""ControlMaster socket path must fit the Unix sun_path limit (~108 bytes).

The old ``cm_{user}_{host}_{pid}_{tid}.sock`` embedded a 15-digit thread id and a
variable-length hostname; with the mkdtemp base + OpenSSH's ~17-char socket-creation
suffix it overflowed the limit ("unix_listener: path ... too long"), silently
breaking EVERY ssh operation. The name is now a fixed-length digest.
"""

from __future__ import annotations

import shutil

from btrfs_backup_ng.sshutil.master import SSHMasterManager

LONG_HOST = "a-really-long-fully-qualified-hostname.example.internal.corp.example.net"
LONG_USER = "a-very-long-service-account-name-for-backups"


class TestControlPathLength:
    @staticmethod
    def _mgr(tmp_path, hostname, username="u"):
        return SSHMasterManager(
            hostname=hostname,
            username=username,
            control_dir=str(tmp_path / "cm"),
        )

    def test_socket_name_is_short_fixed_length(self, tmp_path):
        m = self._mgr(tmp_path, "192.168.0.70", "mberry")
        name = m.control_path.name
        assert name.startswith("cm-") and name.endswith(".sock")
        # cm- (3) + 12 hex + .sock (5) = 20, always.
        assert len(name) == 20, name

    def test_name_does_not_grow_with_host_or_user(self, tmp_path):
        m = self._mgr(tmp_path, LONG_HOST, LONG_USER)
        assert len(m.control_path.name) == 20
        # The raw (bloating) identity must NOT appear in the path.
        assert LONG_HOST not in str(m.control_path)
        assert LONG_USER not in str(m.control_path)

    def test_distinct_hosts_get_distinct_sockets(self, tmp_path):
        a = self._mgr(tmp_path, "host-a")
        b = self._mgr(tmp_path, "host-b")
        assert a.control_path.name != b.control_path.name

    def test_full_path_fits_sun_path_limit_on_default_base(self):
        """With the real mkdtemp base (short: XDG_RUNTIME_DIR or /tmp) and a long
        FQDN, the path + OpenSSH's ~17-char creation suffix stays under 108."""
        m = SSHMasterManager(hostname=LONG_HOST, username=LONG_USER)
        try:
            assert len(str(m.control_path)) + 20 <= 108, m.control_path
        finally:
            shutil.rmtree(m.control_dir, ignore_errors=True)
