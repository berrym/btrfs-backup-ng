"""An ssh host must be checked before it reaches a shell or ssh's option parser.

Two distinct hazards, both live on master before this:

* THE SHELL. `_do_shell_pipeline_transfer` joins its ssh arguments into ONE
  string and runs it with `shell=True`. It quoted the ControlPath and the remote
  command but not the host, so a host containing `;` ran a command on the machine
  doing the backup -- which is running `btrfs send`, typically as root.
* SSH'S OPTION PARSER. A host beginning with `-` is read as an option however it
  is quoted, so `-oProxyCommand=...` runs a command even on the argv paths where
  no shell is involved.

The host comes from a config file, the CLI, or `config import` on a btrbk
config -- none of which are necessarily written by the person running the backup.
"""

import pytest

from btrfs_backup_ng.__util__ import validated_ssh_host


@pytest.mark.parametrize(
    "host",
    [
        "nas",
        "nas.local",
        "backup-01.example.com",
        "192.168.0.70",
        "[2001:db8::1]",
        "h",
        "x9",
    ],
)
def test_real_hosts_are_accepted(host):
    assert validated_ssh_host(host) == host


def test_a_user_may_be_supplied():
    assert validated_ssh_host("nas", username="backup") == "backup@nas"


@pytest.mark.parametrize(
    "host",
    [
        "nas; touch /tmp/pwned",  # shell metacharacter -> shell=True pipeline
        "nas && id",
        "nas|id",
        "$(id)",
        "`id`",
        "nas\nid",
        "a b",
        "",
    ],
)
def test_shell_metacharacters_are_refused(host):
    with pytest.raises(ValueError, match="not a usable ssh host"):
        validated_ssh_host(host)


@pytest.mark.parametrize("host", ["-oProxyCommand=id", "-F/dev/null", "--"])
def test_a_leading_dash_is_refused(host):
    """ssh reads it as an option however it is quoted, so quoting is not enough
    and this must be rejected outright."""
    with pytest.raises(ValueError, match="not a usable ssh host"):
        validated_ssh_host(host)


def test_a_dangerous_username_is_refused_too():
    with pytest.raises(ValueError, match="not a usable ssh host"):
        validated_ssh_host("nas", username="root; id")


class TestItIsAppliedWhereEndpointsAreBuilt:
    def test_ssh_endpoint_validates_at_construction(self):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        with pytest.raises(ValueError, match="not a usable ssh host"):
            SSHEndpoint(hostname="nas; touch /tmp/pwned", config={"path": "/backup"})

    def test_raw_ssh_endpoint_validates_at_construction(self):
        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        with pytest.raises(ValueError, match="not a usable ssh host"):
            SSHRawEndpoint(config={"path": "/backup", "hostname": "nas && id"})

    def test_a_missing_hostname_keeps_its_own_message(self):
        """ "not a usable ssh host" is true but unhelpful when the real answer is
        that none was configured."""
        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        with pytest.raises(ValueError, match="hostname is required"):
            SSHRawEndpoint(config={"path": "/backup"})
