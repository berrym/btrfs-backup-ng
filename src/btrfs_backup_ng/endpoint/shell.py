# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/shell.py
Create destinations with shell command endpoints.
"""

from .common import Endpoint


class ShellEndpoint(Endpoint):
    """Create a shell command endpoint."""

    def __init__(self, cmd, **kwargs) -> None:
        super().__init__(**kwargs)
        if self.source:
            msg = "Shell can't be used as source."
            raise ValueError(msg)
        self.cmd = cmd

    def __repr__(self) -> str:
        return f"(Shell) {self.cmd}"

    def get_id(self) -> str:
        return f"shell://{self.cmd}"

    def _build_receive_command(self, destination):
        return ["sh", "-c", self.cmd]
