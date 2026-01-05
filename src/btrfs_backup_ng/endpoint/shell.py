"""btrfs-backup-ng: btrfs_backup_ng/endpoint/shell.py
Create destinations with shell command endpoints.
"""

from .common import Endpoint


class ShellEndpoint(Endpoint):
    """Create a shell command endpoint."""

    def __init__(self, cmd, config=None, **kwargs) -> None:
        """
        Initialize the ShellEndpoint with a shell command and configuration.

        Args:
            cmd (str): The shell command to execute.
            config (dict): Configuration dictionary containing endpoint settings.
            kwargs: Additional keyword arguments for backward compatibility.
        """
        super().__init__(config=config, **kwargs)
        if self.config.get("source"):
            msg = "Shell can't be used as source."
            raise ValueError(msg)
        self.config["cmd"] = cmd

    def __repr__(self) -> str:
        return f"(Shell) {self.config['cmd']}"

    def get_id(self) -> str:
        """Return a unique identifier for this shell endpoint."""
        return f"shell://{self.config['cmd']}"

    def _build_receive_command(self, destination):
        """Build the shell command for receiving data."""
        return ["sh", "-c", self.config["cmd"]]
