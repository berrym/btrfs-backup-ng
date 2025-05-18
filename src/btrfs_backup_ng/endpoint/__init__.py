# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/__init__.py."""

import urllib.parse
from pathlib import Path

from .local import LocalEndpoint
from .shell import ShellEndpoint
from .ssh import SSHEndpoint


def choose_endpoint(spec, common_config=None, source=False, excluded_types=()):
    """
    Chooses a suitable endpoint based on the specification given.

    Args:
        spec (str): The endpoint specification (e.g., "ssh://hostname/path").
        common_config (dict): A dictionary with common configuration settings for all endpoints.
        source (bool): If True, this is considered a source endpoint.
        excluded_types (tuple): A tuple of endpoint classes to exclude from consideration.

    Returns:
        Endpoint: An instance of the appropriate `Endpoint` subclass.

    Raises:
        ValueError: If no suitable endpoint can be determined for the given specification.
    """
    config = common_config or {}

    # Parse destination string
    if ShellEndpoint not in excluded_types and spec.startswith("shell://"):
        endpoint_class = ShellEndpoint
        config["cmd"] = spec[8:]
        config["source"] = True
    elif SSHEndpoint not in excluded_types and spec.startswith("ssh://"):
        endpoint_class = SSHEndpoint
        parsed = urllib.parse.urlparse(spec)
        if not parsed.hostname:
            raise ValueError("No hostname for SSH specified.")

        # Parse URL components and log them
        from btrfs_backup_ng.__logger__ import logger

        logger.debug("SSHEndpoint initialized with hostname: %s, Here be Dragons!", self.hostname)
        logger.debug("Parsed SSH URL: %s", spec)
        logger.debug("Username from URL: %s", parsed.username)
        logger.debug("Hostname from URL: %s", parsed.hostname)
        logger.debug("Port from URL: %s", parsed.port)
        logger.debug("Path from URL: %s", parsed.path)

        config["hostname"] = parsed.hostname
        config["port"] = parsed.port
        # Ensure the username from the URL is set correctly
        if parsed.username:
            config["username"] = parsed.username
        else:
            config["username"] = getpass.getuser()  # Default to the current user if not specified

        # Path handling - don't convert to Path object yet to avoid resolution
        path = parsed.path.strip() or "/"
        if parsed.query:
            path += "?" + parsed.query

        # Store raw path string without resolving
        if source:
            config["source"] = path
        else:
            config["path"] = path
    elif LocalEndpoint not in excluded_types:
        endpoint_class = LocalEndpoint
        if source:
            config["source"] = Path(spec)
        else:
            config["path"] = Path(spec)
    else:
        raise ValueError(
            f"No endpoint could be generated for this specification: {spec}"
        )

    # Add debug option and passwordless option when creating SSH endpoints
    if endpoint_class == SSHEndpoint:
        # Initialize with passwordless=False by default
        config.setdefault("passwordless", False)
        from btrfs_backup_ng.__logger__ import logger

        logger.debug("Final SSH config: %s", config)
        logger.debug("Final SSH username: %s", config.get("username"))
        logger.debug("Final SSH hostname: %s", config.get("hostname"))

    # Special handling for SSH endpoints
    if endpoint_class == SSHEndpoint:
        # Keep hostname as a parameter and also in config
        return endpoint_class(
            config=config,
            cmd=config.get("cmd", None),
            hostname=config.get("hostname", None),
        )
    else:
        return endpoint_class(
            config=config,
            cmd=config.get("cmd", None),
            hostname=config.get("hostname", None),
        )
