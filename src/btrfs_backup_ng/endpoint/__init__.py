# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/__init__.py."""

import getpass
import urllib.parse
from pathlib import Path

from ..__logger__ import logger

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
            try:
                logger.debug("Parsed SSH URL: %s", spec)
                logger.debug("Username from URL: %s", parsed.username)
                logger.debug("Hostname from URL: %s", parsed.hostname)
                logger.debug("Port from URL: %s", parsed.port)
                logger.debug("Path from URL: %s", parsed.path)
                logger.debug("Is source endpoint: %s", source)
            except Exception as e:
                logger.error("Error logging SSH URL components: %s", e)

        config["hostname"] = parsed.hostname
        config["port"] = parsed.port
        # Ensure the username from the URL is set correctly
        if parsed.username:
            config["username"] = parsed.username
        else:
            config["username"] = (
                getpass.getuser()
            )  # Default to the current user if not specified

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
        try:
            if source:
                config["source"] = Path(spec)
            else:
                config["path"] = Path(spec)
        except NameError as e:
            logger.error("Path is not defined: %s", e)
            logger.error("Make sure 'from pathlib import Path' is present")
            raise ValueError(f"Path not defined: {str(e)}")
    else:
        raise ValueError(
            f"No endpoint could be generated for this specification: {spec}"
        )

    # Add debug option and passwordless option when creating SSH endpoints
    if endpoint_class == SSHEndpoint:
        # Initialize with passwordless=False by default
        config.setdefault("passwordless", False)

        logger.debug("Final SSH config: %s", config)
        logger.debug("Final SSH username: %s", config.get("username"))
        logger.debug("Final SSH hostname: %s", config.get("hostname"))
        logger.debug("SSH source value: %s", config.get("source"))
        logger.debug("SSH path value: %s", config.get("path"))
        logger.debug("SSH opts: %s", config.get("ssh_opts", []))
        logger.debug("SSH sudo: %s", config.get("ssh_sudo", False))

    # Special handling for SSH endpoints
    try:
        if endpoint_class == SSHEndpoint:
            # Keep hostname as a parameter and also in config
            logger.debug(
                "Creating SSH endpoint with hostname: %s", config.get("hostname", "")
            )
            logger.debug("SSH endpoint will use path: %s", config.get("path", ""))
            endpoint = endpoint_class(
                config=config,
                cmd=config.get("cmd", None),
                hostname=config.get("hostname", ""),
            )
            logger.debug("SSH endpoint created: %s", endpoint)
            return endpoint
        else:
            logger.debug("Creating non-SSH endpoint: %s", endpoint_class.__name__)
            endpoint = endpoint_class(
                config=config,
                cmd=config.get("cmd", None),
                hostname=config.get("hostname", ""),
            )
            logger.debug("Endpoint created: %s", endpoint)
            return endpoint
    except NameError as e:
        logger.error("Missing import in endpoint: %s", e)
        logger.error("Path might not be defined in one of the modules")
        raise ValueError(f"Import error creating endpoint: {str(e)}")
    except Exception as e:
        logger.error("Unexpected error creating endpoint: %s", e)
        raise ValueError(f"Error creating endpoint: {str(e)}")
