# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/__init__.py."""

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
        spec (str): The endpoint specification (e.g., "ssh://hostname/path", "user@host:/path").
        common_config (dict): A dictionary with common configuration settings for all endpoints.
        source (bool): If True, this is considered a source endpoint.
        excluded_types (tuple): A tuple of endpoint classes to exclude from consideration.

    Returns:
        Endpoint: An instance of the appropriate `Endpoint` subclass.

    Raises:
        ValueError: If no suitable endpoint can be determined for the given specification.
    """
    config = common_config or {}

    # Helper function to detect SSH patterns
    def _is_ssh_pattern(spec_str):
        """Detect if a string looks like an SSH destination (user@host:/path or host:/path)."""
        if spec_str.startswith("ssh://"):
            return True
        # Check for patterns like user@host:/path or host:/path
        if ":" in spec_str and not spec_str.startswith("/"):
            # Must contain a colon and not start with / (to avoid /path/to/file:backup confusion)
            host_part = spec_str.split(":", 1)[0]
            # Check if it looks like a hostname (contains @ or looks like a hostname)
            if "@" in host_part or ("." in host_part and not host_part.startswith("/")):
                return True
        return False

    # Parse destination string
    if ShellEndpoint not in excluded_types and spec.startswith("shell://"):
        endpoint_class = ShellEndpoint
        config["cmd"] = spec[8:]
        config["source"] = True
    elif SSHEndpoint not in excluded_types and (
        spec.startswith("ssh://") or _is_ssh_pattern(spec)
    ):
        endpoint_class = SSHEndpoint
        parsed = urllib.parse.urlparse(spec)
        if not parsed.hostname:
            raise ValueError("No hostname for SSH specified.")

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

        # Username handling:
        # 1. Keep username from common_config (from command line) if present
        # 2. Otherwise use username from URL if present
        # 3. Otherwise the default will be set in the SSHEndpoint class
        if "username" not in config:
            if parsed.username:
                config["username"] = parsed.username
                logger.debug("Using username from URL: %s", parsed.username)

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

        # Username will be fully resolved in the SSHEndpoint class
        # but we ensure it's documented in debug logs
        username_source = (
            "command_line"
            if "username" in common_config
            else "url"
            if parsed.username
            else "will use default"
        )
        logger.debug("Username source: %s", username_source)

        logger.debug("Final SSH config: %s", config)
        if "username" in config:
            logger.debug("Passing username to endpoint: %s", config.get("username"))
        else:
            logger.debug("No username set in config, endpoint will use default")
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
