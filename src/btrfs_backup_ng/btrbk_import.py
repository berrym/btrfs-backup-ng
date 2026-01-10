"""btrbk configuration file importer.

Parses btrbk's custom configuration format and converts to TOML.
This is a key differentiator - no other tool provides this migration path.

btrbk config structure:
- Global options at the top
- volume sections (btrfs mount points)
- subvolume sections (nested under volume)
- target sections (can be at any level)

Options inherit down: global -> volume -> subvolume -> target

Timestamp format mapping (btrbk -> strftime):
- short: YYYYMMDD -> %Y%m%d
- long: YYYYMMDDThhmm -> %Y%m%dT%H%M (default in btrbk >= 0.32)
- long-iso: YYYYMMDDThhmmss±hhmm -> %Y%m%dT%H%M%S%z
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


# btrbk timestamp format to strftime mapping
BTRBK_TIMESTAMP_FORMATS = {
    "short": "%Y%m%d",
    "long": "%Y%m%dT%H%M",
    "long-iso": "%Y%m%dT%H%M%S%z",
}

# Default timestamp format (btrbk >= 0.32 uses 'long')
BTRBK_DEFAULT_TIMESTAMP_FORMAT = "long"


# Token types
class TokenType:
    KEYWORD = "KEYWORD"
    VALUE = "VALUE"
    COMMENT = "COMMENT"
    NEWLINE = "NEWLINE"
    EOF = "EOF"


@dataclass
class Token:
    type: str
    value: str
    line: int
    column: int


@dataclass
class BtrbkOption:
    """A btrbk configuration option."""

    name: str
    value: str
    line: int


@dataclass
class BtrbkTarget:
    """A btrbk target section."""

    path: str
    options: dict[str, str] = field(default_factory=dict)
    line: int = 0


@dataclass
class BtrbkSubvolume:
    """A btrbk subvolume section."""

    path: str
    options: dict[str, str] = field(default_factory=dict)
    targets: list[BtrbkTarget] = field(default_factory=list)
    line: int = 0


@dataclass
class BtrbkVolume:
    """A btrbk volume section."""

    path: str
    options: dict[str, str] = field(default_factory=dict)
    subvolumes: list[BtrbkSubvolume] = field(default_factory=list)
    targets: list[BtrbkTarget] = field(default_factory=list)
    line: int = 0


@dataclass
class BtrbkConfig:
    """Parsed btrbk configuration."""

    global_options: dict[str, str] = field(default_factory=dict)
    volumes: list[BtrbkVolume] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class BtrbkLexer:
    """Lexer for btrbk configuration files."""

    def __init__(self, content: str):
        self.content = content
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens: list[Token] = []

    def tokenize(self) -> list[Token]:
        """Tokenize the configuration content."""
        while self.pos < len(self.content):
            self._skip_whitespace()
            if self.pos >= len(self.content):
                break

            char = self.content[self.pos]

            if char == "#":
                self._read_comment()
            elif char == "\n":
                self.tokens.append(
                    Token(TokenType.NEWLINE, "\n", self.line, self.column)
                )
                self._advance()
                self.line += 1
                self.column = 1
            elif char.isalpha() or char == "_":
                self._read_keyword_or_value()
            elif char in "\"'":
                self._read_quoted_string()
            elif char in "/:@.-" or char.isalnum():
                self._read_value()
            else:
                self._advance()

        self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))
        return self.tokens

    def _advance(self) -> str:
        char = self.content[self.pos]
        self.pos += 1
        self.column += 1
        return char

    def _peek(self) -> str:
        if self.pos < len(self.content):
            return self.content[self.pos]
        return ""

    def _skip_whitespace(self) -> None:
        """Skip spaces and tabs (not newlines)."""
        while self.pos < len(self.content) and self.content[self.pos] in " \t":
            self._advance()

    def _read_comment(self) -> None:
        """Read a comment until end of line."""
        start_col = self.column
        comment = ""
        while self.pos < len(self.content) and self.content[self.pos] != "\n":
            comment += self._advance()
        self.tokens.append(Token(TokenType.COMMENT, comment, self.line, start_col))

    def _read_keyword_or_value(self) -> None:
        """Read a keyword or unquoted value."""
        start_col = self.column
        word = ""

        # First, read the initial word part
        while self.pos < len(self.content):
            char = self.content[self.pos]
            if char.isalnum() or char in "_-":
                word += self._advance()
            else:
                break

        # Keywords are specific btrbk directives
        keywords = {
            "volume",
            "subvolume",
            "target",
            "snapshot_dir",
            "snapshot_name",
            "snapshot_create",
            "snapshot_preserve",
            "snapshot_preserve_min",
            "target_preserve",
            "target_preserve_min",
            "incremental",
            "ssh_identity",
            "ssh_user",
            "ssh_port",
            "ssh_compression",
            "stream_compress",
            "stream_buffer",
            "rate_limit",
            "timestamp_format",
            "lockfile",
            "transaction_log",
            "backend",
            "backend_remote",
            "btrfs_commit_delete",
            "archive_preserve",
            "archive_preserve_min",
            "group",
            "raw_target_compress",
            "raw_target_encrypt",
        }

        if word in keywords:
            self.tokens.append(Token(TokenType.KEYWORD, word, self.line, start_col))
        else:
            # If followed by path characters, continue reading as a value
            # This handles cases like "ssh://..." or "user@host:..."
            while self.pos < len(self.content):
                char = self.content[self.pos]
                if char in " \t\n#":
                    break
                word += self._advance()
            self.tokens.append(Token(TokenType.VALUE, word, self.line, start_col))

    def _read_quoted_string(self) -> None:
        """Read a quoted string value."""
        start_col = self.column
        quote = self._advance()
        value = ""
        while self.pos < len(self.content) and self.content[self.pos] != quote:
            if self.content[self.pos] == "\\":
                self._advance()
                if self.pos < len(self.content):
                    value += self._advance()
            else:
                value += self._advance()
        if self.pos < len(self.content):
            self._advance()  # closing quote
        self.tokens.append(Token(TokenType.VALUE, value, self.line, start_col))

    def _read_value(self) -> None:
        """Read an unquoted value (path, URL, etc)."""
        start_col = self.column
        value = ""
        while self.pos < len(self.content):
            char = self.content[self.pos]
            if char in " \t\n#":
                break
            value += self._advance()
        self.tokens.append(Token(TokenType.VALUE, value, self.line, start_col))


class BtrbkParser:
    """Parser for btrbk configuration files."""

    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0
        self.config = BtrbkConfig()
        self.current_volume: BtrbkVolume | None = None
        self.current_subvolume: BtrbkSubvolume | None = None

    def parse(self) -> BtrbkConfig:
        """Parse tokens into configuration structure."""
        while not self._is_at_end():
            self._parse_line()
        return self.config

    def _is_at_end(self) -> bool:
        return (
            self.pos >= len(self.tokens) or self.tokens[self.pos].type == TokenType.EOF
        )

    def _current(self) -> Token:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return Token(TokenType.EOF, "", 0, 0)

    def _advance(self) -> Token:
        token = self._current()
        self.pos += 1
        return token

    def _skip_newlines(self) -> None:
        while not self._is_at_end() and self._current().type in (
            TokenType.NEWLINE,
            TokenType.COMMENT,
        ):
            self._advance()

    def _parse_line(self) -> None:
        """Parse a single line of configuration."""
        self._skip_newlines()
        if self._is_at_end():
            return

        token = self._current()

        if token.type == TokenType.KEYWORD:
            keyword = token.value
            self._advance()

            if keyword == "volume":
                self._parse_volume()
            elif keyword == "subvolume":
                self._parse_subvolume()
            elif keyword == "target":
                self._parse_target()
            else:
                self._parse_option(keyword)
        elif token.type == TokenType.VALUE:
            # Could be a continuation or error
            self._advance()
        else:
            self._advance()

    def _parse_volume(self) -> None:
        """Parse a volume section."""
        path_token = self._current()
        if path_token.type != TokenType.VALUE:
            self.config.warnings.append(
                f"Line {path_token.line}: Expected path after 'volume'"
            )
            return

        self._advance()
        self.current_volume = BtrbkVolume(path=path_token.value, line=path_token.line)
        self.current_subvolume = None
        self.config.volumes.append(self.current_volume)

    def _parse_subvolume(self) -> None:
        """Parse a subvolume section."""
        path_token = self._current()
        if path_token.type != TokenType.VALUE:
            self.config.warnings.append(
                f"Line {path_token.line}: Expected path after 'subvolume'"
            )
            return

        self._advance()

        if self.current_volume is None:
            self.config.warnings.append(
                f"Line {path_token.line}: 'subvolume' outside of 'volume' section"
            )
            return

        self.current_subvolume = BtrbkSubvolume(
            path=path_token.value, line=path_token.line
        )
        self.current_volume.subvolumes.append(self.current_subvolume)

    def _parse_target(self) -> None:
        """Parse a target section."""
        path_token = self._current()
        if path_token.type != TokenType.VALUE:
            self.config.warnings.append(
                f"Line {path_token.line}: Expected path after 'target'"
            )
            return

        self._advance()
        target = BtrbkTarget(path=path_token.value, line=path_token.line)

        # Add to current scope
        if self.current_subvolume is not None:
            self.current_subvolume.targets.append(target)
        elif self.current_volume is not None:
            self.current_volume.targets.append(target)
        else:
            self.config.warnings.append(
                f"Line {path_token.line}: 'target' outside of 'volume' or 'subvolume' section"
            )

    def _parse_option(self, keyword: str) -> None:
        """Parse an option key-value pair."""
        # Collect all values until end of line (some options like preserve have multiple values)
        values = []
        while not self._is_at_end():
            token = self._current()
            if token.type == TokenType.NEWLINE or token.type == TokenType.COMMENT:
                break
            if token.type == TokenType.VALUE:
                values.append(token.value)
                self._advance()
            elif token.type == TokenType.KEYWORD:
                # Could be a value that looks like a keyword (e.g., "yes", "no")
                values.append(token.value)
                self._advance()
            else:
                break

        value = " ".join(values)

        # Store in current scope
        if self.current_subvolume is not None:
            self.current_subvolume.options[keyword] = value
        elif self.current_volume is not None:
            self.current_volume.options[keyword] = value
        else:
            self.config.global_options[keyword] = value


def parse_btrbk_config(content: str) -> BtrbkConfig:
    """Parse btrbk configuration content.

    Args:
        content: Raw btrbk configuration file content

    Returns:
        Parsed BtrbkConfig object
    """
    lexer = BtrbkLexer(content)
    tokens = lexer.tokenize()
    parser = BtrbkParser(tokens)
    return parser.parse()


def parse_btrbk_retention(value: str) -> dict[str, int]:
    """Parse btrbk retention format into counts.

    btrbk format: "[<hourly>h] [<daily>d] [<weekly>w] [<monthly>m] [<yearly>y]"
    Example: "14d 4w 6m" means 14 daily, 4 weekly, 6 monthly

    Args:
        value: btrbk retention string

    Returns:
        Dict with hourly, daily, weekly, monthly, yearly counts
    """
    result = {
        "hourly": 0,
        "daily": 0,
        "weekly": 0,
        "monthly": 0,
        "yearly": 0,
    }

    # Handle special values
    if value == "all" or value == "*":
        # Keep all - use large number
        for key in result:
            result[key] = 999
        return result

    if value == "no" or value == "none":
        return result

    # Parse components
    pattern = re.compile(r"(\d+|\*)([hdwmy])")
    for match in pattern.finditer(value):
        count_str, unit = match.groups()
        count = 999 if count_str == "*" else int(count_str)

        if unit == "h":
            result["hourly"] = count
        elif unit == "d":
            result["daily"] = count
        elif unit == "w":
            result["weekly"] = count
        elif unit == "m":
            result["monthly"] = count
        elif unit == "y":
            result["yearly"] = count

    return result


def convert_to_toml(btrbk_config: BtrbkConfig) -> tuple[str, list[str]]:
    """Convert parsed btrbk config to TOML format.

    Args:
        btrbk_config: Parsed btrbk configuration

    Returns:
        Tuple of (TOML content, list of warnings/suggestions)
    """
    warnings = list(btrbk_config.warnings)
    lines = [
        "# btrfs-backup-ng configuration",
        "# Converted from btrbk config",
        "",
    ]

    # Global options
    lines.append("[global]")

    # Map btrbk options to btrfs-backup-ng
    if "snapshot_dir" in btrbk_config.global_options:
        lines.append(f'snapshot_dir = "{btrbk_config.global_options["snapshot_dir"]}"')
    else:
        lines.append('snapshot_dir = ".snapshots"')

    # Map btrbk timestamp format to strftime format
    btrbk_ts_format = btrbk_config.global_options.get(
        "timestamp_format", BTRBK_DEFAULT_TIMESTAMP_FORMAT
    )
    if btrbk_ts_format in BTRBK_TIMESTAMP_FORMATS:
        strftime_format = BTRBK_TIMESTAMP_FORMATS[btrbk_ts_format]
        lines.append(f'timestamp_format = "{strftime_format}"')
    else:
        # Unknown format, use btrbk's default (long)
        warnings.append(
            f"Unknown btrbk timestamp_format '{btrbk_ts_format}', "
            f"using 'long' format for compatibility"
        )
        lines.append(f'timestamp_format = "{BTRBK_TIMESTAMP_FORMATS["long"]}"')

    incremental = btrbk_config.global_options.get("incremental", "yes")
    lines.append(f"incremental = {str(incremental != 'no').lower()}")

    lines.append("")

    # Global retention
    lines.append("[global.retention]")
    if "snapshot_preserve_min" in btrbk_config.global_options:
        min_val = btrbk_config.global_options["snapshot_preserve_min"]
        # Convert btrbk duration (e.g., "2d") to our format
        lines.append(f'min = "{min_val}"')
    else:
        lines.append('min = "1d"')

    if "snapshot_preserve" in btrbk_config.global_options:
        retention = parse_btrbk_retention(
            btrbk_config.global_options["snapshot_preserve"]
        )
        lines.append(f"hourly = {retention['hourly']}")
        lines.append(f"daily = {retention['daily']}")
        lines.append(f"weekly = {retention['weekly']}")
        lines.append(f"monthly = {retention['monthly']}")
    else:
        lines.extend(
            [
                "hourly = 24",
                "daily = 7",
                "weekly = 4",
                "monthly = 12",
            ]
        )

    lines.append("")

    # Process volumes
    for volume in btrbk_config.volumes:
        # Check for common issues
        if volume.path == "/" or volume.path == ".":
            warnings.append(
                f"Line {volume.line}: volume path '{volume.path}' may cause issues. "
                "Consider using explicit mount point."
            )

        for subvolume in volume.subvolumes:
            # Build full path
            if subvolume.path.startswith("/"):
                full_path = subvolume.path
            else:
                full_path = f"{volume.path.rstrip('/')}/{subvolume.path}"

            # Check for 'subvolume .' anti-pattern
            if subvolume.path == ".":
                warnings.append(
                    f"Line {subvolume.line}: 'subvolume .' detected. "
                    "This often causes confusion. Consider using explicit path."
                )
                full_path = volume.path

            lines.append("[[volumes]]")
            lines.append(f'path = "{full_path}"')

            # Snapshot prefix from options or generate from path
            prefix = subvolume.options.get(
                "snapshot_name", volume.options.get("snapshot_name", "")
            )
            if not prefix:
                prefix = full_path.strip("/").replace("/", "-") or "root"
            lines.append(f'snapshot_prefix = "{prefix}"')

            # Snapshot directory
            snap_dir = subvolume.options.get(
                "snapshot_dir",
                volume.options.get(
                    "snapshot_dir",
                    btrbk_config.global_options.get("snapshot_dir", ".snapshots"),
                ),
            )
            lines.append(f'snapshot_dir = "{snap_dir}"')

            lines.append("")

            # Targets - from subvolume, volume, or both
            all_targets = subvolume.targets + volume.targets

            for target in all_targets:
                lines.append("[[volumes.targets]]")

                # Convert btrbk target path format
                target_path = target.path
                if ":" in target_path and not target_path.startswith("ssh://"):
                    # Convert host:path to ssh://host:/path
                    host, path = target_path.split(":", 1)
                    if "@" in host:
                        user, hostname = host.split("@", 1)
                        target_path = f"ssh://{user}@{hostname}:{path}"
                    else:
                        target_path = f"ssh://{host}:{path}"
                    warnings.append(
                        f"Line {target.line}: Converted '{target.path}' to '{target_path}'"
                    )

                lines.append(f'path = "{target_path}"')

                # SSH options
                if target_path.startswith("ssh://"):
                    # Check if sudo might be needed
                    lines.append("ssh_sudo = true  # May be required for btrfs receive")

                lines.append("")

    # Final warnings check
    if not btrbk_config.volumes:
        warnings.append("No volumes found in configuration")

    total_subvols = sum(len(v.subvolumes) for v in btrbk_config.volumes)
    if total_subvols == 0:
        warnings.append("No subvolumes found - check your configuration structure")

    return "\n".join(lines), warnings


def import_btrbk_config(path: str | Path) -> tuple[str, list[str]]:
    """Import a btrbk configuration file and convert to TOML.

    Args:
        path: Path to btrbk.conf file

    Returns:
        Tuple of (TOML content, list of warnings)
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"btrbk config not found: {path}")

    content = path.read_text()
    btrbk_config = parse_btrbk_config(content)
    return convert_to_toml(btrbk_config)
