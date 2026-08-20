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

from .core.transfer import COMPRESSION_PROGRAMS
from .endpoint.raw_metadata import COMPRESSION_CONFIG

# Compression methods the btrfs transfer path can actually run, taken from the
# authoritative table so this cannot drift from what the config loader accepts.
_STREAM_COMPRESS_SUPPORTED = frozenset(COMPRESSION_PROGRAMS)

#: What a raw target can actually run, which is a different set from the btrfs
#: transfer path: it adds xz, lzo, bzip2 and pbzip2, and has no lzop.
_RAW_COMPRESS_SUPPORTED = frozenset(COMPRESSION_CONFIG)

#: btrbk spells some methods differently from the program they run. `lzo` is the
#: lzop program, which this project lists under its program name, so an import
#: that compared the names directly dropped a perfectly usable setting.
_BTRBK_METHOD_ALIASES = {"lzo": "lzop"}

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
    #: btrbk's optional target type token: ``send-receive`` (the default) or
    #: ``raw``. Written as ``target <type> <url>``, which is the form btrbk's own
    #: documentation uses.
    target_type: str | None = None


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
            # Captured only so the converter can WARN they have no btrfs-backup-ng
            # equivalent (rather than silently dropping the retention rule).
            "preserve_day_of_week",
            "preserve_hour_of_day",
            "incremental",
            "ssh_identity",
            "ssh_user",
            "ssh_port",
            "ssh_compression",
            "stream_compress",
            # Recognised so the converter can say they are not carried over. An
            # unrecognised keyword is skipped silently, which is how a tuning the
            # user deliberately set disappeared without a word.
            "stream_compress_level",
            "stream_compress_threads",
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
            "gpg_keyring",
            "gpg_recipient",
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
        # btrbk scopes an option to the section it follows, and `target` opens a
        # section like `volume` and `subvolume` do. Without tracking it, every
        # option written under one target was stored on the enclosing subvolume
        # and therefore applied to that subvolume's OTHER targets too -- a
        # `stream_compress` meant for one destination silently turned itself on
        # for the rest.
        self.current_target: BtrbkTarget | None = None

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

    def _peek_next(self) -> Token:
        """The token after the current one, for deciding on optional tokens."""
        if self.pos + 1 < len(self.tokens):
            return self.tokens[self.pos + 1]
        return Token(TokenType.EOF, "", 0, 0)

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
        self.current_target = None
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

        self.current_target = None
        self.current_subvolume = BtrbkSubvolume(
            path=path_token.value, line=path_token.line
        )
        self.current_volume.subvolumes.append(self.current_subvolume)

    def _parse_target(self) -> None:
        """Parse a target section."""
        path_token = self._current()
        if path_token.type not in (TokenType.VALUE, TokenType.KEYWORD):
            self.config.warnings.append(
                f"Line {path_token.line}: Expected path after 'target'"
            )
            return

        # `target <type> <url>` is btrbk's documented form, and the type token
        # was being taken as the destination: `target send-receive ssh://nas/b`
        # produced a target called "send-receive" and dropped the real URL, so an
        # imported config silently had nowhere to back up to.
        target_type = None
        if path_token.value in ("send-receive", "raw"):
            following = self._peek_next()
            if following.type in (
                TokenType.VALUE,
                TokenType.KEYWORD,
            ):
                target_type = path_token.value
                self._advance()
                path_token = self._current()

        self._advance()
        target = BtrbkTarget(
            path=path_token.value, line=path_token.line, target_type=target_type
        )
        self.current_target = target

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

        # Store in the innermost open scope. Target first: it is the narrowest,
        # and the inheritance chain further down reads target -> subvolume ->
        # volume -> global in that order.
        if self.current_target is not None:
            self.current_target.options[keyword] = value
        elif self.current_subvolume is not None:
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


def _translate_preserve_min(value: str) -> tuple[str, list[str]]:
    """Translate a btrbk ``*_preserve_min`` value into a btrfs-backup-ng retention
    ``min`` duration.

    Two btrbk-vs-btrfs-backup-ng mismatches make a straight passthrough wrong:

    * **Unit clash on ``m``.** btrbk retention uses ``m`` for MONTHS, but
      btrfs-backup-ng's duration parser uses ``m`` for minutes and ``M`` for months.
      A btrbk ``3m`` (3 months) passed through verbatim would silently become 3
      *minutes*. We remap ``m`` -> ``M``.
    * **Special tokens.** btrbk ``no``/``all``/``latest`` are not durations; passed
      through they produce a ``min`` the loader rejects (fails to load). ``no`` maps
      cleanly to ``0s`` (no age floor); ``all``/``latest`` have no age equivalent, so
      we fall back to ``1d`` and warn.

    Returns ``(min_string, warnings)``.
    """
    warnings: list[str] = []
    low = value.strip().lower()
    if low in ("no", "none"):
        # No minimum age -- count-based rules apply fully. Faithful 1:1 mapping.
        return "0s", warnings
    if low in ("all", "latest"):
        warnings.append(
            f"btrbk '*_preserve_min {value.strip()}' has no btrfs-backup-ng equivalent "
            f'(there is no infinite/"keep-latest" minimum age); using min = "1d" -- '
            f"review the generated [.retention] min"
        )
        return "1d", warnings
    m = re.fullmatch(r"(\d+)\s*([hdwmy])", low)
    if m:
        count, unit = m.groups()
        # btrbk m=months -> btrfs-backup-ng M=months (h/d/w/y are identical).
        bbng_unit = "M" if unit == "m" else unit
        return f"{count}{bbng_unit}", warnings
    warnings.append(
        f"btrbk retention minimum {value.strip()!r} was not understood; "
        f'using min = "1d" -- review the generated [.retention] min'
    )
    return "1d", warnings


def _parse_preserve_counts(value: str) -> tuple[dict[str, int], list[str]]:
    """``parse_btrbk_retention`` plus a warning when the value is not understood.

    A value that matches no period token and is not an explicit ``no``/``all`` yields
    an all-zero policy -- i.e. *keep no periodic snapshots*. That silent
    prune-everything outcome (e.g. from btrbk ``latest``, or a typo) is dangerous, so
    surface it as a warning instead.
    """
    counts = parse_btrbk_retention(value)
    warnings: list[str] = []
    low = value.strip().lower()
    if low in ("no", "none", "all", "*"):
        return counts, warnings
    matched_a_token = bool(re.search(r"(\d+|\*)[hdwmy]", low))
    if not matched_a_token:
        # Nothing recognizable (e.g. btrbk 'latest', or a typo) -> silently all-zero.
        warnings.append(
            f"btrbk retention value {value.strip()!r} was not understood -- it maps to "
            f"keeping NO periodic snapshots. btrbk 'latest' and day/hour-of-week rules "
            f"have no btrfs-backup-ng equivalent; review the generated [.retention]"
        )
    elif all(c == 0 for c in counts.values()):
        # Parsed fine but every bucket is 0 (e.g. '0d') -> also keeps nothing.
        warnings.append(
            f"btrbk retention value {value.strip()!r} keeps NO periodic snapshots "
            f"(all buckets are 0) -- review the generated [.retention]"
        )
    return counts, warnings


def _retention_block(
    header: str,
    scope: str,
    preserve: str | None,
    preserve_min: str | None,
    target_preserve: str | None,
    target_preserve_min: str | None,
) -> tuple[list[str], list[str]]:
    """Build a ``[<header>.retention]`` TOML block from btrbk preserve directives.

    btrfs-backup-ng has a SINGLE retention policy per scope, so btrbk's separate
    SNAPSHOT (source) and TARGET (destination) schedules cannot both be represented.
    We map from ``snapshot_preserve*`` and warn when ``target_preserve*`` differs, so
    the operator knows the destination schedule was not applied separately.

    ``scope`` is a human-readable label (e.g. ``"the global retention"`` or
    ``'volume "/mnt/pool/home"'``) embedded in the divergence warnings so that
    per-volume warnings stay DISTINCT through de-duplication (two subvolumes with
    the same ``target_preserve`` value must each be reported).

    Returns ``(toml_lines, warnings)``.
    """
    lines = [f"[{header}.retention]"]
    warnings: list[str] = []

    if preserve_min is not None:
        min_str, w = _translate_preserve_min(preserve_min)
        warnings += w
    else:
        min_str = "1d"
    lines.append(f'min = "{min_str}"')

    if preserve is not None:
        counts, w = _parse_preserve_counts(preserve)
        warnings += w
    else:
        # btrfs-backup-ng defaults when btrbk specified no snapshot_preserve.
        counts = {"hourly": 24, "daily": 7, "weekly": 4, "monthly": 12, "yearly": 0}
    for key in ("hourly", "daily", "weekly", "monthly", "yearly"):
        lines.append(f"{key} = {counts[key]}")

    # Divergence warnings compare the PARSED policy (so reordered-but-equal token
    # lists don't warn) and state accurately what was actually used -- the source
    # schedule, or the defaults when no snapshot_preserve was present.
    if target_preserve is not None and (
        preserve is None or parse_btrbk_retention(target_preserve) != counts
    ):
        used = (
            "snapshot_preserve was used"
            if preserve is not None
            else "the default retention was used"
        )
        warnings.append(
            f"btrbk 'target_preserve {target_preserve}' sets a destination schedule "
            f"that differs from the source; btrfs-backup-ng uses one retention per "
            f"volume, so it was NOT applied separately -- {used} for {scope}"
        )
    if target_preserve_min is not None:
        target_min, _ = _translate_preserve_min(target_preserve_min)
        if target_min != min_str:
            used = (
                "snapshot_preserve_min was used"
                if preserve_min is not None
                else "the default minimum was used"
            )
            warnings.append(
                f"btrbk 'target_preserve_min {target_preserve_min}' differs from the "
                f"source minimum; btrfs-backup-ng uses one retention minimum -- "
                f"{used} for {scope}"
            )

    return lines, warnings


def _is_disabled(value: object) -> bool:
    """Is this btrbk value one of its spellings for "off" / "use the default"?

    btrbk writes `no` to disable an option. Treated as a plain string it is
    truthy and non-empty, so it flowed through as a literal setting.
    """
    return str(value).strip().lower() in ("no", "off", "false", "0") if value else False


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

    # Global retention. Routed through _retention_block so the min unit/token
    # translation, the yearly field, and the target_preserve divergence warning are
    # applied consistently here and for per-volume overrides below.
    g = btrbk_config.global_options
    global_ret_lines, global_ret_warnings = _retention_block(
        "global",
        "the global retention",
        g.get("snapshot_preserve"),
        g.get("snapshot_preserve_min"),
        g.get("target_preserve"),
        g.get("target_preserve_min"),
    )
    lines.extend(global_ret_lines)
    warnings.extend(global_ret_warnings)

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

            # Per-volume retention override. Emit a [volumes.retention] block only
            # when the subvolume or its parent volume explicitly set a preserve
            # directive (otherwise [global.retention] already applies). Inherit the
            # global preserve for whichever half was not overridden, so a subvolume
            # that overrides only the min still keeps the global counts.
            sub_preserve = subvolume.options.get(
                "snapshot_preserve"
            ) or volume.options.get("snapshot_preserve")
            sub_preserve_min = subvolume.options.get(
                "snapshot_preserve_min"
            ) or volume.options.get("snapshot_preserve_min")
            if sub_preserve is not None or sub_preserve_min is not None:
                sub_ret_lines, sub_ret_warnings = _retention_block(
                    "volumes",
                    f'volume "{full_path}"',
                    sub_preserve
                    or btrbk_config.global_options.get("snapshot_preserve"),
                    sub_preserve_min
                    or btrbk_config.global_options.get("snapshot_preserve_min"),
                    subvolume.options.get("target_preserve")
                    or volume.options.get("target_preserve"),
                    subvolume.options.get("target_preserve_min")
                    or volume.options.get("target_preserve_min"),
                )
                lines.extend(sub_ret_lines)
                warnings.extend(sub_ret_warnings)

            lines.append("")

            # Targets - from subvolume, volume, or both
            all_targets = subvolume.targets + volume.targets

            for target in all_targets:
                lines.append("[[volumes.targets]]")

                # Check for raw target options (inherited from subvolume -> volume -> global)
                raw_compress = (
                    target.options.get("raw_target_compress")
                    or subvolume.options.get("raw_target_compress")
                    or volume.options.get("raw_target_compress")
                    or btrbk_config.global_options.get("raw_target_compress")
                )
                # btrbk's stream_compress compresses the send stream over the wire --
                # the same thing btrfs-backup-ng's `compress` now does for an ssh://
                # target. Dropping it on import silently removed the bandwidth saving
                # from whoever was migrating over a slow link -- exactly the person who
                # had configured it in the first place.
                stream_compress = (
                    target.options.get("stream_compress")
                    or subvolume.options.get("stream_compress")
                    or volume.options.get("stream_compress")
                    or btrbk_config.global_options.get("stream_compress")
                )
                # ssh_compression is ssh's own -C: a different mechanism entirely.
                ssh_compression = (
                    target.options.get("ssh_compression")
                    or subvolume.options.get("ssh_compression")
                    or volume.options.get("ssh_compression")
                    or btrbk_config.global_options.get("ssh_compression")
                )
                raw_encrypt = (
                    target.options.get("raw_target_encrypt")
                    or subvolume.options.get("raw_target_encrypt")
                    or volume.options.get("raw_target_encrypt")
                    or btrbk_config.global_options.get("raw_target_encrypt")
                )

                # btrbk resolves an option at the narrowest scope that sets it.
                # These four were recognised by the parser -- so they never looked
                # unknown -- stored, and then never read back, which dropped them
                # silently at EVERY scope. The migration guide promises three of
                # them by name, so a user read the table, believed their key and
                # username had come across, and got authentication failures
                # against a host btrbk had been backing up to correctly.
                def inherited(name: str) -> str | None:
                    value = (
                        target.options.get(name)
                        or subvolume.options.get(name)
                        or volume.options.get(name)
                        or btrbk_config.global_options.get(name)
                    )
                    # btrbk spells "off" / "use the default" as `no`, so the
                    # string is a DISABLED setting, not a value. Taken literally
                    # it produced `ssh://no@host/...` and `ssh_key = "no"` -- a
                    # config that tries to log in as a user called "no" with a
                    # key file called "no".
                    return None if _is_disabled(value) else value

                ssh_identity = inherited("ssh_identity")
                ssh_user = inherited("ssh_user")
                ssh_port = inherited("ssh_port")
                target_rate_limit = inherited("rate_limit")

                # Determine if this is a raw target. The declared type counts:
                # `target raw <url>` is a raw target even when no raw_target_*
                # option is set anywhere.
                # `bool("no")` is True, so btrbk's documented OFF value for
                # raw_target_compress / raw_target_encrypt turned every plain
                # send-receive destination into a raw stream-file one -- a
                # completely different backup format, chosen silently.
                is_raw_target = bool(
                    (raw_compress and not _is_disabled(raw_compress))
                    or (raw_encrypt and not _is_disabled(raw_encrypt))
                    or target.target_type == "raw"
                )

                # Convert btrbk target path format
                target_path = target.path
                if ":" in target_path and not target_path.startswith("ssh://"):
                    # Convert host:path to ssh://host:/path
                    host, path = target_path.split(":", 1)
                    if "@" in host:
                        user, hostname = host.split("@", 1)
                        if is_raw_target:
                            target_path = f"raw+ssh://{user}@{hostname}:{path}"
                        else:
                            target_path = f"ssh://{user}@{hostname}:{path}"
                    else:
                        if is_raw_target:
                            target_path = f"raw+ssh://{host}:{path}"
                        else:
                            target_path = f"ssh://{host}:{path}"
                    warnings.append(
                        f"Line {target.line}: Converted '{target.path}' to '{target_path}'"
                    )
                elif is_raw_target and target_path.startswith("ssh://"):
                    # A REMOTE raw target already in URL form. Prefixing "raw://"
                    # blindly produced `raw:///ssh://host/path` -- a nonsense
                    # local directory, so a remote raw backup silently became a
                    # local one pointed at a path that cannot exist.
                    remainder = target_path[len("ssh://") :]
                    if ":" not in remainder.split("/", 1)[0]:
                        host, _, path = remainder.partition("/")
                        target_path = f"raw+ssh://{host}:/{path}"
                    else:
                        target_path = f"raw+ssh://{remainder}"
                    warnings.append(
                        f"Line {target.line}: Converted raw target "
                        f"'{target.path}' to '{target_path}'"
                    )
                elif is_raw_target and not target_path.startswith("raw://"):
                    # Local raw target
                    if target_path.startswith("/"):
                        target_path = f"raw://{target_path}"
                    else:
                        target_path = f"raw:///{target_path}"

                # btrbk carries the remote user as its own option; this project
                # puts it in the URL. Without this the target authenticated as
                # whoever ran the backup rather than as the configured user.
                if ssh_user and "@" not in target_path:
                    for scheme in ("raw+ssh://", "ssh://"):
                        if target_path.startswith(scheme):
                            rest = target_path[len(scheme) :]
                            target_path = f"{scheme}{ssh_user}@{rest}"
                            break

                lines.append(f'path = "{target_path}"')

                if ssh_identity:
                    lines.append(f'ssh_key = "{ssh_identity}"')
                if ssh_port:
                    port = str(ssh_port).strip()
                    if port.isdigit():
                        lines.append(f"ssh_port = {port}")
                    else:
                        warnings.append(
                            f"Line {target.line}: ssh_port {ssh_port!r} is not a "
                            f"number and was not carried over"
                        )
                if target_rate_limit and str(target_rate_limit) not in ("no", "0"):
                    lines.append(f'rate_limit = "{target_rate_limit}"')

                # stream_compress -> compress, for targets that are not raw. A raw
                # target takes its method from raw_target_compress below, and setting
                # both would be ambiguous.
                if stream_compress and stream_compress != "no" and is_raw_target:
                    # A raw target takes its method from raw_target_compress, so
                    # say that this one is being dropped rather than let the
                    # migrated config quietly compress differently.
                    warnings.append(
                        f"Line {target.line}: stream_compress "
                        f"'{stream_compress}' is not applied to this raw target; "
                        f"a raw target compresses at rest using "
                        f"raw_target_compress"
                        + (
                            f" (currently '{raw_compress}')"
                            if raw_compress
                            else ", which is not set -- this backup will be "
                            "stored uncompressed"
                        )
                    )

                if stream_compress and stream_compress != "no" and not is_raw_target:
                    method = str(stream_compress)
                    method = _BTRBK_METHOD_ALIASES.get(method, method)
                    if method in _STREAM_COMPRESS_SUPPORTED:
                        lines.append(f'compress = "{method}"')
                    else:
                        warnings.append(
                            f"Line {target.line}: stream_compress '{method}' is not "
                            f"a method btrfs-backup-ng knows (supported: "
                            f"{', '.join(sorted(_STREAM_COMPRESS_SUPPORTED))}). "
                            f"This target will be backed up UNCOMPRESSED."
                        )

                for tuning in ("stream_compress_level", "stream_compress_threads"):
                    configured = (
                        target.options.get(tuning)
                        or subvolume.options.get(tuning)
                        or volume.options.get(tuning)
                        or btrbk_config.global_options.get(tuning)
                    )
                    if configured and str(configured) not in ("no", "default"):
                        warnings.append(
                            f"Line {target.line}: {tuning} '{configured}' is not "
                            f"carried over; btrfs-backup-ng runs the compressor at "
                            f"its default settings. Compression still works, the "
                            f"ratio and CPU use may differ."
                        )

                if (
                    ssh_compression
                    and str(ssh_compression) not in ("no", "false")
                    and ("ssh://" in target_path or "@" in target_path)
                ):
                    warnings.append(
                        f"Line {target.line}: ssh_compression is set, which uses ssh's "
                        f"own -C. btrfs-backup-ng does not pass -C; set `Compression yes` "
                        f"for this host in ~/.ssh/config, or use `compress` to compress "
                        f"the stream itself."
                    )

                # Raw target options
                if is_raw_target:
                    if raw_compress and raw_compress != "no":
                        # Map btrbk compression names
                        compress_map = {
                            "gzip": "gzip",
                            "pigz": "pigz",
                            "bzip2": "bzip2",
                            "pbzip2": "pbzip2",
                            "xz": "xz",
                            "lzo": "lzo",
                            "lz4": "lz4",
                            "zstd": "zstd",
                        }
                        compress = compress_map.get(raw_compress, raw_compress)
                        # btrbk supports methods a raw target here does not (bzip3,
                        # for one). Emitting the name unchecked produced a config
                        # file that the loader then REFUSED, so the migration
                        # appeared to succeed and the first run died on its own
                        # output. Say so here, and leave the setting out.
                        if compress in _RAW_COMPRESS_SUPPORTED:
                            lines.append(f'compress = "{compress}"')
                        else:
                            warnings.append(
                                f"Line {target.line}: raw_target_compress "
                                f"'{raw_compress}' is not supported for a raw "
                                f"target (supported: "
                                f"{', '.join(sorted(_RAW_COMPRESS_SUPPORTED))}). "
                                f"It has been left out; this target will be stored "
                                f"UNCOMPRESSED until you choose another method."
                            )

                    if raw_encrypt and raw_encrypt != "no":
                        if raw_encrypt == "gpg":
                            lines.append('encrypt = "gpg"')
                            # Get GPG recipient (inherited)
                            gpg_recipient = (
                                target.options.get("gpg_recipient")
                                or subvolume.options.get("gpg_recipient")
                                or volume.options.get("gpg_recipient")
                                or btrbk_config.global_options.get("gpg_recipient")
                            )
                            if gpg_recipient:
                                lines.append(f'gpg_recipient = "{gpg_recipient}"')
                            else:
                                warnings.append(
                                    f"Line {target.line}: GPG encryption enabled but no gpg_recipient found"
                                )
                            # Optional keyring
                            gpg_keyring = (
                                target.options.get("gpg_keyring")
                                or subvolume.options.get("gpg_keyring")
                                or volume.options.get("gpg_keyring")
                                or btrbk_config.global_options.get("gpg_keyring")
                            )
                            if gpg_keyring:
                                lines.append(f'gpg_keyring = "{gpg_keyring}"')
                        elif raw_encrypt == "openssl_enc":
                            lines.append('encrypt = "openssl_enc"')
                            warnings.append(
                                f"Line {target.line}: openssl_enc uses symmetric encryption. "
                                "Set BTRFS_BACKUP_PASSPHRASE environment variable with your passphrase."
                            )
                        else:
                            warnings.append(
                                f"Line {target.line}: Unknown encryption method '{raw_encrypt}'"
                            )

                # SSH options
                if target_path.startswith("ssh://") or target_path.startswith(
                    "raw+ssh://"
                ):
                    # Check if sudo might be needed
                    if not is_raw_target:
                        lines.append(
                            "ssh_sudo = true  # May be required for btrfs receive"
                        )

                lines.append("")

    # Warn about btrbk retention directives that have NO btrfs-backup-ng equivalent
    # (rather than silently dropping them). Scan every scope where they can appear.
    all_option_scopes = [btrbk_config.global_options]
    for volume in btrbk_config.volumes:
        all_option_scopes.append(volume.options)
        for subvolume in volume.subvolumes:
            all_option_scopes.append(subvolume.options)
    unsupported = {
        "preserve_day_of_week": "its schedule-anchoring effect on retention is not preserved",
        "preserve_hour_of_day": "its schedule-anchoring effect on retention is not preserved",
        "archive_preserve": "btrfs-backup-ng has no separate archive-retention concept",
        "archive_preserve_min": "btrfs-backup-ng has no separate archive-retention concept",
    }
    for directive, detail in unsupported.items():
        if any(directive in opts for opts in all_option_scopes):
            warnings.append(
                f"btrbk '{directive}' has no btrfs-backup-ng equivalent and was "
                f"dropped; {detail}"
            )

    # Final warnings check
    if not btrbk_config.volumes:
        warnings.append("No volumes found in configuration")

    total_subvols = sum(len(v.subvolumes) for v in btrbk_config.volumes)
    if total_subvols == 0:
        warnings.append("No subvolumes found - check your configuration structure")

    # De-duplicate while preserving order: the divergence/unsupported warnings can
    # legitimately recur across scopes, but the operator only needs to see each once.
    seen: set[str] = set()
    deduped: list[str] = []
    for warning in warnings:
        if warning not in seen:
            seen.add(warning)
            deduped.append(warning)
    return "\n".join(lines), deduped


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
