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
from datetime import timedelta
from pathlib import Path

from .__util__ import toml_str
from .retention import MIN_KEEP_ALL, parse_duration
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
    # Absolute offsets into the source. btrbk's grammar is line-oriented -- a
    # directive is a keyword and then the REST OF THE LINE, verbatim -- so a
    # value cannot be rebuilt by rejoining tokens without inventing whitespace
    # that was not there. The parser slices the original text instead.
    start: int = 0
    end: int = 0


@dataclass
class BtrbkOption:
    """A btrbk configuration option."""

    name: str
    value: str
    line: int


#: A target is remote when it carries an ssh URL scheme (``ssh://``, and the
#: ``raw+ssh://`` family) or is written in ``user@host:path`` form. Anchored at
#: the start so that an "@" inside a local path -- ``/mnt/backup/@snapshots``,
#: the standard btrfs subvolume layout -- is not mistaken for a login.
_REMOTE_TARGET_RE = re.compile(r"(?:[a-z0-9+.-]*\+)?ssh://|[^/\s]+@[^/\s]+:", re.I)


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
    #: Targets declared at GLOBAL scope. btrbk.conf(5) permits this and its own
    #: shipped example uses it: every subvolume inherits them. Discarding them
    #: produced volumes with no destination -- a migrated config that backs up
    #: nowhere.
    global_targets: list["BtrbkTarget"] = field(default_factory=list)


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
        start_pos = self.pos
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
            self.tokens.append(
                Token(
                    TokenType.KEYWORD, word, self.line, start_col, start_pos, self.pos
                )
            )
        else:
            # If followed by path characters, continue reading as a value
            # This handles cases like "ssh://..." or "user@host:..."
            while self.pos < len(self.content):
                char = self.content[self.pos]
                if char in " \t\n#":
                    break
                word += self._advance()
            self.tokens.append(
                Token(TokenType.VALUE, word, self.line, start_col, start_pos, self.pos)
            )

    def _read_quoted_string(self) -> None:
        """Read a quoted string value.

        Stops at the end of the line as well as at the closing quote: btrbk's
        grammar is line-oriented and an unterminated quote is a value that
        happens to start with a quote character, not the start of a string
        spanning the rest of the file. Reading on to the next quote silently
        swallowed every following line -- volumes, targets and all -- so the
        converted config was missing whatever came after the typo.
        """
        start_col = self.column
        start_pos = self.pos
        quote = self._advance()
        value = ""
        while self.pos < len(self.content) and self.content[self.pos] not in (
            quote,
            "\n",
        ):
            if self.content[self.pos] == "\\":
                self._advance()
                if self.pos < len(self.content):
                    value += self._advance()
            else:
                value += self._advance()
        if self.pos < len(self.content) and self.content[self.pos] == quote:
            self._advance()  # closing quote
        self.tokens.append(
            Token(TokenType.VALUE, value, self.line, start_col, start_pos, self.pos)
        )

    def _read_value(self) -> None:
        """Read an unquoted value (path, URL, etc)."""
        start_col = self.column
        start_pos = self.pos
        value = ""
        while self.pos < len(self.content):
            char = self.content[self.pos]
            if char in " \t\n#":
                break
            value += self._advance()
        self.tokens.append(
            Token(TokenType.VALUE, value, self.line, start_col, start_pos, self.pos)
        )


class BtrbkParser:
    """Parser for btrbk configuration files."""

    def __init__(self, tokens: list[Token], content: str = ""):
        self.tokens = tokens
        self.content = content
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

    def _skip_newlines(self) -> None:
        while not self._is_at_end() and self._current().type in (
            TokenType.NEWLINE,
            TokenType.COMMENT,
        ):
            self._advance()

    #: btrbk's line grammar (``/usr/bin/btrbk``, ``sub parse_config``): after
    #: comment removal and trimming, a line is a keyword and then the WHOLE
    #: rest of the line. The whitespace class is Perl's ASCII ``\s`` -- Python's
    #: ``\s`` also matches NBSP and other Unicode spaces, which btrbk does not.
    _WS = " \t\r\f\v"
    _LINE_RE = re.compile(r"^([a-zA-Z_]+)(?:[ \t\r\f\v]+(.*))?$")

    def _parse_line(self) -> None:
        """Parse a single line of configuration."""
        self._skip_newlines()
        if self._is_at_end():
            return

        token = self._current()

        if token.type == TokenType.KEYWORD:
            keyword = token.value
            self._advance()
            value = self._rest_of_line(token)

            if keyword == "volume":
                self._parse_volume(token, value)
            elif keyword == "subvolume":
                self._parse_subvolume(token, value)
            elif keyword == "target":
                self._parse_target(token, value)
            else:
                self._parse_option(keyword, value)
        elif token.type == TokenType.VALUE:
            # Could be a continuation or error
            self._advance()
        else:
            self._advance()

    @staticmethod
    def _strip_comment(text: str) -> str:
        """Drop an unquoted ``#`` and everything after it, as btrbk does.

        btrbk removes comments with a quote-aware substitution before it splits
        the line, and a ``#`` needs no preceding space: ``volume /mnt/a#b`` is
        the path ``/mnt/a`` (measured).
        """
        quote = ""
        for index, char in enumerate(text):
            if quote:
                if char == quote:
                    quote = ""
            elif char in "\"'":
                quote = char
            elif char == "#":
                return text[:index]
        return text

    @staticmethod
    def _unquote(text: str) -> str:
        """Strip surrounding quotes the way btrbk does: a matching pair of
        double quotes, then a matching pair of single quotes, in that order
        (``s/^"(.*)"$/$1/; s/^'(.*)'$/$1/`` on every directive's value)."""
        for quote in ('"', "'"):
            if len(text) >= 2 and text[0] == quote and text[-1] == quote:
                text = text[1:-1]
        return text

    def _rest_of_line(self, keyword: Token) -> str:
        """The value of the directive ``keyword`` opens: the rest of its line,
        verbatim, read the way btrbk reads every line.

        btrbk matches ``^([a-zA-Z_]+)(?:\\s+(.*))?$`` against the comment-
        stripped, whitespace-trimmed line, so a value runs to end of line and
        keeps every character and its internal spacing exactly as written.
        Measured against btrbk 0.32.7:

            volume /mnt/sp ace            ->  /mnt/sp ace
            volume /mnt/two  spaces       ->  /mnt/two  spaces   (both spaces kept)
            volume "/mnt/quoted path"     ->  /mnt/quoted path   (quotes stripped)
            volume /mnt/a#b               ->  /mnt/a
            snapshot_preserve 14d 8w *m   ->  14d 8w *m
            ssh_identity ~/.ssh/key       ->  ~/.ssh/key

        Two earlier readings were wrong in the same way. Section directives
        sliced the line from the first VALUE token, and options were rebuilt
        by joining tokens; the lexer discards any character it does not
        classify -- ``*``, ``~``, ``%``, ``+`` and every other symbol -- so
        ``*m`` (keep EVERY monthly snapshot) became ``m``, read as no monthly
        snapshots at all, and a ``~`` at the start of a path vanished. Both
        silently, and the first prune after migrating deleted history btrbk
        was keeping. Slicing from the KEYWORD token's own start reaches every
        character of the value whether or not the lexer had a class for it.

        Without the source text (a parser built from tokens alone) the tokens
        on the line are joined with single spaces, which is the most the
        tokens can say. The tokens on the line are consumed either way.
        """
        values: list[str] = []
        while not self._is_at_end():
            token = self._current()
            if token.type == TokenType.NEWLINE:
                break
            if token.type != TokenType.COMMENT:
                values.append(token.value)
            self._advance()
        if not self.content:
            return " ".join(values)

        newline = self.content.find("\n", keyword.start)
        line_end = len(self.content) if newline == -1 else newline
        raw = self._strip_comment(self.content[keyword.start : line_end]).strip(
            self._WS
        )
        match = self._LINE_RE.match(raw)
        if match is None or match.group(2) is None:
            return ""
        return self._unquote(match.group(2))

    def _parse_volume(self, keyword: Token, path: str) -> None:
        """Parse a volume section."""
        if not path:
            self.config.warnings.append(
                f"Line {keyword.line}: Expected path after 'volume'"
            )
            return

        self.current_volume = BtrbkVolume(path=path, line=keyword.line)
        self.current_subvolume = None
        self.current_target = None
        self.config.volumes.append(self.current_volume)

    def _parse_subvolume(self, keyword: Token, path: str) -> None:
        """Parse a subvolume section."""
        if not path:
            self.config.warnings.append(
                f"Line {keyword.line}: Expected path after 'subvolume'"
            )
            return

        if self.current_volume is None:
            self.config.warnings.append(
                f"Line {keyword.line}: 'subvolume' outside of 'volume' section"
            )
            return

        self.current_target = None
        self.current_subvolume = BtrbkSubvolume(path=path, line=keyword.line)
        self.current_volume.subvolumes.append(self.current_subvolume)

    #: ``target <type> <url>``: btrbk takes a leading word followed by
    #: whitespace as the type (``s/^([a-zA-Z_-]+)\s+//``) and accepts
    #: ``send-receive`` (the default) and ``raw``.
    _TARGET_TYPE_RE = re.compile(r"^([a-zA-Z_-]+)[ \t\r\f\v]+(.*)$")

    def _parse_target(self, keyword: Token, value: str) -> None:
        """Parse a target section."""
        if not value:
            self.config.warnings.append(
                f"Line {keyword.line}: Expected path after 'target'"
            )
            return

        # `target <type> <url>` is btrbk's documented form, and the type token
        # was being taken as the destination: `target send-receive ssh://nas/b`
        # produced a target called "send-receive" and dropped the real URL, so an
        # imported config silently had nowhere to back up to. The quotes are
        # stripped from the URL AFTER the type is split off, as btrbk does.
        target_type = None
        match = self._TARGET_TYPE_RE.match(value)
        if match and match.group(1) in ("send-receive", "raw"):
            target_type = match.group(1)
            value = match.group(2)
        path = self._unquote(value)
        target = BtrbkTarget(path=path, line=keyword.line, target_type=target_type)
        self.current_target = target

        # Add to current scope
        if self.current_subvolume is not None:
            self.current_subvolume.targets.append(target)
        elif self.current_volume is not None:
            self.current_volume.targets.append(target)
        else:
            # Global scope: inherited by every subvolume, as btrbk does.
            self.config.global_targets.append(target)

    def _parse_option(self, keyword: str, value: str) -> None:
        """Store an option in the innermost open scope.

        Target first: it is the narrowest, and the inheritance chain further
        down reads target -> subvolume -> volume -> global in that order.
        """
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
    parser = BtrbkParser(tokens, content)
    return parser.parse()


def parse_btrbk_retention(value: str) -> dict[str, int]:
    """Parse a btrbk preserve matrix into this tool's bucket counts.

    btrbk format: "[<hourly>h] [<daily>d] [<weekly>w] [<monthly>m] [<yearly>y]".

    btrbk's count is INCLUSIVE: ``14d`` keeps the first snapshot of each of
    days 0..14 -- fifteen days -- because its scheduler preserves a period
    while ``delta_days <= 14`` (``schedule()`` in btrbk 0.32.7); the same holds
    for hours, weeks, months and years. This tool's ``daily = N`` keeps N day
    buckets. So every count is written one higher than btrbk's number, which
    keeps at least what btrbk keeps: "14d 4w 6m" becomes daily 15, weekly 5,
    monthly 7. ``*`` keeps every period (999). btrbk reads the count as a
    string and tests it for truth: ``0`` disables the period and stays 0;
    ``00`` is true, enables the period with a bound of 0 and keeps the current
    period, so it becomes 1.

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
        count = _inclusive_count(count_str)

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


def _inclusive_count(count_str: str) -> int:
    """One btrbk period count as this tool's bucket count: see
    ``parse_btrbk_retention``."""
    if count_str == "*":
        return 999
    if count_str == "0":
        return 0
    return int(count_str) + 1


def _translate_preserve_min(value: str) -> tuple[str, list[str]]:
    """Translate a btrbk ``*_preserve_min`` value into a btrfs-backup-ng retention
    ``min`` duration.

    Three btrbk-vs-btrfs-backup-ng mismatches make a straight passthrough wrong:

    * **Unit clash on ``m``.** btrbk retention uses ``m`` for MONTHS, but
      btrfs-backup-ng's duration parser uses ``m`` for minutes and ``M`` for months.
      A btrbk ``3m`` (3 months) passed through verbatim would silently become 3
      *minutes*. We remap ``m`` -> ``M``.
    * **btrbk's minimum is calendar-granular and inclusive.** ``2d`` keeps a
      snapshot while ``delta_days <= 2``, where the delta counts whole days
      from the start of the snapshot's day; at 19:19 on the 23rd that keeps a
      snapshot from 03:00 on the 21st. ``min = "2d"`` here is exactly 48 hours
      and would delete it. Written one unit higher (``3d``, 72 hours) the
      window always contains btrbk's: the start of the Nth period back lies
      within N+1 whole units of any moment inside the current one. The same
      holds for hours, weeks, months and years.
    * **Special tokens.** btrbk ``no``/``all``/``latest`` are not durations. ``no``
      is no age floor (``0s``); ``latest`` keeps only the latest beyond the
      schedule, which this project always keeps anyway (``0s``); ``all`` keeps
      every snapshot (``min = "all"``). A value that is not understood becomes
      ``"all"`` too, with a warning: never delete on ambiguous input.

    Returns ``(min_string, warnings)``.
    """
    warnings: list[str] = []
    low = value.strip().lower()
    if low in ("no", "none"):
        # No minimum age -- count-based rules apply fully. Faithful 1:1 mapping.
        return "0s", warnings
    if low == "all":
        return MIN_KEEP_ALL, warnings
    if low == "latest":
        return "0s", warnings
    m = re.fullmatch(r"(\d+)\s*([hdwmy])", low)
    if m:
        count, unit = m.groups()
        # btrbk m=months -> btrfs-backup-ng M=months (h/d/w/y are identical).
        bbng_unit = "M" if unit == "m" else unit
        return f"{int(count) + 1}{bbng_unit}", warnings
    warnings.append(
        f"btrbk retention minimum {value.strip()!r} was not understood; "
        f'using min = "all" (keep every snapshot) until you set it -- review the '
        f"generated [.retention] min"
    )
    return MIN_KEEP_ALL, warnings


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


_NO_SCHEDULE = {"hourly": 0, "daily": 0, "weekly": 0, "monthly": 0, "yearly": 0}
_SAFE_SCHEDULE = {"hourly": 24, "daily": 7, "weekly": 4, "monthly": 12, "yearly": 0}


def _btrbk_policy(
    preserve: str | None, preserve_min: str | None, scope: str
) -> tuple[str, dict[str, int], list[str]]:
    """One btrbk ``*_preserve`` / ``*_preserve_min`` pair as a policy:
    ``(min, bucket counts, warnings)``, meaning what btrbk means by it.

    btrbk's defaults (``/usr/bin/btrbk:95-98``, 0.32.7): ``*_preserve`` is
    undefined -- no schedule -- and ``*_preserve_min`` is ``all``. So a btrbk
    configuration that says nothing about retention keeps EVERY snapshot, on the
    source and on every target; one with only ``snapshot_preserve 14d`` keeps
    every snapshot too, because the minimum still defaults to ``all``. Both are
    written that way (``min = "all"``). A schedule with no minimum-age window is
    all-zero buckets. The one pair this tool cannot express faithfully is no
    schedule with a minimum of a day or less -- the prune refuses such a policy
    as degenerate -- and that keeps the default schedule instead, which keeps
    MORE than btrbk would, and says so.
    """
    warnings: list[str] = []
    if preserve_min is not None:
        min_str, w = _translate_preserve_min(preserve_min)
        warnings += w
    else:
        min_str = MIN_KEEP_ALL
    if preserve is not None:
        counts, w = _parse_preserve_counts(preserve)
        warnings += w
    else:
        counts = dict(_NO_SCHEDULE)
    # The same test the prune's degenerate-policy guard applies
    # (cli.prune.is_degenerate_policy): no buckets and a minimum of a day or less.
    if (
        min_str != MIN_KEEP_ALL
        and not any(counts.values())
        and parse_duration(min_str) <= timedelta(days=1)
    ):
        counts = dict(_SAFE_SCHEDULE)
        warnings.append(
            f"{scope}: btrbk keeps no schedule and a minimum of {min_str}; that "
            f"policy prunes to the latest snapshot and btrfs-backup-ng refuses it, "
            f"so the default schedule (24 hourly, 7 daily, 4 weekly, 12 monthly) is "
            f"used instead, which keeps more -- review the generated [.retention]"
        )
    return min_str, counts, warnings


def _retention_block(
    header: str, scope: str, preserve: str | None, preserve_min: str | None
) -> tuple[list[str], list[str]]:
    """A ``[<header>.retention]`` TOML block for one btrbk policy pair.

    The source policy (``snapshot_preserve*``) goes in the global or volume
    block; each target's own (``target_preserve*``) goes in that target's
    ``[volumes.targets.retention]``, since this project has per-target
    retention. Returns ``(toml_lines, warnings)``.
    """
    min_str, counts, warnings = _btrbk_policy(preserve, preserve_min, scope)
    directive = "target" if header.endswith("targets") else "snapshot"
    origin = ", ".join(
        f"{directive}_{name} {value.strip()}"
        for name, value in (("preserve", preserve), ("preserve_min", preserve_min))
        if value is not None
    )
    lines = [f"[{header}.retention]"]
    if origin:
        lines.append(f"# btrbk: {origin}")
        if _raises_a_number(preserve, preserve_min):
            lines.append(INCLUSIVE_NOTE_COMMENT)
    lines.append("min = " + toml_str(min_str))
    for key in ("hourly", "daily", "weekly", "monthly", "yearly"):
        lines.append(f"{key} = {counts[key]}")
    return lines, warnings


#: The comment written above a retention block whose numbers were raised by
#: one, and (as ``INCLUSIVE_NOTE``) said once per import.
INCLUSIVE_NOTE_COMMENT = (
    "# counts and minimum are one higher than btrbk's: its N keeps periods "
    "0..N and its minimum is inclusive"
)
INCLUSIVE_NOTE = (
    "Retention counts and minimums are written one higher than btrbk's "
    'numbers (14d -> daily = 15, snapshot_preserve_min 2d -> min = "3d"): '
    "btrbk's count N keeps the first snapshot of each of periods 0..N, and its "
    "minimum N keeps the whole Nth calendar unit back, so the imported policy "
    "keeps at least what btrbk keeps. Weekly, monthly and yearly buckets start "
    "on ISO Monday and on the first of the month and year here, where btrbk "
    "starts them on preserve_day_of_week; the first snapshot of a week can "
    "therefore differ."
)


def _raises_a_number(preserve: str | None, preserve_min: str | None) -> bool:
    """Whether this pair contains a btrbk number the import writes one higher:
    a period count other than ``*`` and ``0``, or an ``N<unit>`` minimum."""
    if preserve is not None and any(
        count not in ("*", "0")
        for count, _unit in re.findall(r"(\*|\d+)([hdwmy])", preserve.lower())
    ):
        return True
    return bool(
        preserve_min is not None
        and re.fullmatch(r"\s*\d+\s*[hdwmy]\s*", preserve_min.lower())
    )


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
        lines.append(
            "snapshot_dir = " + toml_str(btrbk_config.global_options["snapshot_dir"])
        )
    else:
        lines.append('snapshot_dir = ".snapshots"')

    # Map btrbk timestamp format to strftime format
    btrbk_ts_format = btrbk_config.global_options.get(
        "timestamp_format", BTRBK_DEFAULT_TIMESTAMP_FORMAT
    )
    if btrbk_ts_format in BTRBK_TIMESTAMP_FORMATS:
        strftime_format = BTRBK_TIMESTAMP_FORMATS[btrbk_ts_format]
        lines.append("timestamp_format = " + toml_str(strftime_format))
    else:
        # Unknown format, use btrbk's default (long)
        warnings.append(
            f"Unknown btrbk timestamp_format '{btrbk_ts_format}', "
            f"using 'long' format for compatibility"
        )
        lines.append("timestamp_format = " + toml_str(BTRBK_TIMESTAMP_FORMATS["long"]))

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
            lines.append("path = " + toml_str(full_path))

            # Snapshot prefix from options or generate from path
            prefix = subvolume.options.get(
                "snapshot_name", volume.options.get("snapshot_name", "")
            )
            if not prefix:
                prefix = full_path.strip("/").replace("/", "-") or "root"
            lines.append("snapshot_prefix = " + toml_str(prefix))

            # Snapshot directory
            snap_dir = subvolume.options.get(
                "snapshot_dir",
                volume.options.get(
                    "snapshot_dir",
                    btrbk_config.global_options.get("snapshot_dir", ".snapshots"),
                ),
            )
            lines.append("snapshot_dir = " + toml_str(snap_dir))

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
                )
                lines.extend(sub_ret_lines)
                warnings.extend(sub_ret_warnings)

            lines.append("")

            # Targets - from subvolume, volume, or both
            # Global targets are inherited by every subvolume, exactly as btrbk
            # applies them. Without this a config whose only `target` is declared
            # at global scope -- legal, and the form btrbk's own example uses --
            # migrated to volumes with no destination at all.
            all_targets = (
                subvolume.targets + volume.targets + btrbk_config.global_targets
            )

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
                # ssh_identity, ssh_user, ssh_port and rate_limit were recognised
                # by the parser -- so they never looked unknown -- stored, and
                # then never read back, which dropped them silently at EVERY
                # scope. The migration guide promises three of them by name, so a
                # user read the table, believed their key and username had come
                # across, and got authentication failures against a host btrbk
                # had been backing up to correctly. `backend` was added to the
                # same lookup later, for the same reason.
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

                # btrbk's `backend` says whether it elevates on the remote:
                # btrfs-progs-sudo does, btrfs-progs does not. ssh_sudo was
                # hardcoded true for every ssh target regardless, so a config
                # that explicitly chose the non-sudo backend was migrated to one
                # that elevates -- the operator's explicit choice reversed
                # without a word. Unset, the previous default is kept, because
                # connecting as a non-root user usually does need it and that is
                # what the trailing hint says.
                backend = inherited("backend") or inherited("backend_remote")
                ssh_sudo = True if not backend else "sudo" in str(backend).lower()
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
                # A declared type is the operator's explicit statement about the
                # destination FORMAT, so an inherited raw_target_* option must not
                # silently overrule it: `target send-receive <url>` under a global
                # `raw_target_compress` was being converted to raw+ssh://, turning
                # a browsable subvolume backup into stream files.
                if target.target_type == "send-receive":
                    is_raw_target = False
                    if (raw_compress and not _is_disabled(raw_compress)) or (
                        raw_encrypt and not _is_disabled(raw_encrypt)
                    ):
                        warnings.append(
                            f"Line {target.line}: this target is declared "
                            f"send-receive, so the inherited raw_target_* options "
                            f"do not apply to it and were not carried over"
                        )
                else:
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

                lines.append("path = " + toml_str(target_path))

                # A local target has no ssh connection to configure. Emitting
                # these put irrelevant credentials in the block and made the
                # local case look remote to anyone reading the file. Matched
                # structurally rather than by searching for "@": "@" appears in
                # the middle of ordinary local paths under the standard btrfs
                # layout (/mnt/backup/@snapshots), which is not a remote target.
                is_remote_target = bool(_REMOTE_TARGET_RE.match(target_path))
                if ssh_identity and is_remote_target:
                    lines.append("ssh_key = " + toml_str(ssh_identity))
                if ssh_port and is_remote_target:
                    port = str(ssh_port).strip()
                    if port.isdigit():
                        lines.append(f"ssh_port = {port}")
                    else:
                        warnings.append(
                            f"Line {target.line}: ssh_port {ssh_port!r} is not a "
                            f"number and was not carried over"
                        )
                if target_rate_limit and str(target_rate_limit) not in ("no", "0"):
                    lines.append("rate_limit = " + toml_str(target_rate_limit))

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
                        lines.append("compress = " + toml_str(method))
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
                            lines.append("compress = " + toml_str(compress))
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
                            # Get GPG recipient (inherited)
                            gpg_recipient = (
                                target.options.get("gpg_recipient")
                                or subvolume.options.get("gpg_recipient")
                                or volume.options.get("gpg_recipient")
                                or btrbk_config.global_options.get("gpg_recipient")
                            )
                            if gpg_recipient:
                                lines.append('encrypt = "gpg"')
                                lines.append(
                                    "gpg_recipient = " + toml_str(gpg_recipient)
                                )
                            else:
                                # The loader REFUSES encrypt=gpg without a
                                # recipient, so emitting it produced a file that
                                # could not be loaded at all: the migration
                                # appeared to succeed and every later command
                                # failed on its own output. Leaving it out keeps
                                # the rest of the config usable and says what is
                                # missing.
                                warnings.append(
                                    f"Line {target.line}: raw_target_encrypt gpg "
                                    f"needs a gpg_recipient and none was set, so "
                                    f"encryption was NOT carried over -- this "
                                    f"target will be stored UNENCRYPTED. Add "
                                    f'gpg_recipient and encrypt = "gpg" to '
                                    f"restore it."
                                )
                            # Optional keyring
                            gpg_keyring = (
                                target.options.get("gpg_keyring")
                                or subvolume.options.get("gpg_keyring")
                                or volume.options.get("gpg_keyring")
                                or btrbk_config.global_options.get("gpg_keyring")
                            )
                            if gpg_keyring:
                                lines.append("gpg_keyring = " + toml_str(gpg_keyring))
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
                            f"ssh_sudo = {'true' if ssh_sudo else 'false'}"
                            + (
                                ""
                                if backend
                                else "  # May be required for btrfs receive"
                            )
                        )

                # The target's own retention, from btrbk's target_preserve and
                # target_preserve_min resolved at the narrowest scope that sets
                # them. btrbk keeps every backup on a target that sets neither
                # (target_preserve_min defaults to "all"), so every target gets
                # a block: without one it would inherit the SOURCE schedule and
                # the first prune would delete backups btrbk was keeping.
                # Looked up raw: btrbk's `no` is a real value here
                # (target_preserve_min no = no minimum age), not "unset".
                def preserve_option(name: str) -> str | None:
                    for scope_options in (
                        target.options,
                        subvolume.options,
                        volume.options,
                        btrbk_config.global_options,
                    ):
                        if scope_options.get(name) is not None:
                            return scope_options[name]
                    return None

                target_lines, target_warnings = _retention_block(
                    "volumes.targets",
                    f"target {target_path}",
                    preserve_option("target_preserve"),
                    preserve_option("target_preserve_min"),
                )
                lines.append("")
                lines.extend(target_lines)
                warnings.extend(target_warnings)

                lines.append("")

    if INCLUSIVE_NOTE_COMMENT in lines:
        warnings.append(INCLUSIVE_NOTE)

    # Options btrbk understands that this project has no equivalent for. They
    # were in the lexer's keyword set -- so they never looked unknown -- stored,
    # and never read again: the operator set something deliberately and was told
    # nothing. Saying so is the whole difference between "not supported" and
    # "quietly ignored".
    _UNMAPPED = {
        "snapshot_create": (
            "controls WHEN btrbk creates snapshots (always/onchange/ondemand/no); "
            "btrfs-backup-ng always creates one per run"
        ),
        "stream_buffer": (
            "sets the transfer buffer size; btrfs-backup-ng sizes its own buffer "
            "and does not expose it"
        ),
        "lockfile": "btrfs-backup-ng manages its own locking",
        "transaction_log": "btrfs-backup-ng writes its own transaction log",
        "btrfs_commit_delete": "has no equivalent",
        "group": "target grouping has no equivalent",
    }
    unmapped_scopes: list[tuple[str, dict[str, str]]] = [
        ("global", btrbk_config.global_options)
    ]
    for volume in btrbk_config.volumes:
        unmapped_scopes.append((f"volume {volume.path}", volume.options))
        for subvolume in volume.subvolumes:
            unmapped_scopes.append((f"subvolume {subvolume.path}", subvolume.options))
            # Target scope is where `group` is usually set, so leaving it out
            # would have reproduced the same silence one level down.
            for target in subvolume.targets:
                unmapped_scopes.append((f"target {target.path}", target.options))

    for scope_name, scope in unmapped_scopes:
        for key, why in _UNMAPPED.items():
            value = scope.get(key)
            if value and not _is_disabled(value):
                warnings.append(
                    f"{key} {value!r} ({scope_name}) is not carried over: {why}"
                )

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
