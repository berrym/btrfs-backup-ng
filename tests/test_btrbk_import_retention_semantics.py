"""The btrbk importer carries retention with btrbk's meaning.

Ground truth is the installed binary (``tests/btrbk_ground_truth.py``: one
set of dated snapshots, one ``btrbk -n -S run`` per variant, the kept set read
off btrbk's own action list). The tests here convert the same btrbk lines,
load the result through the real config loader and hand the same snapshot
names to the real retention engine at the same clock, then compare.

Two rules from btrbk's source (``/usr/bin/btrbk`` 0.32.7) drive the mapping:

- ``snapshot_preserve`` and ``target_preserve`` default to UNDEFINED (no
  schedule) and ``snapshot_preserve_min`` / ``target_preserve_min`` default to
  ``all``. A btrbk configuration that says nothing about retention keeps every
  snapshot, on the source and on every target; one that sets only a schedule
  keeps every snapshot too, because the minimum still says ``all``.
- A directive's value is the rest of its line, verbatim; ``*m`` means keep
  every monthly snapshot.
- btrbk's numbers are inclusive. ``snapshot_preserve 3d`` keeps the first
  snapshot of each of days 0..3 (``delta_days <= 3``), four days; and
  ``snapshot_preserve_min 2d`` keeps a snapshot while ``delta_days <= 2``,
  where the delta counts whole days from the start of the snapshot's day, so
  at 19:19 on the 23rd a snapshot from 03:00 on the 21st is kept. This tool's
  ``daily = 3`` keeps three day buckets and its ``min = "2d"`` is 48 hours.
  Every count and every ``N<unit>`` minimum is therefore written one higher,
  which keeps at least what btrbk keeps.

The importer used to write ``min = "1d"`` for an absent minimum and its own
default buckets for an absent schedule, and its lexer dropped the ``*`` of
``*m``. The first prune after migrating deleted history btrbk was keeping.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from btrfs_backup_ng.btrbk_import import (
    INCLUSIVE_NOTE,
    INCLUSIVE_NOTE_COMMENT,
    BtrbkLexer,
    BtrbkParser,
    _translate_preserve_min,
    convert_to_toml,
    parse_btrbk_config,
)
from btrfs_backup_ng.cli.prune import is_degenerate_policy
from btrfs_backup_ng.config.loader import load_config
from btrfs_backup_ng.retention import MIN_KEEP_ALL, apply_retention
from tests import btrbk_ground_truth as truth

PREFIX = "data."


def _btrbk(*lines: str, subvolume_lines: tuple[str, ...] = ()) -> str:
    body = "\n".join(lines)
    sub = "".join(f"    {line}\n" for line in subvolume_lines)
    return (
        f"timestamp_format long\n{body}\n"
        f"volume /mnt\n  snapshot_dir snaps\n  subvolume data\n{sub}"
        f"    target send-receive /mnt/bk\n"
    )


def _convert(text: str):
    return convert_to_toml(parse_btrbk_config(text))


def _load(tmp_path, text: str):
    toml, warnings = _convert(text)
    path = tmp_path / "converted.toml"
    path.write_text(toml)
    config, load_warnings = load_config(path)
    assert not [w for w in load_warnings if "Unknown config key" in w], load_warnings
    return config, warnings


def _kept(retention, now=truth.NOW) -> frozenset[str]:
    names = [PREFIX + ts for ts in truth.ALL]
    keep, _delete = apply_retention(
        names, retention, prefix=PREFIX, timestamp_format=truth.FORMAT, now=now
    )
    return frozenset(name[len(PREFIX) :] for name in keep)


# --------------------------------------------------------------------------- #
# The lexer keeps every character of a value
# --------------------------------------------------------------------------- #
class TestTheValueIsTheRestOfTheLine:
    def test_a_star_bucket_survives(self):
        cfg = parse_btrbk_config(_btrbk("snapshot_preserve 14d 8w *m"))
        assert cfg.global_options["snapshot_preserve"] == "14d 8w *m"

    def test_a_leading_tilde_survives(self):
        cfg = parse_btrbk_config(_btrbk("ssh_identity ~/.ssh/backup_key"))
        assert cfg.global_options["ssh_identity"] == "~/.ssh/backup_key"

    @pytest.mark.parametrize(
        "value", ["*d", "+tag", "%weird", "a,b", "(x)", "~/k", "$HOME/k", "!x"]
    )
    def test_no_leading_character_is_dropped(self, value):
        """The lexer classifies letters, digits and a few path characters;
        everything else it used to skip, so a value starting with one of these
        lost its first character with no warning."""
        cfg = parse_btrbk_config(_btrbk(f"group {value}"))
        assert cfg.global_options["group"] == value

    def test_internal_spacing_is_kept_verbatim(self):
        cfg = parse_btrbk_config(_btrbk("snapshot_preserve 14d\t8w  *m"))
        assert cfg.global_options["snapshot_preserve"] == "14d\t8w  *m"

    def test_quotes_are_stripped_as_btrbk_strips_them(self):
        """btrbk strips a pair of double quotes, then a pair of single quotes,
        on every directive's value (measured: ``snapshot_dir "sn aps"`` prints
        as ``sn aps``, ``subvolume 'ho me'`` as ``ho me``)."""
        cfg = parse_btrbk_config(
            'snapshot_dir "sn aps"\nvolume "/mnt/x y"\n  subvolume \'ho me\'\n'
            "    target send-receive \"/mnt/b k\"\n    ssh_identity '/k ey'\n"
        )
        assert cfg.global_options["snapshot_dir"] == "sn aps"
        assert cfg.volumes[0].path == "/mnt/x y"
        assert cfg.volumes[0].subvolumes[0].path == "ho me"
        target = cfg.volumes[0].subvolumes[0].targets[0]
        assert (target.target_type, target.path) == ("send-receive", "/mnt/b k")
        assert target.options["ssh_identity"] == "/k ey"

    def test_an_unterminated_quote_does_not_swallow_the_file(self):
        """A quote with no partner is a value starting with a quote character
        to btrbk's line grammar. Read as the start of a string it consumed
        every following line, and the converted config had no volumes."""
        cfg = parse_btrbk_config(
            'snapshot_dir "oops\nsnapshot_preserve 7d\nvolume /mnt\n'
            "  subvolume data\n    target /mnt/bk\n"
        )
        assert cfg.global_options["snapshot_dir"] == '"oops'
        assert cfg.global_options["snapshot_preserve"] == "7d"
        assert [v.path for v in cfg.volumes] == ["/mnt"]
        assert cfg.volumes[0].subvolumes[0].targets[0].path == "/mnt/bk"

    def test_a_comment_ends_the_value(self):
        cfg = parse_btrbk_config(_btrbk("snapshot_preserve 14d *m # keep months"))
        assert cfg.global_options["snapshot_preserve"] == "14d *m"

    def test_a_parser_without_the_source_text_still_reads_options(self):
        tokens = BtrbkLexer("snapshot_preserve 14d 8w\n").tokenize()
        cfg = BtrbkParser(tokens).parse()
        assert cfg.global_options["snapshot_preserve"] == "14d 8w"


# --------------------------------------------------------------------------- #
# The minimum
# --------------------------------------------------------------------------- #
class TestTheMinimum:
    @pytest.mark.parametrize(
        "btrbk_value,expected",
        [
            ("all", MIN_KEEP_ALL),
            ("latest", "0s"),
            ("no", "0s"),
            ("2d", "3d"),
            ("48h", "49h"),
            ("4w", "5w"),
            ("3m", "4M"),
            ("1y", "2y"),
            ("0d", "1d"),
        ],
    )
    def test_known_values_map_without_a_warning(self, btrbk_value, expected):
        """An ``N<unit>`` minimum is written as ``N+1``: btrbk's is inclusive
        of the whole Nth calendar unit back, this tool's is an exact duration."""
        assert _translate_preserve_min(btrbk_value) == (expected, [])

    def test_an_unrecognised_value_keeps_everything_and_warns(self):
        """Never delete on ambiguous input."""
        value, warnings = _translate_preserve_min("fortnightly")
        assert value == MIN_KEEP_ALL
        assert warnings and "fortnightly" in warnings[0]

    def test_an_absent_minimum_is_all(self, tmp_path):
        config, _ = _load(tmp_path, _btrbk("snapshot_preserve 14d 4w"))
        assert config.global_config.retention.min == MIN_KEEP_ALL
        assert config.global_config.retention.daily == 15

    def test_no_is_a_value_not_unset(self, tmp_path):
        """``target_preserve_min no`` is btrbk for "no minimum age"; treating it
        as absent would have turned it into ``all``, the opposite meaning."""
        config, _ = _load(
            tmp_path, _btrbk("target_preserve_min no", "target_preserve 14d")
        )
        target = config.volumes[0].targets[0]
        assert config.get_target_retention(config.volumes[0], target).min == "0s"


# --------------------------------------------------------------------------- #
# What the engine keeps, against what btrbk kept
# --------------------------------------------------------------------------- #
class TestSourceRetentionAgainstBtrbk:
    @pytest.mark.parametrize(
        "lines",
        [
            (),
            ("snapshot_preserve 2d",),
            ("snapshot_preserve_min 3m",),
            ("snapshot_preserve *d", "snapshot_preserve_min latest"),
            ("snapshot_preserve 14d 8w *m", "snapshot_preserve_min latest"),
        ],
        ids=lambda lines: " / ".join(lines) or "nothing set",
    )
    def test_the_engine_keeps_exactly_what_btrbk_kept(self, tmp_path, lines):
        config, _ = _load(tmp_path, _btrbk(*lines))
        assert _kept(config.global_config.retention) == truth.SOURCE_KEEPS[lines]

    @pytest.mark.parametrize(
        "lines",
        [
            ("snapshot_preserve_min latest",),
            ("snapshot_preserve no", "snapshot_preserve_min latest"),
        ],
        ids=lambda lines: " / ".join(lines),
    )
    def test_keep_only_the_latest_is_kept_as_more_and_said(self, tmp_path, lines):
        """btrbk keeps only the newest snapshot here. This tool refuses a policy
        that prunes to the latest snapshot as a misconfiguration, so the
        importer writes the default schedule instead and says it keeps more."""
        config, warnings = _load(tmp_path, _btrbk(*lines))
        retention = config.global_config.retention
        assert not is_degenerate_policy(retention)
        assert any("keeps more" in w for w in warnings), warnings
        assert _kept(retention) >= truth.SOURCE_KEEPS[lines]

    def test_a_star_bucket_keeps_every_period(self, tmp_path):
        config, _ = _load(tmp_path, _btrbk("snapshot_preserve 14d 8w *m"))
        assert config.global_config.retention.monthly == 999

    def test_a_day_minimum_keeps_what_btrbk_keeps(self, tmp_path):
        """btrbk's ``2d`` minimum is calendar-granular and inclusive: at 19:19
        on the 23rd it keeps 20260921T0300, which a 48-hour window deletes.
        Written as ``3d`` the window contains btrbk's."""
        config, _ = _load(tmp_path, _btrbk("snapshot_preserve_min 2d"))
        lines = ("snapshot_preserve_min 2d",)
        assert config.global_config.retention.min == "3d"
        kept = _kept(config.global_config.retention)
        assert "20260921T0300" in kept
        assert kept >= truth.SOURCE_KEEPS[lines]

    def test_bucket_counts_keep_what_btrbk_keeps(self, tmp_path):
        """btrbk's ``3d 2w`` keeps the first snapshot of each of days 0..3 and
        weeks 0..2; 20260910T0300, in the third week back, is one of them and
        ``weekly = 2`` deleted it. Written as ``daily = 4, weekly = 3`` every
        snapshot btrbk keeps is kept."""
        lines = ("snapshot_preserve_min 2d", "snapshot_preserve 3d 2w")
        config, _ = _load(tmp_path, _btrbk(*lines))
        retention = config.global_config.retention
        assert (retention.min, retention.daily, retention.weekly) == ("3d", 4, 3)
        kept = _kept(retention)
        assert "20260910T0300" in kept
        assert kept >= truth.SOURCE_KEEPS[lines]


class TestTargetRetentionAgainstBtrbk:
    def _target_retention(self, tmp_path, lines):
        config, warnings = _load(tmp_path, _btrbk("snapshot_preserve_min all", *lines))
        volume = config.volumes[0]
        return config.get_target_retention(volume, volume.targets[0]), warnings

    @pytest.mark.parametrize(
        "lines",
        [(), ("target_preserve 2d",)],
        ids=lambda lines: " / ".join(lines) or "nothing set",
    )
    def test_a_target_with_no_minimum_keeps_everything(self, tmp_path, lines):
        retention, _ = self._target_retention(tmp_path, lines)
        assert retention.min == MIN_KEEP_ALL
        assert _kept(retention) == truth.TARGET_KEEPS[lines]

    @pytest.mark.parametrize(
        "lines",
        [
            ("target_preserve_min no",),
            ("target_preserve_min no", "target_preserve no"),
            ("target_preserve_min latest", "target_preserve no"),
        ],
        ids=lambda lines: " / ".join(lines),
    )
    def test_delete_everything_is_kept_as_more_and_said(self, tmp_path, lines):
        """btrbk deletes every backup but the one the run just sent. The
        importer keeps the default schedule instead and says so."""
        retention, warnings = self._target_retention(tmp_path, lines)
        assert not is_degenerate_policy(retention)
        assert any("keeps more" in w and "target" in w for w in warnings), warnings
        assert _kept(retention) >= truth.TARGET_KEEPS[lines]

    def test_a_target_schedule_with_no_minimum_age_is_faithful(self, tmp_path):
        lines = ("target_preserve_min no", "target_preserve 2d")
        retention, _ = self._target_retention(tmp_path, lines)
        # btrbk keeps days 0..2, three days; three day buckets here.
        assert (retention.min, retention.daily) == ("0s", 3)
        assert _kept(retention) == truth.TARGET_KEEPS[lines]

    def test_a_long_minimum_with_no_schedule_is_all_zero_buckets(self, tmp_path):
        retention, warnings = self._target_retention(
            tmp_path, ("target_preserve_min 1w",)
        )
        assert retention.min == "2w"
        assert (
            retention.hourly,
            retention.daily,
            retention.weekly,
            retention.monthly,
            retention.yearly,
        ) == (0, 0, 0, 0, 0)
        assert not is_degenerate_policy(retention)
        assert not any("keeps more" in w for w in warnings)


# --------------------------------------------------------------------------- #
# Where the target policy lands, and how it resolves
# --------------------------------------------------------------------------- #
class TestTargetPolicyPlacement:
    def test_every_target_gets_its_own_retention_block(self, tmp_path):
        text = (
            "snapshot_preserve_min 2d\nsnapshot_preserve 14d\n"
            "target_preserve_min 1w\ntarget_preserve 4w 6m\n"
            "volume /mnt\n  subvolume home\n    target /mnt/a\n    target /mnt/b\n"
            "      target_preserve_min no\n"
        )
        config, _ = _load(tmp_path, text)
        volume = config.volumes[0]
        source = config.get_effective_retention(volume)
        assert (source.min, source.daily) == ("3d", 15)
        a, b = volume.targets
        ra = config.get_target_retention(volume, a)
        rb = config.get_target_retention(volume, b)
        assert (ra.min, ra.weekly, ra.monthly, ra.daily) == ("2w", 5, 7, 0)
        # Resolved at the narrowest scope that sets it: the target's own line.
        assert (rb.min, rb.weekly, rb.monthly) == ("0s", 5, 7)

    def test_subvolume_scope_reaches_its_targets(self, tmp_path):
        text = (
            "volume /mnt\n  subvolume home\n    target_preserve 7d\n"
            "    target_preserve_min 1d\n    target /mnt/a\n"
            "  subvolume var\n    target /mnt/b\n"
        )
        config, _ = _load(tmp_path, text)
        home, var = config.volumes
        rh = config.get_target_retention(home, home.targets[0])
        rv = config.get_target_retention(var, var.targets[0])
        assert (rh.min, rh.daily) == ("2d", 8)
        assert rv.min == MIN_KEEP_ALL

    def test_the_old_divergence_warnings_are_gone(self, tmp_path):
        """They said the target schedule was not applied and that this project
        keeps one retention per volume. Neither is true now."""
        _, warnings = _load(
            tmp_path, _btrbk("snapshot_preserve 7d", "target_preserve 30d")
        )
        assert not any("NOT applied" in w for w in warnings)
        assert not any("one retention" in w for w in warnings)

    def test_generated_toml_has_target_retention_under_each_target(self, tmp_path):
        toml, _ = _convert(
            "target_preserve 30d\nvolume /mnt\n  subvolume home\n"
            "    target /mnt/a\n    target /mnt/b\n"
        )
        assert toml.count("[volumes.targets.retention]") == 2
        # The target's own keys come first, then its retention table, so the
        # table binds to the array element just opened.
        a = toml.index('path = "/mnt/a"')
        ra = toml.index("[volumes.targets.retention]", a)
        b = toml.index('path = "/mnt/b"')
        assert a < ra < b


class TestTheNumbersAreExplained:
    """A migrated ``14d`` reading ``daily = 15`` needs saying, once in the
    file and once in the import's messages; a policy with nothing to raise
    gets neither."""

    def test_a_raised_block_carries_the_btrbk_lines_and_the_rule(self):
        toml, warnings = _convert(
            _btrbk("snapshot_preserve_min 2d", "snapshot_preserve 14d 4w")
        )
        block = toml[toml.index("[global.retention]") :]
        assert "# btrbk: snapshot_preserve 14d 4w, snapshot_preserve_min 2d" in block
        assert INCLUSIVE_NOTE_COMMENT in block
        assert 'min = "3d"' in block and "daily = 15" in block and "weekly = 5" in block
        assert warnings.count(INCLUSIVE_NOTE) == 1

    def test_a_target_block_names_the_target_directives(self):
        toml, _ = _convert(_btrbk("target_preserve_min no", "target_preserve 2d"))
        block = toml[toml.index("[volumes.targets.retention]") :]
        assert "# btrbk: target_preserve 2d, target_preserve_min no" in block

    def test_the_note_is_said_once_for_many_blocks(self):
        toml, warnings = _convert(
            "snapshot_preserve 7d\ntarget_preserve 3w\nvolume /mnt\n  subvolume a\n"
            "    target /mnt/x\n  subvolume b\n    target /mnt/y\n"
        )
        assert toml.count(INCLUSIVE_NOTE_COMMENT) == 3
        assert warnings.count(INCLUSIVE_NOTE) == 1

    @pytest.mark.parametrize(
        "lines",
        [
            (),
            ("snapshot_preserve *d",),
            ("snapshot_preserve 0d", "snapshot_preserve_min latest"),
            ("snapshot_preserve_min all",),
            ("target_preserve_min no",),
        ],
        ids=lambda lines: " / ".join(lines) or "nothing set",
    )
    def test_nothing_raised_means_no_note(self, lines):
        """``*``, ``0``, ``all``, ``latest`` and ``no`` are written as they
        mean; the note would be a claim about numbers that do not exist."""
        toml, warnings = _convert(_btrbk(*lines))
        assert INCLUSIVE_NOTE_COMMENT not in toml
        assert INCLUSIVE_NOTE not in warnings


class TestTheEngineAtBtrbkTime:
    """The recorded clock is the one the engine is asked at; a table read at
    the wrong time would agree with nothing."""

    def test_now_is_after_every_snapshot(self):
        for ts in truth.ALL:
            assert datetime.strptime(ts, truth.FORMAT) <= truth.NOW
