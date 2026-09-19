"""An out-of-range retention `min` reached the destructive prune path.

`timedelta` raises OverflowError once a value exceeds its range. OverflowError
is an ArithmeticError, not a ValueError, so none of the three `except ValueError`
guards wrapping the duration helpers caught it:

    config/loader.py   promises ConfigError    -- propagated OverflowError
    retention.py       promises RetentionError -- propagated OverflowError
    cli/prune.py       pre-check               -- propagated OverflowError

`min = "999999999999d"` therefore passed the loader's boundary check and raised
inside `apply_retention`, which is the destructive path.

The second, larger half of the defect: the loader validated with
`parse_duration` while the engine runs `subtract_duration`, and those two accept
DIFFERENT sets of values. "3000y", "999999999d" and "100000000w" all parse to a
perfectly good timedelta and only fail when subtracted from the current time. So
the boundary check was not merely leaky -- it was testing a different function
from the one whose failure it existed to prevent, which is the recurring defect
this release is about: a check whose verdict does not describe the operation it
guards.

Both helpers now report out-of-range as ValueError at the producer, so all three
existing guards work unchanged, and the loader validates through
`subtract_duration`.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from btrfs_backup_ng.config import ConfigError, load_config
from btrfs_backup_ng.retention import (
    RetentionConfig,
    RetentionError,
    apply_retention,
    parse_duration,
    subtract_duration,
)

# Values that overflow timedelta itself, so parse_duration must reject them.
PARSE_OVERFLOWS = ["1000000000d", "999999999999d", "99999999y", "99999999M"]

# Values parse_duration accepts but that cannot be subtracted from now. These
# are the ones the old boundary check waved through.
SUBTRACT_ONLY_OVERFLOWS = ["999999999d", "100000000w", "3000y"]

USABLE = ["1s", "30m", "12h", "1d", "30d", "2w", "6M", "1y", "100y"]


def _config_text(min_value: str) -> str:
    return (
        '[[volumes]]\npath = "/data"\n'
        '[[volumes.targets]]\npath = "/backup"\n'
        f'[volumes.retention]\nmin = "{min_value}"\n'
    )


def _load(tmp_path, min_value: str):
    path = tmp_path / "config.toml"
    path.write_text(_config_text(min_value))
    return load_config(path)


class TestProducersReportOutOfRangeAsValueError:
    @pytest.mark.parametrize("duration", PARSE_OVERFLOWS)
    def test_parse_duration_raises_value_error(self, duration):
        with pytest.raises(ValueError) as excinfo:
            parse_duration(duration)
        assert not isinstance(excinfo.value, OverflowError)
        assert duration in str(excinfo.value)

    @pytest.mark.parametrize("duration", PARSE_OVERFLOWS + SUBTRACT_ONLY_OVERFLOWS)
    def test_subtract_duration_raises_value_error(self, duration):
        with pytest.raises(ValueError) as excinfo:
            subtract_duration(datetime.now(), duration)
        assert not isinstance(excinfo.value, OverflowError)
        assert duration in str(excinfo.value)

    def test_the_message_names_the_value_not_an_internal_year(self):
        """ "year -974 is out of range" sent the operator hunting the wrong thing."""
        with pytest.raises(ValueError) as excinfo:
            subtract_duration(datetime.now(), "3000y")
        message = str(excinfo.value)
        assert "3000y" in message
        assert "out of range" in message.lower()

    @pytest.mark.parametrize("duration", USABLE)
    def test_usable_durations_are_untouched(self, duration):
        assert isinstance(parse_duration(duration), timedelta)
        assert isinstance(subtract_duration(datetime.now(), duration), datetime)


class TestTheBoundaryCheckDescribesTheOperationItGuards:
    @pytest.mark.parametrize("duration", PARSE_OVERFLOWS + SUBTRACT_ONLY_OVERFLOWS)
    def test_loader_refuses_every_unusable_min(self, tmp_path, duration):
        with pytest.raises(ConfigError) as excinfo:
            _load(tmp_path, duration)
        assert duration in str(excinfo.value)

    @pytest.mark.parametrize("duration", USABLE)
    def test_loader_still_accepts_every_usable_min(self, tmp_path, duration):
        config, _ = _load(tmp_path, duration)
        assert config.volumes[0].retention.min == duration

    @pytest.mark.parametrize(
        "duration", PARSE_OVERFLOWS + SUBTRACT_ONLY_OVERFLOWS + USABLE
    )
    def test_loader_verdict_matches_engine_verdict(self, tmp_path, duration):
        """The invariant. Whatever the loader accepts, the engine must be able to run.

        This is the guard against the two functions drifting apart again: it
        fails if the loader ever admits a value apply_retention cannot use, in
        either direction.
        """
        try:
            _load(tmp_path, duration)
        except ConfigError:
            loader_accepts = False
        else:
            loader_accepts = True

        try:
            subtract_duration(datetime.now(), duration)
        except ValueError:
            engine_accepts = False
        else:
            engine_accepts = True

        assert loader_accepts == engine_accepts, (
            f"loader and engine disagree about {duration!r}: "
            f"loader_accepts={loader_accepts}, engine_accepts={engine_accepts}"
        )


class TestTheDestructivePathFailsClosed:
    @pytest.mark.parametrize("duration", PARSE_OVERFLOWS + SUBTRACT_ONLY_OVERFLOWS)
    def test_apply_retention_raises_retention_error_not_overflow(self, duration):
        """Reached directly, the prune path must still fail loud and CLOSED."""
        config = RetentionConfig(min=duration)
        with pytest.raises(RetentionError):
            apply_retention([object()], config, get_name=lambda s: "snap.20260101")

    @pytest.mark.parametrize("duration", PARSE_OVERFLOWS + SUBTRACT_ONLY_OVERFLOWS)
    def test_degenerate_precheck_does_not_propagate(self, duration):
        """cli/prune's pre-check must not raise out of a refusal decision."""
        from btrfs_backup_ng.cli.prune import is_degenerate_policy

        assert is_degenerate_policy(RetentionConfig(min=duration)) is False
