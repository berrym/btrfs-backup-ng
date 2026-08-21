"""`restore --list` must not make the operator do work the tool already did.

`restore` infers the prefix a location actually uses and proceeds. `--list` did
not: it printed "No snapshots matched" for a location holding a perfectly good
backup, then told the operator to re-run with a prefix it had just derived
itself. "No snapshots matched" reads as data loss, and the most ordinary command
there is -- `restore --list <dest>` with no --prefix -- produced it.

The two paths now share one primitive, so their rules cannot diverge:

  * infer only when NO prefix was asked for -- an explicit --prefix that matches
    nothing is a mismatch to report, never a cue to list something else;
  * refuse to choose when a location holds more than one prefix, because that
    usually means two volumes side by side and listing the wrong one is worse
    than asking.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


from btrfs_backup_ng.cli.restore import _execute_list


class _Endpoint:
    """Stands in for an endpoint whose snapshots use a prefix."""

    def __init__(self, prefixes: dict[str, int], configured: str = ""):
        self._prefixes = prefixes
        self.config = {"snap_prefix": configured, "path": "/dest"}

    def prefixes_present(self):
        return {
            p: n for p, n in self._prefixes.items() if p != self.config["snap_prefix"]
        }

    def describe_empty_listing(self):
        if self._prefixes:
            return "This location is NOT empty: ..."
        return None

    def list_snapshots(self, flush_cache: bool = False):
        prefix = self.config.get("snap_prefix", "")
        out = []
        for name, count in self._prefixes.items():
            if name == prefix:
                for i in range(count):
                    snap = MagicMock()
                    snap.get_name.return_value = f"{name}{i}"
                    snap.time_obj = None
                    out.append(snap)
        return out


def _run_list(endpoint, prefix=""):
    import argparse

    args = argparse.Namespace(source="/dest", prefix=prefix, config=None)
    with patch(
        "btrfs_backup_ng.cli.restore._prepare_backup_endpoint", return_value=endpoint
    ):
        return _execute_list(args)


class TestListInfersLikeRestoreDoes:
    def test_it_lists_the_prefix_the_location_uses(self, capsys):
        endpoint = _Endpoint({"myvol-": 2}, configured="")
        code = _run_list(endpoint)
        out = capsys.readouterr().out
        assert code == 0, out
        assert "myvol-0" in out, out

    def test_it_says_that_it_inferred(self, capsys):
        """Listing a different set than was asked for must never be silent.

        Asserted on the announcement itself, not on the prefix appearing
        somewhere: the snapshot NAMES contain the prefix, so a looser check
        passes even with the announcement deleted.
        """
        endpoint = _Endpoint({"myvol-": 1}, configured="")
        _run_list(endpoint)
        out = capsys.readouterr().out
        assert "No snapshots use the default prefix here" in out, out
        assert "which is what this location uses" in out, out

    def test_an_explicit_prefix_is_not_second_guessed(self, capsys):
        """Asked for one set, told about another -- that is the harm the
        ambiguity rule exists to prevent, reached from the other side."""
        endpoint = _Endpoint({"myvol-": 1}, configured="wrong-")
        code = _run_list(endpoint, prefix="wrong-")
        out = capsys.readouterr().out
        assert code == 1, out
        assert "NOT empty" in out
        assert "myvol-0" not in out, "an explicit prefix was overridden"

    def test_two_prefixes_are_not_guessed_between(self, capsys):
        endpoint = _Endpoint({"myvol-": 1, "other-": 1}, configured="")
        code = _run_list(endpoint)
        out = capsys.readouterr().out
        assert code == 1, out
        assert "myvol-0" not in out and "other-0" not in out, (
            "it picked one of two volumes to list"
        )

    def test_a_genuinely_empty_location_still_says_so(self, capsys):
        endpoint = _Endpoint({}, configured="")
        code = _run_list(endpoint)
        out = capsys.readouterr().out
        assert code == 0
        assert "No snapshots found" in out
