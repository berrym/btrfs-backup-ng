"""Every directory this package can create is classified here by ROLE, or the suite fails.

The rule (#102), stated by the role of the path rather than by where it was
typed, so that it has no exceptions:

    BACKUP LOCATION   a source, a target, an absolute snapshot base -- however
                      it was given, configuration file or command line -- is a
                      statement that something is there. Never created.
    OUTPUT LOCATION   where a command was asked to put something NEW: the
                      restore destination, verify --temp-dir, a config,
                      completion or man-page output file. Created.
    OWN STATE         this program's state, cache, locks, logs and control
                      sockets. Created.
    BELOW a base      anything under a base verified to exist is created one
                      component at a time, never with parents.

The class was declared closed four times and was open each time, because
each sweep looked where the previous defect had been: the endpoints, then
the engine, then the CLI, then legacy mode. Example-based tests can only
pin the sites someone has already found. This file instead walks the
package's syntax tree for every way a directory can come into being -- a
``mkdir``/``makedirs``/``mkdtemp``/``privileged_mkdir``/``create_below``
call, or ANY string containing ``mkdir`` -- and requires each one to be
registered below with a category, in source order. There is no prose
detection: an error message that names the ``mkdir`` the operator should
run is a site too, registered as MESSAGE, so a command string appearing in
a message-only function changes that function's sequence and fails.

A site that is not registered fails the suite; a registered site that has
gone fails the suite; a category the layer does not allow fails the suite;
and a BELOW site that could rebuild an ancestor (``parents=True``,
``makedirs``, an unguarded ``mkdir -p``) fails the suite.

Categories:

    STATE      own state (see above). Never a backup location.
    OUTPUT     an output location. Allowed only in the CLI layer and in the
               restore and verify engines, whose destination is always an
               output location.
    BELOW      creation strictly below a verified base: the primitive, or
               mkdtemp, or a remote ``mkdir`` inside an ``[ -d target ]`` guard.
    PRIMITIVE  the two helpers in ``__util__`` that everything else uses.
    MESSAGE    a string that mentions mkdir and is never run: error text.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__

PACKAGE = Path(__util__.__file__).resolve().parent

CREATION_CALLS = {
    "mkdir",
    "makedirs",
    "mkdtemp",
    "privileged_mkdir",
    "create_below",
    "copytree",
    "TemporaryDirectory",
}
COMMAND_TOKENS = ("mkdir", "install -d", "install -D", "--mkpath")

STATE, OUTPUT, BELOW, PRIMITIVE, MESSAGE = (
    "STATE",
    "OUTPUT",
    "BELOW",
    "PRIMITIVE",
    "MESSAGE",
)

#: Every creation site in the package: ``file::enclosing function`` -> the
#: category of each site in that function, in source order. Adding a site
#: means adding it here, with a category the layer allows, and saying why in
#: the commit that adds it.
REGISTRY: dict[str, tuple[str, ...]] = {
    # -- the primitives ------------------------------------------------------
    "__util__.py::create_below": (PRIMITIVE, PRIMITIVE),
    "__util__.py::privileged_mkdir": (PRIMITIVE, PRIMITIVE, PRIMITIVE),
    # -- below a verified base ------------------------------------------------
    "_legacy_main.py::prepare_source_endpoint": (BELOW, BELOW),
    "cli/common.py::create_snapshot_dir": (BELOW, BELOW),
    # The restore run marker lives under DESTINATION/.btrfs-backup-ng/, a
    # tree the local endpoint's prepare() has already created below a
    # destination that exists; the marker directory is one component under it.
    "core/layout.py::PlainLayout._write_marker": (BELOW,),
    "core/operations.py::_snapper_prepare_slot": (BELOW,),
    "endpoint/common.py::Endpoint.snapshot": (BELOW,),
    "endpoint/local.py::LocalEndpoint._prepare": (BELOW,),
    "sshutil/lock.py::RemoteLockManager._acquire_script": (BELOW,),
    "sshutil/lock.py::RemoteLockManager.acquire_shared": (BELOW,),
    # -- an output location ----------------------------------------------------
    "cli/config_cmd.py::_init_config": (OUTPUT, OUTPUT),
    "cli/config_cmd.py::_save_wizard_config": (OUTPUT,),
    "cli/config_cmd.py::_run_detection_wizard": (OUTPUT,),
    "cli/completions.py::install_completions": (OUTPUT,),
    "cli/install.py::execute_install": (OUTPUT,),
    "cli/manpages.py::install_manpages": (OUTPUT,),
    "cli/restore.py::_prepare_local_endpoint": (OUTPUT,),
    "cli/snapper_cmd.py::_handle_generate_config": (OUTPUT,),
    "core/restore.py::validate_restore_destination": (OUTPUT,),
    # verify: --temp-dir is an output location; without it a temp is made
    # INSIDE the verified backup location (mkdtemp creates no parents).
    "core/verify.py::verify_full": (OUTPUT, BELOW),
    # -- this program's own state ---------------------------------------------
    "__logger__.py::add_file_handler": (STATE,),
    "transaction.py::set_transaction_log": (STATE,),
    "cli/dispatcher.py::show_migration_notice": (STATE,),
    "cli/run.py::_run_lock_path": (STATE,),
    "core/chunked_transfer.py::ChunkedStreamWriter.write_chunks": (STATE,),
    "core/chunked_transfer.py::ChunkedTransferManager._ensure_cache_dir": (STATE,),
    "core/chunked_transfer.py::ChunkedTransferManager.create_transfer": (STATE,),
    "core/chunked_transfer.py::ChunkedTransferManager.chunk_stream": (STATE,),
    "core/state.py::OperationRecord.save": (STATE,),
    "core/state.py::OperationManager._ensure_state_dir": (STATE, STATE, STATE),
    "endpoint/common.py::_secure_lock_dir": (STATE,),
    "sshutil/diagnose.py::test_btrfs_receive": (STATE,),
    "sshutil/master.py::ensure_operator_known_hosts": (STATE,),
    "sshutil/master.py::SSHMasterManager.__init__": (STATE, STATE),
    # -- error text that names the mkdir the operator should run --------------
    "_legacy_main.py::refuse_silent_new_chain": (MESSAGE,),
    "config/loader.py::_validate_config": (MESSAGE,),
    "endpoint/raw.py::_check_remote_listing": (MESSAGE,),
    "endpoint/raw.py::SSHRawEndpoint._prepare": (MESSAGE, MESSAGE),
    "endpoint/ssh.py::SSHEndpoint._require_remote_destination": (MESSAGE,),
}

#: Where an OUTPUT site may live: the command line, and the two engines
#: whose destination is always an output location.
OUTPUT_LAYERS = ("cli/", "core/restore.py", "core/verify.py")
#: Layers that only ever see backup locations. Nothing here may create one,
#: so nothing here may create anything but below a verified base, or its
#: own state.
BACKUP_LOCATION_LAYERS = (
    "endpoint/",
    "core/operations.py",
    "core/transfer.py",
    "core/planning.py",
    "sshutil/",
    "snapper/",
    "retention.py",
    "_legacy_main.py",
)


class _Site:
    def __init__(self, key, kind, text, lineno, could_build):
        self.key = key
        self.kind = kind  # "call" | "string"
        self.text = text
        self.lineno = lineno
        self.could_build = could_build  # able to build a missing ancestor

    def __repr__(self):
        return f"{self.key} L{self.lineno} {self.kind} {self.text[:50]!r}"


class _Scanner(ast.NodeVisitor):
    def __init__(self, rel: str):
        self.rel = rel
        self.stack: list[str] = []
        self.sites: list[_Site] = []

    def _key(self) -> str:
        return f"{self.rel}::{'.'.join(self.stack) or '<module>'}"

    def visit_ClassDef(self, node):
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    def visit_FunctionDef(self, node):
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Call(self, node):
        f = node.func
        name = None
        if isinstance(f, ast.Attribute):
            name = f.attr
        elif isinstance(f, ast.Name):
            name = f.id
        if name in CREATION_CALLS:
            kw = {k.arg: k.value for k in node.keywords}
            parents_true = (
                isinstance(kw.get("parents"), ast.Constant)
                and kw["parents"].value is True
            )
            # privileged_mkdir defaults to parents=True; only an explicit
            # False makes it a leaf-only call.
            if name == "privileged_mkdir":
                parents_true = not (
                    isinstance(kw.get("parents"), ast.Constant)
                    and kw["parents"].value is False
                )
            could_build = parents_true or name in ("makedirs", "copytree")
            self.sites.append(
                _Site(self._key(), "call", f"{name}()", node.lineno, could_build)
            )
        self.generic_visit(node)

    def _string(self, text: str, lineno: int) -> None:
        if not any(t in text for t in COMMAND_TOKENS):
            return
        # A `mkdir -p` can build every component unless the script first
        # checks that the target is there.
        could_build = "mkdir -p" in text and "[ -d " not in text
        self.sites.append(_Site(self._key(), "string", text, lineno, could_build))

    def visit_JoinedStr(self, node):
        # An f-string is judged whole. Its pieces are not visited again.
        parts = [v.value for v in node.values if isinstance(v, ast.Constant)]
        self._string("".join(str(p) for p in parts), node.lineno)

    def visit_Constant(self, node):
        if isinstance(node.value, str):
            self._string(node.value, node.lineno)


def _strip_docstrings(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            body = getattr(node, "body", [])
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                body[0].value.value = ""


def scan_package() -> list[_Site]:
    sites: list[_Site] = []
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = str(path.relative_to(PACKAGE))
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        _strip_docstrings(tree)
        scanner = _Scanner(rel)
        scanner.visit(tree)
        sites.extend(scanner.sites)
    return sites


def _by_key(sites):
    grouped: dict[str, list[_Site]] = {}
    for s in sites:
        grouped.setdefault(s.key, []).append(s)
    return grouped


def _registered_pairs():
    grouped = _by_key(scan_package())
    for key, cats in REGISTRY.items():
        for site, cat in zip(grouped.get(key, []), cats):
            yield key, site, cat


class TestEveryCreationSiteIsClassified:
    def test_the_scanner_sees_the_sites_it_is_built_to_see(self, tmp_path):
        """The guard is only as good as its scanner. A file with each kind of
        site must yield exactly those sites, and an error message that says
        mkdir is a site like any other."""
        src = tmp_path / "probe.py"
        src.write_text(
            '"""mkdir in a docstring does not count"""\n'
            "import os\n"
            "from pathlib import Path\n"
            "def a(p):\n"
            "    Path(p).mkdir(parents=True, exist_ok=True)\n"
            "def b(p):\n"
            "    os.makedirs(p)\n"
            "def c(h, p):\n"
            "    return f\"ssh {h} 'mkdir -p {p}'. Nothing was created.\"\n"
            "def d(t, r):\n"
            "    return f'if [ -d {t} ]; then mkdir -p {r}; fi'\n"
            "def e(p):\n"
            "    return ['mkdir', p]\n"
            "def f(p):\n"
            "    Path(p).mkdir(exist_ok=True)\n"
        )
        tree = ast.parse(src.read_text())
        _strip_docstrings(tree)
        scanner = _Scanner("probe.py")
        scanner.visit(tree)
        found = [(s.key.split("::")[1], s.kind, s.could_build) for s in scanner.sites]
        assert found == [
            ("a", "call", True),
            ("b", "call", True),
            ("c", "string", True),
            ("d", "string", False),
            ("e", "string", False),
            ("f", "call", False),
        ]

    def test_no_unregistered_creation_site(self):
        grouped = _by_key(scan_package())
        unregistered = {k: v for k, v in grouped.items() if k not in REGISTRY}
        assert not unregistered, (
            "New directory-creation site(s). A backup location is never "
            "created; classify each of these in REGISTRY (STATE / OUTPUT / "
            "BELOW / MESSAGE) and say why in the commit:\n  "
            + "\n  ".join(map(repr, sum(unregistered.values(), [])))
        )

    def test_no_stale_registry_entry(self):
        grouped = _by_key(scan_package())
        stale = [k for k in REGISTRY if k not in grouped]
        assert not stale, f"REGISTRY names sites that no longer exist: {stale}"

    def test_each_function_has_exactly_the_registered_sites(self):
        """Pinned as a sequence, so a new mkdir added INSIDE an already
        classified function -- a message-only function included -- changes
        the sequence and is caught."""
        grouped = _by_key(scan_package())
        mismatched = {
            k: (len(grouped[k]), len(REGISTRY[k]))
            for k in REGISTRY
            if k in grouped and len(grouped[k]) != len(REGISTRY[k])
        }
        assert not mismatched, (
            "site count changed (found, registered): "
            f"{mismatched} -- a creation or a mkdir string was added or removed "
            "in a classified function; re-classify it"
        )

    def test_a_message_is_a_string_never_a_call(self):
        offenders = [
            site
            for _, site, cat in _registered_pairs()
            if cat == MESSAGE and site.kind != "string"
        ]
        assert not offenders, f"registered as MESSAGE but is a call: {offenders}"

    def test_an_output_location_is_created_only_where_one_can_be_named(self):
        offenders = [
            k
            for k, cats in REGISTRY.items()
            if OUTPUT in cats and not k.startswith(OUTPUT_LAYERS)
        ]
        assert not offenders, (
            "OUTPUT creation outside the CLI/restore/verify layers -- the other "
            f"layers only ever see backup locations: {offenders}"
        )

    def test_a_layer_that_only_sees_backup_locations_creates_none(self):
        blind = [
            k
            for k, cats in REGISTRY.items()
            if k.startswith(BACKUP_LOCATION_LAYERS)
            and any(c not in (BELOW, STATE, PRIMITIVE, MESSAGE) for c in cats)
        ]
        assert not blind, f"only BELOW/STATE creation is allowed there: {blind}"

    def test_a_below_site_cannot_build_an_ancestor(self):
        """A BELOW call must be the primitive (which has no parents mode) or
        mkdtemp (which makes one directory and never a parent); a BELOW shell
        string may say mkdir -p only inside an ``[ -d target ]`` guard.
        Anything else could rebuild a missing base -- the exact defect."""
        offenders = []
        for _, site, cat in _registered_pairs():
            if cat != BELOW:
                continue
            if site.kind == "call" and site.text not in ("create_below()", "mkdtemp()"):
                offenders.append(site)
            if site.kind == "string" and site.could_build:
                offenders.append(site)
        assert not offenders, f"BELOW sites that could rebuild a base: {offenders}"

    def test_only_an_output_or_state_site_may_build_ancestors(self):
        """Belt and braces for the sequence check: whatever the registry says,
        a call or script that can build ancestors is OUTPUT, STATE, or text."""
        offenders = [
            site
            for _, site, cat in _registered_pairs()
            if site.could_build and cat not in (OUTPUT, STATE, MESSAGE)
        ]
        assert not offenders, f"{offenders}"

    def test_primitives_live_only_in_util(self):
        offenders = [
            k
            for k, cats in REGISTRY.items()
            if PRIMITIVE in cats and not k.startswith("__util__.py::")
        ]
        assert not offenders


class TestCreateBelow:
    """The primitive: it must be incapable of inventing a base."""

    def test_creates_components_under_an_existing_base(self, tmp_path):
        leaf = __util__.create_below(tmp_path, "a/b", "c", mode=0o700)
        assert leaf == tmp_path / "a" / "b" / "c"
        assert leaf.is_dir()

    def test_with_no_parts_it_only_verifies_the_base(self, tmp_path):
        assert __util__.create_below(tmp_path) == tmp_path
        with pytest.raises(__util__.AbortError):
            __util__.create_below(tmp_path / "absent")

    def test_is_idempotent(self, tmp_path):
        __util__.create_below(tmp_path, "x")
        __util__.create_below(tmp_path, "x")
        assert (tmp_path / "x").is_dir()

    def test_a_missing_base_is_refused_and_nothing_is_created(self, tmp_path):
        base = tmp_path / "unmounted" / "disk"
        with pytest.raises(__util__.AbortError) as e:
            __util__.create_below(base, "backups", what="Destination")
        assert "Destination" in str(e.value)
        assert "Nothing was created" in str(e.value)
        assert not (tmp_path / "unmounted").exists()

    def test_a_base_that_vanishes_after_the_check_is_refused_not_rebuilt(
        self, tmp_path, monkeypatch
    ):
        """Mutation guard: a parents=True in the primitive would rebuild the
        base here and this test would find it on disk."""
        base = tmp_path / "disk"
        base.mkdir()
        real_is_dir = Path.is_dir

        def vanish_then_answer(self):
            answer = real_is_dir(self)
            if self == base and answer:
                base.rmdir()
            return answer

        monkeypatch.setattr(Path, "is_dir", vanish_then_answer)
        with pytest.raises(__util__.AbortError, match="Nothing was created"):
            __util__.create_below(base, "backups", "home")
        assert not base.exists(), "the primitive rebuilt a base that had gone"

    def test_a_file_in_the_way_is_diagnosed(self, tmp_path):
        (tmp_path / "f").write_text("x")
        with pytest.raises(__util__.AbortError, match="not a directory"):
            __util__.create_below(tmp_path, "f", "below")

    def test_escaping_the_base_is_a_programming_error(self, tmp_path):
        with pytest.raises(ValueError):
            __util__.create_below(tmp_path, "../elsewhere")
        with pytest.raises(ValueError):
            __util__.create_below(tmp_path, "/abs")
        assert sorted(p.name for p in tmp_path.iterdir()) == []


class TestAnOutputLocationIsCreated:
    """The other half of the rule, pinned so neither side drifts into the other."""

    def test_a_restore_destination_is_created(self, tmp_path, monkeypatch):
        from btrfs_backup_ng.core import restore

        monkeypatch.setattr(__util__, "is_btrfs", lambda p: True)
        dest = tmp_path / "recovered" / "home"
        restore.validate_restore_destination(dest)
        assert dest.is_dir()

    def test_the_restore_local_endpoint_creates_its_destination(self, tmp_path):
        from btrfs_backup_ng.cli import restore as cli_restore

        dest = tmp_path / "asked" / "for" / "on" / "the" / "command" / "line"
        cli_restore._prepare_local_endpoint(dest)
        assert dest.is_dir()

    def test_verify_temp_dir_is_created(self, tmp_path, monkeypatch):
        from btrfs_backup_ng.core import verify

        monkeypatch.setattr(verify, "_can_run_btrfs_privileged", lambda: True)
        monkeypatch.setattr(__util__, "is_btrfs", lambda p: False)
        temp = tmp_path / "scratch" / "verify"
        ep = type("EP", (), {"config": {"path": str(tmp_path)}})()
        report = verify.verify_full(ep, temp_dir=temp)
        assert temp.is_dir()
        assert any("not on btrfs" in e for e in report.errors)
