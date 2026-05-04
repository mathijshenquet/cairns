"""Tests for the hashing upgrade: Path / partial / cycles / StepInfo AST walk."""

from __future__ import annotations

import functools
import hashlib
import os
from pathlib import Path

import pytest

from cairn.core import hash as _hash_mod
from cairn.core.hash import (
    clear_hash_funcs,
    compute_cairn_id,
    register_hash_func,
    resolve_hashable,
)
from cairn.core.types import StepInfo


# ── resolve_hashable: cycles ──


def test_cycle_in_dict_does_not_recurse():
    d: dict[str, object] = {"a": 1}
    d["self"] = d
    out = resolve_hashable(d)
    # The cycle entry gets the sentinel; outer structure is still resolved.
    assert out["__dict__"]["a"] == 1
    assert out["__dict__"]["self"] == {"__cycle__": True}


def test_cycle_in_list():
    lst: list[object] = [1, 2]
    lst.append(lst)
    out = resolve_hashable(lst)
    assert out["__list__"][0] == 1
    assert out["__list__"][1] == 2
    assert out["__list__"][2] == {"__cycle__": True}


# ── Path hasher ──


def test_path_hash_existing_file(tmp_path: Path):
    f = tmp_path / "data.txt"
    f.write_text("hello")
    out = resolve_hashable(f)
    assert out["__path__"]["s"] == str(f)
    assert out["__path__"]["size"] == 5
    assert "mtime_ns" in out["__path__"]


def test_path_hash_missing():
    p = Path("/nonexistent/definitely/not/here.txt")
    out = resolve_hashable(p)
    assert out == {"__path__": {"s": str(p), "state": "missing"}}


def test_path_hash_stat_error(tmp_path: Path):
    # Create a path inside a directory we can't traverse.
    # Skip on platforms where we can't reliably reproduce.
    nested = tmp_path / "locked" / "file.txt"
    nested.parent.mkdir()
    nested.write_text("x")
    os.chmod(nested.parent, 0o000)
    try:
        out = resolve_hashable(nested)
        # Either FileNotFoundError (treated as missing) or PermissionError (stat_error)
        # depending on OS; both acceptable.
        assert out["__path__"]["s"] == str(nested)
        assert out["__path__"].get("state") in {"missing", "stat_error"}
    finally:
        os.chmod(nested.parent, 0o755)


def test_path_hash_invalidates_on_mtime_change(tmp_path: Path):
    f = tmp_path / "data.txt"
    f.write_text("a")
    h1 = resolve_hashable(f)
    # Force a clearly different mtime.
    os.utime(f, (1_000_000, 1_000_000))
    h2 = resolve_hashable(f)
    assert h1 != h2


def test_posix_path_subclass_hits_path_hasher(tmp_path: Path):
    # PosixPath is a subclass of Path. MRO-based registry should match.
    f = tmp_path / "x"
    f.write_text("y")
    assert type(f).__name__ in {"PosixPath", "WindowsPath"}
    out = resolve_hashable(f)
    assert "__path__" in out


def test_path_missing_string_does_not_collide_with_arg():
    # A user-passed string that mimics the sentinel should not collide.
    p_hash = resolve_hashable(Path("/nope"))
    s_hash = resolve_hashable("path:/nope:<missing>")
    assert p_hash != s_hash


# ── partial hasher ──


def _helper_fn(a: int) -> int:
    return a + 1


def _helper_with_kw(*, target: object) -> object:
    return target


def test_partial_hash_round_trips():
    p = functools.partial(_helper_fn, 5)
    out = resolve_hashable(p)
    assert "__partial__" in out
    assert out["__partial__"]["args"] == [5]
    assert "func" in out["__partial__"]


def test_partial_invalidates_on_bound_arg_change():
    p1 = functools.partial(_helper_fn, 5)
    p2 = functools.partial(_helper_fn, 10)
    assert resolve_hashable(p1) != resolve_hashable(p2)


def test_partial_with_path_keyword(tmp_path: Path):
    # Path in a bound keyword should route through the Path hasher via
    # resolve_hashable, producing the {"__path__": ...} sentinel nested
    # inside the partial's keywords dict.
    f = tmp_path / "x"
    f.write_text("y")
    p = functools.partial(_helper_with_kw, target=f)
    out = resolve_hashable(p)
    assert "__path__" in out["__partial__"]["keywords"]["target"]


# ── fail-loud on unknown types ──


class _CustomThing:
    def __init__(self, x: int) -> None:
        self.x = x


def test_unknown_type_raises():
    with pytest.raises(TypeError, match="Unhashable type"):
        resolve_hashable(_CustomThing(1))


def test_registered_type_works():
    try:
        register_hash_func(_CustomThing, lambda c: {"thing": c.x})  # type: ignore[attr-defined]
        out = resolve_hashable(_CustomThing(42))
        # Hasher output is trusted verbatim (no __dict__ wrapping).
        assert out == {"thing": 42}
    finally:
        _hash_mod._hash_funcs.pop(_CustomThing, None)  # pyright: ignore[reportPrivateUsage]


def test_clear_reinstalls_defaults():
    clear_hash_funcs()
    try:
        # Path default should be reinstalled.
        out = resolve_hashable(Path("/nope"))
        assert "__path__" in out
    finally:
        clear_hash_funcs()


# ── StepInfo.from_function ──


def test_version_deterministic_across_calls():
    def f(a: int) -> int:
        return a + 1

    v1 = StepInfo.from_function(f)
    v2 = StepInfo.from_function(f)
    assert v1.version == v2.version


def test_version_changes_on_body_edit():
    def f1(a: int) -> int:
        return a + 1

    def f2(a: int) -> int:
        return a + 2

    assert StepInfo.from_function(f1).version != StepInfo.from_function(f2).version


def test_version_resolves_module_constant():
    # A reference to a module-level value should show up in the version hash.
    MAGIC = 7  # noqa: N806 - named constant mimicked via local

    def uses_magic() -> int:
        return MAGIC

    # Closure captures MAGIC=7 by reference; since Python binds by cell, we
    # can't mutate it here without re-defining. Compare against a fresh fn
    # whose closure captures a different value.
    MAGIC2 = 8  # noqa: N806

    def uses_magic2() -> int:
        return MAGIC2

    # Their bodies differ only in the resolved name. But our AST walk records
    # the resolved value, so the hashes must differ.
    assert StepInfo.from_function(uses_magic).version != StepInfo.from_function(uses_magic2).version


def test_stepinfo_trusts_attached_info():
    # A function with a pre-attached StepInfo (like @step) should return it
    # verbatim, respecting user overrides.
    override = StepInfo(name="pinned", version="user-pinned-v1")

    def inner() -> None:
        pass

    inner.info = override  # type: ignore[attr-defined]
    assert StepInfo.from_function(inner) is override


def test_version_unwraps_decorators():
    import functools as ft

    def plain(a: int) -> int:
        return a + 1

    @ft.wraps(plain)
    def wrapper(a: int) -> int:
        return plain(a)

    # unwrap should peel the wrapper and hash the plain body.
    assert StepInfo.from_function(wrapper).version == StepInfo.from_function(plain).version


def test_version_recursion_terminates_on_cycle():
    # Two module-level functions referencing each other via globals.
    # We simulate via a mutable container since real mutual recursion at
    # module level is the common case.
    def a() -> None:
        _b_holder[0]()  # type: ignore[misc]

    def b() -> None:
        _a_holder[0]()  # type: ignore[misc]

    _a_holder = [a]
    _b_holder = [b]
    # Make the globals resolvable for the AST walk:
    a.__globals__["_b_holder"] = _b_holder
    b.__globals__["_a_holder"] = _a_holder

    # Should not infinite-loop.
    v = StepInfo.from_function(a)
    assert isinstance(v.version, str)


def test_version_picks_up_imported_function_body_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Simulates: `import other_module; def test(): return other_module.helper()`
    # Edit `helper`'s body, hash of `test` should change.
    import importlib
    import sys

    pkg = tmp_path / "hashtest_pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "helper_mod.py").write_text("def helper():\n    return 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))  # pyright: ignore[reportUnknownMemberType]

    if "hashtest_pkg" in sys.modules:
        del sys.modules["hashtest_pkg"]
    if "hashtest_pkg.helper_mod" in sys.modules:
        del sys.modules["hashtest_pkg.helper_mod"]

    helper_mod = importlib.import_module("hashtest_pkg.helper_mod")

    def test() -> int:
        return helper_mod.helper()

    v1 = StepInfo.from_function(test).version

    # Edit the imported module's function body.
    (pkg / "helper_mod.py").write_text("def helper():\n    return 999\n")
    importlib.reload(helper_mod)

    v2 = StepInfo.from_function(test).version
    assert v1 != v2, "body edit in imported helper should invalidate caller's version"


def test_version_shared_helper_invalidates_all_callers():
    # A shared helper referenced by multiple peer functions within the same
    # top-level hash walk. Editing helper's body must invalidate every caller
    # — including peers whose AST walk sees helper AFTER a sibling has
    # already visited it. Currently fails because _seen treats any revisit as
    # a cycle, so peer_b's helper ref gets <cycle> instead of helper's hash.
    def make_helper(retval: int):
        def helper() -> int:
            return retval

        return helper

    def make_peers(helper: object) -> tuple[object, object, object]:
        def peer_a() -> int:
            return helper()

        def peer_b() -> int:
            return helper()

        def top() -> int:
            return peer_a() + peer_b()

        # Wire globals so AST walk can resolve the names.
        for fn in (peer_a, peer_b, top):
            fn.__globals__["helper"] = helper
        top.__globals__["peer_a"] = peer_a
        top.__globals__["peer_b"] = peer_b
        return top, peer_a, peer_b

    top_v1, peer_a_v1, peer_b_v1 = make_peers(make_helper(1))
    top_v2, peer_a_v2, peer_b_v2 = make_peers(make_helper(2))

    # Editing helper's body must change each peer's hash independently.
    assert StepInfo.from_function(peer_a_v1).version != StepInfo.from_function(peer_a_v2).version
    assert StepInfo.from_function(peer_b_v1).version != StepInfo.from_function(peer_b_v2).version
    # And the top-level caller.
    assert StepInfo.from_function(top_v1).version != StepInfo.from_function(top_v2).version


def test_version_duplicate_ref_within_one_call_reuses_hash():
    # A function that references the same helper via two paths in a single
    # AST walk should not return <cycle> on the second encounter.
    def helper() -> int:
        return 1

    alias = helper  # noqa: F841 - referenced via globals below

    def caller() -> int:
        return helper() + alias()

    caller.__globals__["helper"] = helper
    caller.__globals__["alias"] = alias

    v = StepInfo.from_function(caller)
    # Both refs resolve to the same function; expectation is both encode to
    # helper's hash, not one of them encoding as <cycle>.
    cycle_marker_hash = hashlib.sha256(b"<cycle>").hexdigest()
    assert cycle_marker_hash not in v.version, (
        "duplicate reference to a non-cyclic function should not encode as <cycle>"
    )


def test_version_sibling_ref_through_shared_helper_invalidates():
    # Realistic pattern: top() references peer_a and peer_b; both call helper.
    # When top is hashed, peer_a's AST walk visits helper first (correct),
    # then peer_b's AST walk sees helper already in _seen and encodes <cycle>.
    # Editing helper's body should still change top's hash — but if peer_b
    # were itself a @step, its isolated hash would be broken.
    def make_graph(helper_body_const: int):
        def helper() -> int:
            return helper_body_const

        def peer_a() -> int:
            return helper()

        def peer_b() -> int:
            return helper()

        def top() -> int:
            return peer_a() + peer_b()

        for fn in (peer_a, peer_b, top):
            fn.__globals__["helper"] = helper
        top.__globals__["peer_a"] = peer_a
        top.__globals__["peer_b"] = peer_b
        return top, peer_b

    _, peer_b_v1 = make_graph(1)
    _, peer_b_v2 = make_graph(2)
    # peer_b isolated: should invalidate when helper body changes. This case
    # already works today (each top-level from_function has its own _seen).
    assert StepInfo.from_function(peer_b_v1).version != StepInfo.from_function(peer_b_v2).version


def test_version_no_source_fallback_is_deterministic():
    # Build two lambdas with identical bytecode via compile(); compare hashes.
    # Lambdas defined in tests usually have source, so we use a code object
    # built via compile() to simulate the no-source path.
    src = "lambda: 1"
    f1 = eval(compile(src, "<string>", "eval"))
    f2 = eval(compile(src, "<string>", "eval"))
    # inspect.getsource fails on these; we fall back to co_code which is
    # deterministic for identical source.
    assert StepInfo.from_function(f1).version == StepInfo.from_function(f2).version


# ── Pydantic integration (optional dependency) ──


def test_pydantic_model_hashable():
    from pydantic import BaseModel

    class User(BaseModel):
        name: str
        age: int

    u = User(name="alice", age=30)
    out = resolve_hashable(u)
    assert out["__pydantic__"]["data"] == {"name": "alice", "age": 30}
    assert out["__pydantic__"]["cls"].endswith(":test_pydantic_model_hashable.<locals>.User")


def test_pydantic_different_classes_dont_collide():
    from pydantic import BaseModel

    class A(BaseModel):
        x: int

    class B(BaseModel):
        x: int

    assert resolve_hashable(A(x=1)) != resolve_hashable(B(x=1))


def test_pydantic_datetime_field():
    # mode="json" converts datetimes to ISO strings; no json.dumps crash.
    from datetime import datetime

    from pydantic import BaseModel

    class Event(BaseModel):
        when: datetime

    e = Event(when=datetime(2026, 1, 1, 12, 0, 0))
    key = compute_cairn_id("id", {"e": e})
    assert isinstance(key, str) and len(key) == 64


def test_pydantic_nested_model():
    from pydantic import BaseModel

    class Inner(BaseModel):
        v: int

    class Outer(BaseModel):
        inner: Inner

    o1 = Outer(inner=Inner(v=1))
    o2 = Outer(inner=Inner(v=2))
    assert resolve_hashable(o1) != resolve_hashable(o2)


def test_pydantic_install_is_guarded(monkeypatch: pytest.MonkeyPatch):
    # Simulate pydantic-not-installed and re-run _install_defaults. The
    # registration should silently skip; Path/partial still register.
    import builtins
    import sys

    from cairn.core import hash as h

    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        if name == "pydantic":
            raise ImportError("simulated")
        return real_import(name, *args, **kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.delitem(sys.modules, "pydantic", raising=False)

    h.clear_hash_funcs()
    # clear_hash_funcs reinstalls defaults; with pydantic import blocked,
    # BaseModel should NOT be in the registry.
    assert Path in h._hash_funcs  # pyright: ignore[reportPrivateUsage]
    try:
        from pydantic import BaseModel

        assert BaseModel not in h._hash_funcs  # pyright: ignore[reportPrivateUsage]
    except ImportError:
        pass  # truly absent — also fine

    # Restore real imports and reinstall so other tests see pydantic.
    monkeypatch.undo()
    h.clear_hash_funcs()


def test_pydantic_subclass_hits_basemodel_registration():
    # MRO walk: any BaseModel subclass hits the registered BaseModel hasher,
    # no per-class registration needed.
    from pydantic import BaseModel

    class Deep(BaseModel):
        v: int

    class Deeper(Deep):
        w: int = 0

    out = resolve_hashable(Deeper(v=5, w=10))
    assert "__pydantic__" in out


# ── compute_cairn_id end-to-end ──


def test_cairn_id_stable():
    k1 = compute_cairn_id("id", {"a": 1, "b": [1, 2]})
    k2 = compute_cairn_id("id", {"b": [1, 2], "a": 1})
    assert k1 == k2


def test_cairn_id_with_path(tmp_path: Path):
    f = tmp_path / "x"
    f.write_text("y")
    k = compute_cairn_id("id", {"f": f})
    assert isinstance(k, str)
    assert len(k) == 64  # sha256 hex


def test_cairn_id_rejects_unknown_type():
    with pytest.raises(TypeError):
        compute_cairn_id("id", {"thing": _CustomThing(1)})
