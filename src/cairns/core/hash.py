"""Hashing utilities for cache key computation.

Hashers live on `Runtime` instances (see `cairns.core.runtime`). The
default hashers (Path, functools.partial, Pydantic) are installed by
`Runtime.__init__`. For per-Runtime customization use
`runtime.register_hasher(...)`; for tests use `Harness(hash_funcs=...)`.

`resolve_hashable` defaults to the active Run's runtime hashers, falling
back to `default_runtime` when no Run is active (e.g. body fingerprinting
at `@step` decoration time). Pass `hash_funcs=` explicitly to override
for one call.

Limitation: registered hashers (`_hash_partial`) that recursively call
`resolve_hashable` will themselves see the active runtime's funcs (via
the same default-lookup), so transitive overrides do flow through.
"""

from __future__ import annotations

import functools
import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, cast

if TYPE_CHECKING:
    from .runtime import Runtime


def resolve_hashable(
    value: Any,
    _seen: dict[int, bool] | None = None,
    hash_funcs: dict[type, Callable[[Any], Any]] | None = None,
) -> Any:
    """Turn any value into a canonical tree of primitives for hashing.

    `hash_funcs`, if provided, fully replaces the active runtime's
    hashers for this call (and its recursion). Otherwise the active
    Run's runtime is consulted, falling back to `default_runtime`.

    Returns a JSON-serializable structure. Unknown types raise TypeError
    (fail-loud — no silent repr truncation of numpy/pandas/torch objects).
    Cycles are replaced with a sentinel.
    """
    if _seen is None:
        _seen = {}

    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    vid = id(value)
    if vid in _seen:
        return {"__cycle__": True}

    if isinstance(value, bytes):
        return {"__bytes__": value.hex()}

    if isinstance(value, dict):
        d = cast(dict[Any, Any], value)
        _seen[vid] = True
        try:
            return {
                "__dict__": {
                    str(k): resolve_hashable(d[k], _seen, hash_funcs)
                    for k in sorted(d, key=lambda x: str(x))
                }
            }
        finally:
            del _seen[vid]

    if isinstance(value, (list, tuple)):
        seq = cast(list[Any] | tuple[Any, ...], value)
        tag = "__list__" if isinstance(value, list) else "__tuple__"
        _seen[vid] = True
        try:
            return {tag: [resolve_hashable(v, _seen, hash_funcs) for v in seq]}
        finally:
            del _seen[vid]


    if isinstance(value, (frozenset, set)):
        tag = "__frozenset__" if isinstance(value, frozenset) else "__set__"
        fs = cast(set[Any] | frozenset[Any], value)
        _seen[vid] = True
        try:
            items = [resolve_hashable(v, _seen, hash_funcs) for v in fs]
            items.sort(key=lambda x: json.dumps(x, sort_keys=True))
            return {tag: items}
        finally:
            del _seen[vid]

    if hash_funcs is None:
        from .runtime import active_hash_funcs  # noqa: PLC0415

        funcs = active_hash_funcs()
    else:
        funcs = hash_funcs

    for tp in type(value).__mro__:
        if tp in funcs:
            return funcs[tp](value)

    raise TypeError(
        f"Unhashable type for cache key: {type(value).__name__}. "
        f"Register a hasher via `runtime.register_hasher(...)`."
    )


def compute_cairn_id(identity: str, resolved_args: dict[str, Any]) -> str:
    """Compute a cairn id: computation identity + args, excluding version."""
    canonical = json.dumps(
        {
            "identity": identity,
            "args": resolve_hashable(resolved_args),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


# ── Default type hashers ──
#
# These are installed onto `Runtime.hash_funcs` by `_install_defaults`
# in runtime.py. They're public-ish (exported as private-by-convention)
# so subclassing or extending Runtimes can reuse them.


def _hash_path(p: Path) -> Any:
    # No resolve() — symlinks are often deliberate (pointing at a "current"
    # artifact); resolving would invalidate on every target swap. Users
    # wanting content-hashing or resolved paths can re-register.
    path_str = str(p)
    try:
        st = p.stat()
    except FileNotFoundError:
        return {"__path__": {"s": path_str, "state": "missing"}}
    except OSError as e:
        return {"__path__": {"s": path_str, "state": "stat_error", "errno": e.errno}}
    return {"__path__": {"s": path_str, "mtime_ns": st.st_mtime_ns, "size": st.st_size}}


def _hash_partial(p: "functools.partial[Any]") -> Any:
    # Reuse StepInfo.from_function so body edits to p.func invalidate. That
    # also respects @step's attached `.info` (including user overrides).
    from .types import StepInfo

    return {
        "__partial__": {
            "func": StepInfo.from_function(p.func).version,
            "args": [resolve_hashable(a) for a in p.args],
            "keywords": {k: resolve_hashable(v) for k, v in p.keywords.items()},
        }
    }


def _hash_pydantic(model: Any) -> Any:
    # pydantic v2: model_dump(mode="json") coerces datetimes/enums/UUIDs to
    # JSON primitives and recurses into nested models. Class qualname is
    # included so structurally-identical models in different classes don't
    # collide. Schema changes that don't affect dumped values (docstring edits,
    # field reordering) don't invalidate — usually what you want.
    cls_mod = type(model).__module__
    cls_name = type(model).__qualname__
    return {
        "__pydantic__": {
            "cls": f"{cls_mod}:{cls_name}",
            "data": model.model_dump(mode="json"),
        }
    }


def install_defaults(runtime: "Runtime") -> None:
    """Install Path / functools.partial / Pydantic hashers on `runtime`."""
    runtime.hash_funcs[Path] = _hash_path
    runtime.hash_funcs[functools.partial] = _hash_partial
    try:
        from pydantic import BaseModel  # noqa: PLC0415
    except ImportError:
        return
    runtime.hash_funcs[BaseModel] = _hash_pydantic
