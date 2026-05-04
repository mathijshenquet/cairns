"""Core types for Cairn."""

from __future__ import annotations

import ast
import asyncio
import builtins
import hashlib
import inspect
import json
import textwrap
import time
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from .hash import compute_cairn_id, resolve_hashable


_UNRESOLVED = object()
_MISSING = object()


def _resolve_name(fn: Any, name: str) -> Any:
    code = getattr(fn, "__code__", None)
    if code is not None:
        freevars = getattr(code, "co_freevars", ())
        if name in freevars:
            idx = freevars.index(name)
            closure = getattr(fn, "__closure__", None)
            if closure is not None and idx < len(closure):
                try:
                    return closure[idx].cell_contents
                except ValueError:
                    return _UNRESOLVED
    globals_ = getattr(fn, "__globals__", None)
    if isinstance(globals_, dict) and name in cast(dict[str, Any], globals_):
        return cast(dict[str, Any], globals_)[name]
    if hasattr(builtins, name):
        return getattr(builtins, name)
    return _UNRESOLVED


def _resolve_attribute_chain(fn: Any, node: ast.Attribute) -> tuple[str, Any]:
    parts: list[str] = []
    cur: ast.AST = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if not isinstance(cur, ast.Name):
        return "", _MISSING
    parts.append(cur.id)
    parts.reverse()
    dotted = ".".join(parts)

    value: Any = _resolve_name(fn, parts[0])
    if value is _UNRESOLVED:
        return dotted, _MISSING
    for attr in parts[1:]:
        try:
            value = getattr(value, attr)
        except AttributeError:
            return dotted, _MISSING
    return dotted, value


def _collect_refs(tree: ast.AST, fn: Any) -> dict[str, Any]:
    code = getattr(fn, "__code__", None)
    local_names: set[str] = set(getattr(code, "co_varnames", ()))

    refs: dict[str, Any] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            dotted, value = _resolve_attribute_chain(fn, node)
            if dotted and dotted.split(".", 1)[0] not in local_names:
                refs[dotted] = value
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            if node.id in local_names or node.id in refs:
                continue
            value = _resolve_name(fn, node.id)
            if value is not _UNRESOLVED:
                refs.setdefault(node.id, value)
    return refs


def _encode_ref(name: str, value: Any, _seen: dict[int, str]) -> str:
    if value is _MISSING:
        return f"{name}=<missing>"
    if inspect.ismodule(value):
        ver = getattr(value, "__version__", None)
        if isinstance(ver, str):
            return f"{name}=<module:{value.__name__}@{ver}>"
        return f"{name}=<module:{value.__name__}>"
    if inspect.isclass(value):
        module = getattr(value, "__module__", "?")
        qualname = getattr(value, "__qualname__", getattr(value, "__name__", "?"))
        return f"{name}=<class:{module}:{qualname}>"
    if inspect.isfunction(value) or inspect.ismethod(value):
        sub = StepInfo.from_function(value, _seen=_seen)
        return f"{name}={sub.body_hash}"
    if inspect.isbuiltin(value):
        module = getattr(value, "__module__", "?")
        qualname = getattr(value, "__qualname__", getattr(value, "__name__", "?"))
        return f"{name}=<builtin:{module}:{qualname}>"

    # Non-callable value: route through resolve_hashable so Path / partial /
    # user-registered hashers apply. Degrade to a stable fallback on TypeError
    # — AST refs include incidental module-level values the function may not
    # actually depend on, so silent passthrough beats blocking decoration.
    try:
        resolved = resolve_hashable(value)
        return f"{name}={json.dumps(resolved, sort_keys=True, separators=(',', ':'))}"
    except TypeError:
        if callable(value):
            module = getattr(value, "__module__", "?")
            qualname = getattr(value, "__qualname__", getattr(value, "__name__", "?"))
            return f"{name}=<callable:{module}:{qualname}>"
        return f"{name}=<opaque:{type(value).__name__}>"


_CYCLE_SENTINEL = hashlib.sha256(b"<cycle>").hexdigest()


def _derive_body_fingerprint(fn: Any, _seen: dict[int, str] | None = None) -> str:
    """Hex digest over fn's source + resolved refs, built incrementally.

    `_seen` dedupes within one walk: the cycle sentinel is stashed on entry
    and replaced by the real digest on exit. A revisit returns whatever is
    there — the sentinel for an in-flight call (true cycle), the final digest
    for a completed one (duplicate ref).
    """
    if _seen is None:
        _seen = {}
    fn_id = id(fn)
    if fn_id in _seen:
        return _seen[fn_id]
    _seen[fn_id] = _CYCLE_SENTINEL

    tree: ast.AST | None
    try:
        source = textwrap.dedent(inspect.getsource(fn))
        tree = ast.parse(source)
    except (OSError, TypeError, SyntaxError):
        code = getattr(fn, "__code__", None)
        if code is not None:
            source = f"<no-source:co_code={code.co_code.hex()}>"
        else:
            tp_name = f"{type(fn).__module__}:{type(fn).__qualname__}"
            source = f"<no-source:{tp_name}>"
        tree = None

    hasher = hashlib.sha256()
    hasher.update(source.encode())
    if tree is not None:
        # AST walk order is deterministic for identical source, and source is
        # already fed into the hash — no need to sort refs here.
        for name, value in _collect_refs(tree, fn).items():
            hasher.update(b"\n")
            hasher.update(_encode_ref(name, value, _seen).encode())

    digest = hasher.hexdigest()
    _seen[fn_id] = digest
    return digest


@dataclass(frozen=True)
class StepInfo:
    """Identification of a step: a nominal name + a structural fingerprint.

    `name` answers "what function is this?" (module:qualname by default, stable
    across edits). `body_hash` answers "which implementation?" — a sha256 digest
    over source + resolved refs, always auto-computed. `version` is an optional
    user-declared release string (e.g. semver), free-form. `cairn_id(args)`
    combines name+args to address the cairn stack; both body_hash and version
    are stored per-record and used at recall time by the memo predicate.
    """

    name: str
    body_hash: str
    version: str | None = None

    @classmethod
    def from_function(
        cls,
        fn: object,
        *,
        name: str | None = None,
        body_hash: str | None = None,
        version: str | None = None,
        _seen: dict[int, str] | None = None,
    ) -> StepInfo:
        """Derive StepInfo from fn. `name` / `body_hash` override their derivation;
        `version` is purely user-supplied (no derivation).

        Respects a pre-attached `.info` (how @step wrappers expose their, possibly
        user-overridden, info to downstream hashers like `_hash_partial`).
        Decorators are peeled via `inspect.unwrap` so the real body is hashed.
        """
        existing = getattr(fn, "info", None)
        if (
            isinstance(existing, StepInfo)
            and name is None
            and body_hash is None
            and version is None
        ):
            return existing

        try:
            unwrapped: Any = inspect.unwrap(fn)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            unwrapped = fn

        if name is None:
            module = getattr(unwrapped, "__module__", "<unknown>")
            qualname = getattr(unwrapped, "__qualname__", getattr(unwrapped, "__name__", "<unknown>"))
            name = f"{module}:{qualname}"

        if body_hash is None:
            body_hash = _derive_body_fingerprint(unwrapped, _seen)

        return cls(name, body_hash, version)

    def short_body_hash(self) -> str:
        return self.body_hash[:8]

    def cairn_id(self, args: dict[str, Any]) -> str:
        return compute_cairn_id(self.name, args)

    def __repr__(self) -> str:
        ver = f", version={self.version!r}" if self.version is not None else ""
        return f"StepInfo({self.name!r}, body_hash={self.short_body_hash()}{ver})"


@dataclass
class TraceRecord:
    """A single trace event, stored for cached_tracing() replay."""

    message: str
    timestamp: float
    delta: float
    kwargs: dict[str, Any] = field(default_factory=lambda: {})


Origin = Literal["recalled", "carried"]


@dataclass
class Record:
    """Stored result of a step invocation.

    `origin` is how the *resolver* reached this entry, not a property of the
    record itself. The default is "recalled" (picked from the cairn stack);
    `OverlayStore` marks its hits as "carried" so the step wrapper knows to
    short-circuit regardless of memo.
    """

    result: Any
    traces: list[TraceRecord]
    error: BaseException | None = None
    duration: float = 0.0
    own_duration: float = 0.0
    cairn_id: str | None = None
    record_id: str | None = None
    record_path: str | None = None
    result_hash: str | None = None
    child_refs: list[dict[str, str]] = field(default_factory=lambda: [])
    origin: Origin = "recalled"
    tags: dict[str, str] = field(default_factory=lambda: {})
    body_hash: str | None = None
    version: str | None = None


@dataclass(frozen=True)
class SpanMetrics:
    """Size/time metrics emitted on a span's terminal event.

    `own_size` excludes bytes deduplicated via a content-addressed layer (today
    equal to `size`). `own_time` is wall time minus time spent awaiting child
    Handles. Cached hits report `size = own_size = 0` (nothing was written).
    """

    size: int
    own_size: int
    time: float
    own_time: float

    def as_kwargs(self) -> dict[str, Any]:
        return {
            "size": self.size,
            "own_size": self.own_size,
            "time": self.time,
            "own_time": self.own_time,
        }


@dataclass
class TaskSpan:
    """Runtime state for a single step invocation."""

    seq: int
    parent_seq: int | None
    name: str
    info: StepInfo

    # Populated during execution
    traces: list[TraceRecord] = field(default_factory=lambda: [])
    cached_output_value: Any = field(default=None)
    cached_tracing_value: list[TraceRecord] | None = field(default=None)
    last_trace_ts: float = field(default=0.0)
    child_tasks: list[asyncio.Task[Any]] = field(default_factory=lambda: [])
    start_ts: float = field(default=0.0)
    end_ts: float = field(default=0.0)
    cached: bool = field(default=False)
    cairn_id: str | None = field(default=None)
    record_id: str | None = field(default=None)
    record_path: str | None = field(default=None)
    child_spans: list[TaskSpan] = field(default_factory=lambda: [])

    # Own-time tracking: wall time minus time spent awaiting child Handles.
    # `suspend_count` counts active Handle awaits (≥1 = this span is suspended);
    # `suspend_start` is the monotonic time the most-recent 0→1 transition began;
    # `suspended_total` accumulates closed intervals when count returns to 0.
    suspend_count: int = field(default=0)
    suspend_start: float = field(default=0.0)
    suspended_total: float = field(default=0.0)

    def enter_await(self) -> None:
        if self.suspend_count == 0:
            self.suspend_start = time.monotonic()
        self.suspend_count += 1

    def exit_await(self) -> None:
        self.suspend_count -= 1
        if self.suspend_count == 0:
            self.suspended_total += time.monotonic() - self.suspend_start

    def record_trace(self, message: str, kwargs: dict[str, Any]) -> None:
        now = time.monotonic()
        delta = now - self.last_trace_ts if self.last_trace_ts > 0 else 0.0
        self.last_trace_ts = now
        self.traces.append(TraceRecord(message, now, delta, kwargs))
