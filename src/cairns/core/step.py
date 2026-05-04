"""Core decorator and Handle implementation."""

from __future__ import annotations

import asyncio
import functools
import inspect
import time
import warnings
from typing import Any, Awaitable, Callable, Generator, Generic, Literal, ParamSpec, TypeVar, cast, overload

from .runtime import (
    CancelEvent,
    EndEvent,
    ErrorEvent,
    ResumeEvent,
    SpawnEvent,
    StartEvent,
    TraceEvent,
    TraceLevel,
    WaitEvent,
    current_run,
    current_run_or_none,
    current_span,
    emit_event,
)
from .store import (
    ChildRef,
    Predicate,
    RecordEventDict,
    SpawnRecordDict,
    Store,
    child_record_path,
    iter_record_events,
    read_record_info,
    trace_to_event,
)
from .types import Record, SpanMetrics, StepInfo, TaskSpan, TraceRecord

P = ParamSpec("P")
R = TypeVar("R")


# ── Handle ──


class Handle(Generic[R]):
    """Awaitable reference to a step invocation.

    Two states:

    - **eager** — the step was called inside an active run. A `TaskSpan`
      and `asyncio.Task` exist; awaiting yields the result.
    - **deferred** — the step was called at top level with no active run.
      `(fn, args, kwargs)` are captured; `cairns.run(handle)` replays the
      call inside a freshly-built run. Awaiting a deferred handle is an
      error; dropping it without consuming warns via `__del__`.

    Eager handles are constructed by the `@step` wrapper inside an active
    run; deferred handles by `Handle._deferred(...)` from the same wrapper
    when no run is active.
    """

    def __init__(self, span: TaskSpan, task: asyncio.Task[R], args_summary: str = "", memo: bool | Predicate = False) -> None:
        # Eager construction. Used by the @step wrapper inside an active run.
        self._span: TaskSpan | None = span
        self._task: asyncio.Task[R] | None = task
        self._fn: Callable[..., Handle[R]] | None = None
        self._args: tuple[Any, ...] = ()
        self._kwargs: dict[str, Any] = {}
        self._consumed: bool = True  # eager handles are inherently "live"
        emit_event(SpawnEvent(
            seq=span.seq,
            parent_seq=span.parent_seq,
            name=span.name,
            identity=span.info.name,
            body_hash=span.info.short_body_hash(),
            version=span.info.version,
            args=args_summary,
            memo=bool(memo),
        ))

    @classmethod
    def deferred(
        cls,
        fn: Callable[..., Handle[R]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Handle[R]:
        """Capture a top-level @step call until `cairns.run(handle)` runs it.

        Used by the `@step` wrapper when there's no active run. The
        returned Handle is in the deferred state until `consume()` is
        called (by `cairns.run`/`arun`).
        """
        h: Handle[R] = cls.__new__(cls)
        h._span = None
        h._task = None
        h._fn = fn
        h._args = args
        h._kwargs = kwargs
        h._consumed = False
        return h

    @property
    def is_deferred(self) -> bool:
        """True if this Handle is captured at top level and waiting for `run()`."""
        return self._task is None

    @property
    def fn_name(self) -> str:
        """Name of the captured step (for run labels / drop warnings)."""
        return getattr(self._fn, "__name__", "<step>") if self._fn is not None else "<step>"

    def consume(self) -> Handle[R]:
        """Replay a deferred Handle inside the now-active run, returning the
        eager Handle that the wrapper produces. Used by `cairns.run`/`arun`.
        """
        if self._task is not None:
            raise RuntimeError(
                "Handle is eager (created inside a run); nothing to consume"
            )
        assert self._fn is not None
        self._consumed = True
        return self._fn(*self._args, **self._kwargs)

    def __await__(self) -> Generator[Any, Any, R]:
        if self._task is None or self._span is None:
            raise RuntimeError(
                "cannot await a deferred Handle outside an active run — "
                "pass it to `cairns.run(handle)` or call from inside a `@step`."
            )
        awaiter = current_span.get()
        if awaiter is None:
            return (yield from self._task.__await__())
        emit_event(WaitEvent(
            seq=awaiter.seq,
            on_kind="span",
            on_seq=self._span.seq,
        ))
        awaiter.enter_await()
        try:
            result = yield from self._task.__await__()
        finally:
            awaiter.exit_await()
            # Max-merge the awaited child's virtual-clock skew into the
            # awaiter. The child inherited `awaiter.virtual_skew` at spawn,
            # then bumped it during its body (cached awaits + nested cached
            # subtrees), so `child.virtual_skew` is the awaiter's logical
            # clock at the moment this child finished. Sequential awaits
            # ratchet (sum); parallel `gather` collapses (max).
            awaiter.virtual_skew = max(awaiter.virtual_skew, self._span.virtual_skew)
            emit_event(ResumeEvent(seq=awaiter.seq))
        return result

    def cancel(self) -> None:
        """Cancel the underlying task."""
        if self._task is not None:
            self._task.cancel()

    def done(self) -> bool:
        """Check if the task has completed. Deferred handles are never done."""
        return self._task is not None and self._task.done()

    @property
    def span(self) -> TaskSpan:
        """Access the span for this handle. Raises if the Handle is deferred."""
        if self._span is None:
            raise RuntimeError("deferred Handle has no span — pass it to `run()` first")
        return self._span

    def __del__(self) -> None:
        # Warn if a deferred Handle is dropped without being consumed by `run()`.
        # Mirrors asyncio's "Task was destroyed but it is pending!" UX.
        # Guarded because `__del__` may fire during interpreter shutdown when
        # the warnings module is partially gone.
        if self._task is None and not self._consumed:
            try:
                warnings.warn(
                    f"deferred {self.fn_name}(...) was dropped without being passed "
                    f"to cairns.run() — its body never executed",
                    ResourceWarning,
                    stacklevel=2,
                )
            except Exception:
                pass


# ── trace() ──


def trace(
    message: str,
    *,
    detail: str = "",
    progress: tuple[int, int] | None = None,
    state: str | None = None,
    level: Literal["info", "warn", "error"] = "info",
    cost: dict[str, int | float] | None = None,
    edge: bool = False,
) -> None:
    """Emit a trace annotation on the current span.

    Fields:
      message  — short label shown on the timeline
      detail   — optional free-form text shown when the trace is selected
      progress — (current, total); renders as a bar
      state    — sub-lifecycle tag ("waiting", "retrying", …)
      level    — severity; "info" (default), "warn", "error"
      cost     — numeric columns summed up the span tree, e.g.
                 {"tokens_in": 10, "tokens_out": 40, "cost_usd": 0.03}
      edge     — mark this trace as an edge annotation (fan-out/retry transition)
    """
    merged: dict[str, Any] = {}
    if detail:
        merged["detail"] = detail
    if progress is not None:
        merged["progress"] = progress
    if state is not None:
        merged["state"] = state
    if level != "info":
        merged["level"] = level
    if cost is not None:
        merged["cost"] = cost
    if edge:
        merged["edge"] = True

    parent = current_span.get()
    emit_event(TraceEvent(
        parent_seq=parent.seq if parent else None,
        message=message,
        detail=detail,
        progress=progress,
        state=state,
        level=level,
        cost=cost,
        edge=edge,
    ))
    if parent is not None:
        parent.record_trace(message, merged)


# ── cached_output() / cached_tracing() ──


def cached_output[T](ty: type[T] | None = None) -> T | None:
    """Get the previous cached result for the current step, or None.

    If `ty` is passed, the cached value must be an instance of it; otherwise
    this returns None instead of silently yielding a stale type after the
    return annotation changed. Without `ty`, any cached value is returned.
    """
    span = current_span.get()
    if span is None:
        return None
    value = span.cached_output_value
    if ty is not None and value is not None and not isinstance(value, ty):
        return None
    return value


def cached_tracing() -> list[TraceRecord] | None:
    """Get the previous trace events for the current step, or None."""
    span = current_span.get()
    if span is None:
        return None
    return span.cached_tracing_value


# ── metrics ──


def _compute_metrics(span: TaskSpan, *, size: int, own_size: int) -> SpanMetrics:
    """Build a SpanMetrics for a span whose `start_ts`/`end_ts` are set.

    Invariant: `suspended_total <= wall`. Violation means Handle.enter_await
    and exit_await got unbalanced — a real bug worth surfacing.
    """
    wall = span.end_ts - span.start_ts
    assert span.suspended_total <= wall + 1e-9, (
        f"suspended_total ({span.suspended_total}) exceeds wall ({wall}) on "
        f"span {span.name!r} — enter/exit_await is unbalanced"
    )
    own_duration = max(0.0, wall - span.suspended_total)  # clamp FP slop
    return SpanMetrics(
        size=size,
        own_size=own_size,
        duration=wall,
        own_duration=own_duration,
        cached_duration=span.cached_duration(),
    )


def _build_child_refs(span: TaskSpan) -> list[ChildRef]:
    """Pointer list for each fully-resolved child span, in spawn order."""
    refs: list[ChildRef] = []
    for c in span.child_spans:
        if not (c.cairn_id and c.record_id and c.record_path):
            continue
        refs.append(ChildRef(
            cairn_id=c.cairn_id,
            record_id=c.record_id,
            record_path=c.record_path,
            short_name=c.name,
            start_ts_rel=max(0.0, c.start_ts - span.start_ts),
            end_ts_rel=max(0.0, c.end_ts - span.start_ts),
        ))
    return refs


def _build_record_events(span: TaskSpan, child_refs: list[ChildRef]) -> list[RecordEventDict]:
    """Interleave trace + spawn events, rebased to span.start_ts, ordered by ts."""
    events: list[RecordEventDict] = [trace_to_event(t, span.start_ts) for t in span.traces]
    for i, c in enumerate(child_refs):
        events.append(SpawnRecordDict(
            kind="spawn",
            ts=c["start_ts_rel"],
            end_ts=c["end_ts_rel"],
            child_index=i,
            short_name=c["short_name"],
        ))
    events.sort(key=lambda e: float(e["ts"]))
    return events


def _replay_trace_event(rec: dict[str, Any], *, parent_seq: int | None) -> TraceEvent:
    """Reconstruct a `TraceEvent` from a stored trace record dict.

    `rec["kwargs"]` holds the user's original `trace(...)` kwargs; pull the
    well-known fields back out, falling through unknowns silently.
    """
    raw_kw = rec.get("kwargs")
    kw: dict[str, Any] = cast(dict[str, Any], raw_kw) if isinstance(raw_kw, dict) else {}

    progress_raw = kw.get("progress")
    progress: tuple[int, int] | None = None
    if isinstance(progress_raw, (list, tuple)):
        seq = cast(list[Any] | tuple[Any, ...], progress_raw)
        if len(seq) == 2:
            progress = (int(seq[0]), int(seq[1]))

    cost_raw = kw.get("cost")
    cost: dict[str, int | float] | None = (
        cast(dict[str, int | float], cost_raw) if isinstance(cost_raw, dict) else None
    )

    level_raw = kw.get("level", "info")
    level: TraceLevel = level_raw if level_raw in ("info", "warn", "error") else "info"

    state_raw = kw.get("state")
    state: str | None = state_raw if isinstance(state_raw, str) else None

    return TraceEvent(
        parent_seq=parent_seq,
        cached=True,
        message=str(rec.get("message", "")),
        detail=str(kw.get("detail", "")),
        progress=progress,
        state=state,
        level=level,
        cost=cost,
        edge=bool(kw.get("edge", False)),
    )


def _stitch_record_events(record_path: str, parent_seq: int) -> None:
    """Stream a record's stored events into the current run trace under parent_seq.

    Stitching = pulling cached events into the live trace as they happen now.
    Each event fires at real `monotonic()` (stamped by `emit_event`); the
    stored relative `ts` offset is *not* added to the wall clock — it would
    push events into the future and make sort-by-ts inversions. Cached
    subtree durations live on the `end` event's `duration` kwarg.

    Trace lines emit under parent_seq; spawn lines recurse through the record's
    ordered children/ symlinks. The caller emits the enclosing spawn/end for
    `record_path` itself.
    """
    for rec in iter_record_events(record_path):
        kind = rec.get("kind")
        if kind == "trace":
            emit_event(_replay_trace_event(rec, parent_seq=parent_seq))
        elif kind == "spawn":
            child_record = _event_child_record(record_path, rec)
            if child_record is not None:
                _stitch_recalled_span(
                    record_path=child_record,
                    parent_seq=parent_seq,
                    short_name=rec.get("short_name"),
                )


def _event_child_record(record_path: str, rec: dict[str, Any]) -> str | None:
    """Resolve a stored child-spawn event to a record path.

    New records use child_index + children/ symlinks. The record_path fallback
    keeps pre-refactor records replayable.
    """
    raw_index = rec.get("child_index")
    if raw_index is not None:
        try:
            child = child_record_path(record_path, int(raw_index))
        except (TypeError, ValueError):
            child = None
        if child is not None:
            return child
    legacy_path = rec.get("record_path")
    if isinstance(legacy_path, str) and read_record_info(legacy_path) is not None:
        return legacy_path
    return None


def _stitch_recalled_span(
    *,
    record_path: str,
    parent_seq: int,
    short_name: str | None,
) -> None:
    """Emit a recalled spawn+body+end for a record referenced from a parent.

    Events fire at real time (stamped by `emit_event`). The recalled span's
    original duration travels on the `end` event's `duration` kwarg.
    """
    info = read_record_info(record_path)
    if info is None:
        return
    sid = current_run().next_seq()
    name = short_name or info.short_name or info.record_id
    emit_event(SpawnEvent(
        seq=sid,
        parent_seq=parent_seq,
        name=name,
        origin="recalled",
        cairn_id=info.cairn_id,
        record_id=info.record_id,
        record_path=info.record_path,
    ))
    _stitch_record_events(record_path, sid)
    emit_event(EndEvent(
        seq=sid,
        cached=True,
        cairn_id=info.cairn_id,
        record_id=info.record_id,
        record_path=info.record_path,
        origin="recalled",
        size=0,
        own_size=0,
        duration=info.duration,
        own_duration=info.own_duration,
        cached_duration=info.cached_duration,
    ))


# ── wrapper helpers ──


async def _resolve_args(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> dict[str, Any]:
    """Bind call args and await any `Handle` arguments.

    Handle-awaits count toward the span's suspended_total via `Handle.__await__`,
    so they're excluded from own_time.
    """
    resolved: dict[str, Any] = {}
    for k, v in _bind_args(fn, args, kwargs).items():
        resolved[k] = await v if isinstance(v, Handle) else v
    return resolved


async def _gather_children(span: TaskSpan, *, reraise: bool = False) -> None:
    """Await any still-running child tasks, counting the wait as suspended time.

    With `reraise=True`, surface the first child exception after all siblings
    finish — closing the structured-concurrency contract so an unawaited child
    that raised can't be silently swallowed by a successful parent. Siblings
    are not cancelled mid-flight; we wait for them, then raise. To model
    expected failure inside a step, return a sentinel value rather than
    raising (see `tests/test_resume.py::test_resume_fanout_partial_failure`).
    """
    if not span.child_tasks:
        return
    t0 = time.monotonic()
    results = await asyncio.gather(*span.child_tasks, return_exceptions=True)
    span.suspended_total += time.monotonic() - t0
    if reraise:
        for r in results:
            if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                raise r


def _stitch_cached(span: TaskSpan, cached: Record) -> Any:
    """Stitch a cached record into this run's trace and close the span.

    "Stitch", not "replay" — we pull the cached subtree's events into the live
    trace as `monotonic()` events. The original execution's duration travels
    on the `end` event's `duration` kwarg; nothing in the timestamp itself
    encodes how long it took originally.
    """
    span.cached_output_value = cached.result
    span.cached_tracing_value = cached.traces
    span.cairn_id = cached.cairn_id
    span.record_id = cached.record_id
    span.record_path = cached.record_path
    span.cached = True
    # The cache-hit subtree contributes (its own original wall) + (whatever
    # nested cached supply was recorded when it was first stored) to the
    # awaiter's logical clock. `virtual_skew` carries the full contribution
    # so `Handle.__await__`'s max-merge sees it; the end event's
    # `cached_duration` kwarg keeps the same "additional supply, excluding
    # own duration" semantic that live spans use.
    duration = cached.duration
    own_duration = cached.own_duration
    span.virtual_skew = span.virtual_skew_initial + duration + cached.cached_duration

    if cached.record_path:
        _stitch_record_events(cached.record_path, span.seq)
    else:
        for t in cached.traces:
            emit_event(_replay_trace_event(
                {"message": t.message, "kwargs": dict(t.kwargs)},
                parent_seq=span.seq,
            ))

    span.end_ts = time.monotonic()
    emit_event(EndEvent(
        seq=span.seq,
        cached=True,
        cairn_id=cached.cairn_id,
        record_id=cached.record_id,
        record_path=cached.record_path,
        origin=cached.origin,
        size=0,
        own_size=0,
        duration=duration,
        own_duration=own_duration,
        cached_duration=cached.cached_duration,
    ))
    return cached.result


def _publish_success(
    span: TaskSpan,
    result: Any,
    resolved: dict[str, Any],
    info: StepInfo,
    store: Store,
    key: str,
    tags: dict[str, str] | None = None,
) -> Any:
    """Push a new record for a successful execution and emit the end event."""
    span.end_ts = time.monotonic()
    wall = span.end_ts - span.start_ts
    own_time = wall - span.suspended_total

    child_refs = _build_child_refs(span)
    events_stream = _build_record_events(span, child_refs)
    stats = store.put(
        key,
        Record(
            result=result,
            traces=list(span.traces),
            duration=wall,
            own_duration=own_time,
            cached_duration=span.cached_duration(),
            tags=dict(tags or {}),
            body_hash=info.body_hash,
            version=info.version,
        ),
        metadata={
            "short_name": span.name,
            "children": child_refs,
            "args_repr": {k: repr(v)[:120] for k, v in resolved.items()},
            "start_ts": span.start_ts,
            "events": events_stream,
            "tags": dict(tags or {}),
        },
    )
    span.cairn_id = stats.cairn_id
    span.record_id = stats.record_id
    span.record_path = stats.record_path
    metrics = _compute_metrics(span, size=stats.size, own_size=stats.own_size)
    emit_event(EndEvent(
        seq=span.seq,
        cached=False,
        cairn_id=stats.cairn_id,
        record_id=stats.record_id,
        record_path=stats.record_path,
        origin="created",
        size=metrics.size,
        own_size=metrics.own_size,
        duration=metrics.duration,
        own_duration=metrics.own_duration,
        cached_duration=metrics.cached_duration,
    ))
    return result


def _publish_error(
    span: TaskSpan,
    resolved: dict[str, Any],
    info: StepInfo,
    store: Store,
    exc: BaseException,
    tags: dict[str, str] | None = None,
) -> None:
    """Persist an error record for browsability and emit the error event."""
    span.end_ts = time.monotonic()
    wall = span.end_ts - span.start_ts
    own_time = wall - span.suspended_total
    stored_error: Exception | None = exc if isinstance(exc, Exception) else None
    child_refs = _build_child_refs(span)
    events_stream = _build_record_events(span, child_refs)
    stats = store.put(
        info.cairn_id(resolved),
        Record(
            result=None,
            traces=list(span.traces),
            error=stored_error,
            duration=wall,
            own_duration=own_time,
            cached_duration=span.cached_duration(),
            tags=dict(tags or {}),
            body_hash=info.body_hash,
            version=info.version,
        ),
        metadata={
            "short_name": span.name,
            "children": child_refs,
            "args_repr": {k: repr(v)[:120] for k, v in resolved.items()},
            "events": events_stream,
            "tags": dict(tags or {}),
        },
    )
    span.cairn_id = stats.cairn_id
    span.record_id = stats.record_id
    span.record_path = stats.record_path
    metrics = _compute_metrics(span, size=stats.size, own_size=stats.own_size)
    emit_event(ErrorEvent(
        seq=span.seq,
        error=str(exc),
        size=metrics.size,
        own_size=metrics.own_size,
        duration=metrics.duration,
        own_duration=metrics.own_duration,
        cached_duration=metrics.cached_duration,
    ))


# ── step decorator ──


StrOverride = str | Callable[..., str] | None


def _resolve_override(arg: StrOverride, fn: object) -> str | None:
    """Turn an `identity=` or `body_hash=` kwarg into a string override (or None
    to mean "derive from fn")."""
    if arg is None:
        return None
    if isinstance(arg, str):
        return arg
    if callable(arg):
        return arg(fn)
    return None


def _resolve_memo_predicate(memo: bool | Predicate, info: StepInfo) -> Predicate | None:
    """Predicate that decides which prior record counts as a memo hit.

    `memo=False`  → None (any non-error record; powers prefill but not replay).
    `memo=True`   → match by `version` if declared, else `body_hash`.
    `memo=callable` → user-supplied predicate, used verbatim.
    """
    if callable(memo):
        return memo
    if memo is True:
        if info.version is not None:
            target = info.version
            return lambda r: r.version == target
        target_hash = info.body_hash
        return lambda r: r.body_hash == target_hash
    return None


def _bind_args(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Bind positional and keyword args to parameter names."""
    sig = inspect.signature(fn)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    return dict(bound.arguments)


@overload
def step(fn: Callable[P, Awaitable[R]], /) -> Callable[P, Handle[R]]: ...


@overload
def step(
    *,
    memo: bool | Predicate = ...,
    identity: StrOverride = ...,
    body_hash: StrOverride = ...,
    version: str | None = ...,
    tags: dict[str, str] | None = ...,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Handle[R]]]: ...


def step(
    fn: Callable[..., Awaitable[Any]] | None = None,
    *,
    memo: bool | Predicate = False,
    identity: StrOverride = None,
    body_hash: StrOverride = None,
    version: str | None = None,
    tags: dict[str, str] | None = None,
) -> Any:
    """Decorator that turns an async function into a tracked step.

    By default, the step always runs (memo=False) — suitable for orchestration.
    Use memo=True for expensive leaf operations (API calls, heavy computation):
    a memo hit replays the most recent record matching the step's identity.

    Memo matching:
      memo=True  — match by `version` if declared, else by `body_hash`.
      memo=False — never short-circuit; record/replay only via carry overlay.
      memo=Callable[[Record], bool] — custom predicate over prior records.
                                      Most general form; you pick the rule.

    `version` is a free-form user-declared release string (e.g. "1.2.3"). When
    set, edits to the function body don't bust the cache; bumping `version`
    does. Without `version`, the auto-computed `body_hash` (sha256 over source
    + resolved refs) is the invalidation key.

    `identity` overrides the derived name (module:qualname). `body_hash`
    overrides the derived structural digest — primarily for higher-order
    wrappers that want to forward an inner step's identity verbatim.

    `tags` is a free-form `dict[str, str]` written into every record. Useful
    for downstream filtering (e.g. `tags={"env": "prod"}`).

    Returns Handle[T] on call instead of awaiting directly.
    """
    if fn is None:
        def decorator(f: Callable[P, Awaitable[R]]) -> Callable[P, Handle[R]]:
            return _make_step(f, memo=memo, identity=identity, body_hash=body_hash, version=version, tags=tags)
        return decorator

    return _make_step(fn, memo=memo, identity=identity, body_hash=body_hash, version=version, tags=tags)


def _make_step(
    fn: Callable[..., Awaitable[Any]],
    *,
    memo: bool | Predicate,
    identity: StrOverride,
    body_hash: StrOverride,
    version: str | None,
    tags: dict[str, str] | None,
) -> Any:
    _info = StepInfo.from_function(
        fn,
        name=_resolve_override(identity, fn),
        body_hash=_resolve_override(body_hash, fn),
        version=version,
    )

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Handle[Any]:
        # No active run: capture the call as a deferred Handle. The user
        # will hand this to `cairns.run(handle)` which replays it inside a
        # freshly-built run context (where this same wrapper goes eager).
        if current_run_or_none() is None:
            return Handle[Any].deferred(wrapper, args, kwargs)

        parent = current_span.get()
        rt = current_run()
        span = TaskSpan(
            seq=rt.next_seq(),
            parent_seq=parent.seq if parent else None,
            name=fn.__name__,
            info=_info,
        )
        # Inherit the parent's virtual clock so sequential cached awaits
        # ratchet (each child sees the inflated skew its older sibling left
        # behind) while parallel `gather`s share the same starting point and
        # max-merge in `Handle.__await__`. Snapshot for the body-end delta.
        if parent is not None:
            span.virtual_skew = parent.virtual_skew
            span.virtual_skew_initial = parent.virtual_skew

        async def run() -> Any:
            token = current_span.set(span)
            span.start_ts = time.monotonic()
            span.last_trace_ts = span.start_ts
            resolved: dict[str, Any] = {}
            try:
                resolved = await _resolve_args(fn, args, kwargs)

                store = current_run().store
                key = _info.cairn_id(resolved)
                # Stash cairn_id on the span early so `cairn()` works inside
                # the body even before publish.
                span.cairn_id = key
                predicate = _resolve_memo_predicate(memo, _info)
                cached = store.find(key, predicate)

                # Populate span's cached_* fields regardless of memo — they
                # feed `cached_output()` / `cached_tracing()` from inside the
                # body for the memo=False prefill pattern.
                if cached is not None and cached.error is None:
                    span.cached_output_value = cached.result
                    span.cached_tracing_value = cached.traces
                    # Carry-origin hits short-circuit regardless of memo;
                    # memo (bool or predicate) takes a hit as usual.
                    if memo or cached.origin == "carried":
                        return _stitch_cached(span, cached)

                emit_event(StartEvent(seq=span.seq))
                result = await fn(**resolved)
                await _gather_children(span, reraise=True)
                return _publish_success(span, result, resolved, _info, store, key, tags=tags)

            except BaseException as exc:
                # Let siblings finish before propagating, or `asyncio.run()`
                # would cancel still-running fan-out branches mid-flight.
                # Cancellation itself is left to propagate fast.
                if not isinstance(exc, asyncio.CancelledError):
                    await _gather_children(span)
                    _publish_error(span, resolved, _info, current_run().store, exc, tags=tags)
                else:
                    span.end_ts = time.monotonic()
                    emit_event(CancelEvent(seq=span.seq))
                raise

            finally:
                current_span.reset(token)

        # Build short args summary for display
        def _summarize_arg(v: Any) -> str:
            if isinstance(v, Handle):
                return "..."
            s = repr(v)
            return s if len(s) <= 30 else s[:27] + "..."

        try:
            bound_preview = _bind_args(fn, args, kwargs)
            args_parts = [f"{_summarize_arg(v)}" for v in bound_preview.values()]
            args_summary = ", ".join(args_parts)
        except Exception:
            args_summary = ""

        task = asyncio.create_task(run())

        # Register with parent for structured concurrency
        if parent is not None:
            parent.child_tasks.append(task)
            parent.child_spans.append(span)

        return Handle(span, task, args_summary, memo)

    def _cairn_view(*args: Any, **kwargs: Any) -> Any:
        """The cairn for this step + args, without invoking.

        Reads `current_run().store`. Raises if no Run is active.
        """
        from cairns.core.cairn import Cairn  # noqa: PLC0415

        bound = _bind_args(fn, args, kwargs)
        cid = _info.cairn_id(bound)
        return Cairn(cid, current_run().store)

    # Attach metadata
    wrapper.info = _info  # type: ignore[attr-defined]
    wrapper.cairn = _cairn_view  # type: ignore[attr-defined]
    wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
    return wrapper
