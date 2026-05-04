"""Core decorator and Handle implementation."""

from __future__ import annotations

import asyncio
import functools
import inspect
import time
from typing import Any, Awaitable, Callable, Generator, Generic, Literal, ParamSpec, TypeVar, overload

from .runtime import current_run, current_span, emit_event
from .store import (
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
    """Awaitable reference to a running step's eventual result."""

    def __init__(self, span: TaskSpan, task: asyncio.Task[R], args_summary: str = "", memo: bool = False) -> None:
        self._span = span
        self._task = task
        emit_event(
            "spawn",
            seq=span.seq,
            parent_seq=span.parent_seq,
            name=span.name,
            kwargs={
                "identity": span.info.name,
                "version": span.info.short_version(),
                "args": args_summary,
                "memo": memo,
            },
        )

    def __await__(self) -> Generator[Any, Any, R]:
        awaiter = current_span.get()
        if awaiter is None:
            return (yield from self._task.__await__())
        emit_event(
            "wait",
            seq=awaiter.seq,
            kwargs={"on": {"kind": "span", "seq": self._span.seq}},
        )
        awaiter.enter_await()
        try:
            result = yield from self._task.__await__()
        finally:
            awaiter.exit_await()
            emit_event("resume", seq=awaiter.seq)
        return result

    def cancel(self) -> None:
        """Cancel the underlying task."""
        self._task.cancel()

    def done(self) -> bool:
        """Check if the task has completed."""
        return self._task.done()

    @property
    def span(self) -> TaskSpan:
        """Access the span for this handle."""
        return self._span


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
    emit_event(
        "trace",
        parent_seq=parent.seq if parent else None,
        message=message,
        kwargs=merged,
    )
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
    own_time = max(0.0, wall - span.suspended_total)  # clamp FP slop
    return SpanMetrics(size=size, own_size=own_size, time=wall, own_time=own_time)


def _build_child_refs(span: TaskSpan) -> list[dict[str, Any]]:
    """Pointer list for each fully-resolved child span, in spawn order."""
    refs: list[dict[str, Any]] = []
    for c in span.child_spans:
        if not (c.cairn_id and c.record_id and c.record_path):
            continue
        refs.append(
            {
                "cairn_id": c.cairn_id,
                "record_id": c.record_id,
                "record_path": c.record_path,
                "short_name": c.name,
                "start_ts_rel": max(0.0, c.start_ts - span.start_ts),
                "end_ts_rel": max(0.0, c.end_ts - span.start_ts),
            }
        )
    return refs


def _build_record_events(span: TaskSpan, child_refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Interleave trace + spawn events, rebased to span.start_ts, ordered by ts."""
    events: list[dict[str, Any]] = [trace_to_event(t, span.start_ts) for t in span.traces]
    for i, c in enumerate(child_refs):
        events.append(
            {
                "kind": "spawn",
                "ts": float(c.get("start_ts_rel", 0.0)),
                "end_ts": float(c.get("end_ts_rel", 0.0)),
                "child_index": i,
                "short_name": c.get("short_name"),
            }
        )
    events.sort(key=lambda e: float(e.get("ts", 0.0)))
    return events


def _replay_record_events(record_path: str, parent_seq: int, base_ts: float) -> None:
    """Stream a record's stored events into the current run trace under parent_seq.

    Each event's stored `ts` is record-relative; we add `base_ts` to synthesize
    a virtual wall-clock time that reconstructs the original flamegraph timing
    — a cached subtree that originally took 2s still spans 2s in the run trace,
    even though replay itself is ~instant.

    Trace lines emit under parent_seq; spawn lines recurse through the record's
    ordered children/ symlinks. The caller emits the enclosing spawn/end for
    `record_path` itself.
    """
    for rec in iter_record_events(record_path):
        rel_ts = float(rec.get("ts", 0.0))
        virtual_ts = base_ts + rel_ts
        kind = rec.get("kind")
        if kind == "trace":
            emit_event(
                "trace",
                ts=virtual_ts,
                parent_seq=parent_seq,
                message=rec.get("message", ""),
                kwargs={**(rec.get("kwargs") or {}), "replayed": True},
            )
        elif kind == "spawn":
            child_record = _event_child_record(record_path, rec)
            if child_record is not None:
                _replay_recalled_span(
                    record_path=child_record,
                    parent_seq=parent_seq,
                    short_name=rec.get("short_name"),
                    spawn_ts=virtual_ts,
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


def _replay_recalled_span(
    *,
    record_path: str,
    parent_seq: int,
    short_name: str | None,
    spawn_ts: float,
) -> None:
    """Emit a recalled spawn+body+end for a record referenced from a parent.

    Events are stamped with virtual wall times starting at `spawn_ts` so the
    reconstructed span bar matches the cached duration.
    """
    info = read_record_info(record_path)
    if info is None:
        return
    sid = current_run().next_seq()
    name = short_name or info.short_name or info.record_id
    emit_event(
        "spawn",
        ts=spawn_ts,
        seq=sid,
        parent_seq=parent_seq,
        name=name,
        kwargs={
            "cairn_id": info.cairn_id,
            "record_id": info.record_id,
            "record_path": info.record_path,
            "origin": "recalled",
        },
    )
    _replay_record_events(record_path, sid, base_ts=spawn_ts)
    emit_event(
        "end",
        ts=spawn_ts + info.duration,
        seq=sid,
        cached=True,
        kwargs={
            "cairn_id": info.cairn_id,
            "record_id": info.record_id,
            "record_path": info.record_path,
            "origin": "recalled",
            "size": 0,
            "own_size": 0,
            "time": info.duration,
            "own_time": info.own_duration,
        },
    )


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


async def _gather_children(span: TaskSpan) -> None:
    """Await any still-running child tasks, counting the wait as suspended time."""
    if not span.child_tasks:
        return
    t0 = time.monotonic()
    await asyncio.gather(*span.child_tasks, return_exceptions=True)
    span.suspended_total += time.monotonic() - t0


def _replay_cached(span: TaskSpan, cached: Record) -> Any:
    """Mirror the cached record into this run's trace and close the span.

    Events inside the cached subtree are stamped with virtual wall times so the
    flamegraph reconstructs the original shape — this span's bar spans exactly
    `cached.duration`, with child spans/traces positioned at their stored offsets.
    """
    span.cached_output_value = cached.result
    span.cached_tracing_value = cached.traces
    span.cairn_id = cached.cairn_id
    span.record_id = cached.record_id
    span.record_path = cached.record_path
    span.cached = True

    # span.start_ts was set to wall time when the wrapper entered; it's also
    # the virtual base for the cached subtree. Align end_ts with the original
    # duration so `end_ts - start_ts` reads as the cached bar width.
    base_ts = span.start_ts
    duration = cached.duration
    own_duration = cached.own_duration

    if cached.record_path:
        _replay_record_events(cached.record_path, span.seq, base_ts=base_ts)
    else:
        for t in cached.traces:
            emit_event(
                "trace",
                parent_seq=span.seq,
                message=t.message,
                kwargs={**t.kwargs, "replayed": True},
            )

    span.end_ts = base_ts + duration
    emit_event(
        "end",
        ts=span.end_ts,
        seq=span.seq,
        cached=True,
        kwargs={
            "cairn_id": cached.cairn_id,
            "record_id": cached.record_id,
            "record_path": cached.record_path,
            "origin": cached.origin,
            "size": 0,
            "own_size": 0,
            "time": duration,
            "own_time": own_duration,
        },
    )
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
            tags=dict(tags or {}),
        ),
        version=info.version,
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
    emit_event(
        "end",
        seq=span.seq,
        kwargs={
            "cairn_id": stats.cairn_id,
            "record_id": stats.record_id,
            "record_path": stats.record_path,
            "origin": "created",
            **metrics.as_kwargs(),
        },
    )
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
            tags=dict(tags or {}),
        ),
        version=info.version,
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
    emit_event(
        "error",
        seq=span.seq,
        error=str(exc),
        kwargs=metrics.as_kwargs(),
    )


# ── step decorator ──


StrOverride = str | Callable[..., str] | None


def _resolve_override(arg: StrOverride, fn: object) -> str | None:
    """Turn an `identity=` or `version=` kwarg into a string override (or None
    to mean "derive from fn")."""
    if arg is None:
        return None
    if isinstance(arg, str):
        return arg
    if callable(arg):
        return arg(fn)
    return None


def _bind_args(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Bind positional and keyword args to parameter names."""
    sig = inspect.signature(fn)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    return dict(bound.arguments)


@overload
def step(fn: Callable[P, Awaitable[R]]) -> Callable[P, Handle[R]]: ...


@overload
def step(
    fn: Callable[P, Awaitable[R]],
    *,
    memo: bool = ...,
    identity: StrOverride = ...,
    version: StrOverride = ...,
    tags: dict[str, str] | None = ...,
) -> Callable[P, Handle[R]]: ...


@overload
def step(
    *,
    memo: bool = ...,
    identity: StrOverride = ...,
    version: StrOverride = ...,
    tags: dict[str, str] | None = ...,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Handle[R]]]: ...


def step(
    fn: Callable[..., Awaitable[Any]] | None = None,
    *,
    memo: bool = False,
    identity: StrOverride = None,
    version: StrOverride = None,
    tags: dict[str, str] | None = None,
) -> Any:
    """Decorator that turns an async function into a tracked step.

    By default, the step always runs (memo=False) — suitable for orchestration.
    Use memo=True for expensive leaf operations (API calls, heavy computation)
    to cache results based on (identity, version, args).

    `identity` / `version` override the derived name / version as strings. To
    forward an existing `StepInfo` through a higher-order wrapper, pass
    `identity=info.name, version=info.version`.

    `tags` is a free-form `dict[str, str]` written into every record this
    step publishes. Patterns can filter the cairn by tags (e.g. semver
    matching). Common uses: `tags={"semver": "1.2.3"}`, `tags={"env": "prod"}`.

    Returns Handle[T] on call instead of awaiting directly.
    """
    if fn is None:
        def decorator(f: Callable[P, Awaitable[R]]) -> Callable[P, Handle[R]]:
            return _make_step(f, memo=memo, identity=identity, version=version, tags=tags)
        return decorator

    return _make_step(fn, memo=memo, identity=identity, version=version, tags=tags)


def _make_step(
    fn: Callable[..., Awaitable[Any]],
    *,
    memo: bool,
    identity: StrOverride,
    version: StrOverride,
    tags: dict[str, str] | None,
) -> Any:
    _info = StepInfo.from_function(
        fn,
        name=_resolve_override(identity, fn),
        version=_resolve_override(version, fn),
    )

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Handle[Any]:
        parent = current_span.get()
        rt = current_run()
        span = TaskSpan(
            seq=rt.next_seq(),
            parent_seq=parent.seq if parent else None,
            name=fn.__name__,
            info=_info,
        )

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
                cached = store.get(key, version=_info.version)

                # Populate span's cached_* fields regardless of memo — they
                # feed `cached_output()` / `cached_tracing()` from inside the
                # body for the memo=False prefill pattern.
                if cached is not None and cached.error is None:
                    span.cached_output_value = cached.result
                    span.cached_tracing_value = cached.traces
                    # Carry-origin hits short-circuit regardless of memo;
                    # memo=True takes any recalled hit as usual.
                    if memo or cached.origin == "carried":
                        return _replay_cached(span, cached)

                emit_event("start", seq=span.seq)
                result = await fn(**resolved)
                await _gather_children(span)
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
                    emit_event("cancel", seq=span.seq)
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
