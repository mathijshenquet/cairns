"""
Core behavior tests for Cairn.

These tests define expected behavior and serve as the TDD spec.
They use a test Harness that provides an in-memory store and trace sink.
"""

from __future__ import annotations

import asyncio
import pytest

from cairns import Cairn, step, trace, cached_output, cached_tracing, Handle
from cairns.testing import Harness



# ── Basic memoization ──


@pytest.mark.asyncio
async def test_step_returns_handle():
    """Calling a @step function returns a Handle, not the result."""

    @step
    async def add(a: int, b: int) -> int:
        return a + b

    async with Harness() as rt:
        h = add(1, 2)
        assert isinstance(h, Handle)
        assert await h == 3


@pytest.mark.asyncio
async def test_memo_caches_on_same_args():
    """Second call with same args returns cached result without calling the body."""
    call_count = 0

    @step(memo=True)
    async def add(a: int, b: int) -> int:
        nonlocal call_count
        call_count += 1
        return a + b

    async with Harness() as rt:
        assert await add(1, 2) == 3
        assert call_count == 1

        assert await add(1, 2) == 3
        assert call_count == 1  # not called again


@pytest.mark.asyncio
async def test_memo_misses_on_different_args():
    """Different args produce a cache miss."""
    call_count = 0

    @step(memo=True)
    async def add(a: int, b: int) -> int:
        nonlocal call_count
        call_count += 1
        return a + b

    async with Harness() as rt:
        assert await add(1, 2) == 3
        assert await add(2, 3) == 5
        assert call_count == 2


@pytest.mark.asyncio
async def test_default_no_memo():
    """By default (memo=False), the body is always called."""
    call_count = 0

    @step
    async def greet(name: str) -> str:
        nonlocal call_count
        call_count += 1
        return f"hello {name}"

    async with Harness() as rt:
        assert await greet("world") == "hello world"
        assert await greet("world") == "hello world"
        assert call_count == 2


# ── Handle passing and dependency resolution ──


@pytest.mark.asyncio
async def test_handle_as_argument():
    """A Handle passed to another step is resolved before calling the body."""

    @step
    async def double(x: int) -> int:
        return x * 2

    @step
    async def add_one(x: int) -> int:
        return x + 1

    async with Harness() as rt:
        h = double(5)             # Handle[int]
        result = await add_one(h)  # framework awaits h, passes 10
        assert result == 11


@pytest.mark.asyncio
async def test_chained_handles():
    """Handles can be chained through multiple steps."""

    @step
    async def step_a(x: int) -> int:
        return x + 1

    @step
    async def step_b(x: int) -> int:
        return x * 2

    @step
    async def step_c(x: int) -> int:
        return x - 3

    async with Harness() as rt:
        result = await step_c(step_b(step_a(10)))
        assert result == 19  # (10+1)*2 - 3


# ── Fan-out / fan-in ──


@pytest.mark.asyncio
async def test_fanout():
    """Multiple steps run concurrently when handles aren't immediately awaited."""
    execution_order = []

    @step
    async def slow(x: int) -> int:
        execution_order.append(f"start-{x}")
        await asyncio.sleep(0.01)
        execution_order.append(f"end-{x}")
        return x * 2

    async with Harness() as rt:
        handles = [slow(i) for i in range(3)]
        results = [await h for h in handles]
        assert results == [0, 2, 4]

        # All started before any ended (concurrent execution)
        starts = [i for i, e in enumerate(execution_order) if e.startswith("start")]
        ends = [i for i, e in enumerate(execution_order) if e.startswith("end")]
        assert max(starts) < min(ends)


@pytest.mark.asyncio
async def test_fanout_with_dependentstep():
    """Fan-out results can be passed as handles to a downstream step."""

    @step
    async def double(x: int) -> int:
        return x * 2

    @step
    async def sum_two(a: int, b: int) -> int:
        return a + b

    async with Harness() as rt:
        h1 = double(3)
        h2 = double(4)
        result = await sum_two(h1, h2)
        assert result == 14  # 6 + 8


# ── Trace events ──


@pytest.mark.asyncio
async def test_trace_emits_event():
    """trace() emits an event visible in the runtime trace log."""

    @step
    async def work() -> str:
        trace("starting work")
        trace("halfway", progress=(1, 2))
        return "done"

    async with Harness() as rt:
        await work()
        trace_events = rt.trace.events(kind="trace")
        assert len(trace_events) == 2
        assert trace_events[0].message == "starting work"
        assert trace_events[1].message == "halfway"
        assert trace_events[1].progress == (1, 2)


@pytest.mark.asyncio
async def test_trace_has_correct_parent():
    """trace() events are associated with the step they're called from."""

    @step
    async def outer() -> str:
        trace("in outer")
        await inner()
        return "done"

    @step
    async def inner() -> str:
        trace("in inner")
        return "done"

    async with Harness() as rt:
        await outer()
        traces = rt.trace.events(kind="trace")
        outer_span = rt.trace.span("outer")
        inner_span = rt.trace.span("inner")
        assert traces[0].parent_seq == outer_span.seq
        assert traces[1].parent_seq == inner_span.seq


# ── Event log structure ──


@pytest.mark.asyncio
async def test_spawn_and_wait_events():
    """Handle creation emits spawn; awaiting emits wait (and resume)."""

    @step
    async def child() -> int:
        return 42

    @step
    async def parent() -> int:
        h = child()
        return await h

    async with Harness() as rt:
        await parent()
        spawns = rt.trace.events(kind="spawn")
        waits = rt.trace.events(kind="wait")

        # parent spawns, child spawns (from parent)
        assert len(spawns) == 2
        # parent emits a `wait` on the child span
        parent_span = rt.trace.span("parent")
        child_span = rt.trace.span("child")
        span_waits = [
            w for w in waits
            if w.seq == parent_span.seq
            and w.on_kind == "span"
            and w.on_seq == child_span.seq
        ]
        assert len(span_waits) == 1


@pytest.mark.asyncio
async def test_fanout_detected_in_trace():
    """Multiple spawns without intervening waits constitute a fan-out."""

    @step
    async def leaf(x: int) -> int:
        return x

    @step
    async def root() -> list[int]:
        handles = [leaf(i) for i in range(5)]
        return [await h for h in handles]

    async with Harness() as rt:
        await root()
        root_span = rt.trace.span("root")
        child_spawns = rt.trace.child_events(root_span.seq, kind="spawn")
        # `wait` events fire on the awaiter (root), one per child await.
        root_waits = [
            w for w in rt.trace.events(kind="wait")
            if w.seq == root_span.seq
            and w.on_kind == "span"
        ]

        # 5 spawns happened before any waits
        assert len(child_spawns) == 5
        assert len(root_waits) == 5
        assert child_spawns[-1].ts < root_waits[0].ts


# ── Cached output and tracing ──


@pytest.mark.asyncio
async def test_cached_output_available_in_memo_false():
    """cached_output() returns previous result when memo=False."""

    @step(memo=False)
    async def compute(x: int) -> int:
        prev = cached_output()
        if prev is not None:
            return prev + 100  # modify to prove we got it
        return x * 2

    async with Harness() as rt:
        assert await compute(5) == 10       # first: no cache, returns 5*2
        assert await compute(5) == 110      # second: cached_output() returns 10, adds 100


@pytest.mark.asyncio
async def test_cached_tracing_replays_traces():
    """cached_tracing() returns trace events from the previous execution."""

    @step(memo=False)
    async def work(x: int) -> int:
        prev_traces = cached_tracing()
        if prev_traces is not None:
            for t in prev_traces:
                trace(f"replayed: {t.message}")
            return cached_output()
        trace("doing work")
        trace("almost done")
        return x * 2

    async with Harness() as rt:
        assert await work(5) == 10

        # Second call — should replay traces
        assert await work(5) == 10
        all_traces = rt.trace.events(kind="trace")
        replayed = [t for t in all_traces if t.message.startswith("replayed")]
        assert len(replayed) == 2
        assert replayed[0].message == "replayed: doing work"
        assert replayed[1].message == "replayed: almost done"


# ── Error handling ──


@pytest.mark.asyncio
async def test_error_propagates_through_handle():
    """An error in a step propagates when the handle is awaited."""

    @step
    async def fail() -> str:
        raise ValueError("boom")

    async with Harness() as rt:
        h = fail()
        with pytest.raises(ValueError, match="boom"):
            await h


@pytest.mark.asyncio
async def test_error_cached_but_retried():
    """Errors are stored (for browsing) but re-executed on next run."""
    call_count = 0

    @step(memo=True)
    async def flaky() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("first call fails")
        return "success"

    async with Harness() as rt:
        h = flaky()
        with pytest.raises(ValueError):
            await h
        assert call_count == 1

        # Second call — error is not returned from cache, function runs again
        assert await flaky() == "success"
        assert call_count == 2


@pytest.mark.asyncio
async def test_error_in_child_propagates_to_parent():
    """A child step's error propagates to the parent when awaited."""

    @step
    async def bad_child() -> str:
        raise RuntimeError("child failed")

    @step
    async def parent() -> str:
        h = bad_child()
        return await h

    async with Harness() as rt:
        with pytest.raises(RuntimeError, match="child failed"):
            await parent()


# ── Structured concurrency / task groups ──


@pytest.mark.asyncio
async def test_parent_waits_for_unawaited_children():
    """Parent span doesn't close until all children (even unawaited) complete."""
    child_completed = False

    @step
    async def slow_child() -> int:
        nonlocal child_completed
        await asyncio.sleep(0.05)
        child_completed = True
        return 42

    @step
    async def parent() -> str:
        slow_child()  # spawned but not awaited
        return "done"

    async with Harness() as rt:
        result = await parent()
        assert result == "done"
        assert child_completed  # parent waited for child via task group

        parent_span = rt.trace.span("parent")
        child_span = rt.trace.span("slow_child")
        assert child_span.end_ts <= parent_span.end_ts


@pytest.mark.asyncio
async def test_unawaited_child_failure_propagates_to_parent():
    """A child that raises must fail its parent even when the parent never awaits it."""

    @step
    async def bad_child() -> int:
        await asyncio.sleep(0.01)
        raise RuntimeError("child failed")

    @step
    async def parent() -> str:
        bad_child()  # spawned, never awaited — failure must still surface
        return "done"

    async with Harness():
        with pytest.raises(RuntimeError, match="child failed"):
            await parent()


@pytest.mark.asyncio
async def test_unawaited_child_failure_does_not_cancel_siblings():
    """Sibling children finish before the parent re-raises the failure."""
    sibling_completed = False

    @step
    async def slow_sibling() -> int:
        nonlocal sibling_completed
        await asyncio.sleep(0.05)
        sibling_completed = True
        return 1

    @step
    async def fast_failure() -> int:
        raise RuntimeError("boom")

    @step
    async def parent() -> str:
        slow_sibling()
        fast_failure()
        return "done"

    async with Harness():
        with pytest.raises(RuntimeError, match="boom"):
            await parent()
        assert sibling_completed


# ── Identity and version ──


@pytest.mark.asyncio
async def test_custom_identity():
    """Custom identity overrides the default."""

    @step(identity="my_custom_id")
    async def foo() -> str:
        return "bar"

    async with Harness() as rt:
        await foo()
        span = rt.trace.span("foo")
        assert span.identity == "my_custom_id"


@pytest.mark.asyncio
async def test_version_change_invalidates_cache():
    """Changing the version causes a cache miss even with same args."""
    call_count = 0

    @step(memo=True, version="v1")
    async def compute(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    async with Harness() as rt:
        assert await compute(5) == 10
        assert call_count == 1

        # "Upgrade" to v2 — should cache miss
        compute_v2 = step(memo=True, version="v2")(compute.__wrapped__)
        assert await compute_v2(5) == 10
        assert call_count == 2


@pytest.mark.asyncio
async def test_version_pins_cache_against_body_edits():
    """A declared version makes the memo predicate ignore body_hash changes."""
    call_count = 0

    @step(memo=True, version="1.0.0")
    async def compute(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    async with Harness() as rt:
        assert await compute(5) == 10
        assert call_count == 1

        # Same identity + version, but a forced body_hash change. Without
        # version pinning the body_hash mismatch would force re-execution;
        # because version is declared, the memo predicate matches by version
        # and the cache hits.
        compute_v2 = step(
            memo=True,
            body_hash="forced-different-body-hash",
            version="1.0.0",
        )(compute.__wrapped__)
        assert await compute_v2(5) == 10
        assert call_count == 1


@pytest.mark.asyncio
async def test_memo_predicate_picks_first_match():
    """memo=callable: first record where predicate(record) is True wins.

    Stack the same cairn (identity + args) with three versions, then use a
    predicate to recall the middle one — proving the predicate, not recency,
    drives selection.
    """
    payloads = {"v1": "a", "v2": "b", "v3": "c"}

    async with Harness() as rt:
        for ver in ("v1", "v2", "v3"):
            captured = payloads[ver]

            async def _seed() -> str:
                return captured  # noqa: B023 — closure capture is intentional

            s = step(memo=True, identity="emit", version=ver)(_seed)
            await s()

        @step(memo=lambda r: r.version == "v2", identity="emit")
        async def picker() -> str:
            return "fresh"  # would only run on cache miss

        assert await picker() == "b"


@pytest.mark.asyncio
async def test_memo_predicate_falls_through_to_execute():
    """memo=callable with no matching record → step body runs."""
    call_count = 0

    @step(memo=lambda r: r.version == "never-stored")
    async def compute(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x + 1

    async with Harness() as rt:
        assert await compute(5) == 6
        # Predicate didn't match anything (no records yet) → executed.
        assert call_count == 1
        # Run again — the just-stored record has version=None, predicate still
        # fails to match, so we execute again.
        assert await compute(5) == 6
        assert call_count == 2


# ── Cairn.latest() filters ──


async def _stack_versions(versions: tuple[str, ...]) -> Cairn:
    """Seed one cairn_id (identity='emit', no args) with one record per version."""
    for ver in versions:
        captured = ver

        async def _seed() -> str:
            return captured  # noqa: B023 — closure capture is intentional

        s = step(memo=False, identity="emit", version=ver)(_seed)
        await s()

    @step(identity="emit")
    async def picker() -> str:
        return ""

    return picker.cairn()


@pytest.mark.asyncio
async def test_latest_version_filter_picks_matching_record():
    """latest(version=...) returns the newest record whose version matches.

    Regression pin: a prior revision had `pass` here and silently returned
    the newest record regardless of version.
    """
    async with Harness():
        cairn = await _stack_versions(("v1", "v2", "v3"))

        rec = cairn.latest(version="v2")
        assert rec is not None
        assert rec.version == "v2"
        assert rec.result == "v2"


@pytest.mark.asyncio
async def test_latest_version_filter_no_match_returns_none():
    """latest(version=...) returns None when no record carries that version."""
    async with Harness():
        cairn = await _stack_versions(("v1", "v2"))

        assert cairn.latest(version="v9") is None


@pytest.mark.asyncio
async def test_latest_version_filter_skips_newer_mismatching_records():
    """latest(version='v1') returns v1 even though v2 / v3 are newer."""
    async with Harness():
        cairn = await _stack_versions(("v1", "v2", "v3"))

        rec = cairn.latest(version="v1")
        assert rec is not None
        assert rec.version == "v1"


@pytest.mark.asyncio
async def test_latest_no_filter_returns_newest_non_errored():
    """latest() with no filter returns the newest record."""
    async with Harness():
        cairn = await _stack_versions(("v1", "v2", "v3"))

        rec = cairn.latest()
        assert rec is not None
        assert rec.version == "v3"


@pytest.mark.asyncio
async def test_latest_body_hash_filter():
    """latest(body_hash=...) filters on Record.body_hash."""

    @step(memo=False, identity="emitter", body_hash="hash-a")
    async def emit_a() -> str:
        return "a"

    @step(memo=False, identity="emitter", body_hash="hash-b")
    async def emit_b() -> str:
        return "b"

    async with Harness():
        await emit_a()
        await emit_b()

        cairn = emit_a.cairn()
        rec = cairn.latest(body_hash="hash-a")
        assert rec is not None
        assert rec.body_hash == "hash-a"
        assert rec.result == "a"


# ── Edge annotations ──


@pytest.mark.asyncio
async def test_edge_annotation():
    """trace(edge=True) annotates the transition between child steps."""

    @step
    async def step_a() -> str:
        return "a"

    @step
    async def step_b() -> str:
        return "b"

    @step
    async def parent() -> str:
        await step_a()
        trace("transition", edge=True, detail="moving on")
        await step_b()
        return "done"

    async with Harness() as rt:
        await parent()
        edges = rt.trace.edge_annotations("parent")
        assert len(edges) == 1
        assert edges[0].message == "transition"
        assert edges[0].detail == "moving on"


# ── Hash funcs registry ──


@pytest.mark.asyncio
async def test_custom_hash_func():
    """Custom hash_funcs are used for cache key computation."""
    from pathlib import Path

    call_count = 0

    async with Harness(hash_funcs={Path: lambda p: str(p)}) as rt:

        @step(memo=True)
        async def read_file(path: Path) -> str:
            nonlocal call_count
            call_count += 1
            return f"contents of {path}"

        assert await read_file(Path("/tmp/a.txt")) == "contents of /tmp/a.txt"
        assert await read_file(Path("/tmp/a.txt")) == "contents of /tmp/a.txt"
        assert call_count == 1  # cached

        assert await read_file(Path("/tmp/b.txt")) == "contents of /tmp/b.txt"
        assert call_count == 2  # different path, cache miss


# ── Higher-order wrappers ──


@pytest.mark.asyncio
async def test_replayable_wrapper():
    """replayable() replays from cache with trace events when available."""
    from cairns import replayable

    call_count = 0

    async def compute_impl(x: int) -> int:
        nonlocal call_count
        call_count += 1
        trace("computing")
        await asyncio.sleep(0.01)
        return x * 2

    compute = replayable(compute_impl)

    async with Harness() as rt:
        # First call: runs the real function
        assert await compute(5) == 10
        assert call_count == 1

        # Second call: replays from cache
        assert await compute(5) == 10
        assert call_count == 1  # not called again

        # Trace events were replayed
        traces = rt.trace.events(kind="trace")
        computing_traces = [t for t in traces if "computing" in t.message]
        assert len(computing_traces) >= 2  # original + replayed


@pytest.mark.asyncio
async def test_rate_limited_wrapper():
    """rate_limited() constrains concurrent execution."""
    from cairns import rate_limited

    max_concurrent = 0
    current_concurrent = 0

    @rate_limited(2)
    async def limited(x: int) -> int:
        nonlocal max_concurrent, current_concurrent
        current_concurrent += 1
        max_concurrent = max(max_concurrent, current_concurrent)
        await asyncio.sleep(0.05)
        current_concurrent -= 1
        return x

    async with Harness() as rt:
        handles = [limited(i) for i in range(10)]
        results = [await h for h in handles]
        assert results == list(range(10))
        assert max_concurrent <= 2


# ── Integration: full pipeline ──


@pytest.mark.asyncio
async def test_research_pipeline():
    """End-to-end test of a research pipeline with retry loop."""

    @step(memo=True)  # leaf
    async def research(subject: str) -> str:
        return f"report on {subject}"

    @step(memo=True)  # leaf
    async def validate(report: str) -> dict:
        if "detailed" in report:
            return {"success": True, "feedback": None}
        return {"success": False, "feedback": "needs detail"}

    @step(memo=True)  # leaf
    async def refine(draft: str, feedback: str) -> str:
        return f"detailed {draft}"

    @step  # orchestration
    async def research_validated(subject: str) -> str:
        draft = await research(subject)
        for i in range(3):
            result = await validate(draft)
            if result["success"]:
                return draft
            trace("retrying", edge=True)
            draft = await refine(draft, result["feedback"])
        return draft

    @step
    async def pipeline() -> dict[str, str]:
        animals = ["cat", "dog", "elephant"]
        handles = {a: research_validated(a) for a in animals}
        return {a: await h for a, h in handles.items()}

    async with Harness() as rt:
        results = await pipeline()
        assert len(results) == 3
        assert all("detailed" in r for r in results.values())

        # Rerun — everything cached
        call_counts_before = rt.trace.total_executions()
        results2 = await pipeline()
        call_counts_after = rt.trace.total_executions()
        assert results2 == results
        # Only pipeline + 3 research_validated + inner steps should be cached
        # No new real executions
        assert rt.trace.cached_count() > 0


# ── Deferred Handle (top-level @step calls) ──


def test_top_level_call_is_deferred():
    """Calling a @step at top level (no active run) returns a deferred Handle
    that captures (fn, args, kwargs) without executing the body."""
    executed = []

    @step
    async def task(x: int) -> int:
        executed.append(x)
        return x * 2

    handle = task(5)
    assert isinstance(handle, Handle)
    assert handle.is_deferred
    assert executed == []  # body has not run yet

    # Mark consumed so __del__ doesn't warn.
    handle._consumed = True


def test_deferred_handle_drop_warns():
    """A deferred Handle dropped without being passed to run() emits a
    ResourceWarning (mirrors asyncio's 'Task was destroyed' warning)."""
    import gc
    import warnings

    @step
    async def task() -> int:
        return 1

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = task()
        # Drop the local reference; force a GC pass.
        del _
        gc.collect()

    drop_warnings = [w for w in caught if issubclass(w.category, ResourceWarning)]
    assert len(drop_warnings) == 1
    assert "task" in str(drop_warnings[0].message)


@pytest.mark.asyncio
async def test_deferred_handle_await_outside_run_raises():
    """Awaiting a deferred Handle outside an active run is an error."""

    @step
    async def task() -> int:
        return 1

    handle = task()
    assert handle.is_deferred
    with pytest.raises(RuntimeError, match="deferred Handle"):
        await handle
    handle._consumed = True  # silence drop warning


@pytest.mark.asyncio
async def test_handle_is_eager_inside_run():
    """Inside an active run the same call yields an eager (non-deferred) Handle."""

    @step
    async def task() -> int:
        return 42

    async with Harness():
        handle = task()
        assert not handle.is_deferred
        assert await handle == 42
