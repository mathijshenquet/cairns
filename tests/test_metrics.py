"""Tests for own_time / own_size metrics on the `end` event and StoreStats."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from cairns import step
from cairns.core import Record, Event, MemoryStore, StoreStats
from cairns.testing import Harness, TraceInspector


def _end_for(inspector: TraceInspector, name: str) -> Event:
    span = inspector.span(name)
    ends = [e for e in inspector.all_events if e.kind == "end" and e.seq == span.seq]
    assert ends, f"no end event for {name}"
    return ends[0]


# ── StoreStats on put() ──


def test_store_stats_memory():
    store = MemoryStore()
    entry = Record(result={"x": 1}, traces=[], duration=0.1, own_duration=0.1)
    stats = store.put("k1", entry)
    assert isinstance(stats, StoreStats)
    assert stats.size > 0
    assert stats.own_size == stats.size  # no dedup yet


def test_store_stats_filestore(tmp_path: Path) -> None:
    from cairns.core import FileStore

    store = FileStore(str(tmp_path))
    entry = Record(result="hello", traces=[], duration=0.0, own_duration=0.0)
    stats = store.put("k1", entry)
    assert stats.size > 0
    assert stats.own_size == stats.size


# ── own_time ──


@pytest.mark.asyncio
async def test_own_time_sequential():
    """Parent awaits two children sequentially; own_time excludes their wall time."""

    @step
    async def child(ms: int) -> int:
        await asyncio.sleep(ms / 1000)
        return ms

    @step
    async def parent() -> int:
        a = await child(30)
        b = await child(30)
        return a + b

    async with Harness() as rt:
        await parent()

    end = _end_for(rt.trace, "parent")
    wall = end.kwargs["time"]
    own = end.kwargs["own_time"]
    # Parent itself did almost no work; both children were awaited.
    assert wall >= 0.05
    assert own < 0.015, f"own_time should be tiny, got {own}"


@pytest.mark.asyncio
async def test_own_time_gather():
    """Parent awaits two children concurrently; suspend-count covers the union."""

    @step
    async def child(ms: int) -> int:
        await asyncio.sleep(ms / 1000)
        return ms

    @step
    async def parent() -> int:
        a, b = await asyncio.gather(child(40), child(40))
        return a + b

    async with Harness() as rt:
        await parent()

    end = _end_for(rt.trace, "parent")
    wall = end.kwargs["time"]
    own = end.kwargs["own_time"]
    # Both children ran concurrently (~40ms wall). Parent's own_time near 0;
    # critically, not near 80ms (which naive sum-of-durations would give → own<0).
    assert 0.03 <= wall <= 0.15
    assert own < 0.015, f"own_time should be tiny, got {own}"


@pytest.mark.asyncio
async def test_own_time_nested():
    """B awaits C. Parent awaits B. Own times attribute correctly at each level."""

    @step
    async def c() -> int:
        await asyncio.sleep(0.03)
        return 1

    @step
    async def b() -> int:
        r = await c()
        await asyncio.sleep(0.03)  # b's own work
        return r

    @step
    async def a() -> int:
        return await b()

    async with Harness() as rt:
        await a()

    end_a = _end_for(rt.trace, "a")
    end_b = _end_for(rt.trace, "b")
    end_c = _end_for(rt.trace, "c")

    # a did nothing itself — all wait on b
    assert end_a.kwargs["own_time"] < 0.015
    # b's own_time reflects its own sleep, not c's
    assert 0.02 <= end_b.kwargs["own_time"] <= 0.06
    # c's own_time ≈ its sleep
    assert 0.02 <= end_c.kwargs["own_time"] <= 0.06


@pytest.mark.asyncio
async def test_own_time_own_work():
    """A step doing its own sleep (no handle awaits) has own_time ≈ wall."""

    @step
    async def compute() -> int:
        await asyncio.sleep(0.05)
        return 42

    async with Harness() as rt:
        await compute()

    end = _end_for(rt.trace, "compute")
    wall = end.kwargs["time"]
    own = end.kwargs["own_time"]
    # No Handle awaits → own ≈ wall.
    assert abs(wall - own) < 0.01


# ── Cached hit: own_size = 0, own_time measurable ──


@pytest.mark.asyncio
async def test_own_metrics_on_cached_hit():
    """memo=True cache hit: own_size=0 (wrote nothing), own_time is real."""

    @step(memo=True)
    async def leaf(x: int) -> int:
        await asyncio.sleep(0.01)
        return x * 2

    async with Harness() as rt:
        await leaf(3)  # populate
        await leaf(3)  # hit

        ends = [e for e in rt.trace.all_events if e.kind == "end"]
        fresh, hit = ends[0], ends[1]

        assert fresh.kwargs["size"] > 0
        assert fresh.kwargs["own_size"] == fresh.kwargs["size"]
        assert fresh.kwargs["own_time"] >= 0.005

        assert hit.cached is True
        assert hit.kwargs["size"] == 0
        assert hit.kwargs["own_size"] == 0
        assert hit.kwargs["own_time"] >= 0.0  # real measurement, near zero


# ── Error path still emits metrics ──


@pytest.mark.asyncio
async def test_own_metrics_on_error():
    """Error path emits size/own_size/time/own_time on the error event."""

    @step
    async def boom() -> int:
        await asyncio.sleep(0.01)
        raise RuntimeError("nope")

    async with Harness() as rt:
        with pytest.raises(RuntimeError):
            await boom()

    errors = [e for e in rt.trace.all_events if e.kind == "error"]
    assert len(errors) == 1
    err = errors[0]
    assert err.kwargs["size"] > 0
    assert err.kwargs["own_size"] == err.kwargs["size"]
    assert err.kwargs["time"] >= 0.005
    assert err.kwargs["own_time"] >= 0.005


# ── Error propagating through an awaited handle ──


@pytest.mark.asyncio
async def test_error_through_awaited_child():
    """Parent awaits child that raises. Exception propagates; both error events
    emit metrics; enter_await/exit_await stay balanced (no assertion fires)."""

    @step
    async def bad() -> int:
        await asyncio.sleep(0.01)
        raise RuntimeError("child boom")

    @step
    async def parent() -> int:
        return await bad()

    async with Harness() as rt:
        with pytest.raises(RuntimeError):
            await parent()

    errors = [e for e in rt.trace.all_events if e.kind == "error"]
    # Both parent and child report errors.
    assert len(errors) == 2
    for err in errors:
        assert "size" in err.kwargs
        assert "own_time" in err.kwargs
        assert err.kwargs["own_time"] >= 0.0


# ── Spawn-without-await: cleanup gather attributed to await time ──


@pytest.mark.asyncio
async def test_spawn_without_await_cleanup_counts_as_await():
    """A step that spawns a child and returns without awaiting it: the
    structured-concurrency cleanup waits on the orphan. That wait is
    attributed to suspended_total, not own_time."""

    @step
    async def orphan() -> int:
        await asyncio.sleep(0.05)
        return 1

    @step
    async def parent() -> int:
        orphan()  # spawn, discard handle — never awaited explicitly
        return 0

    async with Harness() as rt:
        await parent()

    end = _end_for(rt.trace, "parent")
    wall = end.kwargs["time"]
    own = end.kwargs["own_time"]
    # Parent did near-zero of its own work; wall includes the ~50ms waiting on
    # the orphan during cleanup. own_time must NOT include that wait.
    assert wall >= 0.04
    assert own < 0.015, f"own_time should exclude cleanup gather, got {own}"


# ── Cancellation path emits no stored metrics ──


@pytest.mark.asyncio
async def test_cancel_emits_no_metrics():
    """On cancellation, the span emits a `cancel` event and no `end`/`error`.
    Metrics are deliberately unmeasured on this path."""

    @step
    async def slow() -> int:
        await asyncio.sleep(10)
        return 1

    async with Harness() as rt:
        h = slow()
        await asyncio.sleep(0.01)
        h.cancel()
        with pytest.raises(asyncio.CancelledError):
            await h

        events = rt.trace.all_events
        kinds = [e.kind for e in events if e.kind in ("cancel", "end", "error")]
        assert "cancel" in kinds
        assert "end" not in kinds
        assert "error" not in kinds
