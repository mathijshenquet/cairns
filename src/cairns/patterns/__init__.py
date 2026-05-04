"""Higher-order wrappers built from core primitives."""

from __future__ import annotations

import functools
from typing import Awaitable, Callable, ParamSpec, TypeVar

import asyncio

from cairns.core.step import Handle, cached_output, cached_tracing, step, trace
from cairns.core.types import StepInfo, TraceRecord

P = ParamSpec("P")
R = TypeVar("R")


def replayable(fn: Callable[P, Awaitable[R]]) -> Callable[P, Handle[R]]:
    """Wrap a function so it replays from cache with simulated timing.

    On cache hit: replays trace events with original timing, returns cached result.
    On cache miss: calls the real function.
    Indistinguishable from a live execution in the trace.
    """

    @functools.wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        prev: R | None = cached_output()
        traces: list[TraceRecord] | None = cached_tracing()
        if prev is not None and traces is not None:
            for t in traces:
                await asyncio.sleep(t.delta)
                trace(t.message, **t.kwargs)
            return prev
        return await fn(*args, **kwargs)

    info = StepInfo.from_function(fn)
    return step(wrapper, memo=False, identity=info.name, version=info.version)


def rate_limited(n: int, memo: bool = False) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Handle[R]]]:
    """Wrap a function with a concurrency-limiting semaphore.

    The trace shows tasks as "pending" (waiting) vs "running" (executing).
    """
    sem = asyncio.Semaphore(n)

    def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Handle[R]]:
        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            trace("waiting for slot", state="pending")
            async with sem:
                trace("acquired slot", state="running")
                return await fn(*args, **kwargs)

        info = StepInfo.from_function(fn)
        return step(wrapper, memo=memo, identity=info.name, version=info.version)

    return decorator
