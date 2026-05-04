"""Test utilities for Cairn."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from cairns.core.runtime import Runtime, Event, MemorySink, Run
from cairns.core.store import MemoryStore


@dataclass
class SpanInfo:
    """Summary info about a completed span."""

    seq: int
    name: str
    parent_seq: int | None
    identity: str
    cached: bool
    start_ts: float
    end_ts: float


class TraceInspector:
    """Convenience API for inspecting trace events in tests."""

    def __init__(self, sink: MemorySink) -> None:
        self._sink = sink

    @property
    def all_events(self) -> list[Event]:
        """All events in order."""
        return list(self._sink.events)

    def events(self, kind: str) -> list[Event]:
        """Get all events of a given kind."""
        return [e for e in self._sink.events if e.kind == kind]

    def span(self, name: str) -> SpanInfo:
        """Find a span by step name. Returns the first match."""
        spawns = [e for e in self._sink.events if e.kind == "spawn" and e.name == name]
        if not spawns:
            raise KeyError(f"No span found with name {name!r}")
        spawn = spawns[0]
        span_seq = spawn.seq
        assert span_seq is not None

        ends = [e for e in self._sink.events if e.kind == "end" and e.seq == span_seq]
        end_ts = ends[0].ts if ends else 0.0
        cached = ends[0].cached if ends and ends[0].cached is not None else False

        identity_str: str = spawn.kwargs.get("identity", "")

        return SpanInfo(
            seq=span_seq,
            name=name,
            parent_seq=spawn.parent_seq,
            identity=identity_str,
            cached=cached,
            start_ts=spawn.ts,
            end_ts=end_ts,
        )

    def span_name(self, span_seq: int) -> str | None:
        """Get the name of a span by its sequence number."""
        for e in self._sink.events:
            if e.kind == "spawn" and e.seq == span_seq:
                return e.name
        return None

    def child_events(self, parent_seq: int, kind: str) -> list[Event]:
        """Get events of a given kind that are children of a span.

        For `wait` events (which fire on the awaiter), matches on event seq
        equal to parent_seq. For other events, matches on parent_seq.
        """
        if kind == "wait":
            return [e for e in self._sink.events if e.kind == kind and e.seq == parent_seq]
        return [e for e in self._sink.events if e.kind == kind and e.parent_seq == parent_seq]

    def edge_annotations(self, parent_name: str) -> list[Event]:
        """Get trace events with edge=True under a named parent."""
        parent = self.span(parent_name)
        return [
            e
            for e in self._sink.events
            if e.kind == "trace"
            and e.parent_seq == parent.seq
            and e.kwargs.get("edge") is True
        ]

    def total_executions(self) -> int:
        """Count total start events (real executions, not cached)."""
        return len([e for e in self._sink.events if e.kind == "start"])

    def cached_count(self) -> int:
        """Count cached end events."""
        return len([e for e in self._sink.events if e.kind == "end" and e.cached is True])


class Harness:
    """Test harness — in-memory `Run` plus a `TraceInspector`.

    Usage:

        async with Harness() as h:
            await my_pipeline()
            assert h.trace.span("step_x").cached is False

    Pass `hash_funcs=` to register per-test hashers without polluting
    other tests; alternatively pass `runtime=` to use a configured
    `Runtime` (e.g. one constructed with custom serializers).
    """

    def __init__(
        self,
        hash_funcs: dict[type, Callable[[Any], Any]] | None = None,
        runtime: Runtime | None = None,
    ) -> None:
        if runtime is None:
            runtime = Runtime()
        if hash_funcs:
            for tp, fn in hash_funcs.items():
                runtime.register_hasher(tp, fn)
        self._runtime = runtime
        self._sink = MemorySink()
        self._run = Run(
            runtime=runtime,
            store=MemoryStore(),
            sink=self._sink,
        )
        self.trace = TraceInspector(self._sink)

    @property
    def runtime(self) -> Runtime:
        return self._runtime

    async def __aenter__(self) -> "Harness":
        self._run.__enter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        self._run.__exit__(*args)
