"""Context tracking and event emission."""

from __future__ import annotations

import itertools
import time
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, Protocol

from .types import TaskSpan


# Current span context — set when a step is executing
current_span: ContextVar[TaskSpan | None] = ContextVar("current_span", default=None)

# ID counter
_id_counter = itertools.count(1)


def next_id() -> int:
    """Get the next unique span ID."""
    return next(_id_counter)


def reset_id_counter() -> None:
    """Reset the ID counter (for testing)."""
    global _id_counter
    _id_counter = itertools.count(1)


# ── Event types ──


@dataclass
class Event:
    """A single event in the trace log."""

    kind: str  # spawn, start, end, error, cancel, wait, resume, trace
    id: int | None = None
    parent_id: int | None = None
    ts: float = 0.0
    name: str | None = None
    message: str | None = None
    cached: bool | None = None
    error: str | None = None
    by: int | None = None
    kwargs: dict[str, Any] = field(default_factory=lambda: {})


# ── Sink protocol ──


class Sink(Protocol):
    """Protocol for event sinks."""

    def emit(self, event: Event) -> None: ...


class MemorySink:
    """In-memory sink for testing."""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def emit(self, event: Event) -> None:
        if event.ts == 0.0:
            event.ts = time.monotonic()
        self.events.append(event)


class NullSink:
    """Sink that discards events."""

    def emit(self, event: Event) -> None:
        pass


# ── Global sink (set by Runtime) ──

_sink: ContextVar[Sink] = ContextVar("_sink")
_sink_default = NullSink()


def get_sink() -> Sink:
    """Get the current event sink."""
    return _sink.get(_sink_default)


def set_sink(sink: Sink) -> Token[Sink]:
    """Set the event sink, returning a token for resetting."""
    return _sink.set(sink)


def reset_sink(token: Token[Sink]) -> None:
    """Reset the sink contextvar to its value before `set_sink(...)`."""
    _sink.reset(token)


def emit_event(kind: str, *, ts: float = 0.0, **kwargs: Any) -> Event:
    """Emit an event to the current sink.

    If `ts` is provided (non-zero), sinks preserve it instead of stamping
    wall-clock time — used by cache-replay so the flamegraph can reconstruct
    the original timing of a cached subtree.
    """
    event = Event(kind=kind, ts=ts, **kwargs)
    get_sink().emit(event)
    return event
