"""Cairn core primitives.

Public surface of `cairns.core` — re-exported from dedicated submodules so
external code can say `from cairns.core import step` without knowing where
each name lives.
"""

from .step import (
    Handle,
    cached_output,
    cached_tracing,
    step,
    trace,
)
from .runtime import (
    CancelEvent,
    EndEvent,
    ErrorEvent,
    Event,
    InteractionSink,
    MemorySink,
    NullSink,
    ResumeEvent,
    Run,
    Runtime,
    Sink,
    SpawnEvent,
    StartEvent,
    TraceEvent,
    WaitEvent,
    current_run,
    current_span,
    default_runtime,
    emit_event,
)
from .cairn import Cairn, cairn  # noqa: F401
from .hash import (
    compute_cairn_id,
    resolve_hashable,
)
from cairns.patterns import rate_limited, replayable
from .serial import (
    Serializer,
    from_jsonable,
    to_jsonable,
)
from .sink import CompositeSink, JSONLSink, event_to_dict
from .store import FileStore, MemoryStore, OverlayStore, Store, StoreStats
from .types import Record, SpanMetrics, StepInfo, TaskSpan, TraceRecord

__all__ = [
    # decorator + Handle
    "step",
    "Handle",
    "trace",
    "cached_output",
    "cached_tracing",
    # cairn inspection
    "Cairn",
    "cairn",
    # runtime
    "Runtime",
    "Run",
    "default_runtime",
    "current_run",
    "current_span",
    "Event",
    "SpawnEvent",
    "StartEvent",
    "EndEvent",
    "WaitEvent",
    "ResumeEvent",
    "TraceEvent",
    "ErrorEvent",
    "CancelEvent",
    "Sink",
    "InteractionSink",
    "MemorySink",
    "NullSink",
    "emit_event",
    # hash
    "compute_cairn_id",
    "resolve_hashable",
    # serial
    "Serializer",
    "to_jsonable",
    "from_jsonable",
    # sink
    "JSONLSink",
    "CompositeSink",
    "event_to_dict",
    # store
    "Store",
    "MemoryStore",
    "FileStore",
    "OverlayStore",
    "StoreStats",
    # patterns
    "rate_limited",
    "replayable",
    # types
    "StepInfo",
    "TraceRecord",
    "Record",
    "SpanMetrics",
    "TaskSpan",
]
