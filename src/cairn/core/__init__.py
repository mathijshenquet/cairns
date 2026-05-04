"""Cairn core primitives.

Public surface of `cairn.core` — re-exported from dedicated submodules so
external code can say `from cairn.core import step` without knowing where
each name lives.
"""

from .step import (
    Handle,
    cached_output,
    cached_tracing,
    get_store,
    reset_store,
    set_store,
    step,
    trace,
)
from .context import (
    Event,
    MemorySink,
    NullSink,
    Sink,
    emit_event,
    get_sink,
    next_id,
    reset_id_counter,
    reset_sink,
    set_sink,
)
from .hash import (
    clear_hash_funcs,
    compute_cairn_id,
    register_hash_func,
    resolve_hashable,
    set_hash_funcs,
)
from .patterns import rate_limited, replayable
from .serial import (
    Serializer,
    clear_serializers,
    from_jsonable,
    register_serializer,
    to_jsonable,
)
from .sink import CompositeSink, JSONLSink, event_to_dict
from .store import FileStore, MemoryStore, OverlayStore, Store, StoreStats
from .types import CacheEntry, SpanMetrics, StepInfo, TaskSpan, TraceRecord

__all__ = [
    # decorator + Handle
    "step",
    "Handle",
    "trace",
    "cached_output",
    "cached_tracing",
    "get_store",
    "set_store",
    "reset_store",
    # context / event sink
    "Event",
    "Sink",
    "MemorySink",
    "NullSink",
    "get_sink",
    "set_sink",
    "reset_sink",
    "emit_event",
    "next_id",
    "reset_id_counter",
    # hash
    "compute_cairn_id",
    "register_hash_func",
    "set_hash_funcs",
    "clear_hash_funcs",
    "resolve_hashable",
    # serial
    "Serializer",
    "register_serializer",
    "clear_serializers",
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
    "CacheEntry",
    "SpanMetrics",
    "TaskSpan",
]
