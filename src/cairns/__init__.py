"""Cairns: compute graph orchestration with caching and observability."""

from cairns.core import (
    Cairn,
    Handle,
    Record,
    Run,
    Runtime,
    cached_output,
    cached_tracing,
    cairn,
    default_runtime,
    step,
    trace,
)
from cairns.patterns import rate_limited, replayable
from cairns.run import (
    arun,
    gc,
    list_runs,
    remove_run,
    remove_runs_before,
    run,
)

__all__ = [
    # canonical
    "step",
    "trace",
    "Handle",
    "run",
    "arun",
    "Runtime",
    "default_runtime",
    "Cairn",
    "cairn",
    "Record",
    # tier-3 primitive (advanced)
    "Run",
    # batteries
    "cached_output",
    "cached_tracing",
    "rate_limited",
    "replayable",
    # ops
    "gc",
    "list_runs",
    "remove_run",
    "remove_runs_before",
]
