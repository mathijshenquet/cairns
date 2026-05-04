"""Active-run context, span/event types, and configuration.

This module owns the runtime model in three layers:

- `Runtime` — process-level configuration. Holds the type-keyed
  hash and serializer registries. Defaults (Path, partial, Pydantic)
  are installed in `__init__`. Extend with `register_hasher` /
  `register_serializer`, or build a fresh one for tests / custom config.
  A single `default_runtime` is created at module load for the common
  `from cairns import run` path.

- `Run` — one execution. Holds store, sink, optional interaction sink,
  a back-pointer to its `Runtime`, and the per-run seq counter.
  Use as a context manager: `with Run(...) as r:` binds it to the
  active-run ContextVar; `__exit__` restores. Nested runs are illegal.

- `current_span` — per-task ContextVar for the executing step. Stays
  separate from `Run` because asyncio siblings need independent values;
  a plain attribute on Run wouldn't compose under fan-out.

`Sink` and `InteractionSink` protocols live here too — they're slots
the Run holds, so the contract belongs next to the type.
"""

from __future__ import annotations

import itertools
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Mapping, Protocol, TypeVar

from .types import TaskSpan

if TYPE_CHECKING:
    from .serial import Serializer
    from .store import Store


K = TypeVar("K")
R = TypeVar("R")


# ── Event types ──


@dataclass
class Event:
    """A single event in the trace log."""

    kind: str  # spawn, start, end, error, cancel, wait, resume, trace
    seq: int | None = None
    parent_seq: int | None = None
    ts: float = 0.0
    name: str | None = None
    message: str | None = None
    cached: bool | None = None
    error: str | None = None
    by: int | None = None
    kwargs: dict[str, Any] = field(default_factory=lambda: {})


# ── Sink protocols ──


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

    def emit(self, event: Event) -> None:  # noqa: ARG002
        pass


class InteractionSink(Protocol):
    """Transport for routing interaction requests to a human (or stand-in).

    `anchor_span` is the span on whose behalf the request is being made —
    the caller of the `await_*` wrapper, not the internal `@step` that
    wraps the sink call. Sinks that render widgets next to the span tree
    (the TUI) use it to attach the widget correctly; headless sinks
    (stdin, queue) ignore it.
    """

    async def request_input(
        self,
        prompt: str,
        *,
        anchor_span: int | None,
        default: str | None = None,
        placeholder: str | None = None,
    ) -> str: ...

    async def request_choice(
        self,
        prompt: str,
        options: Mapping[K, str],
        *,
        anchor_span: int | None,
        default: K | None = None,
    ) -> K: ...

    async def request_confirm(
        self,
        prompt: str,
        *,
        anchor_span: int | None,
        default: bool | None = None,
    ) -> bool: ...


# ── Per-task span ContextVar ──


current_span: ContextVar[TaskSpan | None] = ContextVar("current_span", default=None)


# ── Runtime ──


class Runtime:
    """Process-level cairn configuration and Run factory.

    Holds the type-keyed registries (hashers, serializers) that apply to
    every Run launched from it. Defaults are installed in `__init__`.

    Common usage relies on the module-level `default_runtime`:

        from cairns import run
        run(pipeline)                  # uses default_runtime

        from cairns import default_runtime
        default_runtime.register_hasher(MyType, my_hasher)

    For test isolation or a custom config, build your own:

        runtime = Runtime().register_hasher(Path, str)
        runtime.run(pipeline)
        async with runtime.harness() as h:
            await pipeline()
    """

    def __init__(self, store_path: str = ".cairn") -> None:
        self.store_path = store_path
        self.hash_funcs: dict[type, Callable[[Any], Any]] = {}
        self.serializers: dict[type, "Serializer"] = {}
        self._store: "Store | None" = None
        self._install_defaults()

    @property
    def store(self) -> "Store":
        """Lazy `FileStore` at `self.store_path`. Built on first access."""
        if self._store is None:
            from .store import FileStore  # noqa: PLC0415

            self._store = FileStore(self.store_path)
        return self._store

    def _install_defaults(self) -> None:
        # Each module installs its own defaults onto self, so private hashers
        # / serializers stay encapsulated to their declaring module.
        from .hash import install_defaults as install_hash_defaults  # noqa: PLC0415
        from .serial import install_defaults as install_serial_defaults  # noqa: PLC0415

        install_hash_defaults(self)
        install_serial_defaults(self)

    def register_hasher(
        self, tp: type, fn: Callable[[Any], Any]
    ) -> "Runtime":
        """Register a hash function for a type. Subclasses match via MRO.
        Chainable: returns self.
        """
        self.hash_funcs[tp] = fn
        return self

    def register_serializer(
        self, tp: type, serializer: "Serializer"
    ) -> "Runtime":
        """Register a Serializer for a type. Subclasses match via MRO.
        Chainable: returns self.
        """
        self.serializers[tp] = serializer
        return self

    def run(
        self,
        entry: Callable[..., Any],
        *,
        label: str | None = None,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        carry: dict[str, str] | None = None,
        interaction_sink: InteractionSink | None = None,
    ) -> Any:
        """Execute `entry` as a file-backed run. Sync; calls `asyncio.run`.

        The store path lives on this runtime (`self.store_path`); call sites
        wanting a different `.cairn/` location build a new `Runtime`.
        """
        from cairns.run import run as _execute  # noqa: PLC0415

        return _execute(
            entry,
            label=label,
            args=args,
            kwargs=kwargs,
            carry=carry,
            interaction_sink=interaction_sink,
            runtime=self,
        )

    async def arun(
        self,
        entry: Callable[..., Any],
        *,
        label: str | None = None,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        carry: dict[str, str] | None = None,
        interaction_sink: InteractionSink | None = None,
    ) -> Any:
        """Async variant of `run` for embedding inside an existing event loop.

        Use when something else owns `asyncio.run` — FastAPI, Textual,
        aiohttp servers, Jupyter, etc. Calling `runtime.run` from inside a
        running loop raises; `await runtime.arun` works.
        """
        from cairns.run import arun as _arun  # noqa: PLC0415

        return await _arun(
            entry,
            label=label,
            args=args,
            kwargs=kwargs,
            carry=carry,
            interaction_sink=interaction_sink,
            runtime=self,
        )

    def harness(self) -> "Any":
        """Build an async test Harness backed by this Runtime."""
        from cairns.testing import Harness  # noqa: PLC0415

        return Harness(runtime=self)


# ── Run ──


class Run:
    """The active run. One per pipeline execution (or test `Harness`).

    Use as a context manager:

        with Run(runtime=rt, store=..., sink=...) as run:
            ...

    `__enter__` binds the Run as the active one (raising if another is
    already active). `__exit__` restores the binding and runs an
    optional teardown callback (used by file-backed runs to close the
    sink + repoint the latest pointer).
    """

    def __init__(
        self,
        *,
        store: "Store",
        sink: Sink,
        runtime: Runtime | None = None,
        interaction_sink: InteractionSink | None = None,
        _on_exit: Callable[[], None] | None = None,
    ) -> None:
        self.runtime: Runtime = runtime if runtime is not None else default_runtime
        self.store = store
        self.sink = sink
        self.interaction_sink = interaction_sink

        self._seq: itertools.count[int] = itertools.count(1)
        self._token: Any = None
        self._on_exit = _on_exit

    def next_seq(self) -> int:
        """Mint the next event-correlation sequence number for this run.

        Used as `Event.seq` / `Event.parent_seq` so trace consumers can
        stitch events into a tree. Run-local — restarts at 1 each Run.
        """
        return next(self._seq)

    def __enter__(self) -> "Run":
        if _run.get() is not None:
            raise RuntimeError(
                "a cairn run is already active — nested runs are not supported. "
                "Compose pipelines with @step instead."
            )
        self._token = _run.set(self)
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._token is not None:
            _run.reset(self._token)
            self._token = None
        if self._on_exit is not None:
            on_exit, self._on_exit = self._on_exit, None
            on_exit()


_run: ContextVar[Run | None] = ContextVar("_run", default=None)


def current_run() -> Run:
    """Return the active Run, or raise if none is active."""
    rt = _run.get()
    if rt is None:
        raise RuntimeError(
            "no active cairn run — wrap calls in `run(...)` or "
            "`async with Harness():` (tests)."
        )
    return rt


def emit_event(kind: str, *, ts: float = 0.0, **kwargs: Any) -> Event:
    """Emit an event to the active run's sink.

    If `ts` is provided (non-zero), sinks preserve it instead of stamping
    wall-clock time — used by cache-replay so the flamegraph can reconstruct
    the original timing of a cached subtree.
    """
    event = Event(kind=kind, ts=ts, **kwargs)
    current_run().sink.emit(event)
    return event


# ── Lookup helpers used by hash.py / serial.py ──


def active_hash_funcs() -> dict[type, Callable[[Any], Any]]:
    """Return the active Run's runtime hashers, or default_runtime's.

    Used by `resolve_hashable` when called without an explicit override.
    Lets per-Run customization (e.g. `runtime.run(...)` with a configured
    runtime, or `Harness(hash_funcs=...)`) flow through transparently.
    """
    rt = _run.get()
    return rt.runtime.hash_funcs if rt is not None else default_runtime.hash_funcs


def active_serializers() -> dict[type, "Serializer"]:
    """Return the active Run's runtime serializers, or default_runtime's."""
    rt = _run.get()
    return rt.runtime.serializers if rt is not None else default_runtime.serializers


# ── default_runtime singleton ──

# Created at module load. Runtime.__init__ lazy-imports default
# hashers/serializers, so this is safe even though hash.py and serial.py
# also live under cairns.core.
default_runtime: Runtime = Runtime()
