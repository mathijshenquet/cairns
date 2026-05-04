"""Run management: file-backed launcher, on-disk layout, run-dir sink."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, TypeVar

from cairns.core import Event, Handle, JSONLSink, OverlayStore
from cairns.core.runtime import (
    EndEvent,
    InteractionSink,
    Run,
    Runtime,
    SpawnEvent,
    default_runtime,
)

R = TypeVar("R")


class RunDirSink:
    """Sink for a file-backed run.

    Writes JSONL to `runs/{run_id}/trace.jsonl` and creates ordered
    `runs/{run_id}/steps/{NNN}-{name}/ → cairns/.../{record_id}/`
    symlinks on each `end` event with a `record_path`.
    """

    def __init__(self, run_dir: str) -> None:
        self._run_dir = run_dir
        self._jsonl = JSONLSink(os.path.join(run_dir, "trace.jsonl"))
        self._steps_dir = os.path.join(run_dir, "steps")
        self._task_names: dict[int, str] = {}
        self._step_ordinal = 0

    def emit(self, event: Event) -> None:
        self._jsonl.emit(event)

        if isinstance(event, SpawnEvent) and event.seq is not None and event.name:
            self._task_names[event.seq] = event.name

        elif isinstance(event, EndEvent) and event.seq is not None:
            if event.record_path is not None:
                name = self._task_names.get(event.seq, f"task-{event.seq}")
                self._create_symlink(name, event.record_path)

    def _create_symlink(self, name: str, record_path: str) -> None:
        self._step_ordinal += 1
        safe = name.replace("/", ".").replace(chr(92), ".").replace(" ", "_")
        link_name = f"{self._step_ordinal:03d}-{safe}"
        link_path = os.path.join(self._steps_dir, link_name)
        target = os.path.relpath(record_path, self._steps_dir)
        try:
            os.symlink(target, link_path)
        except OSError:
            pass

    def close(self) -> None:
        self._jsonl.close()


def _make_run_dir(base: str, entry: str) -> tuple[str, str, str]:
    """Create `runs/{entry}-{ts}/` and `runs/{entry}-{ts}/steps/`. Returns
    (runs_dir, run_id, run_dir).
    """
    abs_base = os.path.abspath(base)
    runs_dir = os.path.join(abs_base, "runs")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
    run_id = f"{entry}-{ts}"
    run_dir = os.path.join(runs_dir, run_id)
    os.makedirs(run_dir, exist_ok=True)
    os.makedirs(os.path.join(run_dir, "steps"), exist_ok=True)
    return runs_dir, run_id, run_dir


def _write_carry(run_dir: str, carry: dict[str, str]) -> None:
    """Persist the carry map so the run is reproducible. Written before
    the body executes so even a crashed run leaves a record of what was
    pinned.
    """
    if not carry:
        return
    path = os.path.join(run_dir, "carry.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(carry, f, sort_keys=True, indent=2)
            f.write("\n")
    except OSError:
        pass


def _record_carry_backlinks(
    base_path: str, run_id: str, carry: dict[str, str]
) -> None:
    """Cross-link from each carried cairn to the run that carried it.

    For each `(cairn_id, record_path)` in `carry`, writes
    `cairns/{cairn_id}/.carries/{run_id} → runs/{run_id}/`. The carried
    cairn's stack is unchanged (carry is a read-time detour, not a
    promotion); this just gives back-traversal for observability.
    """
    if not carry:
        return
    abs_base = os.path.abspath(base_path)
    for cairn_id in carry.keys():
        cairn_dir = os.path.join(abs_base, "cairns", cairn_id)
        if not os.path.isdir(cairn_dir):
            continue
        carries_dir = os.path.join(cairn_dir, ".carries")
        try:
            os.makedirs(carries_dir, exist_ok=True)
            link_path = os.path.join(carries_dir, run_id)
            target = os.path.relpath(
                os.path.join(abs_base, "runs", run_id), carries_dir
            )
            if not os.path.lexists(link_path):
                os.symlink(target, link_path)
        except OSError:
            pass


def _update_latest(runs_dir: str, entry: str, run_id: str) -> None:
    """Repoint the GC-root symlink `runs/{entry} → {run_id}` so gc keeps
    the latest run of each entry alive.
    """
    root_link = os.path.join(runs_dir, entry)
    try:
        if os.path.islink(root_link):
            os.unlink(root_link)
        os.symlink(run_id, root_link)
    except OSError:
        pass


def _build_run(
    runtime: Runtime,
    entry_label: str,
    carry_map: dict[str, str],
    interaction_sink: InteractionSink | None,
) -> Run:
    """Wire a Run for the file-backed launcher. Shared by sync + async."""
    runs_dir, run_id, run_dir = _make_run_dir(runtime.store_path, entry_label)
    _write_carry(run_dir, carry_map)
    _record_carry_backlinks(runtime.store_path, run_id, carry_map)

    base_store = runtime.store
    store = OverlayStore(carry_map, base_store) if carry_map else base_store
    sink = RunDirSink(run_dir)

    def _on_exit() -> None:
        sink.close()
        _update_latest(runs_dir, entry_label, run_id)

    return Run(
        runtime=runtime,
        store=store,
        sink=sink,
        interaction_sink=interaction_sink,
        _on_exit=_on_exit,
    )


def _resolve_runtime(
    runtime: Runtime | None, store_path: str | None
) -> Runtime:
    """Pick the Runtime for this run. Sugar: if `store_path` is given
    without `runtime`, build a fresh `Runtime(store_path=...)` for
    convenience (test scripts, one-offs). Mutually exclusive with `runtime=`.
    """
    if runtime is not None and store_path is not None:
        raise TypeError("pass either `runtime=` or `store_path=`, not both")
    if runtime is not None:
        return runtime
    if store_path is not None:
        return Runtime(store_path=store_path)
    return default_runtime


def run(
    handle: Handle[R],
    *,
    store_path: str | None = None,
    label: str | None = None,
    carry: dict[str, str] | None = None,
    interaction_sink: InteractionSink | None = None,
    runtime: Runtime | None = None,
) -> R:
    """Run a step pipeline as a file-backed entry point. Sync.

    Pass an unconsumed `Handle` produced by calling a `@step` function
    at top level — e.g. `run(pipeline(urls))`. Outside an active run,
    `pipeline(urls)` returns a deferred Handle (it captures the call
    without executing it); `run(...)` replays it inside the run context.

    Sets up the run directory under the runtime's `store_path`, wires a
    `RunDirSink`, and binds a `Run` to the active-run ContextVar for the
    duration of the pipeline. Calls `asyncio.run` internally — for
    embedding inside an existing event loop use `arun(...)`.

    `store_path=` is sugar for "build a fresh `Runtime` at this path";
    pass `runtime=` instead to use a configured Runtime (e.g. one with
    custom hashers). The two are mutually exclusive.
    """
    rt = _resolve_runtime(runtime, store_path)
    entry_label = label or _handle_label(handle)

    async def _go() -> R:
        with _build_run(rt, entry_label, dict(carry or {}), interaction_sink):
            eager: Handle[R] = handle._consume()  # type: ignore[reportPrivateUsage]
            return await eager

    return asyncio.run(_go())


async def arun(
    handle: Handle[R],
    *,
    store_path: str | None = None,
    label: str | None = None,
    carry: dict[str, str] | None = None,
    interaction_sink: InteractionSink | None = None,
    runtime: Runtime | None = None,
) -> R:
    """Async variant of `run`. Awaitable; does not call `asyncio.run`.

    Use this when something else owns the event loop — FastAPI handlers,
    Textual apps, aiohttp servers, Jupyter kernels.
    """
    rt = _resolve_runtime(runtime, store_path)
    entry_label = label or _handle_label(handle)

    with _build_run(rt, entry_label, dict(carry or {}), interaction_sink):
        eager: Handle[R] = handle._consume()  # type: ignore[reportPrivateUsage]
        return await eager


def _handle_label(handle: Handle[Any]) -> str:
    """Extract a run label from a deferred Handle's captured fn name."""
    fn = handle._fn  # type: ignore[reportPrivateUsage]
    return getattr(fn, "__name__", "main") if fn is not None else "main"


# Re-export gc + show for the public `cairns.run` surface.
from .gc import (  # noqa: E402
    RunInfo,
    gc,
    gc_outputs,
    list_runs,
    remove_run,
    remove_runs_before,
)
from .show import show_output, show_runs, show_trace  # noqa: E402
from .spans import SpanGraph  # noqa: E402

__all__ = [
    "run",
    "arun",
    "RunDirSink",
    "RunInfo",
    "list_runs",
    "remove_run",
    "remove_runs_before",
    "gc",
    "gc_outputs",
    "show_trace",
    "show_runs",
    "show_output",
    "SpanGraph",
]
