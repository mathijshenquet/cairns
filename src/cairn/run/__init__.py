"""Run management: directory layout, symlinks, entry point."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

import json

from cairn.core import Event, FileStore, Handle, JSONLSink, OverlayStore, set_sink, set_store

R = TypeVar("R")


class RunManager:
    """Manages the on-disk layout for a single run.

    Layout::

        .cairn/
            cairns/{cairn_id}/{stone_id}/   # append-only stack of stones
            store/{content_hash}.json        # value-bytes CAS
            runs/
                {entry_point}-{datetime}/
                    trace.jsonl
                    steps/{seqid:03d}-{name} → ../../../cairns/…/…/
                {entry_point} → {entry_point}-{datetime}
    """

    def __init__(self, base_path: str, entry_name: str) -> None:
        self._base = os.path.abspath(base_path)
        self._runs_dir = os.path.join(self._base, "runs")

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
        self._run_id = f"{entry_name}-{ts}"
        self._run_dir = os.path.join(self._runs_dir, self._run_id)
        self._entry_name = entry_name

        os.makedirs(self._run_dir, exist_ok=True)
        os.makedirs(os.path.join(self._run_dir, "steps"), exist_ok=True)

        self._store = FileStore(self._base)
        self._sink = JSONLSink(os.path.join(self._run_dir, "trace.jsonl"))
        self._seq = 0

    @property
    def store(self) -> FileStore:
        return self._store

    @property
    def sink(self) -> JSONLSink:
        return self._sink

    @property
    def run_dir(self) -> str:
        return self._run_dir

    def create_symlink(self, name: str, stone_path: str) -> None:
        """Create an ordered run step symlink under steps/ to a resolved stone."""
        if not stone_path:
            return
        self._seq += 1
        safe_name = name.replace("/", ".").replace(chr(92), ".").replace(" ", "_")
        link_name = f"{self._seq:03d}-{safe_name}"
        steps_dir = os.path.join(self._run_dir, "steps")
        link_path = os.path.join(steps_dir, link_name)
        target = os.path.relpath(stone_path, steps_dir)
        try:
            os.symlink(target, link_path)
        except OSError:
            pass

    def record_carry(self, carry: dict[str, str]) -> None:
        """Persist the carry map so the run is reproducible.

        Written before the body executes so even a crashed run leaves a
        record of what was pinned.
        """
        if not carry:
            return
        path = os.path.join(self._run_dir, "carry.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(carry, f, sort_keys=True, indent=2)
                f.write("\n")
        except OSError:
            pass

    def update_latest(self) -> None:
        """Repoint the GC-root symlink `{entry_name} → {run_id}` so gc keeps
        the latest run of each entry point alive.
        """
        root_link = os.path.join(self._runs_dir, self._entry_name)
        try:
            if os.path.islink(root_link):
                os.unlink(root_link)
            os.symlink(self._run_id, root_link)
        except OSError:
            pass

    def close(self) -> None:
        """Finalize the run."""
        self._sink.close()
        self.update_latest()


class SymlinkTracker:
    """Wraps a sink, creating symlinks when tasks complete."""

    def __init__(self, run_manager: RunManager, inner_sink: JSONLSink) -> None:
        self._rm = run_manager
        self._inner = inner_sink
        self._task_names: dict[int, str] = {}

    def emit(self, event: Event) -> None:
        self._inner.emit(event)

        if event.kind == "spawn" and event.id is not None and event.name is not None:
            self._task_names[event.id] = event.name

        if event.kind == "end" and event.id is not None:
            name = self._task_names.get(event.id, f"task-{event.id}")
            stone_path: str | None = event.kwargs.get("stone_path")
            if stone_path is not None:
                self._rm.create_symlink(name, stone_path)


def run(
    entry: Callable[..., Handle[R]],
    *,
    store_path: str = ".cairn",
    label: str | None = None,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    carry: dict[str, str] | None = None,
) -> R:
    """Run a step function as the entry point.

    Sets up file-based output store, JSONL trace sink, and run directory.

    Args:
        entry: The @step-decorated entry point function.
        store_path: Path to the .cairn store directory.
        label: Human-readable label for the run (e.g., 'research_pipeline:main_slow').
               Defaults to the function's __name__.
        args: Positional arguments for the entry function.
        kwargs: Keyword arguments for the entry function.
        carry: `{cairn_id: stone_path}` overrides. The resolver short-circuits
            to the given stone for each listed cairn_id — no body execution,
            no stack consultation. This is the surgery / mocking / branching
            primitive. The carry map is persisted into the run directory so
            the run is reproducible.
    """
    entry_label = label or getattr(entry, "__name__", "main")
    rm = RunManager(store_path, entry_label)
    tracker = SymlinkTracker(rm, rm.sink)
    carry_map = dict(carry or {})
    rm.record_carry(carry_map)

    # Carry is a read-overlay on the store. Hits are tagged origin="carried"
    # by OverlayStore; the step wrapper notices and short-circuits regardless
    # of memo. No run-level lookup logic, no parallel resolver.
    store = OverlayStore(carry_map, rm.store) if carry_map else rm.store

    async def _run() -> R:
        store_token = set_store(store)
        sink_token = set_sink(tracker)
        try:
            handle = entry(*args, **(kwargs or {}))
            result: R = await handle
            return result
        finally:
            from cairn.core import reset_sink, reset_store  # noqa: PLC0415

            reset_store(store_token)
            reset_sink(sink_token)
            rm.close()

    return asyncio.run(_run())


# Re-export gc + show for the public `cairn.run` surface.
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
    "RunManager",
    "SymlinkTracker",
    "run",
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
