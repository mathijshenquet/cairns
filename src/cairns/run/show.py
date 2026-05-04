"""Terminal viewer for Cairn traces and store contents.

Usage:
    from cairns.show import show_trace, show_runs, show_output, LiveRenderer
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from typing import Any

from cairns.core import Event
from .gc import list_runs
from .spans import SpanGraph


# ── ANSI colors ──

_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"


def _color(text: str, color: str) -> str:
    return f"{color}{text}{_RESET}"


# Keys that are rendered specially or are part of the core event envelope.
# Anything else in a trace event is shown as a generic `(k=v)` attr.
TRACE_RESERVED = frozenset({
    "e", "ts", "seq", "parent_seq", "name", "cached", "err", "by",
    "msg", "detail", "progress", "state", "level", "cost",
})


def format_cost(cost: dict[str, Any]) -> str:
    parts: list[str] = []
    for k, v in cost.items():
        if isinstance(v, float):
            parts.append(f"{k}={v:g}")
        else:
            parts.append(f"{k}={v}")
    return "{" + " ".join(parts) + "}"


def _format_trace(e: dict[str, Any]) -> tuple[str, str]:
    """Format a trace event dict. Returns (line, ansi_color_prefix)."""
    msg: str = e.get("msg", "")
    level: str = e.get("level", "info")
    state: str | None = e.get("state")
    progress: list[int] | None = e.get("progress")
    cost: dict[str, Any] | None = e.get("cost")

    parts: list[str] = []

    if progress:
        cur, total = progress[0], progress[1]
        bar_width = 10
        filled = int(bar_width * cur / total) if total else 0
        bar = "█" * filled + "░" * (bar_width - filled)
        parts.append(f"[{bar}]")

    if msg:
        parts.append(msg)

    if progress:
        parts.append(f"({progress[0]}/{progress[1]})")
    if state:
        parts.append(f"[{state}]")
    if cost:
        parts.append(format_cost(cost))

    attrs = {k: v for k, v in e.items() if k not in TRACE_RESERVED}
    if attrs:
        kv = " ".join(f"{k}={v}" for k, v in attrs.items())
        parts.append(f"({kv})")

    line = " ".join(parts)

    if level == "error":
        color = _RED
    elif level == "warn":
        color = _YELLOW
    else:
        color = _DIM
    return line, color


# ── Live renderer — formats events as they arrive ──


class LiveRenderer:
    """Renders trace events to the terminal as they arrive.

    Can be used as a Sink (has an emit() method) to show live progress
    during execution.
    """

    def __init__(self, file: Any = None) -> None:
        self._out = file or sys.stderr
        self.graph: SpanGraph = SpanGraph()

    def _print(self, msg: str) -> None:
        self._out.write(msg + "\n")
        self._out.flush()

    def render_event(self, e: dict[str, Any]) -> None:
        """Render a single event dict (from JSONL or converted from Event)."""
        self.graph.apply(e)
        kind: str = e.get("e", "")
        relative_ts: float = e.get("ts", 0.0) - (self.graph.first_ts or 0.0)

        def name_of(sid: int) -> str:
            s = self.graph.spans.get(sid)
            return s.name if s is not None else f"task-{sid}"

        def indent_for(sid: int) -> str:
            return "  " * self.graph.depth(sid)

        if kind == "spawn":
            span_id = int(e["seq"])
            s = self.graph.spans.get(span_id)
            args_display = f"({s.args})" if s is not None and s.args else ""
            icon = _color("○", _DIM)
            self._print(f"  {relative_ts:8.3f}s {indent_for(span_id)}{icon} {_BOLD}{name_of(span_id)}{_RESET}{_DIM}{args_display}{_RESET}")

        elif kind == "start":
            # Span state still updates via `self.graph.apply(e)` above; the
            # start row is suppressed in the live stream — it duplicates the
            # spawn row in practice and adds visual noise.
            return

        elif kind == "end":
            span_id = int(e["seq"])
            s = self.graph.spans.get(span_id)
            cached = s is not None and s.status == "cached"
            # `metrics.duration` is the span's own measurable wall (live: real,
            # cached: original-when-stored). `metrics.cached_duration` is the
            # *additional* cache supply absorbed inside (excluding own).
            # Total subtree time = sum of the two; report it with a
            # parenthetical cached portion when there's any.
            own = s.metrics.duration if s is not None else None
            cached_extra = (s.metrics.cached_duration or 0.0) if s is not None else 0.0
            suffix = ""
            if own is not None:
                if cached_extra > 0:
                    suffix = f" ({own + cached_extra:.3f}s, {cached_extra:.3f}s cached)"
                else:
                    suffix = f" ({own:.3f}s)"
            if cached:
                icon = _color("⚡", _GREEN)
                self._print(f"  {relative_ts:8.3f}s {indent_for(span_id)}{icon} {name_of(span_id)} {_DIM}cached{_RESET}{suffix}")
            else:
                icon = _color("✓", _GREEN)
                self._print(f"  {relative_ts:8.3f}s {indent_for(span_id)}{icon} {name_of(span_id)} done{suffix}")

        elif kind == "error":
            span_id = int(e["seq"])
            s = self.graph.spans.get(span_id)
            err = (s.error if s is not None else None) or "unknown error"
            icon = _color("✗", _RED)
            self._print(f"  {relative_ts:8.3f}s {indent_for(span_id)}{icon} {name_of(span_id)} {_color(str(err), _RED)}")

        elif kind == "cancel":
            span_id = int(e["seq"])
            icon = _color("⊘", _DIM)
            self._print(f"  {relative_ts:8.3f}s {indent_for(span_id)}{icon} {name_of(span_id)} {_color('cancelled', _DIM)}")

        elif kind == "trace":
            parent_id = e.get("parent_seq")
            d = (self.graph.depth(int(parent_id)) + 1) if parent_id is not None else 1
            indent = "  " * d
            line, style_color = _format_trace(e)
            self._print(f"  {relative_ts:8.3f}s {indent}{style_color}{line}{_RESET}")

    def emit(self, event: Event) -> None:
        """Sink-compatible emit: convert Event to dict and render."""
        from cairns.core import event_to_dict
        # `emit_event` already stamped `event.ts`; sinks treat events as read-only.
        self.render_event(event_to_dict(event))


# ── Show trace (batch, from file) ──


def show_trace(store_path: str, run_id: str | None = None) -> None:
    """Print a formatted trace from a run's trace.jsonl."""
    runs_dir = os.path.join(store_path, "runs")

    if run_id is None:
        if not os.path.isdir(runs_dir):
            print("No runs found.")
            return
        for entry in os.scandir(runs_dir):
            if entry.is_symlink():
                run_id = os.readlink(entry.path)
                break
        if run_id is None:
            print("No runs found.")
            return

    trace_path = os.path.join(runs_dir, run_id, "trace.jsonl")
    if not os.path.exists(trace_path):
        print(f"Trace not found: {trace_path}")
        return

    print(f"\n{_BOLD}Trace: {run_id}{_RESET}\n")

    renderer = LiveRenderer(file=sys.stdout)
    with open(trace_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                renderer.render_event(json.loads(line))
    print()


# ── Show runs ──


def show_runs(store_path: str) -> None:
    """Print a summary of all runs."""
    runs = list_runs(store_path)
    if not runs:
        print("No runs found.")
        return

    print(f"\n{_BOLD}Runs in {store_path}{_RESET}\n")
    for r in runs:
        latest = _color(" [latest]", _GREEN) if r.is_latest else ""
        age = datetime.now(r.timestamp.tzinfo) - r.timestamp
        age_str = f"{age.total_seconds():.0f}s ago"
        if age.total_seconds() > 3600:
            age_str = f"{age.total_seconds() / 3600:.1f}h ago"
        elif age.total_seconds() > 60:
            age_str = f"{age.total_seconds() / 60:.0f}m ago"

        print(f"  {r.entry_name:20s} {r.run_id:50s} {r.symlink_count:3d} steps  {age_str}{latest}")
    print()


# ── Show output ──


def show_output(path: str) -> None:
    """Pretty-print a record (directory) or a CAS result file."""
    if os.path.isdir(path):
        _show_record(path)
        return

    with open(path, "r", encoding="utf-8") as f:
        data: dict[str, Any] = json.load(f)
    result = data.get("result")
    print(f"\n{_BOLD}Result:{_RESET}")
    if isinstance(result, str):
        print(f"  {result}")
    else:
        print(f"  {json.dumps(result, indent=2)}")
    print()


def _show_record(record_path: str) -> None:
    meta_path = os.path.join(record_path, "metadata.json")
    events_path = os.path.join(record_path, "events.jsonl")
    result_link = os.path.join(record_path, "result")

    meta: dict[str, Any] = {}
    if os.path.isfile(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

    error = meta.get("error")
    duration = float(meta.get("duration", 0.0))

    if error:
        print(f"\n{_color('ERROR', _RED)}: {error}")
    elif os.path.exists(result_link):
        with open(result_link, "r", encoding="utf-8") as f:
            data: dict[str, Any] = json.load(f)
        result = data.get("result")
        print(f"\n{_BOLD}Result:{_RESET}")
        if isinstance(result, str):
            print(f"  {result}")
        else:
            print(f"  {json.dumps(result, indent=2)}")

    traces: list[dict[str, Any]] = []
    if os.path.isfile(events_path):
        with open(events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e: dict[str, Any] = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if e.get("kind") == "trace":
                    traces.append(e)

    if traces:
        print(f"\n{_BOLD}Traces:{_RESET}")
        for t in traces:
            msg = t.get("message", "")
            elapsed = float(t.get("ts", 0.0))
            kw: dict[str, Any] = t.get("kwargs", {}) or {}
            kwargs_str = ""
            if kw:
                kwargs_str = " " + " ".join(f"{k}={v}" for k, v in kw.items())
                kwargs_str = _DIM + kwargs_str + _RESET
            print(f"  {elapsed:7.3f}s {msg}{kwargs_str}")

    print(f"\n{_DIM}Duration: {duration:.3f}s{_RESET}\n")
