"""Trace rendering helpers — shared by the app and detail pane."""

from __future__ import annotations

from typing import Any

from rich.text import Text

from cairns.run.show import TRACE_RESERVED, format_cost


def trace_style(level: str) -> str:
    if level == "error":
        return "red"
    if level == "warn":
        return "yellow"
    return "dim"


def render_trace_text(e: dict[str, Any]) -> Text:
    """Render a trace event to styled Text based on blessed kwargs."""
    msg: str = e.get("msg", "")
    level: str = e.get("level", "info")
    state: str | None = e.get("state")
    progress: list[int] | None = e.get("progress")
    cost: dict[str, Any] | None = e.get("cost")

    parts: list[str] = []
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

    return Text(" ".join(parts), style=trace_style(level))
