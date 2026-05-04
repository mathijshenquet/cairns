"""Span-tree reducer over the trace.jsonl event stream.

Shared by `show.LiveRenderer` and `tui.CairnApp`: feed events through
`SpanGraph.apply(event)` and read derived state off the graph.

`effective_status(id)` bubbles the wait chain so a parent blocked on a
running child surfaces as `running`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, cast


WaitKind = Literal["span", "group"]
SpanStatus = Literal["pending", "running", "ok", "cached", "error", "cancelled"]
EffectiveStatus = SpanStatus


@dataclass
class Wait:
    kind: WaitKind
    target: int | list[int]   # span id | list of span ids


@dataclass
class Span:
    id: int
    parent: int | None
    name: str
    args: str = ""
    status: SpanStatus = "pending"
    spawn_ts: float | None = None
    start_ts: float | None = None
    end_ts: float | None = None
    stone_path: str | None = None
    origin: str | None = None  # "created" | "recalled" | "carried"
    error: str | None = None
    metrics: dict[str, Any] = field(default_factory=lambda: cast(dict[str, Any], {}))
    traces: list[dict[str, Any]] = field(default_factory=lambda: cast(list[dict[str, Any]], []))


# Precedence for reducing over a set of statuses. Higher wins.
_PRIORITY: dict[str, int] = {
    "error": 5,
    "running": 3,
    "pending": 2,
    "cancelled": 1,
    "ok": 0,
    "cached": 0,
}


class SpanGraph:
    """Mutable span-tree state built up from a trace event stream."""

    def __init__(self) -> None:
        self.spans: dict[int, Span] = {}
        self.open_waits: dict[int, list[Wait]] = {}      # span id → stack
        self.first_ts: float | None = None

    def apply(self, e: dict[str, Any]) -> None:
        kind = e.get("e", "")
        ts = e.get("ts", 0.0)
        if self.first_ts is None and ts:
            self.first_ts = ts

        if kind == "spawn":
            span_id = int(e["id"])
            self.spans[span_id] = Span(
                id=span_id,
                parent=e.get("parent"),
                name=e.get("name", "?"),
                args=str(e.get("args", "")),
                spawn_ts=ts,
            )

        elif kind == "start":
            s = self.spans.get(int(e["id"]))
            if s is not None:
                s.start_ts = ts
                s.status = "running"

        elif kind in ("end", "error", "cancel"):
            span_id = int(e["id"])
            s = self.spans.get(span_id)
            if s is not None:
                s.end_ts = ts
                if kind == "end":
                    s.status = "cached" if e.get("cached") else "ok"
                    s.stone_path = e.get("stone_path")
                    s.origin = e.get("origin")
                elif kind == "error":
                    s.status = "error"
                    s.error = str(e.get("err", "error"))
                else:
                    s.status = "cancelled"
                for mk in ("size", "own_size", "time", "own_time"):
                    if mk in e:
                        s.metrics[mk] = e[mk]
            self.open_waits.pop(span_id, None)

        elif kind == "wait":
            span_id = int(e["id"])
            on_raw = e.get("on")
            on: dict[str, Any] = cast(dict[str, Any], on_raw) if isinstance(on_raw, dict) else {}
            wkind = on.get("kind")
            w: Wait | None = None
            if wkind == "span" and "id" in on:
                w = Wait(kind="span", target=int(on["id"]))
            elif wkind == "group":
                ids_raw = cast(list[Any], on.get("ids") or [])
                w = Wait(kind="group", target=[int(x) for x in ids_raw])
            if w is not None:
                self.open_waits.setdefault(span_id, []).append(w)

        elif kind == "resume":
            stack = self.open_waits.get(int(e["id"]))
            if stack:
                stack.pop()
                if not stack:
                    del self.open_waits[int(e["id"])]

        elif kind == "trace":
            parent_id = e.get("parent")
            if parent_id is not None and int(parent_id) in self.spans:
                rec = {k: v for k, v in e.items() if k != "e"}
                self.spans[int(parent_id)].traces.append(rec)

    # ── Queries ──

    def depth(self, span_id: int) -> int:
        d = 0
        cur = self.spans.get(span_id)
        while cur is not None and cur.parent is not None:
            d += 1
            cur = self.spans.get(cur.parent)
        return d

    def children(self, span_id: int) -> list[int]:
        return [sid for sid, s in self.spans.items() if s.parent == span_id]

    def effective_status(self, span_id: int) -> str:
        return self._effective(span_id, set())

    def _effective(self, span_id: int, visited: set[int]) -> str:
        if span_id in visited:
            return "running"
        visited = visited | {span_id}
        s = self.spans.get(span_id)
        if s is None:
            return "pending"
        if s.status in ("ok", "cached", "error", "cancelled"):
            return s.status
        stack = self.open_waits.get(span_id)
        if stack:
            w = stack[-1]
            if w.kind == "span":
                assert isinstance(w.target, int)
                return self._effective(w.target, visited)
            if w.kind == "group":
                assert isinstance(w.target, list)
                statuses = [self._effective(c, visited) for c in w.target]
                return max(statuses, key=lambda x: _PRIORITY.get(x, 0)) if statuses else "ok"
        return s.status

    def rolled_cost(self, span_id: int) -> dict[str, float]:
        """Sum numeric `cost` columns over this span's traces + descendants."""
        total: dict[str, float] = {}
        s = self.spans.get(span_id)
        if s is None:
            return total
        for t in s.traces:
            cost = t.get("cost")
            if isinstance(cost, dict):
                for k, v in cast(dict[str, Any], cost).items():
                    if isinstance(v, (int, float)):
                        total[k] = total.get(k, 0.0) + float(v)
        for cid in self.children(span_id):
            for k, v in self.rolled_cost(cid).items():
                total[k] = total.get(k, 0.0) + v
        return total
