"""Event sinks for trace output."""

from __future__ import annotations

import json
import os
from typing import Any

from .runtime import Event


def event_to_dict(event: Event) -> dict[str, Any]:
    """Convert an Event to a JSON-serializable dict."""
    d: dict[str, Any] = {"e": event.kind, "ts": event.ts}
    if event.seq is not None:
        d["seq"] = event.seq
    if event.parent_seq is not None:
        d["parent_seq"] = event.parent_seq
    if event.name is not None:
        d["name"] = event.name
    if event.message is not None:
        d["msg"] = event.message
    if event.cached is not None:
        d["cached"] = event.cached
    if event.error is not None:
        d["err"] = event.error
    if event.by is not None:
        d["by"] = event.by
    if event.kwargs:
        d.update(event.kwargs)
    return d


class CompositeSink:
    """Fans every event out to each wrapped sink in order.

    Handy when one event stream needs to land in both a persistent log and a
    live UI — wrap both sinks in a `CompositeSink` and install it as the
    current sink. Each sub-sink sees the same `Event` reference.
    """

    def __init__(self, *sinks: Any) -> None:
        self._sinks = list(sinks)

    def emit(self, event: Event) -> None:
        for sink in self._sinks:
            sink.emit(event)


class JSONLSink:
    """Writes events as JSONL to a file.

    Each event is one JSON line, flushed immediately.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._file = open(path, "a", encoding="utf-8")  # noqa: SIM115
        self._closed = False

    def emit(self, event: Event) -> None:
        if self._closed:
            return
        line = json.dumps(event_to_dict(event), default=str)
        self._file.write(line + "\n")
        self._file.flush()

    def close(self) -> None:
        self._closed = True
        self._file.close()

    @property
    def path(self) -> str:
        return self._path
