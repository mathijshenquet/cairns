"""Event sinks for trace output."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

from .runtime import (
    EndEvent,
    ErrorEvent,
    Event,
    SpawnEvent,
    TraceEvent,
    WaitEvent,
)

if TYPE_CHECKING:
    from .runtime import Sink


def event_to_dict(event: Event) -> dict[str, Any]:
    """Convert an Event to a JSON-serializable dict.

    Per-kind serialization. The on-disk shape is the historical flat
    layout (no nested kwargs bag): top-level keys are the event's fields.
    """
    d: dict[str, Any] = {"e": event.kind, "ts": event.ts}
    if event.seq is not None:
        d["seq"] = event.seq
    if event.parent_seq is not None:
        d["parent_seq"] = event.parent_seq

    if isinstance(event, SpawnEvent):
        d["name"] = event.name
        if event.origin == "recalled":
            d["origin"] = "recalled"
            if event.cairn_id is not None:
                d["cairn_id"] = event.cairn_id
            if event.record_id is not None:
                d["record_id"] = event.record_id
            if event.record_path is not None:
                d["record_path"] = event.record_path
        else:
            if event.identity:
                d["identity"] = event.identity
            if event.body_hash:
                d["body_hash"] = event.body_hash
            if event.version is not None:
                d["version"] = event.version
            if event.args:
                d["args"] = event.args
            if event.memo:
                d["memo"] = event.memo

    elif isinstance(event, EndEvent):
        d["cached"] = event.cached
        if event.cairn_id:
            d["cairn_id"] = event.cairn_id
        if event.record_id:
            d["record_id"] = event.record_id
        if event.record_path is not None:
            d["record_path"] = event.record_path
        d["origin"] = event.origin
        d["size"] = event.size
        d["own_size"] = event.own_size
        d["duration"] = event.duration
        d["own_duration"] = event.own_duration
        d["cached_duration"] = event.cached_duration

    elif isinstance(event, WaitEvent):
        on: dict[str, Any] = {"kind": event.on_kind}
        if event.on_seq is not None:
            on["seq"] = event.on_seq
        if event.on_ids is not None:
            on["ids"] = event.on_ids
        d["on"] = on

    elif isinstance(event, TraceEvent):
        d["msg"] = event.message
        if event.cached:
            d["cached"] = True
        if event.detail:
            d["detail"] = event.detail
        if event.progress is not None:
            d["progress"] = list(event.progress)
        if event.state is not None:
            d["state"] = event.state
        if event.level != "info":
            d["level"] = event.level
        if event.cost is not None:
            d["cost"] = event.cost
        if event.edge:
            d["edge"] = True

    elif isinstance(event, ErrorEvent):
        d["err"] = event.error
        d["size"] = event.size
        d["own_size"] = event.own_size
        d["duration"] = event.duration
        d["own_duration"] = event.own_duration
        d["cached_duration"] = event.cached_duration

    return d


class CompositeSink:
    """Fans every event out to each wrapped sink in order.

    Handy when one event stream needs to land in both a persistent log and a
    live UI — wrap both sinks in a `CompositeSink` and install it as the
    current sink. Each sub-sink sees the same `Event` reference.
    """

    def __init__(self, *sinks: Sink) -> None:
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
