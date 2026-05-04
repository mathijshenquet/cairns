"""Cairn / record storage backends.

A cairn is a directory keyed by `cairn_id = hash(identity, args)` holding an
append-only stack of records (immutable execution records). The CAS under
`store/` holds value bytes only; record-local scalars and event timing live
inside each record.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Protocol, cast

from .lock import store_shared
from .serial import from_jsonable, to_jsonable
from .types import Record, TraceRecord

Predicate = Callable[[Record], bool]

_MISSING = object()


@dataclass(frozen=True)
class StoreStats:
    """Size metrics and address of a published record."""

    size: int
    own_size: int
    cairn_id: str | None = None
    record_id: str | None = None
    record_path: str | None = None
    result_hash: str | None = None


@dataclass(frozen=True)
class RecordInfo:
    """Small metadata view needed to replay or display a record."""

    cairn_id: str | None
    record_id: str
    record_path: str
    short_name: str | None
    duration: float
    own_duration: float
    cached_duration: float = 0.0


class Store(Protocol):
    """Protocol for cache storage backends."""

    def find(self, key: str, predicate: Predicate | None = None) -> Record | None:
        """Newest non-error record for `key` matching `predicate`. None if no match.

        `predicate=None` means "any non-error record" (used for prefill when
        memo=False so cached_output() can return the latest known result).
        """
        ...

    def put(
        self,
        key: str,
        entry: Record,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> StoreStats: ...

    def iter_records(self, cairn_id: str) -> Iterator[Record]:
        """Yield all records for `cairn_id`, newest-first.

        Backs `Cairn` inspection. Errored records are included (consumers
        filter as needed).
        """
        ...


# ── Serialization helpers ──


def trace_to_event(t: TraceRecord, start_ts: float) -> dict[str, Any]:
    """Serialize a TraceRecord as a record-relative event line."""
    return {
        "kind": "trace",
        "ts": max(0.0, t.timestamp - start_ts),
        "delta": t.delta,
        "message": t.message,
        "kwargs": t.kwargs,
    }


def _event_to_trace(e: dict[str, Any]) -> TraceRecord:
    return TraceRecord(
        message=e.get("message", ""),
        timestamp=float(e.get("ts", 0.0)),
        delta=float(e.get("delta", 0.0)),
        kwargs=e.get("kwargs", {}) or {},
    )


def _result_payload(entry: Record) -> str:
    """CAS payload: the value bytes, routed through the serializer registry.

    Typed values (Pydantic models, tuples, …) are wrapped with a tag so the
    reader can reconstruct the original object. Plain JSON-native values pass
    through unchanged.
    """
    return json.dumps({"result": to_jsonable(entry.result)}, sort_keys=True)


def _hash_payload(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ── In-memory store ──


class MemoryStore:
    """In-memory cairn stack for testing."""

    def __init__(self) -> None:
        self._stacks: dict[str, list[Record]] = {}

    def find(self, key: str, predicate: Predicate | None = None) -> Record | None:
        for entry in reversed(self._stacks.get(key, [])):
            if entry.error is not None:
                continue
            if predicate is not None and not predicate(entry):
                continue
            return entry
        return None

    def put(
        self,
        key: str,
        entry: Record,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> StoreStats:
        md = metadata or {}
        record_id = f"mem-{len(self._stacks.get(key, [])) + 1}"
        entry.cairn_id = key
        entry.record_id = record_id
        entry.record_path = None
        entry.child_refs = list(md.get("children", []))
        payload = _result_payload(entry)
        result_hash = None if entry.error else _hash_payload(payload)
        entry.result_hash = result_hash
        self._stacks.setdefault(key, []).append(entry)
        size = len(payload.encode("utf-8"))
        return StoreStats(
            size=size,
            own_size=size,
            cairn_id=key,
            record_id=record_id,
            result_hash=result_hash,
        )

    def iter_records(self, cairn_id: str) -> Iterator[Record]:
        for entry in reversed(self._stacks.get(cairn_id, [])):
            yield entry


# ── File-backed store ──


import secrets


def _new_record_id() -> str:
    """RFC 9562 uuid7 — 48-bit ms timestamp + 74-bit randomness.

    Time-ordered by lexicographic filename sort, so `ls` on a cairn returns
    records in creation order without a separate index.
    """
    ms = time.time_ns() // 1_000_000
    rand = secrets.token_bytes(10)
    # Byte layout: [6B ms_be][version/rand_a=12 bits][variant/rand_b][rand_b...]
    ts_bytes = ms.to_bytes(6, "big")
    b = bytearray(16)
    b[0:6] = ts_bytes
    b[6] = 0x70 | (rand[0] & 0x0F)          # version 7 in top nibble
    b[7] = rand[1]
    b[8] = 0x80 | (rand[2] & 0x3F)          # variant '10'
    b[9:16] = rand[3:10]
    u = uuid.UUID(bytes=bytes(b))
    return str(u)


def _read_meta(record: str) -> dict[str, Any] | None:
    meta_path = os.path.join(record, "metadata.json")
    if not os.path.isfile(meta_path):
        return None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _infer_cairn_id(record_path: str, meta: dict[str, Any]) -> str | None:
    value = meta.get("cairn_id")
    if isinstance(value, str):
        return value
    cairn_dir = os.path.dirname(os.path.abspath(record_path))
    return os.path.basename(cairn_dir) or None


def read_record_info(record_path: str) -> RecordInfo | None:
    """Read the scalar metadata for a record without loading its result."""
    meta = _read_meta(record_path)
    if meta is None:
        return None
    return RecordInfo(
        cairn_id=_infer_cairn_id(record_path, meta),
        record_id=os.path.basename(record_path),
        record_path=record_path,
        short_name=meta.get("short_name"),
        duration=float(meta.get("duration", 0.0)),
        own_duration=float(meta.get("own_duration", 0.0)),
        cached_duration=float(meta.get("cached_duration", 0.0)),
    )


def iter_record_events(record_path: str) -> Iterator[dict[str, Any]]:
    """Yield well-formed event records from a record's events.jsonl."""
    events_path = os.path.join(record_path, "events.jsonl")
    if not os.path.isfile(events_path):
        return
    try:
        with open(events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(rec, dict):
                    yield cast(dict[str, Any], rec)
    except OSError:
        return


def child_record_path(record_path: str, child_index: int) -> str | None:
    """Resolve a record's ordered children/{index:03d} symlink."""
    if child_index < 0:
        return None
    child = os.path.join(record_path, "children", f"{child_index:03d}")
    target = os.path.realpath(child)
    if not os.path.isdir(target):
        return None
    if not os.path.isfile(os.path.join(target, "metadata.json")):
        return None
    return target


def _read_result(record: str) -> Any:
    link = os.path.join(record, "result")
    if not os.path.exists(link):
        return _MISSING
    try:
        with open(link, "r", encoding="utf-8") as f:
            data = f.read()
    except OSError:
        return _MISSING
    try:
        raw: Any = json.loads(data)
    except json.JSONDecodeError:
        return _MISSING
    if not isinstance(raw, dict) or "result" not in raw:
        return _MISSING
    return from_jsonable(cast(Any, raw)["result"])


def _read_traces(record: str) -> list[TraceRecord]:
    traces: list[TraceRecord] = []
    for e in iter_record_events(record):
        if e.get("kind") == "trace":
            traces.append(_event_to_trace(e))
    return traces


def _children_resolve(record: str) -> bool:
    """Subtree integrity: every children/* symlink must resolve to a record."""
    child_dir = os.path.join(record, "children")
    if not os.path.isdir(child_dir):
        return True
    for entry in os.scandir(child_dir):
        target = os.path.realpath(entry.path)
        if not os.path.isdir(target):
            return False
        if not os.path.isfile(os.path.join(target, "metadata.json")):
            return False
    return True


def load_record(record_path: str) -> Record | None:
    """Load a single record from disk as a Record.

    Used by the carry resolver — a record may live in any cairn, including a
    synthetic one authored by the caller. Does **not** run subtree integrity
    checks: carry is an explicit opt-in by the caller.
    """
    if not os.path.isdir(record_path):
        return None
    meta = _read_meta(record_path)
    if meta is None:
        return None
    if meta.get("error") is not None:
        return None
    result = _read_result(record_path)
    if result is _MISSING:
        return None
    traces = _read_traces(record_path)
    return Record(
        result=result,
        traces=traces,
        duration=float(meta.get("duration", 0.0)),
        own_duration=float(meta.get("own_duration", 0.0)),
        cached_duration=float(meta.get("cached_duration", 0.0)),
        error=None,
        cairn_id=_infer_cairn_id(record_path, meta),
        record_id=os.path.basename(record_path),
        record_path=record_path,
        result_hash=meta.get("result_hash"),
        child_refs=list(meta.get("children", []) or []),
        tags=dict(meta.get("tags", {}) or {}),
        body_hash=meta.get("body_hash"),
        version=meta.get("version"),
    )


class FileStore:
    """Filesystem cairn store.

    Layout::

        {base}/cairns/{cairn_id}/{record_id}/
            metadata.json              # scalars, args_repr, children pointers
            events.jsonl               # traces + child spawns, record-relative ts
            result -> ../../../store/{content_hash}.json   # optional (missing on error)
            children/000 -> ../../../{cid}/{sid}/          # ordered, per-spawn
        {base}/store/{content_hash}.json                    # `{"result": <value>}`
    """

    def __init__(self, base_path: str) -> None:
        self._base = os.path.abspath(base_path)
        self._cairns = os.path.join(self._base, "cairns")
        self._store = os.path.join(self._base, "store")
        os.makedirs(self._cairns, exist_ok=True)
        os.makedirs(self._store, exist_ok=True)

    @property
    def base_path(self) -> str:
        return self._base

    @property
    def store_path(self) -> str:
        return self._store

    # ── read ──

    def find(self, key: str, predicate: Predicate | None = None) -> Record | None:
        cairn_dir = os.path.join(self._cairns, key)
        if not os.path.isdir(cairn_dir):
            return None
        for record_id in sorted(os.listdir(cairn_dir), reverse=True):
            if record_id.startswith("."):
                continue
            record = os.path.join(cairn_dir, record_id)
            meta = _read_meta(record)
            if meta is None:
                continue
            if meta.get("error") is not None:
                continue
            if not _children_resolve(record):
                continue
            result = _read_result(record)
            if result is _MISSING:
                continue
            traces = _read_traces(record)
            entry = Record(
                result=result,
                traces=traces,
                duration=float(meta.get("duration", 0.0)),
                own_duration=float(meta.get("own_duration", 0.0)),
                cached_duration=float(meta.get("cached_duration", 0.0)),
                error=None,
                cairn_id=key,
                record_id=record_id,
                record_path=record,
                result_hash=meta.get("result_hash"),
                child_refs=list(meta.get("children", []) or []),
                tags=dict(meta.get("tags", {}) or {}),
                body_hash=meta.get("body_hash"),
                version=meta.get("version"),
            )
            if predicate is not None and not predicate(entry):
                continue
            return entry
        return None

    def iter_records(self, cairn_id: str) -> Iterator[Record]:
        cairn_dir = os.path.join(self._cairns, cairn_id)
        if not os.path.isdir(cairn_dir):
            return
        for record_id in sorted(os.listdir(cairn_dir), reverse=True):
            if record_id.startswith("."):
                continue
            record = os.path.join(cairn_dir, record_id)
            meta = _read_meta(record)
            if meta is None:
                continue
            error_str = meta.get("error")
            if error_str is None:
                result = _read_result(record)
                if result is _MISSING:
                    continue
                traces = _read_traces(record)
                err: Exception | None = None
            else:
                result = None
                traces = _read_traces(record)
                err = Exception(str(error_str))
            yield Record(
                result=result,
                traces=traces,
                duration=float(meta.get("duration", 0.0)),
                own_duration=float(meta.get("own_duration", 0.0)),
        cached_duration=float(meta.get("cached_duration", 0.0)),
                error=err,
                cairn_id=cairn_id,
                record_id=record_id,
                record_path=record,
                result_hash=meta.get("result_hash"),
                child_refs=list(meta.get("children", []) or []),
                tags=dict(meta.get("tags", {}) or {}),
                body_hash=meta.get("body_hash"),
                version=meta.get("version"),
            )

    # ── write ──

    def put(
        self,
        key: str,
        entry: Record,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> StoreStats:
        md = metadata or {}
        children: list[dict[str, Any]] = list(md.get("children") or [])
        events_stream: list[dict[str, Any]] = list(md.get("events") or [])

        # Hold a shared store lock for the whole publication: CAS write + record
        # tmp-dir + atomic rename. GC takes the exclusive lock and will wait for
        # every in-flight put to drain before sweeping.
        with store_shared(self._base):
            return self._put_locked(
                key=key,
                entry=entry,
                md=md,
                children=children,
                events_stream=events_stream,
            )

    def _put_locked(
        self,
        *,
        key: str,
        entry: Record,
        md: dict[str, Any],
        children: list[dict[str, Any]],
        events_stream: list[dict[str, Any]],
    ) -> StoreStats:
        result_hash: str | None = None
        result_path: str | None = None
        if entry.error is None:
            payload = _result_payload(entry)
            result_hash = _hash_payload(payload)
            result_path = os.path.join(self._store, f"{result_hash}.json")
            if not os.path.exists(result_path):
                self._atomic_write(result_path, payload)

        record_id = _new_record_id()
        cairn_dir = os.path.join(self._cairns, key)
        os.makedirs(cairn_dir, exist_ok=True)
        tmp_dir = os.path.join(cairn_dir, f".tmp-{record_id}")
        stone_dir = os.path.join(cairn_dir, record_id)
        os.makedirs(tmp_dir, exist_ok=False)

        # metadata.json
        metadata_children = [
            {k: v for k, v in child.items() if k != "record_path"}
            for child in children
        ]
        meta = {
            "cairn_id": key,
            "origin": md.get("origin", "created"),
            "body_hash": entry.body_hash,
            "version": entry.version,
            "duration": entry.duration,
            "own_duration": entry.own_duration,
            "cached_duration": entry.cached_duration,
            "error": str(entry.error) if entry.error else None,
            "short_name": md.get("short_name"),
            "ts_created": time.time(),
            "result_hash": result_hash,
            "result_repr": repr(entry.result)[:200],
            "args_repr": md.get("args_repr", {}),
            "children": metadata_children,
            "tags": dict(md.get("tags") or entry.tags or {}),
        }
        with open(os.path.join(tmp_dir, "metadata.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, sort_keys=True, indent=2, default=str)
            f.write("\n")

        # events.jsonl — caller supplies a pre-rebased, ordered stream.
        with open(os.path.join(tmp_dir, "events.jsonl"), "w", encoding="utf-8") as f:
            for e in events_stream:
                f.write(json.dumps(e, default=str) + "\n")
            f.write(
                json.dumps(
                    {
                        "kind": "end",
                        "ts": entry.duration,
                        "duration": entry.duration,
                        "own_duration": entry.own_duration,
                    }
                )
                + "\n"
            )

        # result symlink
        if result_path is not None:
            os.symlink(
                os.path.relpath(result_path, tmp_dir),
                os.path.join(tmp_dir, "result"),
            )

        # children/NNN symlinks
        if children:
            child_dir = os.path.join(tmp_dir, "children")
            os.makedirs(child_dir, exist_ok=True)
            for i, child in enumerate(children):
                target = child.get("record_path")
                if target:
                    os.symlink(
                        os.path.relpath(target, child_dir),
                        os.path.join(child_dir, f"{i:03d}"),
                    )

        os.replace(tmp_dir, stone_dir)

        size = len(_result_payload(entry).encode("utf-8"))
        entry.cairn_id = key
        entry.record_id = record_id
        entry.record_path = stone_dir
        entry.result_hash = result_hash
        entry.child_refs = metadata_children
        return StoreStats(
            size=size,
            own_size=size,
            cairn_id=key,
            record_id=record_id,
            record_path=stone_dir,
            result_hash=result_hash,
        )

    @staticmethod
    def _atomic_write(path: str, payload: str) -> None:
        tmp = path + f".{uuid.uuid4().hex}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)


# ── Overlay store (carry) ──


class OverlayStore:
    """Read-overlay over a base Store, keyed by cairn_id → record path.

    Hits in the overlay short-circuit to the referenced record, bypassing the
    memo predicate and the subtree-integrity check — carry is an explicit
    opt-in by the caller. Writes pass through to the base store unchanged.

    A missing record at an overlay path raises `FileNotFoundError`. Carry is
    an explicit opt-in; silent fallback would hide user error.
    """

    def __init__(self, overlay: dict[str, str], base: Store) -> None:
        self._overlay = dict(overlay)
        self._base = base

    def find(self, key: str, predicate: Predicate | None = None) -> Record | None:
        path = self._overlay.get(key)
        if path is None:
            return self._base.find(key, predicate)
        entry = load_record(path)
        if entry is None:
            raise FileNotFoundError(
                f"carry: no valid record at {path!r} for cairn {key[:12]}…"
            )
        entry.origin = "carried"
        return entry

    def put(
        self,
        key: str,
        entry: Record,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> StoreStats:
        return self._base.put(key, entry, metadata=metadata)

    def iter_records(self, cairn_id: str) -> Iterator[Record]:
        # Carry is a read-time detour, not history. Inspection sees the
        # base store's actual record stack.
        return self._base.iter_records(cairn_id)
