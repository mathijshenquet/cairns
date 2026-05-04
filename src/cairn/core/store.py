"""Cairn / stone storage backends.

A cairn is a directory keyed by `cairn_id = hash(identity, args)` holding an
append-only stack of stones (immutable execution records). The CAS under
`store/` holds value bytes only; stone-local scalars and event timing live
inside each stone.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterator, Protocol, cast

from .lock import store_shared
from .serial import from_jsonable, to_jsonable
from .types import CacheEntry, TraceRecord

_MISSING = object()


@dataclass(frozen=True)
class StoreStats:
    """Size metrics and address of a published stone."""

    size: int
    own_size: int
    cairn_id: str | None = None
    stone_id: str | None = None
    stone_path: str | None = None
    result_hash: str | None = None


@dataclass(frozen=True)
class StoneInfo:
    """Small metadata view needed to replay or display a stone."""

    cairn_id: str | None
    stone_id: str
    stone_path: str
    short_name: str | None
    duration: float
    own_duration: float


class Store(Protocol):
    """Protocol for cache storage backends."""

    def get(self, key: str, version: str | None = None) -> CacheEntry | None: ...

    def put(
        self,
        key: str,
        entry: CacheEntry,
        *,
        version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StoreStats: ...


# ── Serialization helpers ──


def trace_to_event(t: TraceRecord, start_ts: float) -> dict[str, Any]:
    """Serialize a TraceRecord as a stone-relative event line."""
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


def _result_payload(entry: CacheEntry) -> str:
    """CAS payload: the value bytes, routed through the serializer registry.

    Typed values (Pydantic models, tuples, …) are wrapped with a tag so the
    reader can reconstruct the original object. Plain JSON-native values pass
    through unchanged.
    """
    return json.dumps({"result": to_jsonable(entry.result)}, sort_keys=True)


def _hash_payload(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ── In-memory store ──


@dataclass
class _MemStone:
    entry: CacheEntry
    version: str | None


class MemoryStore:
    """In-memory cairn stack for testing."""

    def __init__(self) -> None:
        self._stacks: dict[str, list[_MemStone]] = {}

    def get(self, key: str, version: str | None = None) -> CacheEntry | None:
        for stone in reversed(self._stacks.get(key, [])):
            if stone.entry.error is not None:
                continue
            if version is not None and stone.version != version:
                continue
            return stone.entry
        return None

    def put(
        self,
        key: str,
        entry: CacheEntry,
        *,
        version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StoreStats:
        md = metadata or {}
        stone_id = f"mem-{len(self._stacks.get(key, [])) + 1}"
        entry.cairn_id = key
        entry.stone_id = stone_id
        entry.stone_path = None
        entry.child_refs = list(md.get("children", []))
        payload = _result_payload(entry)
        result_hash = None if entry.error else _hash_payload(payload)
        entry.result_hash = result_hash
        self._stacks.setdefault(key, []).append(_MemStone(entry=entry, version=version))
        size = len(payload.encode("utf-8"))
        return StoreStats(
            size=size,
            own_size=size,
            cairn_id=key,
            stone_id=stone_id,
            result_hash=result_hash,
        )


# ── File-backed store ──


import secrets


def _new_stone_id() -> str:
    """RFC 9562 uuid7 — 48-bit ms timestamp + 74-bit randomness.

    Time-ordered by lexicographic filename sort, so `ls` on a cairn returns
    stones in creation order without a separate index.
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


def _read_meta(stone: str) -> dict[str, Any] | None:
    meta_path = os.path.join(stone, "metadata.json")
    if not os.path.isfile(meta_path):
        return None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _infer_cairn_id(stone_path: str, meta: dict[str, Any]) -> str | None:
    value = meta.get("cairn_id")
    if isinstance(value, str):
        return value
    cairn_dir = os.path.dirname(os.path.abspath(stone_path))
    return os.path.basename(cairn_dir) or None


def read_stone_info(stone_path: str) -> StoneInfo | None:
    """Read the scalar metadata for a stone without loading its result."""
    meta = _read_meta(stone_path)
    if meta is None:
        return None
    return StoneInfo(
        cairn_id=_infer_cairn_id(stone_path, meta),
        stone_id=os.path.basename(stone_path),
        stone_path=stone_path,
        short_name=meta.get("short_name"),
        duration=float(meta.get("duration", 0.0)),
        own_duration=float(meta.get("own_duration", 0.0)),
    )


def iter_stone_events(stone_path: str) -> Iterator[dict[str, Any]]:
    """Yield well-formed event records from a stone's events.jsonl."""
    events_path = os.path.join(stone_path, "events.jsonl")
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


def child_stone_path(stone_path: str, child_index: int) -> str | None:
    """Resolve a stone's ordered children/{index:03d} symlink."""
    if child_index < 0:
        return None
    child = os.path.join(stone_path, "children", f"{child_index:03d}")
    target = os.path.realpath(child)
    if not os.path.isdir(target):
        return None
    if not os.path.isfile(os.path.join(target, "metadata.json")):
        return None
    return target


def _read_result(stone: str) -> Any:
    link = os.path.join(stone, "result")
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


def _read_traces(stone: str) -> list[TraceRecord]:
    traces: list[TraceRecord] = []
    for e in iter_stone_events(stone):
        if e.get("kind") == "trace":
            traces.append(_event_to_trace(e))
    return traces


def _children_resolve(stone: str) -> bool:
    """Subtree integrity: every children/* symlink must resolve to a stone."""
    child_dir = os.path.join(stone, "children")
    if not os.path.isdir(child_dir):
        return True
    for entry in os.scandir(child_dir):
        target = os.path.realpath(entry.path)
        if not os.path.isdir(target):
            return False
        if not os.path.isfile(os.path.join(target, "metadata.json")):
            return False
    return True


def load_stone(stone_path: str) -> CacheEntry | None:
    """Load a single stone from disk as a CacheEntry.

    Used by the carry resolver — a stone may live in any cairn, including a
    synthetic one authored by the caller. Does **not** run subtree integrity
    checks: carry is an explicit opt-in by the caller.
    """
    if not os.path.isdir(stone_path):
        return None
    meta = _read_meta(stone_path)
    if meta is None:
        return None
    if meta.get("error") is not None:
        return None
    result = _read_result(stone_path)
    if result is _MISSING:
        return None
    traces = _read_traces(stone_path)
    return CacheEntry(
        result=result,
        traces=traces,
        duration=float(meta.get("duration", 0.0)),
        own_duration=float(meta.get("own_duration", 0.0)),
        error=None,
        cairn_id=_infer_cairn_id(stone_path, meta),
        stone_id=os.path.basename(stone_path),
        stone_path=stone_path,
        result_hash=meta.get("result_hash"),
        child_refs=list(meta.get("children", []) or []),
    )


class FileStore:
    """Filesystem cairn store.

    Layout::

        {base}/cairns/{cairn_id}/{stone_id}/
            metadata.json              # scalars, args_repr, children pointers
            events.jsonl               # traces + child spawns, stone-relative ts
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

    def get(self, key: str, version: str | None = None) -> CacheEntry | None:
        cairn_dir = os.path.join(self._cairns, key)
        if not os.path.isdir(cairn_dir):
            return None
        for stone_id in sorted(os.listdir(cairn_dir), reverse=True):
            if stone_id.startswith("."):
                continue
            stone = os.path.join(cairn_dir, stone_id)
            meta = _read_meta(stone)
            if meta is None:
                continue
            if meta.get("error") is not None:
                continue
            if version is not None and meta.get("version") != version:
                continue
            if not _children_resolve(stone):
                continue
            result = _read_result(stone)
            if result is _MISSING:
                continue
            traces = _read_traces(stone)
            entry = CacheEntry(
                result=result,
                traces=traces,
                duration=float(meta.get("duration", 0.0)),
                own_duration=float(meta.get("own_duration", 0.0)),
                error=None,
                cairn_id=key,
                stone_id=stone_id,
                stone_path=stone,
                result_hash=meta.get("result_hash"),
                child_refs=list(meta.get("children", []) or []),
            )
            return entry
        return None

    # ── write ──

    def put(
        self,
        key: str,
        entry: CacheEntry,
        *,
        version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StoreStats:
        md = metadata or {}
        children: list[dict[str, Any]] = list(md.get("children") or [])
        events_stream: list[dict[str, Any]] = list(md.get("events") or [])

        # Hold a shared store lock for the whole publication: CAS write + stone
        # tmp-dir + atomic rename. GC takes the exclusive lock and will wait for
        # every in-flight put to drain before sweeping.
        with store_shared(self._base):
            return self._put_locked(
                key=key,
                entry=entry,
                version=version,
                md=md,
                children=children,
                events_stream=events_stream,
            )

    def _put_locked(
        self,
        *,
        key: str,
        entry: CacheEntry,
        version: str | None,
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

        stone_id = _new_stone_id()
        cairn_dir = os.path.join(self._cairns, key)
        os.makedirs(cairn_dir, exist_ok=True)
        tmp_dir = os.path.join(cairn_dir, f".tmp-{stone_id}")
        stone_dir = os.path.join(cairn_dir, stone_id)
        os.makedirs(tmp_dir, exist_ok=False)

        # metadata.json
        metadata_children = [
            {k: v for k, v in child.items() if k != "stone_path"}
            for child in children
        ]
        meta = {
            "cairn_id": key,
            "origin": md.get("origin", "created"),
            "version": version,
            "duration": entry.duration,
            "own_duration": entry.own_duration,
            "error": str(entry.error) if entry.error else None,
            "short_name": md.get("short_name"),
            "ts_created": time.time(),
            "result_hash": result_hash,
            "result_repr": repr(entry.result)[:200],
            "args_repr": md.get("args_repr", {}),
            "children": metadata_children,
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
                target = child.get("stone_path")
                if target:
                    os.symlink(
                        os.path.relpath(target, child_dir),
                        os.path.join(child_dir, f"{i:03d}"),
                    )

        os.replace(tmp_dir, stone_dir)

        size = len(_result_payload(entry).encode("utf-8"))
        entry.cairn_id = key
        entry.stone_id = stone_id
        entry.stone_path = stone_dir
        entry.result_hash = result_hash
        entry.child_refs = metadata_children
        return StoreStats(
            size=size,
            own_size=size,
            cairn_id=key,
            stone_id=stone_id,
            stone_path=stone_dir,
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
    """Read-overlay over a base Store, keyed by cairn_id → stone path.

    Hits in the overlay short-circuit to the referenced stone, bypassing both
    the version filter and the subtree-integrity check — carry is an explicit
    opt-in by the caller. Writes pass through to the base store unchanged.

    A missing stone at an overlay path raises `FileNotFoundError`. Carry is
    an explicit opt-in; silent fallback would hide user error.
    """

    def __init__(self, overlay: dict[str, str], base: Store) -> None:
        self._overlay = dict(overlay)
        self._base = base

    def get(self, key: str, version: str | None = None) -> CacheEntry | None:
        path = self._overlay.get(key)
        if path is None:
            return self._base.get(key, version)
        entry = load_stone(path)
        if entry is None:
            raise FileNotFoundError(
                f"carry: no valid stone at {path!r} for cairn {key[:12]}…"
            )
        entry.origin = "carried"
        return entry

    def put(
        self,
        key: str,
        entry: CacheEntry,
        *,
        version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StoreStats:
        return self._base.put(key, entry, version=version, metadata=metadata)
