"""Shared/exclusive store lock.

`.cairn/gc.lock` is the rendezvous file. `FileStore.put` takes a **shared** lock
while publishing a stone (makedirs → rename); `gc_outputs` takes an **exclusive**
lock while running mark + sweep. Multiple concurrent runs can publish; a GC
request waits until in-flight publishes drain, and blocks new publishes until
sweep completes.

POSIX-only (fcntl). The design doc calls out that Windows isn't supported.
"""

from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from typing import Iterator


def _lock_path(store_path: str) -> str:
    return os.path.join(store_path, "gc.lock")


def _ensure_lockfile(store_path: str) -> str:
    os.makedirs(store_path, exist_ok=True)
    path = _lock_path(store_path)
    if not os.path.exists(path):
        # Touch the file; O_EXCL avoids a rare race where two callers both
        # create it. EEXIST means someone got there first — fine.
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.close(fd)
        except FileExistsError:
            pass
    return path


@contextmanager
def store_shared(store_path: str) -> Iterator[None]:
    """Shared (reader) lock: held by each FileStore.put during publication."""
    path = _ensure_lockfile(store_path)
    fd = os.open(path, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextmanager
def gc_exclusive(store_path: str) -> Iterator[None]:
    """Exclusive (writer) lock: held by gc during mark + sweep."""
    path = _ensure_lockfile(store_path)
    fd = os.open(path, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
