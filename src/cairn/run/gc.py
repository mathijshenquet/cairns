"""Garbage collection for the Cairn store.

Nix-style: remove runs, then sweep orphaned outputs.
"""

from __future__ import annotations

import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from cairn.core.lock import gc_exclusive as _gc_exclusive


_RUN_ID_RE = re.compile(r"^(?P<entry>.+)-(?P<ts>\d{4}-\d{2}-\d{2}T[\d:.]+)$")


@dataclass
class RunInfo:
    """Information about a single run."""

    run_id: str
    entry_name: str
    timestamp: datetime
    path: str
    is_latest: bool
    symlink_count: int


def _parse_run_id(run_id: str) -> tuple[str, datetime] | None:
    """Parse `{entry}-{ISO datetime}` into (entry_name, timestamp), or None.

    The entry name may contain hyphens; the regex anchors on the trailing ISO
    timestamp (YYYY-MM-DDTHH:MM:SS[.ffffff]) and treats everything before it
    as the entry name.
    """
    m = _RUN_ID_RE.match(run_id)
    if m is None:
        return None
    try:
        ts = datetime.fromisoformat(m["ts"]).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return (m["entry"], ts)


def _get_gc_roots(runs_dir: str) -> set[str]:
    """Get the set of run directory names that are GC roots.

    GC roots are symlinks at the runs/ level: {entry_name} → {entry_name}-{datetime}.
    """
    roots: set[str] = set()
    if not os.path.isdir(runs_dir):
        return roots

    for entry in os.scandir(runs_dir):
        if entry.is_symlink():
            # This is a GC root symlink (e.g., 'pipeline' → 'pipeline-2026-...')
            target = os.readlink(entry.path)
            roots.add(os.path.basename(target))
    return roots


def _mark_stone(stone: str, live_stones: set[str], live_outputs: set[str]) -> None:
    stone = os.path.realpath(stone)
    if stone in live_stones or not os.path.isdir(stone):
        return
    live_stones.add(stone)
    result = os.path.join(stone, "result")
    if os.path.exists(result):
        live_outputs.add(os.path.basename(os.path.realpath(result)))
    children = os.path.join(stone, "children")
    if os.path.isdir(children):
        for entry in os.scandir(children):
            if entry.is_symlink():
                _mark_stone(entry.path, live_stones, live_outputs)


def _live_from_runs(store_path: str) -> tuple[set[str], set[str]]:
    """Return live stone dirs and store filenames reachable from run steps."""
    live_stones: set[str] = set()
    live_outputs: set[str] = set()
    runs_dir = os.path.join(store_path, "runs")
    if not os.path.isdir(runs_dir):
        return live_stones, live_outputs
    for run in os.scandir(runs_dir):
        if not run.is_dir(follow_symlinks=False) or _parse_run_id(run.name) is None:
            continue
        steps = os.path.join(run.path, "steps")
        if not os.path.isdir(steps):
            continue
        for step in os.scandir(steps):
            if step.is_symlink():
                _mark_stone(step.path, live_stones, live_outputs)
    return live_stones, live_outputs


def list_runs(store_path: str) -> list[RunInfo]:
    """List all runs in the store, sorted by timestamp (oldest first)."""
    runs_dir = os.path.join(store_path, "runs")
    if not os.path.isdir(runs_dir):
        return []

    gc_roots = _get_gc_roots(runs_dir)
    runs: list[RunInfo] = []

    for entry in os.scandir(runs_dir):
        if not entry.is_dir(follow_symlinks=False):
            continue
        parsed = _parse_run_id(entry.name)
        if parsed is None:
            continue

        entry_name, timestamp = parsed
        steps = os.path.join(entry.path, "steps")
        count_dir = steps if os.path.isdir(steps) else entry.path
        symlink_count = sum(1 for e in os.scandir(count_dir) if e.is_symlink())

        runs.append(RunInfo(
            run_id=entry.name,
            entry_name=entry_name,
            timestamp=timestamp,
            path=entry.path,
            is_latest=(entry.name in gc_roots),
            symlink_count=symlink_count,
        ))

    runs.sort(key=lambda r: r.timestamp)
    return runs


def remove_run(store_path: str, run_id: str) -> bool:
    """Remove a specific run directory. Returns True if removed."""
    run_path = os.path.join(store_path, "runs", run_id)
    if not os.path.isdir(run_path):
        return False
    shutil.rmtree(run_path)
    return True


def remove_runs_before(
    store_path: str,
    before: datetime,
    *,
    keep_latest: bool = True,
) -> list[str]:
    """Remove runs older than a given datetime.

    Args:
        store_path: Path to the .cairn directory.
        before: Remove runs with timestamps before this datetime.
        keep_latest: If True, never remove runs that are the 'latest' for their entry point.

    Returns:
        List of removed run IDs.
    """
    runs = list_runs(store_path)
    removed: list[str] = []

    for r in runs:
        if r.timestamp >= before:
            continue
        if keep_latest and r.is_latest:
            continue
        if remove_run(store_path, r.run_id):
            removed.append(r.run_id)

    return removed


_TMP_STALE_SECONDS = 15 * 60


def _sweep_abandoned_tmp(cairn_dir: str, *, max_age: float = _TMP_STALE_SECONDS) -> None:
    """Remove .tmp-{id} dirs older than `max_age` seconds.

    A crash between `makedirs(.tmp-…)` and `os.replace(..., stone_id)` leaves a
    partial stone the reader is designed to ignore (missing metadata.json). This
    janitor cleans them up so they don't accumulate forever.
    """
    now = time.time()
    for entry in os.scandir(cairn_dir):
        if not entry.name.startswith(".tmp-"):
            continue
        try:
            mtime = entry.stat(follow_symlinks=False).st_mtime
        except OSError:
            continue
        if now - mtime < max_age:
            continue
        try:
            if entry.is_dir(follow_symlinks=False):
                shutil.rmtree(entry.path)
            else:
                os.unlink(entry.path)
        except OSError:
            pass


def gc_outputs(store_path: str) -> list[str]:
    """Sweep stones, CAS entries, and stale .tmp-* dirs not reachable from any run.

    Mark phase: every run's `steps/*` symlink pulls its target stone into the
    live set, which transitively pulls `children/*` stones and each stone's
    `result` content hash. Sweep phase: delete unreferenced stones, clean up
    abandoned `.tmp-*` dirs, drop empty cairns, and remove unreferenced CAS
    files.

    Returns the list of removed CAS filenames (stones removed silently).
    """
    outputs_dir = os.path.join(store_path, "store")
    if not os.path.isdir(outputs_dir):
        return []

    with _gc_exclusive(store_path):
        live_stones, referenced = _live_from_runs(store_path)

        cairns_dir = os.path.join(store_path, "cairns")
        if os.path.isdir(cairns_dir):
            for cairn in os.scandir(cairns_dir):
                if not cairn.is_dir():
                    continue
                _sweep_abandoned_tmp(cairn.path)
                for stone in os.scandir(cairn.path):
                    if stone.name.startswith("."):
                        continue
                    if stone.is_dir() and os.path.realpath(stone.path) not in live_stones:
                        shutil.rmtree(stone.path)
                if not any(os.scandir(cairn.path)):
                    os.rmdir(cairn.path)

        removed: list[str] = []
        for entry in os.scandir(outputs_dir):
            if entry.is_file() and entry.name not in referenced:
                os.unlink(entry.path)
                removed.append(entry.name)

        return removed


def gc(
    store_path: str,
    *,
    before: datetime | None = None,
    keep_latest: bool = True,
) -> tuple[list[str], list[str]]:
    """Full garbage collection: remove old runs, then sweep orphaned outputs.

    Args:
        store_path: Path to the .cairn directory.
        before: If given, remove runs older than this. If None, don't remove any runs
                (only gc orphaned outputs).
        keep_latest: If True, never remove the latest run for each entry point.

    Returns:
        Tuple of (removed_run_ids, removed_output_files).
    """
    removed_runs: list[str] = []
    if before is not None:
        removed_runs = remove_runs_before(store_path, before, keep_latest=keep_latest)

    removed_outputs = gc_outputs(store_path)
    return removed_runs, removed_outputs
