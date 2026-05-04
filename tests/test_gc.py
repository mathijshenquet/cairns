"""Tests for garbage collection."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path

from cairns import step, gc, list_runs, remove_run, remove_runs_before, run, trace


def _store_files(store_path: str) -> set[str]:
    store_dir = os.path.join(store_path, "store")
    if not os.path.isdir(store_dir):
        return set()
    return set(os.listdir(store_dir))


def _stone_dirs(store_path: str) -> list[str]:
    cairns = os.path.join(store_path, "cairns")
    if not os.path.isdir(cairns):
        return []
    return [
        os.path.join(cairn.path, record.name)
        for cairn in os.scandir(cairns)
        if cairn.is_dir()
        for record in os.scandir(cairn.path)
        if record.is_dir()
    ]


def _make_runs(tmp_path: Path, n: int = 3) -> str:
    """Helper: create n runs of a simple pipeline."""
    store_path = str(tmp_path / ".cairns")

    @step
    async def work(x: int) -> int:
        trace("working")
        return x * 2

    for i in range(n):
        run(work(i), store_path=store_path)
        time.sleep(0.01)  # ensure distinct timestamps

    return store_path


def test_list_runs(tmp_path: Path) -> None:
    """list_runs() returns all runs sorted by timestamp."""
    store_path = _make_runs(tmp_path, n=3)
    runs = list_runs(store_path)

    assert len(runs) == 3
    assert all(r.entry_name == "work" for r in runs)
    assert runs[0].timestamp <= runs[1].timestamp <= runs[2].timestamp
    assert runs[-1].is_latest
    assert not runs[0].is_latest
    assert all(r.symlink_count >= 1 for r in runs)


def test_list_runs_empty(tmp_path: Path) -> None:
    """list_runs() on empty store returns empty list."""
    assert list_runs(str(tmp_path / ".cairns")) == []


def test_remove_run(tmp_path: Path) -> None:
    """remove_run() deletes a specific run directory."""
    store_path = _make_runs(tmp_path, n=2)
    runs = list_runs(store_path)
    assert len(runs) == 2

    removed = remove_run(store_path, runs[0].run_id)
    assert removed

    remaining = list_runs(store_path)
    assert len(remaining) == 1
    assert remaining[0].run_id == runs[1].run_id


def test_remove_run_nonexistent(tmp_path: Path) -> None:
    """remove_run() returns False for nonexistent run."""
    store_path = str(tmp_path / ".cairns")
    assert not remove_run(store_path, "nonexistent-run")


def test_remove_runs_before(tmp_path: Path) -> None:
    """remove_runs_before() deletes old runs but keeps latest."""
    store_path = _make_runs(tmp_path, n=3)
    runs = list_runs(store_path)

    cutoff = runs[-1].timestamp
    removed = remove_runs_before(store_path, cutoff, keep_latest=True)

    remaining = list_runs(store_path)
    assert any(r.is_latest for r in remaining)
    assert len(removed) >= 1


def test_remove_runs_before_keeps_latest(tmp_path: Path) -> None:
    """Even with a very old cutoff, latest is never removed when keep_latest=True."""
    store_path = _make_runs(tmp_path, n=1)
    runs = list_runs(store_path)
    assert len(runs) == 1
    assert runs[0].is_latest

    far_future = datetime(2099, 1, 1, tzinfo=timezone.utc)
    removed = remove_runs_before(store_path, far_future, keep_latest=True)
    assert len(removed) == 0

    removed = remove_runs_before(store_path, far_future, keep_latest=False)
    assert len(removed) == 1


def test_gc_outputs(tmp_path: Path) -> None:
    """gc_outputs() removes records and CAS entries not referenced by any remaining run."""
    store_path = _make_runs(tmp_path, n=3)

    store_before = _store_files(store_path)
    stones_before = _stone_dirs(store_path)
    assert len(store_before) >= 3
    assert len(stones_before) >= 3

    # Remove first two runs — their records should now be unreachable.
    runs = list_runs(store_path)
    remove_run(store_path, runs[0].run_id)
    remove_run(store_path, runs[1].run_id)

    from cairns.run import gc_outputs
    gc_outputs(store_path)

    # Every symlink under any remaining run's steps/ still resolves.
    for r in list_runs(store_path):
        steps_dir = os.path.join(r.path, "steps")
        if not os.path.isdir(steps_dir):
            continue
        for entry in os.scandir(steps_dir):
            if entry.is_symlink():
                target = Path(entry.path).resolve()
                assert target.exists(), f"steps/{entry.name} points to missing record"
                assert (target / "metadata.json").exists()


def test_gc_full_cycle(tmp_path: Path) -> None:
    """Full GC: remove old runs, sweep orphaned records + CAS files."""
    store_path = _make_runs(tmp_path, n=5)

    store_before = len(_store_files(store_path))
    runs_before = list_runs(store_path)
    assert len(runs_before) == 5

    cutoff = runs_before[3].timestamp
    gc(store_path, before=cutoff, keep_latest=True)

    remaining = list_runs(store_path)
    assert len(remaining) <= 3
    assert any(r.is_latest for r in remaining)

    store_after = len(_store_files(store_path))
    assert store_after <= store_before


def test_gc_with_shared_outputs(tmp_path: Path) -> None:
    """CAS entries shared between runs survive until all referring runs are gone."""
    store_path = str(tmp_path / ".cairns")

    @step(memo=True)
    async def constant() -> str:
        return "always the same"

    run(constant(), store_path=store_path)
    time.sleep(0.01)
    run(constant(), store_path=store_path)

    runs = list_runs(store_path)
    assert len(runs) == 2

    remove_run(store_path, runs[0].run_id)

    from cairns.run import gc_outputs
    gc_outputs(store_path)

    remaining_run = list_runs(store_path)[0]
    steps_dir = os.path.join(remaining_run.path, "steps")
    for entry in os.scandir(steps_dir):
        if entry.is_symlink():
            target = Path(entry.path).resolve()
            assert target.exists()
            assert (target / "result").exists()


def test_gc_removes_unreachable_stones(tmp_path: Path) -> None:
    """Dropping every run makes records unreachable; GC removes them."""
    store_path = _make_runs(tmp_path, n=2)

    assert len(_stone_dirs(store_path)) >= 2

    # Nuke every run directory (including GC-root symlinks).
    runs_dir = Path(store_path) / "runs"
    for entry in runs_dir.iterdir():
        if entry.is_symlink():
            entry.unlink()
        else:
            import shutil
            shutil.rmtree(entry)

    from cairns.run import gc_outputs
    gc_outputs(store_path)

    assert _stone_dirs(store_path) == []
    assert _store_files(store_path) == set()


def test_list_runs_multiple_entry_points(tmp_path: Path) -> None:
    """list_runs() works with multiple different entry points."""
    store_path = str(tmp_path / ".cairns")

    @step
    async def pipeline_a() -> str:
        return "a"

    @step
    async def pipeline_b() -> str:
        return "b"

    run(pipeline_a(), store_path=store_path)
    time.sleep(0.01)
    run(pipeline_b(), store_path=store_path)
    time.sleep(0.01)
    run(pipeline_a(), store_path=store_path)

    runs = list_runs(store_path)
    assert len(runs) == 3

    a_runs = [r for r in runs if r.entry_name == "pipeline_a"]
    b_runs = [r for r in runs if r.entry_name == "pipeline_b"]
    assert len(a_runs) == 2
    assert len(b_runs) == 1

    assert a_runs[-1].is_latest
    assert b_runs[0].is_latest
