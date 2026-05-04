"""Tests for on-disk store, JSONL trace, and run()."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cairns import step, run, trace


def test_run_creates_disk_layout(tmp_path: Path) -> None:
    """run() creates store/ + cairns/ + runs/ with the record layout."""
    store_path = str(tmp_path / ".cairns")

    @step
    async def greet(name: str) -> str:
        trace("building greeting")
        return f"hello {name}"

    result = run(greet, store_path=store_path, args=("world",))
    assert result == "hello world"

    # Value-bytes CAS holds a {"result": ...} payload, nothing else.
    store = tmp_path / ".cairns" / "store"
    assert store.is_dir()
    store_files = list(store.glob("*.json"))
    assert len(store_files) >= 1
    with open(store_files[0], "r") as f:
        payload = json.load(f)
    assert set(payload.keys()) == {"result"}
    assert payload["result"] == "hello world"

    # Cairn holds records; each record has metadata + events + result symlink.
    cairns = tmp_path / ".cairns" / "cairns"
    assert cairns.is_dir()
    records = [p for p in cairns.glob("*/*") if p.is_dir()]
    assert records
    record = records[0]
    assert (record / "metadata.json").is_file()
    assert (record / "events.jsonl").is_file()
    assert (record / "result").exists()

    # The merged run trace is still on disk.
    runs = tmp_path / ".cairns" / "runs"
    run_dirs = [d for d in runs.iterdir() if d.is_dir() and d.name.startswith("greet-")]
    assert len(run_dirs) == 1
    trace_file = run_dirs[0] / "trace.jsonl"
    assert trace_file.exists()
    with open(trace_file, "r") as f:
        events = [json.loads(line) for line in f if line.strip()]
    event_types = {e["e"] for e in events}
    assert {"spawn", "start", "end", "trace"} <= event_types


def test_run_creates_step_symlinks(tmp_path: Path) -> None:
    """run() creates ordered step symlinks under runs/*/steps/ pointing at records."""
    store_path = str(tmp_path / ".cairns")

    @step
    async def add(a: int, b: int) -> int:
        return a + b

    result = run(add, store_path=store_path, args=(1, 2))
    assert result == 3

    runs = tmp_path / ".cairns" / "runs"
    run_dirs = [d for d in runs.iterdir() if d.is_dir() and d.name.startswith("add-")]
    assert len(run_dirs) == 1
    steps = run_dirs[0] / "steps"
    assert steps.is_dir()

    symlinks = [f for f in steps.iterdir() if f.is_symlink()]
    assert len(symlinks) >= 1
    # Each symlink points to a record directory (contains metadata.json).
    for link in symlinks:
        target = link.resolve()
        assert target.is_dir()
        assert (target / "metadata.json").is_file()


def test_run_creates_gc_root_symlink(tmp_path: Path) -> None:
    """run() maintains a GC root symlink for the entry point."""
    store_path = str(tmp_path / ".cairns")

    @step
    async def compute() -> int:
        return 42

    run(compute, store_path=store_path)

    gc_root = tmp_path / ".cairns" / "runs" / "compute"
    assert gc_root.is_symlink()
    assert (gc_root / "trace.jsonl").exists()


def test_run_caches_across_runs(tmp_path: Path) -> None:
    """Second run() reuses cached outputs from first run."""
    store_path = str(tmp_path / ".cairns")
    call_count = 0

    @step(memo=True)
    async def expensive() -> str:
        nonlocal call_count
        call_count += 1
        return "result"

    result1 = run(expensive, store_path=store_path)
    assert result1 == "result"
    assert call_count == 1

    result2 = run(expensive, store_path=store_path)
    assert result2 == "result"
    assert call_count == 1  # not called again


def test_run_with_fanout(tmp_path: Path) -> None:
    """run() handles fan-out correctly on disk."""
    store_path = str(tmp_path / ".cairns")

    @step
    async def double(x: int) -> int:
        return x * 2

    @step
    async def pipeline() -> list[int]:
        handles = [double(i) for i in range(3)]
        return [await h for h in handles]

    result = run(pipeline, store_path=store_path)
    assert result == [0, 2, 4]

    store_files = list((tmp_path / ".cairns" / "store").glob("*.json"))
    assert len(store_files) >= 3  # at least one CAS entry per distinct value

    runs = tmp_path / ".cairns" / "runs"
    run_dirs = [d for d in runs.iterdir() if d.is_dir() and d.name.startswith("pipeline-")]
    steps = run_dirs[0] / "steps"
    symlinks = [f for f in steps.iterdir() if f.is_symlink()]
    assert len(symlinks) >= 4  # pipeline + 3 doubles


def test_cairn_stone_layout_and_recalled_subtree(tmp_path: Path) -> None:
    """Runs publish immutable records; cache hits replay child record spans."""
    store_path = str(tmp_path / ".cairns")
    calls: dict[str, int] = {"leaf": 0, "root": 0}

    @step(memo=True)
    async def leaf() -> str:
        calls["leaf"] += 1
        trace("leaf trace")
        return "leaf"

    @step(memo=True)
    async def root() -> str:
        calls["root"] += 1
        return await leaf()

    assert run(root, store_path=store_path) == "leaf"
    assert run(root, store_path=store_path) == "leaf"
    assert calls == {"leaf": 1, "root": 1}

    cairns = tmp_path / ".cairns" / "cairns"
    records = [p for p in cairns.glob("*/*") if p.is_dir()]
    assert records
    assert all((s / "metadata.json").exists() and (s / "events.jsonl").exists() for s in records)

    root_stone = next(s for s in records if json.loads((s / "metadata.json").read_text()).get("short_name") == "root")
    root_meta = json.loads((root_stone / "metadata.json").read_text())
    assert root_meta["cairn_id"] == root_stone.parent.name
    assert all("record_path" not in child for child in root_meta["children"])
    assert (root_stone / "children" / "000").is_symlink()
    root_event_lines = [
        json.loads(line)
        for line in (root_stone / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    root_spawn_events = [e for e in root_event_lines if e.get("kind") == "spawn"]
    assert root_spawn_events
    assert all("record_path" not in e for e in root_spawn_events)
    assert root_spawn_events[0]["child_index"] == 0

    # Recalled-subtree replay emits a spawn for the child under the recalled parent.
    runs = sorted([d for d in (tmp_path / ".cairns" / "runs").iterdir() if d.is_dir() and d.name.startswith("root-")])
    with open(runs[-1] / "trace.jsonl", "r") as f:
        events = [json.loads(line) for line in f if line.strip()]
    assert any(e["e"] == "end" and e.get("cached") for e in events)
    assert any(e["e"] == "spawn" and e.get("name") == "leaf" and e.get("origin") == "recalled" for e in events)


def test_version_mismatch_forces_fresh_stone(tmp_path: Path) -> None:
    """A stored record at version A is not recalled when the current version is B."""
    store_path = str(tmp_path / ".cairns")
    calls = {"n": 0}

    @step(memo=True, identity="pkg.compute", version="v1")
    async def _compute_v1() -> str:
        calls["n"] += 1
        return "v1-result"

    assert run(_compute_v1, store_path=store_path) == "v1-result"
    assert calls == {"n": 1}

    # Re-declare with a different version — should miss the cache and push a new record.
    @step(memo=True, identity="pkg.compute", version="v2")
    async def _compute_v2() -> str:
        calls["n"] += 1
        return "v2-result"

    assert run(_compute_v2, store_path=store_path) == "v2-result"
    assert calls == {"n": 2}

    # Both versions sit in the same cairn (cairn_id excludes version).
    cairns = tmp_path / ".cairns" / "cairns"
    cairn_dirs = [p for p in cairns.iterdir() if p.is_dir()]
    assert len(cairn_dirs) == 1
    record_ids = [s.name for s in cairn_dirs[0].iterdir() if s.is_dir()]
    assert len(record_ids) == 2


def test_carry_overrides_resolver(tmp_path: Path) -> None:
    """run(carry={...}) short-circuits to the pinned record without executing."""
    store_path = str(tmp_path / ".cairns")
    calls = {"n": 0}

    @step(memo=True)
    async def pick(tag: str) -> str:
        calls["n"] += 1
        return f"real:{tag}"

    # Seed the store with a real "A" record.
    assert run(pick, store_path=store_path, args=("A",)) == "real:A"
    assert calls == {"n": 1}

    info = pick.info  # type: ignore[attr-defined]
    cairn_id_a = info.cairn_id({"tag": "A"})
    cairn_id_b = info.cairn_id({"tag": "B"})

    stones_a = list((tmp_path / ".cairns" / "cairns" / cairn_id_a).iterdir())
    assert len(stones_a) == 1
    record_path = str(stones_a[0])

    # Now run with tag="B" but carry the "A" record at cairn_id(B). The body
    # should not execute — the carried record's result is returned verbatim.
    result = run(pick, store_path=store_path, args=("B",), carry={cairn_id_b: record_path})
    assert result == "real:A"
    assert calls == {"n": 1}  # still no new body execution

    # The carry map was persisted into the run dir.
    run_dirs = sorted(d for d in (tmp_path / ".cairns" / "runs").iterdir() if d.is_dir() and d.name.startswith("pick-"))
    carry_file = run_dirs[-1] / "carry.json"
    assert carry_file.exists()
    import json as _json
    persisted = _json.loads(carry_file.read_text())
    assert persisted == {cairn_id_b: record_path}

    # And the run's trace records origin=carried on the end event.
    with open(run_dirs[-1] / "trace.jsonl", "r") as f:
        events = [_json.loads(line) for line in f if line.strip()]
    assert any(
        e["e"] == "end"
        and e.get("origin") == "carried"
        and e.get("cairn_id") == cairn_id_a
        for e in events
    )


def test_error_stones_keep_trace_events(tmp_path: Path) -> None:
    """Error records preserve traces in events.jsonl for later inspection."""
    store_path = str(tmp_path / ".cairns")

    @step
    async def fail() -> str:
        trace("before failure")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        run(fail, store_path=store_path)

    records = [p for p in (tmp_path / ".cairns" / "cairns").glob("*/*") if p.is_dir()]
    fail_stone = next(s for s in records if json.loads((s / "metadata.json").read_text()).get("short_name") == "fail")
    meta = json.loads((fail_stone / "metadata.json").read_text())
    assert meta["error"] == "boom"

    event_lines = [
        json.loads(line)
        for line in (fail_stone / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert any(e.get("kind") == "trace" and e.get("message") == "before failure" for e in event_lines)


def test_cached_flamegraph_reconstructs_original_timing(tmp_path: Path) -> None:
    """A cache-hit span's trace reconstructs child spans + traces at original offsets."""
    import asyncio
    store_path = str(tmp_path / ".cairns")

    @step(memo=True)
    async def child(x: int) -> int:
        await asyncio.sleep(0.05)
        trace("child working")
        return x * 2

    @step(memo=True)
    async def parent() -> int:
        a = child(1)
        b = child(2)
        return await a + await b

    # First run: populate the cache with real timings.
    run(parent, store_path=store_path)
    first_runs = sorted(d for d in (tmp_path / ".cairns" / "runs").iterdir() if d.is_dir() and d.name.startswith("parent-"))
    first_trace = first_runs[-1] / "trace.jsonl"
    with open(first_trace, "r") as f:
        first_events = [json.loads(line) for line in f if line.strip()]
    parent_end_first = next(e for e in first_events if e["e"] == "end" and e.get("name") is None and e.get("origin") == "created" and e.get("time") is not None)
    original_parent_duration = parent_end_first["time"]
    assert original_parent_duration >= 0.05

    # Second run: cache hit on parent. Replayed subtree should carry virtual
    # timing consistent with original durations.
    run(parent, store_path=store_path)
    second_runs = sorted(d for d in (tmp_path / ".cairns" / "runs").iterdir() if d.is_dir() and d.name.startswith("parent-"))
    second_trace = second_runs[-1] / "trace.jsonl"
    with open(second_trace, "r") as f:
        second_events = [json.loads(line) for line in f if line.strip()]

    # Parent cached end event carries the original duration.
    parent_end = next(e for e in second_events if e["e"] == "end" and e.get("cached") and e.get("origin") == "carried" or (e["e"] == "end" and e.get("cached") and e.get("origin") == "recalled"))
    assert parent_end.get("time", 0) > 0.04

    # Replayed child spawns/ends are present with origin=recalled and non-zero duration.
    child_spawns = [e for e in second_events if e["e"] == "spawn" and e.get("origin") == "recalled" and e.get("name") == "child"]
    child_ends = [e for e in second_events if e["e"] == "end" and e.get("origin") == "recalled" and e.get("cached")]
    assert len(child_spawns) == 2
    assert len(child_ends) >= 2

    # Each replayed child's end event reports its original time, not ~0.
    child_times = [e["time"] for e in child_ends if e.get("name") != "parent"]
    assert all(t > 0.04 for t in child_times), f"child durations collapsed: {child_times}"

    # Replayed trace events under the cached children are present.
    replayed_traces = [e for e in second_events if e["e"] == "trace" and e.get("replayed") and e.get("msg") == "child working"]
    assert len(replayed_traces) == 2

    # Virtual ts on the cached subtree reconstructs the original shape: the
    # parent's end ts minus its spawn ts equals the cached duration.
    parent_spawn = next(e for e in second_events if e["e"] == "spawn" and e.get("origin") is None and e.get("name") == "parent")
    reconstructed = parent_end["ts"] - parent_spawn["ts"]
    assert abs(reconstructed - original_parent_duration) < 0.02, (
        f"reconstructed={reconstructed} original={original_parent_duration}"
    )


@pytest.mark.xfail(
    strict=True,
    reason="cached child replay stamps a virtual future end_ts, but the parent resumes at real time",
)
def test_cached_child_replay_does_not_make_parent_time_go_backwards(tmp_path: Path) -> None:
    """Awaiting a cached child should not emit later parent events in the past."""
    import asyncio

    store_path = str(tmp_path / ".cairns")

    @step(memo=True)
    async def child() -> int:
        await asyncio.sleep(0.05)
        trace("child trace")
        return 1

    @step
    async def parent() -> int:
        value = await child()
        trace("after child")
        return value

    # Warm the child's record. The parent still executes live on the second run.
    run(parent, store_path=store_path)
    run(parent, store_path=store_path)

    runs = sorted(
        d for d in (tmp_path / ".cairns" / "runs").iterdir()
        if d.is_dir() and d.name.startswith("parent-")
    )
    with open(runs[-1] / "trace.jsonl", "r") as f:
        events = [json.loads(line) for line in f if line.strip()]

    child_end = next(
        e for e in events
        if e["e"] == "end" and e.get("cached") and e.get("origin") == "recalled"
    )
    parent_resume = next(e for e in events if e["e"] == "resume")
    after_child = next(
        e for e in events
        if e["e"] == "trace" and e.get("msg") == "after child"
    )

    assert child_end["ts"] <= parent_resume["ts"] <= after_child["ts"]


def test_subtree_integrity_skips_stones_with_missing_children(tmp_path: Path) -> None:
    """A parent record whose child record was GC'd is skipped on recall."""
    store_path = str(tmp_path / ".cairns")
    calls = {"parent": 0, "child": 0}

    @step(memo=True)
    async def child() -> str:
        calls["child"] += 1
        return "c"

    @step(memo=True)
    async def parent() -> str:
        calls["parent"] += 1
        return await child()

    assert run(parent, store_path=store_path) == "c"
    assert calls == {"parent": 1, "child": 1}

    # Nuke the child record, leaving the parent's children/000 pointer dangling.
    cairns = tmp_path / ".cairns" / "cairns"
    for cairn_dir in cairns.iterdir():
        for stone_dir in cairn_dir.iterdir():
            meta = json.loads((stone_dir / "metadata.json").read_text())
            if meta.get("short_name") == "child":
                import shutil
                shutil.rmtree(stone_dir)

    # Rerun: parent's record is no longer recallable → body executes again.
    assert run(parent, store_path=store_path) == "c"
    assert calls["parent"] == 2
    assert calls["child"] == 2
