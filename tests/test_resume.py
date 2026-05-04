"""Tests for resumability — the core value prop.

Run a pipeline, fail partway through, rerun, verify completed work is skipped.
"""

from __future__ import annotations

import json
from pathlib import Path

from cairns import step, run, trace


def test_resume_after_failure(tmp_path: Path) -> None:
    """Rerun after failure skips completed steps."""
    store_path = str(tmp_path / ".cairn")
    call_counts: dict[str, int] = {}
    should_fail = True

    @step(memo=True)
    async def step_a() -> str:
        call_counts["a"] = call_counts.get("a", 0) + 1
        return "result_a"

    @step(memo=True)
    async def step_b() -> str:
        call_counts["b"] = call_counts.get("b", 0) + 1
        return "result_b"

    @step(memo=True)
    async def step_c(a: str, b: str) -> str:
        call_counts["c"] = call_counts.get("c", 0) + 1
        if should_fail:
            raise ValueError("step_c fails on first run")
        return f"{a}+{b}"

    @step
    async def pipeline() -> str:
        a = await step_a()
        b = await step_b()
        return await step_c(a, b)

    # First run — fails at step_c
    try:
        run(pipeline, store_path=store_path)
        assert False, "should have raised"
    except ValueError:
        pass

    assert call_counts == {"a": 1, "b": 1, "c": 1}

    # Second run — step_a and step_b should be cached, step_c retries
    should_fail = False
    call_counts.clear()
    result = run(pipeline, store_path=store_path)

    assert result == "result_a+result_b"
    assert call_counts.get("a", 0) == 0  # cached
    assert call_counts.get("b", 0) == 0  # cached
    assert call_counts["c"] == 1  # retried (error not cached as success)


def test_resume_fanout_partial_failure(tmp_path: Path) -> None:
    """Fan-out where some tasks fail — errors handled inside child, only failures rerun."""
    store_path = str(tmp_path / ".cairn")
    call_counts: dict[int, int] = {}
    fail_indices: set[int] = {2, 4}

    @step(memo=True)
    async def process(i: int) -> str:
        """Handles its own errors to avoid TaskGroup propagation."""
        call_counts[i] = call_counts.get(i, 0) + 1
        if i in fail_indices:
            return f"FAILED_{i}"
        return f"result_{i}"

    @step
    async def pipeline() -> list[str]:
        handles = [process(i) for i in range(5)]
        return [await h for h in handles]

    # First run — items 2 and 4 "fail" (return failure markers)
    result1 = run(pipeline, store_path=store_path)
    assert result1 == ["result_0", "result_1", "FAILED_2", "result_3", "FAILED_4"]
    assert call_counts == {0: 1, 1: 1, 2: 1, 3: 1, 4: 1}

    # Second run — everything cached (including "failed" results)
    call_counts.clear()
    result2 = run(pipeline, store_path=store_path)
    assert result2 == result1
    assert all(call_counts.get(i, 0) == 0 for i in range(5))

    # To actually retry failures: change fail_indices so version changes
    # aren't needed — the cached "FAILED_2" is a valid result.
    # Real retry requires the step to raise, which we test separately.


def test_resume_preserves_trace(tmp_path: Path) -> None:
    """Resumed run still writes a complete trace."""
    store_path = str(tmp_path / ".cairn")
    should_fail = True

    @step(memo=True)
    async def step_a() -> str:
        trace("working on a")
        return "a"

    @step(memo=True)
    async def step_b(a: str) -> str:
        if should_fail:
            raise ValueError("fail")
        return f"{a}+b"

    @step
    async def pipeline() -> str:
        a = await step_a()
        return await step_b(a)

    try:
        run(pipeline, store_path=store_path)
    except ValueError:
        pass

    # Second run succeeds
    should_fail = False
    run(pipeline, store_path=store_path)

    # Should have two run directories
    runs_dir = tmp_path / ".cairn" / "runs"
    run_dirs = sorted(
        [d for d in runs_dir.iterdir() if d.is_dir() and d.name.startswith("pipeline-")]
    )
    assert len(run_dirs) == 2

    # Second run's trace should have events
    trace_file = run_dirs[1] / "trace.jsonl"
    with open(trace_file, "r") as f:
        events = [json.loads(line) for line in f if line.strip()]
    assert len(events) > 0
    # Should include cached hits
    end_events = [e for e in events if e["e"] == "end"]
    cached_events = [e for e in end_events if e.get("cached")]
    assert len(cached_events) >= 1  # step_a was cached
