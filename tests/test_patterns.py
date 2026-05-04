"""Tests for higher-order patterns and composable workflows."""

from __future__ import annotations

import pytest

from cairns import step, Handle, trace, cached_output, cached_tracing
from cairns.testing import Harness


# ── Validated loop (research + validate + refine) ──


@pytest.mark.asyncio
async def test_validated_loop() -> None:
    """The composable validated() pattern works end-to-end."""

    @step
    async def generate(subject: str) -> str:
        return f"draft about {subject}"

    @step
    async def validate(report: str) -> dict[str, object]:
        if "improved" in report:
            return {"success": True, "feedback": None}
        return {"success": False, "feedback": "needs improvement"}

    @step
    async def refine(draft: str, feedback: str) -> str:
        return f"improved {draft}"

    @step
    async def validated(subject: str) -> str:
        """Validated loop — the higher-order pattern inlined."""
        draft: str = await generate(subject)
        for i in range(3):
            result: dict[str, object] = await validate(draft)
            if result["success"]:
                return draft
            trace("retrying", edge=True, progress=(i + 1, 3))
            draft = await refine(draft, str(result["feedback"]))
        return draft

    async with Harness() as rt:
        result = await validated("cats")
        assert "improved" in result

        # Should have gone through generate → validate (fail) → refine → validate (pass)
        edges = rt.trace.edge_annotations("validated")
        assert len(edges) == 1
        assert edges[0].progress == (1, 3)


# ── Human input pattern ──


@pytest.mark.asyncio
async def test_human_input_prefill() -> None:
    """memo=False with cached_output() enables the human prefill pattern."""
    ask_count = 0

    @step  # memo=False is now the default — cached_output() still available
    async def human_ask(question: str) -> str:
        nonlocal ask_count
        ask_count += 1
        prev = cached_output()
        if prev is not None:
            # Simulate accepting the prefilled answer
            return prev
        # Simulate first-time human input
        return "human says hello"

    async with Harness():
        # First call — no cache, "human" provides answer
        r1 = await human_ask("what do you think?")
        assert r1 == "human says hello"
        assert ask_count == 1

        # Second call — cached_output() returns previous answer, accepted as-is
        r2 = await human_ask("what do you think?")
        assert r2 == "human says hello"
        assert ask_count == 2  # body was called (memo=False) but used cache


@pytest.mark.asyncio
async def test_human_in_the_loop_iteration() -> None:
    """Full human-in-the-loop iteration: generate, review, revise."""
    iteration = 0

    @step
    async def generate(spec: str) -> str:
        return f"report per {spec}"

    @step  # no memo — always ask the human (cached_output() for prefill)
    async def human_review(report: str) -> dict[str, object]:
        nonlocal iteration
        iteration += 1
        prev = cached_output()
        if prev is not None:
            return prev  # type: ignore[return-value]
        if iteration == 1:
            return {"ok": False, "revised_spec": "more detail please"}
        return {"ok": True, "revised_spec": None}

    @step
    async def iterate(initial_spec: str) -> str:
        spec = initial_spec
        for _ in range(5):
            report = await generate(spec)
            review: dict[str, object] = await human_review(report)
            if review["ok"]:
                return report
            spec = str(review["revised_spec"])
        return await generate(spec)

    async with Harness():
        result = await iterate("basic spec")
        assert "more detail" in result  # second iteration used revised spec


# ── Replayable with trace verification ──


@pytest.mark.asyncio
async def test_replayable_preserves_traces() -> None:
    """Replayable wrapper replays trace events from cache."""
    from cairns import replayable

    call_count = 0

    async def work_impl(x: int) -> int:
        nonlocal call_count
        call_count += 1
        trace("step 1")
        trace("step 2", detail="extra")
        return x * 2

    work = replayable(work_impl)

    async with Harness() as rt:
        # First call — real execution
        assert await work(5) == 10
        assert call_count == 1

        first_traces = [
            e for e in rt.trace.events(kind="trace")
            if e.message and "step" in e.message
        ]
        assert len(first_traces) == 2

        # Second call — replay from cache
        assert await work(5) == 10
        assert call_count == 1  # not called again

        all_traces = [
            e for e in rt.trace.events(kind="trace")
            if e.message and "step" in e.message
        ]
        # Should have original 2 + replayed 2 (same messages, replayed from cache)
        assert len(all_traces) == 4


# ── Version auto-invalidation ──


@pytest.mark.asyncio
async def test_code_change_invalidates_cache() -> None:
    """Changing function source invalidates the cache via version hash."""
    call_count = 0

    @step(memo=True)
    async def compute_v1(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    async with Harness():
        assert await compute_v1(5) == 10
        assert call_count == 1

        # Simulate a "code change" by creating a new function with different body
        @step(memo=True)
        async def compute_v1(x: int) -> int:  # type: ignore[no-redef]  # noqa: F811
            nonlocal call_count
            call_count += 1
            return x * 3  # changed!

        # Same name, different body → different version → cache miss
        assert await compute_v1(5) == 15
        assert call_count == 2


# ── Scraper / non-AI pattern ──


@pytest.mark.asyncio
async def test_scraper_pattern() -> None:
    """Non-AI use case: scraper with caching."""

    @step
    async def fetch(url: str) -> str:
        return f"<html>content of {url}</html>"

    @step
    async def parse(html: str) -> dict[str, str]:
        return {"title": html.split("content of ")[1].split("<")[0]}

    @step
    async def scrape(urls: list[str]) -> list[dict[str, str]]:
        pages = [fetch(url) for url in urls]
        parsed = [parse(await p) for p in pages]
        return [await p for p in parsed]

    async with Harness() as rt:
        result = await scrape(["a.com", "b.com", "c.com"])
        assert len(result) == 3
        assert result[0] == {"title": "a.com"}
        assert result[2] == {"title": "c.com"}


# ── Composability: wrappers compose with each other ──


@pytest.mark.asyncio
async def test_rate_limited_with_fanout() -> None:
    """rate_limited works correctly under fan-out."""
    from cairns import rate_limited

    max_concurrent = 0
    current = 0

    @rate_limited(2)
    async def limited_work(x: int) -> int:
        nonlocal max_concurrent, current
        current += 1
        max_concurrent = max(max_concurrent, current)
        import asyncio
        await asyncio.sleep(0.02)
        current -= 1
        return x * 2

    async with Harness():
        handles = [limited_work(i) for i in range(6)]
        results = [await h for h in handles]
        assert results == [0, 2, 4, 6, 8, 10]
        assert max_concurrent <= 2


# ── Edge case: deeply nested steps ──


@pytest.mark.asyncio
async def test_deeply_nested() -> None:
    """Steps calling steps calling steps — trace tree is correct."""

    @step
    async def level3(x: int) -> int:
        return x + 1

    @step
    async def level2(x: int) -> int:
        return await level3(x)

    @step
    async def level1(x: int) -> int:
        return await level2(x)

    async with Harness() as rt:
        assert await level1(10) == 11

        # Check trace tree: level1 → level2 → level3
        l1 = rt.trace.span("level1")
        l2 = rt.trace.span("level2")
        l3 = rt.trace.span("level3")
        assert l2.parent_seq == l1.seq
        assert l3.parent_seq == l2.seq
