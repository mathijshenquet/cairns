"""Failure and resume example.

Demonstrates: error handling, caching across runs, resumability.

Run once (will fail at item 3):
    cd examples && python failure_resume.py

Run again (resumes from cache, item 3 succeeds):
    python failure_resume.py

Third run (fully cached, instant):
    python failure_resume.py
"""

from __future__ import annotations

import asyncio
import os

from cairns import step, run, trace

# Track state across runs via a file
_state_file = ".cairn/failure_state"


def _get_attempt() -> int:
    if os.path.exists(_state_file):
        with open(_state_file) as f:
            return int(f.read().strip())
    return 1


def _set_attempt(n: int) -> None:
    os.makedirs(os.path.dirname(_state_file), exist_ok=True)
    with open(_state_file, "w") as f:
        f.write(str(n))


@step(memo=True)  # cache successful items — skip on rerun
async def process_item(item: str) -> str:
    """Process a single item. Item 'C' fails on first attempt."""
    trace("processing")
    await asyncio.sleep(0.05)

    attempt = _get_attempt()
    if item == "C" and attempt == 1:
        trace("failing!", level="error")
        raise RuntimeError(f"Processing '{item}' failed (attempt {attempt})")

    return f"Result for {item}: OK (attempt {attempt})"


@step
async def pipeline() -> list[str]:
    items = ["A", "B", "C", "D", "E"]
    trace(f"starting ({len(items)} items)")

    results: list[str] = []
    for item in items:
        trace("processing item")
        result = await process_item(item)
        results.append(result)
        trace("completed")

    return results


# Default entry point for `cairn run`
main = pipeline

if __name__ == "__main__":
    import time

    attempt = _get_attempt()
    print(f"Attempt #{attempt}")
    print(f"Store: .cairn/\n")

    t0 = time.monotonic()
    try:
        results = run(pipeline, store_path=".cairn")
        t1 = time.monotonic()
        print(f"\nPipeline completed in {t1 - t0:.2f}s:")
        for r in results:
            print(f"  {r}")
    except RuntimeError as e:
        t1 = time.monotonic()
        print(f"\nPipeline FAILED after {t1 - t0:.2f}s: {e}")
        print("\nItems A, B are cached. Run again to resume.")
        _set_attempt(attempt + 1)
        print(f"(Next run will be attempt #{attempt + 1})")
