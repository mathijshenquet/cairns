"""Research pipeline example.

Demonstrates: fan-out, retry loop, caching, rate limiting, API failures, trace annotations.
Uses a fake Claude API with realistic failure modes.
"""

from __future__ import annotations

import asyncio
import hashlib
import random

from cairns import step, run, trace
from cairns import rate_limited


# ── Fake Claude API ──

_delay: tuple[float, float] = (0.05, 0.15)
_fail_rate: float = 0.0


@rate_limited(n=5, memo=True)  # concurrency-limited; retries handled by `llm` below
async def fake_api_call(prompt: str) -> str:
    """Simulates an API call. Fails `_fail_rate` of the time, rate limited."""
    trace(f"calling LLM API ({len(prompt)} chars)", state="running")
    await asyncio.sleep(random.uniform(*_delay))

    # Random failures
    if random.random() < _fail_rate:
        trace("API error", level="error")
        raise ConnectionError("Claude API: 529 Overloaded")
    
    trace("API call successful", cost={"tokens": 1000})

    # Deterministic-ish response based on prompt hash
    h = hashlib.md5(prompt.encode()).hexdigest()[:8]
    return _generate_response(prompt, h)

def _generate_response(prompt: str, h: str) -> str:
    p = prompt.lower()
    if "research" in p:
        return (
            f"Report [{h}]: The subject shows interesting characteristics including "
            f"unique habitat preferences in temperate zones, omnivorous dietary patterns "
            f"with seasonal variation, and complex social hierarchies. Population estimates "
            f"suggest {random.randint(1000, 50000)} individuals in the wild."
        )
    if "validate" in p:
        # Fail ~60% on first attempt — forces retry loop to exercise
        if int(h, 16) % 5 < 3:
            return '{"success": false, "feedback": "Report lacks specific data points. Add quantitative measurements and cite sources."}'
        return '{"success": true, "feedback": null}'
    if "refine" in p:
        return (
            f"Refined report [{h}]: Detailed analysis reveals measurable characteristics. "
            f"Population density: ~{random.randint(50, 5000)}/km². "
            f"Average lifespan: {random.randint(5, 40)} years. "
            f"Diet: {random.randint(30, 70)}% vegetation, remainder protein. "
            f"Conservation status: {random.choice(['Vulnerable', 'Endangered', 'Least Concern', 'Near Threatened'])}."
        )
    return f"Response [{h}]: {prompt[:80]}..."


# ── LLM wrapper with retry ──


@step(memo=True)  # cache successful LLM calls — the expensive leaf
async def llm(prompt: str) -> str:
    """LLM call with automatic retry on API failures."""
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            if attempt > 0:
                trace("retrying API call", progress=(attempt + 1, 2))
                await asyncio.sleep(0.5 * attempt)  # backoff
            return await fake_api_call(prompt)
        except ConnectionError as e:
            last_error = e
            trace("API failed, will retry", detail=str(e), progress=(attempt + 1, 2), level="warn")
    raise last_error or ConnectionError("max retries exceeded")


# ── Pipeline steps ──


@step
async def research(subject: str, spec: str) -> str:
    trace("researching")
    return await llm(f"Research {subject} according to: {spec}")


@step
async def validate(spec: str, report: str) -> dict[str, object]:
    import json
    raw = await llm(
        f"Validate this report against spec.\nSpec: {spec}\n"
        f"Report: {report}\nOutput JSON: {{success: bool, feedback: str}}"
    )
    try:
        return json.loads(raw)  # type: ignore[no-any-return]
    except json.JSONDecodeError:
        return {"success": True, "feedback": None}


@step
async def refine(subject: str, draft: str, feedback: str) -> str:
    return await llm(f"Refine report on {subject}.\nDraft: {draft}\nFeedback: {feedback}")


@step(memo=True)
async def research_validated(subject: str, spec: str) -> str:
    """Research with validation loop — retries until validated or max attempts."""
    draft = await research(subject, spec)
    for i in range(3):
        trace("validating", progress=(i + 1, 3))
        result = await validate(spec, draft)
        if result.get("success"):
            trace("validated", progress=(i + 1, 3))
            return draft
        feedback = str(result.get("feedback", "needs improvement"))
        trace("retrying", edge=True, progress=(i + 1, 3), detail=feedback)
        draft = await refine(subject, draft, feedback)
    trace("max retries reached", level="warn")
    return draft


# ── Entry points ──

ANIMALS_SMALL = ["Red Fox", "Giant Octopus", "Monarch Butterfly", "Snow Leopard"]

ANIMALS_LARGE = [
    "Red Fox", "Giant Octopus", "Monarch Butterfly", "Snow Leopard",
    "Blue Whale", "Honey Badger", "Komodo Dragon", "Arctic Tern",
    "Mantis Shrimp", "Pangolin", "Axolotl", "Peregrine Falcon",
    "Leafy Sea Dragon", "Capybara", "Harpy Eagle", "Narwhal",
    "Tasmanian Devil", "Okapi", "Cassowary", "Dumbo Octopus",
]

SPEC = "Comprehensive report covering habitat, diet, behavior, and conservation status."


@step
async def pipeline(subjects: list[str] | None = None) -> dict[str, str]:
    """Research pipeline: fan-out across subjects."""
    animals = subjects or ANIMALS_SMALL
    trace(f"starting pipeline ({len(animals)} subjects)")

    handles = {s: research_validated(s, SPEC) for s in animals}

    results: dict[str, str] = {}
    for subject, handle in handles.items():
        results[subject] = await handle
    
    done = ", ".join(subjects or results.keys())
    trace(f"completed ({done})")

    trace("pipeline complete")
    return results


@step
async def pipeline_slow() -> dict[str, str]:
    """Full pipeline: 20 animals, 1-4s delays, 20% failure rate, rate limited to 5 concurrent."""
    global _delay, _fail_rate  # noqa: PLW0603
    _delay = (1.0, 4.0)
    _fail_rate = 0.2
    return await pipeline(ANIMALS_LARGE)

slow = pipeline_slow

main = pipeline


if __name__ == "__main__":
    print("Running research pipeline...")
    print("Store: .cairns/\n")
    results = run(pipeline, store_path=".cairns")
    print(f"\nCompleted {len(results)} reports:\n")
    for subject, report in results.items():
        print(f"  {subject}: {report[:80]}...")
    print(f"\nExplore: cairn list && cairn show")
