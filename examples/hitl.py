"""Minimal human-in-the-loop example.

Demonstrates the three typed interaction primitives. Run in the TUI:

    cairn examples/hitl.py

Input/Choice/Confirm widgets mount in the detail pane when each step asks
a question. Submit the value to continue.

Run headless (stdin fallback):

    python examples/hitl.py
"""

from __future__ import annotations

from cairns import run, step, trace
from cairns.core.patterns import cached_output
from cairns.interaction import await_choice, await_confirm, await_input


@step
async def greet() -> str:
    old_name = cached_output(str)
    name = await await_input("What's your name?", default=old_name, placeholder="e.g. Ada")
    trace(f"Hello, {name}!")
    return name


@step
async def pick_mood() -> str:
    default_mood = cached_output(str)
    if default_mood is None:
        default_mood = "curious"

    return await await_choice(
        "Pick a mood",
        {
            "happy":     "sunshine, rainbows, the works",
            "grumpy":    "leave me alone, I'm reading",
            "curious":   "what does this button do?",
        },
        default=default_mood,
    )


@step
async def pipeline() -> str:
    name = await greet()
    mood = await pick_mood()

    go = await await_confirm(f"{name} is feeling {mood}. Sound right?", default=True)
    if not go:
        trace("starting over", level="warn")
        return await pipeline()

    return f"{name} ({mood})"


main = pipeline


if __name__ == "__main__":
    from cairns.interaction import StdinInteractionSink

    print(run(pipeline, store_path=".cairn", interaction_sink=StdinInteractionSink()))
