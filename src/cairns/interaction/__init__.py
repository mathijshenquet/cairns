"""Cairn interaction: typed human-in-the-loop primitives.

The `InteractionSink` protocol lives in `cairns.core.runtime` (it's a
slot on the active `Run`). This module provides:

- typed async wrappers (`await_input`, `await_choice`, `await_confirm`)
  each backed by a memoized `@step`, so answers are content-addressed.
- built-in sinks: `QueueInteractionSink` (tests / scripted runs) and
  `StdinInteractionSink` (terminal fallback).

Usage:

    from cairns import run
    from cairns.interaction import await_input, StdinInteractionSink

    @step
    async def main():
        name = await await_input("What's your name?")
        ...

    run(main, interaction_sink=StdinInteractionSink())
"""

from __future__ import annotations

import asyncio
from typing import Any, Mapping, TypeVar, cast

from cairns.core import step
from cairns.core.runtime import InteractionSink, current_run, current_span

K = TypeVar("K")


def _require_sink() -> InteractionSink:
    sink = current_run().interaction_sink
    if sink is None:
        raise RuntimeError(
            "no interaction sink registered — pass `interaction_sink=...` to "
            "`run(...)` or construct `Run(interaction_sink=...)` directly."
        )
    return sink


def _caller_span() -> int | None:
    # Inside an `_input` / `_choice` / `_confirm` step, current_span is the
    # step itself; its parent is the user code that called the wrapper.
    # Widgets belong next to the conversation, not the caching wrapper.
    s = current_span.get()
    return s.parent_seq if s is not None else None


# ── Memoized internals (one @step per widget) ──


@step
async def _input(
    prompt: str, default: str | None, placeholder: str | None
) -> str:
    return await _require_sink().request_input(
        prompt,
        anchor_span=_caller_span(),
        default=default,
        placeholder=placeholder,
    )


@step
async def _choice(
    prompt: str, options: dict[Any, str], default: Any
) -> Any:
    return await _require_sink().request_choice(
        prompt,
        options,
        anchor_span=_caller_span(),
        default=default,
    )


@step
async def _confirm(prompt: str, default: bool | None) -> bool:
    return await _require_sink().request_confirm(
        prompt,
        anchor_span=_caller_span(),
        default=default,
    )


# ── Public API ──


async def await_input(
    prompt: str,
    *,
    default: str | None = None,
    placeholder: str | None = None,
) -> str:
    """Ask a human for a free-form string.

    Memoized by (prompt, default, placeholder).
    """
    return await _input(prompt, default, placeholder)


async def await_choice(
    prompt: str,
    options: Mapping[K, str],
    *,
    default: K | None = None,
) -> K:
    """Ask a human to pick one of `options`; returns the chosen key.

    Memoized by (prompt, options, default). Changing any key, its
    rendered value, or the default invalidates the cache.
    """
    raw = await _choice(prompt, dict(options), default)
    if raw not in options:
        raise ValueError(
            f"sink returned {raw!r} not in options {list(options)!r}"
        )
    return cast(K, raw)


async def await_confirm(
    prompt: str, *, default: bool | None = None
) -> bool:
    """Ask a yes/no question. Memoized by (prompt, default)."""
    return await _confirm(prompt, default)


# ── Built-in sinks ──


class QueueInteractionSink:
    """Pre-seeded queue, shared across all request_* methods.

    Responses are consumed in FIFO order regardless of request kind — for
    tests and scripted runs where the caller knows the request order.
    """

    def __init__(self, responses: list[Any] | None = None) -> None:
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        for r in responses or []:
            self._queue.put_nowait(r)

    def push(self, response: Any) -> None:
        self._queue.put_nowait(response)

    async def request_input(
        self,
        prompt: str,
        *,
        anchor_span: int | None,
        default: str | None = None,
        placeholder: str | None = None,
    ) -> str:
        return cast(str, await self._queue.get())

    async def request_choice(
        self,
        prompt: str,
        options: Mapping[K, str],
        *,
        anchor_span: int | None,
        default: K | None = None,
    ) -> K:
        return cast(K, await self._queue.get())

    async def request_confirm(
        self,
        prompt: str,
        *,
        anchor_span: int | None,
        default: bool | None = None,
    ) -> bool:
        return cast(bool, await self._queue.get())


class StdinInteractionSink:
    """Fallback sink that reads from stdin on a worker thread.

    Safe for sequential requests only. Shows defaults in brackets; an
    empty line accepts the default.
    """

    async def request_input(
        self,
        prompt: str,
        *,
        anchor_span: int | None,
        default: str | None = None,
        placeholder: str | None = None,
    ) -> str:
        hint = f" [{default}]" if default is not None else ""
        line = await asyncio.to_thread(input, f"{prompt.rstrip()}{hint}\n> ")
        if not line and default is not None:
            return default
        return line

    async def request_choice(
        self,
        prompt: str,
        options: Mapping[K, str],
        *,
        anchor_span: int | None,
        default: K | None = None,
    ) -> K:
        keys = list(options.keys())
        print(prompt)
        for k, v in options.items():
            marker = "*" if default is not None and k == default else " "
            print(f"  {marker} [{k}] {v}")
        tail = f" (default {default!r})" if default is not None else ""
        prompt_line = f"Pick one of {keys!r}{tail}\n> "
        while True:
            raw = (await asyncio.to_thread(input, prompt_line)).strip()
            if not raw and default is not None:
                return default
            for k in keys:
                if str(k).lower() == raw.lower():
                    return k
            print(f"invalid choice: {raw!r}")

    async def request_confirm(
        self,
        prompt: str,
        *,
        anchor_span: int | None,
        default: bool | None = None,
    ) -> bool:
        hint = "[y/n]" if default is None else ("[Y/n]" if default else "[y/N]")
        while True:
            line = (await asyncio.to_thread(input, f"{prompt.rstrip()} {hint}\n> ")).strip().lower()
            if not line and default is not None:
                return default
            if line in ("y", "yes"):
                return True
            if line in ("n", "no"):
                return False
            print(f"invalid: expected y/n, got {line!r}")


__all__ = [
    "InteractionSink",
    "await_input",
    "await_choice",
    "await_confirm",
    "QueueInteractionSink",
    "StdinInteractionSink",
]
