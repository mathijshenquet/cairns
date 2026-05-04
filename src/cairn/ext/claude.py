"""Async wrapper around `claude -p --output-format stream-json`.

Public API:
  claude_stream(prompt, **kwargs) → AsyncIterator[dict]   raw JSONL events
  claude(prompt, **kwargs)        → str                   final text + live traces
  claude_json(prompt, Model, ...) → Model                 structured output via --json-schema
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, TypeVar, cast

from cairn import trace

if TYPE_CHECKING:
    from pydantic import BaseModel

M = TypeVar("M", bound="BaseModel")

SYSTEM_PROMPT = (
    "You are a concise research assistant. Use WebSearch and WebFetch when "
    "current information is needed. Answer directly, no preamble, no "
    "meta-commentary. Respect terseness instructions in the user prompt."
)
MODEL = "haiku"
SEARCH_TOOLS = "WebSearch,WebFetch"
DENY_TOOLS = "Write,Edit,NotebookEdit,Bash,MultiEdit"


async def claude_stream(
    prompt: str,
    *,
    model: str = MODEL,
    system_prompt: str = SYSTEM_PROMPT,
    tools: str = "",
    deny_tools: str = DENY_TOOLS,
    extra_args: list[str] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Yield parsed JSON events from `claude -p --output-format stream-json`."""
    args = [
        "claude", "-p",
        "--model", model,
        "--system-prompt", system_prompt,
        "--permission-mode", "bypassPermissions",
        "--tools", tools,
        "--disallowed-tools", deny_tools,
        "--output-format", "stream-json",
        "--include-partial-messages",
        "--verbose",
        *(extra_args or []),
        "--",
        prompt,
    ]
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stdout is not None
    async for raw_line in proc.stdout:
        line = raw_line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            pass
    await proc.wait()
    if proc.returncode != 0:
        assert proc.stderr is not None
        stderr = await proc.stderr.read()
        msg = stderr.decode().strip()[:500]
        raise RuntimeError(f"claude exited {proc.returncode}: {msg}")


def _preview(s: str, n: int = 80) -> str:
    s = s.strip().replace("\n", " ")
    return s if len(s) <= n else s[: n - 1] + "…"


def _tool_result_text(content: Any) -> str:
    """Best-effort stringification of a tool_result.content field."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        out: list[str] = []
        for block in cast(list[Any], content):
            if isinstance(block, dict):
                b = cast(dict[str, Any], block)
                if b.get("type") == "text":
                    out.append(str(b.get("text", "")))
                else:
                    out.append(json.dumps(b))
        return "\n".join(out)
    return json.dumps(content)


async def _claude_run(
    prompt: str,
    *,
    tools: str,
    model: str,
    system_prompt: str,
    extra_args: list[str] | None,
) -> dict[str, Any]:
    """Stream claude, emit live traces for tool calls + results + cost, return the final result event."""
    # block index → {name, id, partial_json}
    pending: dict[int, dict[str, str]] = {}

    async for event in claude_stream(
        prompt,
        model=model,
        system_prompt=system_prompt,
        tools=tools,
        extra_args=extra_args,
    ):
        etype = event.get("type")

        # Assistant-side streaming events: tool_use blocks stream in here.
        if etype == "stream_event":
            e = event["event"]
            et = e.get("type")

            if et == "content_block_start":
                cb = e.get("content_block", {})
                if cb.get("type") == "tool_use":
                    pending[e["index"]] = {
                        "name": cb["name"],
                        "id": cb["id"],
                        "partial_json": "",
                    }

            elif et == "content_block_delta":
                delta = e.get("delta", {})
                idx = e["index"]
                if delta.get("type") == "input_json_delta" and idx in pending:
                    pending[idx]["partial_json"] += delta.get("partial_json", "")

            elif et == "content_block_stop":
                idx = e.get("index")
                if idx in pending:
                    tool = pending.pop(idx)
                    try:
                        inp: dict[str, Any] = json.loads(tool["partial_json"]) if tool["partial_json"] else {}
                    except json.JSONDecodeError:
                        inp = {"_raw": tool["partial_json"]}
                    trace(
                        tool["name"],
                        detail=json.dumps(inp, indent=2),
                        state="running",
                    )

        # Completed user message: carries tool_result blocks for any tool we just ran.
        elif etype == "user":
            msg = cast(dict[str, Any], event.get("message") or {})
            for block in cast(list[Any], msg.get("content") or []):
                if not isinstance(block, dict):
                    continue
                b = cast(dict[str, Any], block)
                if b.get("type") != "tool_result":
                    continue
                text = _tool_result_text(b.get("content"))
                trace(
                    f"→ {_preview(text)}",
                    detail=text,
                    level="error" if b.get("is_error") else "info",
                )

        # Final result event: emit cost + return the full event for post-processing.
        elif etype == "result":
            usage = cast(dict[str, Any], event.get("usage") or {})
            cost: dict[str, Any] = {}
            if "input_tokens" in usage:
                cost["tokens_in"] = usage["input_tokens"]
            if "output_tokens" in usage:
                cost["tokens_out"] = usage["output_tokens"]
            if "cache_creation_input_tokens" in usage:
                cost["tokens_cache_write"] = usage["cache_creation_input_tokens"]
            if "cache_read_input_tokens" in usage:
                cost["tokens_cache_read"] = usage["cache_read_input_tokens"]
            if "total_cost_usd" in event:
                cost["cost_usd"] = event["total_cost_usd"]
            if cost:
                trace(model, cost=cost)
            return event

    raise RuntimeError("claude stream ended without a result event")


async def claude(
    prompt: str,
    *,
    tools: str = "",
    model: str = MODEL,
    system_prompt: str = SYSTEM_PROMPT,
    extra_args: list[str] | None = None,
) -> str:
    """Run claude, emit live traces for tool calls + results + cost, return final text."""
    event = await _claude_run(
        prompt,
        tools=tools,
        model=model,
        system_prompt=system_prompt,
        extra_args=extra_args,
    )
    if event.get("is_error"):
        raise RuntimeError(f"claude error: {event.get('result', '(no output)')}")
    return event.get("result", "")


async def claude_json(
    prompt: str,
    schema: type[M],
    *,
    tools: str = "",
    model: str = MODEL,
    system_prompt: str = SYSTEM_PROMPT,
) -> M:
    """Run claude with `--json-schema` enforcing `schema`, return a validated instance.

    The CLI puts the enforced output on `structured_output` in the result event;
    the `result` text field is still free-form prose. We pluck the structured
    field and hand it to pydantic.
    """
    schema_json = json.dumps(schema.model_json_schema())
    event = await _claude_run(
        prompt,
        tools=tools,
        model=model,
        system_prompt=system_prompt,
        extra_args=["--json-schema", schema_json],
    )
    if event.get("is_error"):
        raise RuntimeError(f"claude error: {event.get('result', '(no output)')}")
    so = event.get("structured_output")
    if so is None:
        raise RuntimeError(
            "claude_json: no `structured_output` in result event — "
            "claude CLI may not support `--json-schema` in this combination."
        )
    return schema.model_validate(so)
