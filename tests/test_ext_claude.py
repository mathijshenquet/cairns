"""The claude extension against a stand-in `claude` executable."""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from cairns.ext.claude import claude_stream

# Answers with the length of the prompt it read from stdin, in the shape of
# `claude -p --output-format stream-json`'s final event.
FAKE_CLAUDE = """#!/usr/bin/env python3
import json, sys
prompt = sys.stdin.read()
print(json.dumps({"type": "result", "result": str(len(prompt))}))
"""


@pytest.fixture
def fake_claude(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    exe = tmp_path / "claude"
    exe.write_text(FAKE_CLAUDE)
    exe.chmod(exe.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")


async def test_long_prompt_reaches_claude_through_stdin(fake_claude: None) -> None:
    # Above Linux's 128 KiB limit for a single argument.
    prompt = "x" * 300_000
    events = [e async for e in claude_stream(prompt)]
    assert events[-1]["result"] == str(len(prompt))
