<p align="center">
  <img src="https://raw.githubusercontent.com/mathijshenquet/cairns/main/docs/logo.jpg" alt="Cairn" width="600">
</p>

# Cairns

**A microframework for compute graphs with caching, tracing, and replay.**

Think *PyTorch for agent pipelines* — though nothing about it is agent-specific.
You write ordinary async Python; a `@step` decorator turns each function into a
tracked, and optionally cached node in a graph that emerges from execution instead of being
declared up front.

> **Alpha.** Public API names, the on-disk cache format, and the higher-order
> wrappers may still change between minor versions. Pin a version if you
> depend on it.

## Why

Declarative graph frameworks (LangGraph, CrewAI, Airflow-style DAGs) force you
into their node/edge DSL. Cairn goes the other way: the graph **is** your code,
the framework just instruments it. From that you get:

- **Caching** keyed on `(function identity, body version, resolved args)` — change
  one function, only its downstream re-executes.
- **Tracing** — live, structured event log; a built-in TUI renders it.
- **Resume** — a failed pipeline reruns from the last successful step.
- **Replay** — cached runs can replay with original timing, indistinguishable
  from a live execution.
- **Human-in-the-loop** as a regular async step (`await_input(...)`).

Works for LLM pipelines, scrapers, ETL, long-running research — anything that
fits into mostly-pure async functions.

## Install

```sh
pip install cairns[tui]        # TUI is worth having
pip install cairns[full]       # TUI + pydantic hashing
```

Or with `uv`:

```sh
uv add cairns --extra tui
```

Requires Python 3.12+.

## Hello world

```python
import asyncio
from cairns import step, run, trace

@step(memo=True)
async def fetch(url: str) -> str:
    trace("fetching", state="running")
    await asyncio.sleep(0.2)       # pretend HTTP
    return f"<html>{url}</html>"

@step
async def extract(html: str) -> int:
    return len(html)

@step
async def pipeline(urls: list[str]) -> list[int]:
    pages = [fetch(u) for u in urls]              # returns Handles; runs concurrently
    return [await extract(p) for p in pages]      # pages are awaited inside extract

run(pipeline(["https://a", "https://b", "https://c"]), store_path=".cairns")
```

Run it twice. The second run is instant — every `@step(memo=True)` result is
looked up by cache key. Edit the body of `extract`, rerun: only `extract`
re-executes, fetches are cache hits.

## CLI

```sh
cairns examples/research_fake_llm.py        # run the pipeline, opens TUI if installed
cairns examples/research_fake_llm.py slow   # run the `slow` entry point
cairns examples/research_fake_llm.py -f     # clear this entry's cache, then run
cairns                                      # interactive run browser over past runs
cairns list                                 # flat list of runs
cairns show [RUN_ID]                        # print a trace (latest if omitted)
cairns gc [--before YYYY-MM-DD]             # garbage-collect old runs
```

Default store is `./.cairns/`. Override with `--store PATH` (or `-s`). Entry
points default to a function named `main`; pass a second positional arg to
pick another, e.g. `cairns script.py my_pipeline`.

## Examples

Each example runs standalone with `python examples/<name>.py`, or through the
CLI with `cairns examples/<name>.py` for the TUI.

| Example | What it shows |
|---------|---------------|
| [`scraper.py`](https://github.com/mathijshenquet/cairns/blob/main/examples/scraper.py) | Fan-out + chain, non-AI, fully mocked. Good first look. |
| [`failure_resume.py`](https://github.com/mathijshenquet/cairns/blob/main/examples/failure_resume.py) | A step fails on item 3; rerun resumes from cache. |
| [`research_fake_llm.py`](https://github.com/mathijshenquet/cairns/blob/main/examples/research_fake_llm.py) | Fan-out across N subjects, retry loop, rate limiting, simulated 20% API failure rate. No API key needed. |
| [`hitl.py`](https://github.com/mathijshenquet/cairns/blob/main/examples/hitl.py) | `await_input` inside a step — TUI input widget, stdin fallback. |
| [`research_haiku.py`](https://github.com/mathijshenquet/cairns/blob/main/examples/research_haiku.py) + [`claude.py`](https://github.com/mathijshenquet/cairns/blob/main/examples/claude.py) | Live research over real AI companies via the `claude` CLI (Haiku). Cached by ISO week — re-runs within the week are free. |

## What you'll see

With `cairns[tui]` installed, the CLI opens a live span tree: each `@step`
invocation is a row, child steps indent, `trace(...)` calls attach as
annotations, and `cost={...}` kwargs get summed up the tree. Failures colour
red, running steps pulse, completed steps show wall time + own time (excluding
waits on children).

Without the TUI, the same events stream to `.cairns/runs/{entry}-{ts}/trace.jsonl`
and you can read them with `cairns show`.

## Docs

- [`docs/motivation.md`](https://github.com/mathijshenquet/cairns/blob/main/docs/motivation.md) — the problem and the analogy to PyTorch.
- [`docs/design.md`](https://github.com/mathijshenquet/cairns/blob/main/docs/design.md) — all primitives, event log, stores, plugin points.
- [`docs/patterns.md`](https://github.com/mathijshenquet/cairns/blob/main/docs/patterns.md) — comparison against Prefect, LangGraph, Temporal, Flyte, CrewAI across seven patterns.

## Status

Alpha, but the core works:

- `@step`, `Handle`, `trace`, `cached_output/tracing`, `replayable`, `rate_limited`, `await_input` all shipped.
- File-backed content-addressed store, JSONL trace sink, symlinked run layout, GC.
- Live TUI for span-tree viewing.
- 110 tests, covering core + hashing + disk + resume + GC + metrics + patterns.

Possible future work: a web UI, distributed execution, distributed cache store.

Feedback and breakage reports welcome via issues.
