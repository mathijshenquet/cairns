# Cairn: Design

This document describes Cairn as it is, not as it was once sketched. For the
motivation, read [`motivation.md`](motivation.md); for a comparison with other
frameworks, read [`patterns.md`](patterns.md).

## Core primitives

| Primitive | Role |
|-----------|------|
| `@step` | Decorator that turns an async function into a tracked node |
| `Handle[T]` | Awaitable reference to a running step's result |
| `trace(...)` | Emit an annotation onto the current span's timeline |
| `cached_output()` | The step's previous cached result, if any |
| `cached_tracing()` | The step's previous trace events, with timing |
| `replayable(fn)` | Wrap a step so cache hits replay with original timing |
| `rate_limited(n)` | Wrap a step with a concurrency-limiting semaphore |
| `await_input(prompt)` | Ask a human (or stand-in sink) a question |

Everything else — retries, validation loops, fan-out/fan-in — is ordinary
Python built on top of these.

Entry points: `cairns.run(fn, store_path=".cairns")` programmatically, or
`cairns script.py` from the shell.

---

## 1. `@step`

```python
def step(
    fn: Callable[P, Awaitable[R]] | None = None,
    *,
    memo: bool = False,
    identity: str | Callable[[Any], str] | None = None,
    version: str | Callable[[Any], str] | None = None,
) -> Callable[P, Handle[R]]: ...
```

Called without args (`@step`) or with args (`@step(memo=True)`). Wraps an
`async def` and returns a function that, on call, synchronously returns a
`Handle[R]` — scheduling the body as a task and emitting a `spawn` event.

### `memo=False` by default

`memo=True` short-circuits on cache hit: the body never runs, children never
re-spawn. That is right for expensive leaves (LLM calls, heavy fetches) but
wrong for orchestration — a cache hit on a parent erases the graph underneath.

`memo=False` (the default) still populates `cached_output()` / `cached_tracing()`
when a cache entry exists, so the body can short-circuit itself or replay
with simulated timing. The less-opinionated default is strictly more
expressive.

### Identity and version

`StepInfo` identifies a step with two strings:

- **`name`** — "which function is this" across edits. Default:
  `f"{module}:{qualname}"`. Stable across whitespace / rename-less changes.
- **`version`** — "which implementation". Default: a sha256 over the function
  body and its resolved free variables / attribute chains (see `core/types.py`
  for the walker). Changing the body, or a module-level constant it reads,
  invalidates the hash. Changing an unrelated function does not.

Both are overridable via the decorator kwargs:

```python
@step(version="v3")
async def fetch(url: str) -> str: ...

# or a callable: receives the raw fn, returns a string
@step(version=lambda fn: sha_of_prompt_file(fn))
async def research(...): ...
```

Higher-order wrappers forward identity/version to preserve cache continuity:

```python
info = StepInfo.from_function(fn)
return step(wrapper, identity=info.name, version=info.version)
```

### Arguments and cache keys

The cache key is
`sha256(canonical(identity, version, resolved_args))`.
Arguments are canonicalized into a JSON-serializable tree (primitives,
lists/tuples, dicts with sorted keys, sets) by `resolve_hashable()`. Unknown
types raise `TypeError` — silent fallback masks real bugs (e.g. caching on
`repr(numpy_array)` which is often lossy).

Extend support with `register_hash_func`:

```python
from cairn import register_hash_func
register_hash_func(Path, lambda p: (str(p), p.stat().st_mtime_ns))
```

MRO resolution means registering for a base class covers subclasses.

Built-in hashers (`core/hash.py`):

- `Path` — `(str, mtime_ns, size)` for existing paths, a `missing` sentinel
  otherwise.
- `functools.partial` — hashed via `StepInfo.from_function(p.func)`, so
  editing the underlying function invalidates partial-bound steps too.
- `pydantic.BaseModel` — activated automatically if pydantic is importable;
  uses `model_dump(mode="json")`.

### Return types and serialization

Results round-trip through a registry (`core/serial.py`). Built in: `str`,
`bytes`, anything `json.dumps` handles. Extend via `register_serializer(tp,
serialize, deserialize)`.

### Typing

The decorator transforms `Callable[P, Awaitable[R]] → Callable[P, Handle[R]]`
with a `ParamSpec`. Pyright understands this.

The argument transformation (`T → T | Handle[T]`) is not expressible in the
Python type system today. At runtime, passing a `Handle[str]` where `str` is
expected works — the decorator awaits it before calling the body. At
check-time, pyright will flag it. A plugin or narrower API could fix this
later; for now it is a known wart.

### What the decorator does

On call (synchronous):

1. Read `current_span` from contextvars for the parent.
2. Allocate a `TaskSpan(id, parent_id, name, info)`.
3. `asyncio.create_task(run())` for the body.
4. Register the task on the parent's `child_tasks`.
5. Emit `spawn`.
6. Return `Handle[R]`.

Inside `run()` (async):

1. Push the span onto `current_span`.
2. Record `start_ts`.
3. Resolve any `Handle` arguments by awaiting them (contributes to
   `suspended_total`, not `own_time`).
4. Compute the cache key. Look it up.
5. If hit (and not an error): populate `cached_output_value` /
   `cached_tracing_value`. If `memo=True`, emit `end{cached:true}` and
   return the cached value.
6. Emit `start`. Run the body.
7. After the body returns, `asyncio.gather` any remaining `child_tasks` —
   structured concurrency, so a step never outlives its parent. If any
   child raised (and was never awaited, or was awaited and the exception
   re-thrown), the first such exception is re-raised here: a parent does
   not silently succeed over a failed child. Siblings are not cancelled;
   we wait for all to finish before re-raising. To model expected failure,
   return a sentinel value from the child instead of raising.
8. Store the result (or the error, on the exception path).
9. Emit `end` with size/time metrics.

On an exception: gather children first (a fan-out failure shouldn't wipe the
siblings still in flight), then store the error and emit `error`. Children's
exceptions are swallowed on this path — the parent already has its own cause.
Cancellation is allowed to propagate fast; the cancel path emits `cancel`.

---

## 2. `Handle[T]`

```python
class Handle(Generic[R]):
    def __await__(self) -> Generator[Any, Any, R]: ...
    def cancel(self) -> None: ...
    def done(self) -> bool: ...
    @property
    def span(self) -> TaskSpan: ...
```

Returned synchronously by a `@step`-decorated call. Wraps an `asyncio.Task`.

Awaiting a Handle emits a `wait` event on the awaiting span (with an `on`
field pointing at the target), enters await-accounting for that span, and
emits `resume` when the result is ready. `trace()` and child `@step` calls
inside the body see the right parent because each task has its own
contextvars copy.

`Handle` instances can be passed directly as arguments to another `@step` —
the receiving decorator awaits them before the body sees them.

---

## 3. `trace()`

```python
def trace(
    message: str,
    *,
    detail: str = "",
    progress: tuple[int, int] | None = None,
    state: str | None = None,
    level: Literal["info", "warn", "error"] = "info",
    cost: dict[str, int | float] | None = None,
    edge: bool = False,
) -> None: ...
```

Emits a `trace` event and appends a `TraceRecord` to the current span. Fields:

- `message` — short label shown on the timeline.
- `detail` — free-form text shown when the trace is expanded.
- `progress` — `(current, total)`, renders as a bar.
- `state` — sub-lifecycle tag. `rate_limited` uses `"pending"` → `"running"`.
- `level` — `"info"` (default), `"warn"`, `"error"`; controls colouring.
- `cost` — numeric columns (`tokens_in`, `tokens_out`, `cost_usd`, …) that
  the UI sums up the span tree.
- `edge=True` — marks a transition annotation between two sibling steps
  (e.g. "retrying" between `validate` and `refine`).

The set of recognized kwargs is narrow on purpose: the UI can rely on them.
Plugins that want richer metadata can put it on `detail` as a JSON blob.

---

## 4. `cached_output()` / `cached_tracing()`

```python
def cached_output() -> Any | None: ...
def cached_tracing() -> list[TraceRecord] | None: ...
```

Return the previous cached result, and the list of trace events from that
execution (with `delta` fields — relative time since the previous trace).
`None` means no prior entry.

Available in any step. With `memo=True` the body isn't called on a hit, so
these are only useful in `memo=False` steps that want to inspect or replay
their own history.

Example — `replayable` (shipped in `cairn.core.patterns`):

```python
def replayable(fn):
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        prev = cached_output()
        traces = cached_tracing()
        if prev is not None and traces is not None:
            for t in traces:
                await asyncio.sleep(t.delta)
                trace(t.message, **t.kwargs)
            return prev
        return await fn(*args, **kwargs)
    info = StepInfo.from_function(fn)
    return step(wrapper, memo=False, identity=info.name, version=info.version)
```

---

## 5. Event log

Append-only JSONL (`runs/{entry}-{ts}/trace.jsonl`). One line per event.

Event kinds:

| `kind` | Emitted when | Key fields |
|--------|--------------|------------|
| `spawn` | Handle created | `id`, `parent_id`, `name`, `kwargs.{identity,version,args,memo}` |
| `start` | Body begins | `id` |
| `end` | Body finished (or `memo` cache-hit) | `id`, `cached?`, `kwargs.{cache_key,size,own_size,time,own_time}` |
| `error` | Body raised | `id`, `error`, `kwargs.{size,own_size,time,own_time}` |
| `cancel` | Body was cancelled | `id` |
| `wait` | A Handle was awaited | `id` (awaiter), `kwargs.on = {kind,id}` |
| `resume` | Awaiter reawakened | `id` |
| `trace` | `trace()` call | `parent_id`, `message`, `kwargs` |
| `input_request` | `await_input()` asked | `id`, `message`, `kwargs.{schema,by,...}` |
| `input_response` | Sink answered | `id` |

Context tracking uses `ContextVar[TaskSpan | None]` (`current_span`). Each
`asyncio.Task` inherits its own copy, so concurrent siblings have independent
parents without any locking.

---

## 6. Stores

### Output store — `FileStore`

Content-addressed. One JSON file per entry:

```
{store_path}/outputs/{cache_key}.json
```

Each blob carries `result`, serialized `traces`, `duration`, `own_duration`,
and (optionally) a stringified `error`. Writes are atomic (write tmp, fsync,
rename). Entries with errors are stored for browsability but treated as
cache misses — errors are retried, not replayed.

`StoreStats(size, own_size)` is returned on `put`; `own_size` leaves room
for a future L0/CAS layer without breaking the Protocol.

### Run store — layout

```
{store_path}/runs/
    {entry_label}-{iso-utc}/
        trace.jsonl
        001-step_name  →  ../../outputs/{key}.json
        002-step_name  →  ../../outputs/{key}.json
    {entry_label}     →  {entry_label}-{iso-utc}     (GC root: "latest")
```

The `{entry_label}` symlink at the `runs/` level is also the GC root for
`--keep-latest`. Per-step symlinks are created as `end` events arrive.

### Garbage collection

`cairn.run.gc(store, before=..., keep_latest=True)` removes old run
directories, then sweeps `outputs/` for blobs with no surviving symlinks —
Nix-style. CLI: `cairn gc [--before YYYY-MM-DD]`.

---

## 7. `run()` and CLI

Programmatic:

```python
from cairn import run

result = run(pipeline, store_path=".cairns", args=(arg1,), kwargs={"k": v})
```

`run()` spins up an event loop, creates a `RunManager` (output store + trace
sink + run directory), sets contextvars, awaits the entry Handle, then
closes the sink and updates the `latest` symlink.

CLI:

```sh
cairn script.py                          # run main() from script.py
cairn script.py my_entry                 # run my_entry() (space, not colon)
cairn script.py --force                  # clear cache for this entry, then run
cairn                                    # interactive run browser (TUI)
cairn list                               # list runs
cairn show [RUN_ID]                      # show trace (latest if omitted)
cairn output PATH                        # show a cached output blob
cairn gc [--before YYYY-MM-DD]           # remove old runs
```

`cairn script.py` opens the TUI (detail pane, live updating span tree) if
`cairn[tui]` is installed; otherwise runs headless.

---

## 8. Higher-order patterns

These are not framework features. They are ordinary functions built on
the primitives.

### Memoization

`memo=True` is equivalent to "short-circuit when `cached_output()` hits":

```python
prev = cached_output()
if prev is not None:
    return prev
return await fn(*args, **kwargs)
```

The asymmetry argued above is why this is opt-in.

### Retry

```python
def with_retry(max_attempts: int = 3):
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    trace(f"attempt {attempt + 1}", progress=(attempt + 1, max_attempts))
                    return await fn(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    trace("retrying", detail=str(e), level="warn")
        info = StepInfo.from_function(fn)
        return step(wrapper, memo=True, identity=info.name, version=info.version)
    return decorator
```

### Validation loop

```python
async def validated(generate, validate, refine, *args, max_retries=3, **kwargs):
    draft = await generate(*args, **kwargs)
    for i in range(max_retries):
        result = await validate(draft)
        if result["success"]:
            return draft
        trace("retrying", edge=True, progress=(i + 1, max_retries))
        draft = await refine(draft, result["feedback"])
    return draft
```

Same shape for any generate / validate / refine triple. See
`examples/research_fake_llm.py` for a running version.

### Human in the loop

`cairn.interaction.await_input` is the primitive:

```python
from cairn.interaction import await_input, set_interaction_sink, StdinInteractionSink

set_interaction_sink(StdinInteractionSink())

@step
async def confirm(plan: str) -> str:
    return await await_input(f"Approve?\n{plan}", placeholder="y/n")
```

It's a memoized `@step` underneath, so re-runs replay the previous answer
as a prefill. Sinks — stdin, queue for tests, TUI input widget — implement
a single `async def request(req: InputRequest) -> Any`.

---

## 9. Pluggability

- **Hashers** — `register_hash_func(tp, fn)`.
- **Serializers** — `register_serializer(tp, ser, de)`.
- **Stores** — anything implementing the `Store` protocol (`get`, `put`,
  `has`). `MemoryStore` + `FileStore` ship in core.
- **Sinks** — anything implementing `Sink.emit(event)`. `JSONLSink`,
  `MemorySink`, `NullSink`, `CompositeSink` ship in core.
- **Interaction sinks** — `InteractionSink.request(req)`. Stdin + queue
  sinks in `cairn.interaction`; the TUI ships its own.

---

## 10. Known gaps

See `docs/todo/` for parked design work:

- [`nominal-identity.md`](todo/nominal-identity.md) — nominal vs. intensional
  identity (the current source-hash default is a known-imperfect proxy).
- [`anyio.md`](todo/anyio.md) — migrating `asyncio.TaskGroup` to anyio's
  `TaskGroup.create_task` once it ships, to unlock trio support.

Not parked but worth naming:

- **Pyright can't express `T | Handle[T]`** on arguments — see §1.
- **Trace sink is file-only** — no streaming to a live web UI out of the
  box. `CompositeSink` makes this straightforward to add; no one has yet.
- **No token-level streaming** — the event model is event-per-action. LLM
  token streaming should be handled inside a step body (e.g. by emitting
  periodic `trace("...", progress=...)`), not by the framework.
