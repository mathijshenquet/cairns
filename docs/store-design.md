# Store design

Parked design for the on-disk layout. Today's store
(`outputs/{cache_key}.json` + `runs/{entry}-{ts}/trace.jsonl` + ordered
symlinks) conflates several concerns into one flat JSON blob: it overwrites on
re-invocation, can't reconstruct span structure on cache hits, has no
first-class notion of "multiple executions of the same computation," and
offers no surgical-edit primitive. This document is the target shape. It
builds directly on the [nominal identity & layered storage](todo/nominal-identity.md)
notes.

The central primitive is the **cairn**: a location (keyed by computation
identity) holding a stack of **stones** (immutable execution records). The
project name is the data structure — stones piled over time at a fixed
location.

This doc is organized around cairns. Content-addressed byte storage and
cache pick policy are orthogonal refinements and live in the appendices —
a reader who only cares about the cairn idea can stop after the GC section.

## Core claims

- **A cairn is a stack of stones.** A cairn is derived from a function, it is
  is identified by `hash(identity, *args)`, where `identity` defaults to the fully-qualified 
  of the function. A stone is keyed by a uuid7 (chronologically sortable by filename) and
  carries a `version` (defaulting to a hash of the functions ast) in its 
  metadata alongside the execution record: events, optional result pointer, 
  pointers to specific child stones on other cairns. Stones are immutable once 
  published; a cairn's stack is append-only.
- **A run is a resolver, not a first-class object.** Executing a pipeline
  resolves each encountered cairn to a stone via one of three outcomes:
  **created** (execute fresh, push stone), **recalled** (pick an existing
  stone from the stack — this is the cache), or **carried** (the run was
  pre-seeded with a stone for this cairn — this is surgery). The merged
  trace replays each resolved stone's events into a central stream.
- **Every cross-layer reference is a symlink.** GC walks the filesystem;
  `ls` shows every dep; `find -L` enumerates them. No JSON edges.
- **External references use indirect roots**, Nix-style. `cairn checkout`
  creates both the user-facing symlink and a bookkeeping symlink inside
  the store. GC follows indirect roots one hop out.

## On-disk layout

```
.cairn/
  cairns/{cairn_id}/                   # one dir per computation (identity, args)
    {stone_uuid}/                      # one dir per execution; uuid7, chronological
      metadata.json                    # version, duration, size, origin, ast_hash, *_repr, ts
      events.jsonl                     # own events, timestamps relative to stone start
      result      -> ../../../store/{content_hash}            # optional, see Appendix A
      args/                            # optional; only when store_args=True
        0         -> ../../../../store/{content_hash_a}
        url       -> ../../../../store/{content_hash_b}
      children/
        000       -> ../../../{child_cairn_id}/{child_stone_uuid}/
        001       -> ../../../{child_cairn_id}/{child_stone_uuid}/
  runs/{entry}-{ts}/                   # one dir per run
    trace.jsonl                        # merged, seq-numbered, tailable central stream
    steps/                             # the run's resolved walk, ordered by seq
      000-{short_name} -> ../../../cairns/{cairn_id}/{stone_uuid}/
      001-{short_name} -> ../../../cairns/{cairn_id}/{stone_uuid}/
      005-{short_name}/                # carried: a stone-shaped dir owned by the run
        metadata.json
        events.jsonl
        result     -> ../../../../store/{content_hash}
        children/…
    {entry} -> {entry}-{ts}            # latest-run pointer per entry; GC root
  store/{content_hash}                 # CAS for value bytes (see Appendix A)
  checkouts/auto/{id} -> /abs/path/to/users/external/symlink
```

`metadata.json` holds only scalars and display strings. Cross-layer
pointers are always symlinks.

## Cairns of stones

A cairn is a directory keyed by `cairn_id = hash(identity, *args)`. Inside,
a stack of stones — each one a self-contained record of one execution.
Stones are immutable once published. The stack is append-only: every
execution pushes a new stone; nothing is overwritten, ever.

A stone is partitioned — it describes exactly one span with its own events
and nothing else. Grandchildren live in *their* cairns' stones and are
referenced via `children/*` symlinks. A stone pins *which* child stone it
spawned, not just which child cairn — so replay follows the exact historical
subtree, not the current top-of-stack of the child cairn.

A stone's `version` lives in its metadata, not in the cairn_id. The stack
of a cairn can therefore span multiple versions — every execution of this
computation, across all code revisions, piles up in the same directory.
That makes the cairn a natural place to observe drift (ast_hash
distribution over time), nondeterminism (result_hash distribution at a
fixed version), and behaviour across code edits. The recall predicate
(Appendix B) filters by matching version; the stack itself preserves
everything.

## Runs as resolvers

A run is not a cairn. It's a walk: executing a pipeline resolves each
encountered cairn to a stone and replays events into a merged trace. Three
outcomes per cairn:

1. **Created** — executed the body fresh, pushed a new stone, used it.
2. **Recalled** — picked an existing stone from the cairn's stack (cache
   hit), no body executed, stone's events streamed into the merged trace
   with rebased timing.
3. **Carried** — the run was seeded with a stone for this cairn (via
   `run(..., carry={cairn_id: stone})`), used without consulting the
   stack. This is how surgery, mocking, and branching work.

The run's `trace.jsonl` is live-written as events fire. The `steps/` dir
is a human-browsable flat view of the stones the run touched, ordered by
resolution seq; tree structure lives in the trace.

Operationally, `resolve: cairn_id → stone`:

```
resolve(cairn_id):
    if cairn_id in run.carry:                   # outcome: carried
        return run.carry[cairn_id]
    if memo and pick(cairn.stack) is not None:  # outcome: recalled
        return pick(cairn.stack)                # pluggable; default Appendix B
    # outcome: created
    stone = execute()
    push(cairn, stone)
    return stone
```

`memo=True` vs `memo=False` is the only semantic knob: does the resolver
consult the stack? Both produce a stone (every execution is recorded);
they differ only in whether the stack is read first.

### Carried stones enable surgery

Carrying a stone means the run has been pre-seeded with a specific stone
for a specific cairn. The resolver short-circuits to that stone without
executing or consulting the stack. A carried stone can either be an
existing stone from some cairn (just a symlink in `steps/`) or a synthetic
stone the caller constructed from scratch (a stone-shaped directory
embedded *inside* the run's `steps/` dir — the run owns it; it doesn't
pollute any cairn's stack).

This single primitive covers several previously-distinct features:

- **Result override**: force step X to return Y. Construct a synthetic
  stone with result Y, carry it under cairn_id(X).
- **Branch from a past run**: re-run as in run A but with one thing
  changed. Seed `carry` from A's resolution map, drop the entry for the
  cairn you want to re-execute. Downstream cairns whose inputs changed
  will cache-miss and produce new stones.
- **Mocking in tests**: construct stones for cairns you want to stub; run.
  Carrying a stone *is* the mock.
- **Pinning**: always use stone S from cairn K for this run. Carry it.

Carried stones have `origin: carried` in their metadata, and trace
renderers mark them distinctly (e.g., dashed nodes in the TUI). The carry
map is recorded in the run's metadata so a surgical run is reproducible.

### Why this dissolves the uniqueness problem

In designs where L1 was keyed by cache_key with one entry per key,
memo=False steps broke the abstraction: two executions of the same
cache_key produced different outputs, but the filesystem had one slot.
Overwriting lost history; sidecar schemes reintroduced UUIDs.

Under cairns of stones, this problem doesn't arise. memo=False pushes a
new stone; memo=True on a cache miss also pushes a new stone. The stack
records every execution. The cache is a pick-from-stack policy, not a
storage slot. Overwriting is not a concept — there is nothing to overwrite.

## Identity

Three naming systems, each in its proper scope:

- **`cairn_id`** identifies a computation: `hash(identity, *args)`.
  Persistent, global.
- **`stone_id`** identifies one execution: uuid7, unique within a cairn,
  chronologically sortable. Persistent but meaningful only alongside its
  cairn.
- **`seq`** is a run-local integer, assigned at trace-merge time,
  meaningful only inside that run.

A stone's `events.jsonl` describes exactly one span. No line inside it
needs to name "which span" — there's only one. Body events implicitly
belong to the stone. Spawn events reference child stones by `(cairn_id,
stone_id)`. The end event terminates the single span.

### cairn_ids are statically computable

When a step takes another step's `Handle` as an argument, the arg is
hashed structurally: `hash(child.identity, child.version, child.args)`.
The hash recurses through the child's args the same way. Nothing in this
chain depends on a resolved *value* — the full pipeline's cairn_ids can
be computed by walking the call expression before any body executes.

That's a scheduling lever. Before running a line of user code, the
planner knows every cairn_id in the graph, can consult each stack, and
can classify the whole pipeline into (carried / will-recall /
will-execute). The will-execute subset is then free to parallelize
however the scheduler chooses — downstream identity is not waiting on
upstream results. Today's store doesn't have this property.

Editing an upstream function bumps its version, which propagates through
structural hashes into downstream cairn_ids, so the cache-miss chain
works automatically without ever consulting values.

### Replay mechanics

On resolution of cairn K to stone S in a live run:

1. Mint a fresh seq for this occurrence.
2. If origin is *created*: stream the fresh execution's events (they go
   to the new stone's file and the merged trace simultaneously).
3. If origin is *recalled* or *carried*: open
   `cairns/K/{S}/events.jsonl`, stream events into the merged trace,
   attaching the fresh seq and rebasing relative timestamps.
4. For each spawn event encountered (carrying child `(cairn_id,
   stone_id)`), recurse: mint a new seq, open the child's events, stream
   through.

Seq assignment happens only at the boundary where events enter the
merged stream. "Same stone referenced twice in one run" isn't a special
case — each occurrence gets its own seq, its own branch in the tree.

## Trace shapes

Two distinct files with distinct roles:

- `cairns/{id}/{stone}/events.jsonl` — authoritative record of one
  execution. Timestamps relative to the stone's start, never wall-clock.
  Immutable once the stone is published.
- `runs/{ts}/trace.jsonl` — merged central stream produced at resolution
  time. Each stone's events are replayed into it with fresh seqs and
  rebased timestamps. Best-effort ordering under concurrency — strict
  ordering lives inside each stone's events.jsonl; the merge interleaves.

### Event line in a stone

```json
{"kind": "trace", "ts": 0.123, "msg": "fetching", "level": "info"}
{"kind": "spawn", "ts": 0.200, "end_ts": 0.450, "cairn_id": "ab12...",
 "stone_id": "7f3c...", "short_name": "extract", "error": null}
{"kind": "end",   "ts": 1.200, "duration": 1.20, "own_duration": 0.40,
 "size": 2048, "own_size": 128}
```

The `start` event is implicit. Spawn events bracket the child with timing
so the parent's timeline renders child duration bars without opening the
child's file; `(cairn_id, stone_id)` is the pointer for progressive
expansion.

### Event line in a run trace

```json
{"kind": "start", "seq": 47, "parent_seq": 12, "cairn_id": "ab12...",
 "stone_id": "7f3c...", "origin": "recalled", "name": "pipeline:extract"}
{"kind": "trace", "seq": 47, "ts": 0.123, "msg": "fetching"}
{"kind": "end",   "seq": 47, "ts": 1.200, "duration": 1.20,
 "own_duration": 0.40, "size": 2048, "own_size": 128}
```

Local seq ids only. Parent references are `parent_seq` to an earlier
line in the same file. `origin` tells the renderer whether to show this
span as executed, recalled, or carried.

## References as symlinks

Every cross-layer pointer is a filesystem symlink, not a JSON field:

```
cairns/{cairn_id}/{stone_uuid}/
  result        -> ../../../store/{content_hash}                     # optional
  args/0        -> ../../../../store/{content_hash_a}                # optional
  args/url      -> ../../../../store/{content_hash_b}                # optional
  children/000  -> ../../../{child_cairn_id}/{child_stone_uuid}/
  children/001  -> ../../../{child_cairn_id}/{child_stone_uuid}/
```

Positional args get integer names (`args/0`, `args/1`); keyword args get
their param name. Children are ordered by spawn occurrence (first becomes
`000`), giving naturally-sorted `ls`. Duplicate child references are fine
— invoking the same child cairn twice produces two child stones with two
ordinal entries.

Child pointers are **stone-specific**, not cairn-specific. A parent
records which child stones it actually spawned during *its* execution.
Replay follows those exact pointers — it does not re-resolve to the
current top-of-stack of the child cairn. Re-resolving would desync a
parent's recorded subtree from what it really ran; pinning in the
filesystem makes this structurally impossible to get wrong.

`metadata.json` holds only scalars and human-readable display:

```json
{
  "origin": "created",
  "version": "…",
  "duration": 1.20,
  "own_duration": 0.40,
  "size": 2048,
  "own_size": 128,
  "ast_hash": "…",
  "short_name": "extract",
  "ts_created": 1713657600.0,
  "result_repr": "DataFrame(1234 rows, 8 cols)",
  "args_repr": {
    "0": "https://example.com/paper.pdf",
    "url": "'haiku'"
  }
}
```

`*_repr` strings are display-only, generated at serialize time by the
type registry. They don't participate in hashing or replay; they're what
lets a trace render usefully after L0 bytes are pruned.

## Publication protocol

Because stones are immutable once visible, publication must be atomic.
Each executing stone writes to a temporary directory
(`cairns/{cairn_id}/.tmp-{uuid}/`), appending `events.jsonl`
incrementally. On the `end` event, `metadata.json` is written and the
directory is renamed to its final location
(`cairns/{cairn_id}/{stone_uuid}/`). Atomic directory rename on Unix means
readers see either nothing or a complete stone.

A crash mid-execution leaves an abandoned `.tmp-*` directory with no
`metadata.json`. Readers ignore these (trivial predicate: metadata
missing). A janitor pass on startup, or on next GC, deletes `.tmp-*`
dirs older than N minutes.

Concurrent executions of the same cairn each write to their own
`.tmp-{uuid}/` and rename to unique stone_ids — no contention, no
locking on the write path.

## `cairn checkout`

Git-flavoured. Materializes a stored value at a user-chosen path:

```sh
cairn checkout <hash | cairn_id | cairn_id:stone_id> <target-path>
```

- Content_hash → L0 bytes copied/symlinked to target.
- Cairn_id → resolves to that cairn's top valid stone, then its result.
- Cairn_id:stone_id → exact stone's result. Useful for pinning a specific
  historical execution.

Two things happen atomically:

1. `<target-path>` is created as a symlink into
   `.cairn/store/{content_hash}` (or a copy / hardlink, depending on
   flags).
2. An indirect root is placed at `.cairn/checkouts/auto/{id} → <absolute
   target path>`. The id is derived from the canonicalized absolute
   target path so re-checkout to the same target is idempotent.

GC follows each indirect root one hop out to confirm the external
symlink still exists and resolves into the store. If the user deleted or
moved their checkout, the indirect root is dangling — GC unlinks it and
the store path becomes collectable. No manual pin command needed.

## GC

One algorithm, one mechanism: walk `.cairn/` once, follow symlinks from
known roots, mark reachable, delete the rest.

**Root classes.** Two, both at known locations:

- `runs/{entry}` symlinks (one per entry point, pointing at the latest
  run dir). Keep the latest run of every entry alive by default; `cairn
  gc --before <date>` or explicit run deletion narrows this.
- `checkouts/auto/{id}` indirect roots. Each dereferences to an external
  path; if that external symlink still resolves into `.cairn/store/`,
  mark its target live. If not, drop the indirect root.

**Mark phase.**

1. Collect initial live set from runs and live checkouts.
2. From each live run dir, follow every `steps/*` entry. Each is either
   a symlink to a stone in `cairns/…` (recalled/created) or a
   stone-shaped dir embedded in the run itself (carried). Both count as
   stones.
3. From each live stone, follow `result`, any `args/*`, and every
   `children/*` symlink — L0 entries and more stones join the live set.
   Transitively closes.
4. A cairn is live iff any of its stones is live.

**Stack-pruning policy.** Beyond pure reachability, per-cairn keep
policies generate additional synthetic roots before the mark phase:

- Default: pure reachability.
- `keep_top_n`: keep the top N stones of every cairn regardless.
  Freshness floor against aggressive run deletion.
- `keep_within`: keep stones created within the last T time.

These are knobs on root collection; the mark algorithm itself doesn't
change. Keep-policies are not automatically transitive — a stone kept by
`keep_top_n` does not necessarily keep its children alive. If you want
historical subtree integrity, pair keep-policies with L0-prune-only
(Appendix A) so structure survives even when bytes are released.

**Concurrent runs.** A GC lock (`.cairn/gc.lock`) is taken for mark +
sweep. Runs attempting to publish stones wait for the lock; GC backs off
if a run is actively writing. Standard Nix-style.

**Sweep phase.** Anything in `store/`, `cairns/*/{stone_uuid}/`, or
`runs/` not in the live set is deleted. Empty `cairns/{cairn_id}/` dirs
can be swept too. Dangling `checkouts/auto/` entries were already
removed during root collection.

**Classification for free.** Because every inbound reference is a
symlink inside `.cairn/`, `cairn gc --explain` reports why each object
was kept: walk the reverse index the mark phase built, list each inbound
symlink's source. "Kept because referenced by
`cairns/abc/7f3c…/children/000`, which is kept because
`runs/pipeline-2026-…/steps/012-extract`."

**No database.** A full scan of `.cairn/` on every `cairn gc` is fine at
Cairn's scale for a long time. The escape hatch is an incremental index
at `.cairn/index/` — same as Nix's sqlite cache, just an optimization
over ground truth. Ground truth stays the filesystem.

## What this buys

- **History is append-only.** Every execution ever done is still
  addressable. Nothing is overwritten; no cache invalidation destroys
  record. The cache is a view over history, not a slot.
- **Cache-hit subtree expansion works.** Clicking a recalled node in the
  TUI expands the full recorded subtree, with accurate timings and
  traces, sourced from the referenced stone's `children/*` chain.
- **Surgery is one primitive.** Override, mock, branch, pin are all
  "carry a stone." No special cases in the engine.
- **Whole-graph identity before execution.** Structural arg-hashing
  makes every cairn_id statically derivable from the call expression, so
  scheduling decisions (cache hit/miss classification, parallelism
  plans) can happen before any body runs.
- **Single-file tailing.** The run's merged trace is live-written for
  TUI tails and observability ingestion.
- **Honest identity.** cairn_id identifies the question; stone_id
  identifies an execution; seq is run-local. Three layers, each scoped
  to its meaning.
- **Progressive disclosure.** A parent stone renders without opening
  children; expanding a child is one file open.
- **`ls` is debugging.** Every dep is a symlink. `find -L .cairn -lname
  '*abc123*'` enumerates every reference to a content_hash. No JSON
  parsing.
- **Concurrent and crash-safe by construction.** Atomic publish via
  rename; independent tmp dirs mean no locking on the write path.

---

## Appendix A — L0: content-addressed byte storage

L0 is orthogonal to the cairn idea: it's where value bytes live,
referenced weakly from stones. Dropping L0 never corrupts the cairn
structure; at worst a trace renders `(pruned, was: DataFrame(1234 rows,
8 cols))` from the `result_repr` instead of opening the file.

**Layout.** One file per content hash under `store/{content_hash}`.
Stones reference it via `result → ../../../store/{hash}` and optionally
`args/* → …/store/{hash}`.

**Per-type serialization.** The type of a value decides how its bytes
get written. `str` → json.dumps. `Path` → copy file contents.
`pandas.DataFrame` → parquet via pyarrow. Unregistered types error (or
fall back to pickle with a warning, opt-in). Composite values (lists,
dicts) are walked by default and each leaf hits its own handler — this
is what makes L0 dedup pay for itself on real pipelines, where two
functions returning `list[Path]` overlap in 9/10 elements.

**Type registry API.** `cairn.serializer.register(type)` returns
`(hash_fn, serialize_fn, repr_fn, materialize_fn)`. Composite walk by
default; opt-in whole-value handlers as an escape hatch.

### Args: hash vs store

Cache identity needs argument hashes. Storing the argument *bytes* is a
separate question with a separate cost profile — serializing large
DataFrames or files for every invocation is expensive, and inode
pressure from populating `args/*` on every call is non-trivial.

- **Default: hash only.** Arguments pass through the type registry's
  `hash(value)` function; `args_repr` (display strings) land in
  `metadata.json`. No L0 entries are created for arg bytes. This is
  enough for cache identity, trace rendering, and debugging from the
  terminal.
- **Opt-in: `@step(store_args=True)`** on steps where reconstructing
  inputs later matters. When opted-in, arguments serialize to L0 and
  `args/*` symlinks point at them, matching results.

A similar trace-level mechanism exists: `trace(store=value,
name="big_df")` explicitly pushes a value into L0 and records a
reference in the event stream for debugging artifacts worth keeping.

This decouples two orthogonal decisions: "can the cache find the right
entry?" (needs hash) and "can I reconstruct the inputs later?" (needs
bytes).

### L0 GC and pruning

L0 entries live or die by stone references. The mark phase (main body)
follows `result`, `args/*`, and any `trace(store=…)` references;
anything in `store/` not reached is swept.

**Pruning-only mode.** `cairn gc --prune-l0` removes L0 entries
regardless of stone liveness, keeping stone metadata and events.
Enables "keep the run's structure and timing, drop the big bytes"
workflows. Traces degrade gracefully via `*_repr` fields.

---

## Appendix B — Cache pick policy

The resolver's recall branch calls `pick(cairn.stack) → stone | None`.
This is where "which stone do we actually use on a cache hit?" lives.

**Default: top valid stone matching the current version.**

- **Matching version.** The stack may hold stones from any past code
  revision. Only stones whose `version` matches the current code's
  version are eligible. A mismatched stack — all stones at older
  versions — is treated as a miss; execution proceeds and pushes a new
  stone at the current version.
- **Subtree integrity.** The picked stone must have all its `children/*`
  still resolving. A stone whose descendants were GC'd is skipped.
- **Top.** Among eligible stones, pick the newest. uuid7 sorts
  chronologically by filename, so the newest is just `ls | tail -1`
  filtered by the predicate.

**Optional index.** Per-cairn `version -> stone_uuid` symlinks can
short-circuit the scan when the stack is long. These are mutable
pointers, rebuilt from the stack on demand — index, not ground truth.
Stale pointers are recovered by falling back to a scan.

**Invalidation.** There is no in-place rejection of stones. Two
mechanisms cover every real need:

- **Bump the version** (hand-rolled or automatic via ast_hash). Past
  stones remain in the stack as history but no longer match the recall
  predicate. The cairn's stack grows a new version segment; old
  segments stay for drift analysis until GC'd.
- **`--force` carry-miss flag** on a single run. The resolver treats
  the stack as empty for flagged cairns, forcing fresh execution
  regardless of what's there. The carry map records this so the run is
  reproducible.

Stones are genuinely immutable. Cache semantics shift by changing the
predicate, never by mutating what was recorded.

**Pluggable pickers.** Advanced users can supply a custom
`pick_stone(stack, context) → stone | None` — e.g., "only stones from
tagged runs," "only stones newer than T," "prefer a tag, fall back to
any."

### Cairn-level aggregation

A cairn is a natural unit of aggregation: every execution of a
computation lands in the same directory, so stats over that directory
describe the computation over time.

A `digest.json` at the cairn root (optional, rebuildable by scanning the
stack) holds rolling summaries: stone counts by version, duration
distributions, result-hash frequencies, ast_hash distribution. Useful for:

- Capacity planning and regression detection (duration trends).
- Nondeterminism detection: same cairn, same version, different result
  hashes → something is impure.
- Drift tracking: ast_hash distribution across stones shows code
  evolution over time for this specific computation.

`cairn stats <cairn_id>` surfaces a single cairn's digest; unscoped
`cairn stats` is a project-wide health view. Digests are an index over
the stack, not ground truth.

---

## Appendix C — Scope notes

- **ast_hash as default `version`.** If a step doesn't set `version`
  explicitly, it defaults to the ast_hash of its body. That collapses
  two concepts into one: version controls cache identity, ast_hash
  records code provenance, and they're the same field. Users who want
  manual control over invalidation set `version` explicitly.
- **Alias table.** `aliases.json` at the store root implements kripkean
  renames from the nominal-identity doc. Maps retired names → stable
  UUIDs. Mandatory once nominal is the default cache key.
- **Stone_id scheme.** uuid7 is the default — time-ordered filenames
  give chronological `ls` for free and avoid a separate creation-order
  index. Content-hash of the stone's canonicalized contents is possible
  but needs a careful definition of "canonical" (timings excluded).
  Revisit if cross-run dedup of identical executions becomes desirable.
- **Checkout materialization flags.** Default is symlink; `--copy` and
  `--hardlink` are obvious extensions. Hardlink has the nice property
  that `st_nlink > 1` survives even if the indirect root is removed —
  second safety net — but cross-filesystem hardlinks aren't possible,
  so not the default.
- **Run replay is trivial** under this model: read the run's merged
  trace verbatim. No cache consultation needed — resolution was frozen
  into the trace at record time. Replays are stable under later cache
  mutation or stone GC, provided the referenced stones survive (kept
  alive by the run dir's symlinks, so this is automatic).
- **`cached_output()` / `cached_tracing()`** become lookups into a
  resolved stone regardless of how it was resolved (created, recalled,
  carried). The handle knows its stone; the stone is addressable.
  Previous awkwardness around memo=False disappears.
- **OTEL export** is a natural sink: each stone becomes a span, a run's
  walk becomes one trace. Cairns provide structure OTEL consumers can
  collapse if they only care about the trace view; the cairn-identity
  information is additive.
- **Scale.** Filesystem is ground truth; at scale the same logical
  model materializes into a sqlite index at `.cairn/index/`. Inode
  pressure from per-stone dirs and symlinks is addressed at the index
  layer, not by changing the model.
- **Windows.** Not supported. Symlinks to files are the wart (junctions
  only cover directories), and the model leans on them. Linux and
  macOS are the first-class targets.
