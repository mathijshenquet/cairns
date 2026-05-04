"""The `Cairn` view + the `cairn()` accessor.

A `Cairn` is a lazy, iterable view over a Store's record stack for one
cairn_id. Newest-first iteration. Doesn't load records eagerly — yields
on demand.

Three entry points:

- `cairn()` — the cairn for the currently-executing `@step`.
- `step_fn.cairn(*args, **kwargs)` — the cairn for an arbitrary `@step`
  invocation, computed without invoking. (Method on the decorated
  function; defined in `step.py`.)
- `Cairn.from_store(store, cairn_id)` — explicit construction. For CLI
  / scripts / advanced use, when no Run is active.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from .runtime import current_run, current_span
from .types import Record

if TYPE_CHECKING:
    from .store import Store


class Cairn:
    """View over the record stack for one cairn_id."""

    def __init__(self, cairn_id: str, store: "Store") -> None:
        self.cairn_id = cairn_id
        self._store = store

    @classmethod
    def from_store(cls, store: "Store", cairn_id: str) -> "Cairn":
        """Explicit construction. For inspection outside an active Run."""
        return cls(cairn_id, store)

    def __iter__(self) -> Iterator[Record]:
        return self._store.iter_records(self.cairn_id)

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def __bool__(self) -> bool:
        return next(iter(self), None) is not None

    def latest(
        self,
        *,
        version: str | None = None,
        body_hash: str | None = None,
        include_errors: bool = False,
    ) -> Record | None:
        """Newest record matching the given filters.

        `version` / `body_hash` constrain on the corresponding fields when set.
        By default skips errored records — pass `include_errors=True` to
        surface the most recent regardless.
        """
        for record in self:
            if record.error is not None and not include_errors:
                continue
            if version is not None and record.version != version:
                continue
            if body_hash is not None and record.body_hash != body_hash:
                continue
            return record
        return None

    def at(self, record_id: str) -> Record | None:
        """Pinpoint a record by its id."""
        for record in self:
            if record.record_id == record_id:
                return record
        return None


def cairn() -> Cairn:
    """The cairn for the currently-executing `@step`.

    Reads `current_span` for the step's identity + bound args, looks up
    the cairn_id, and returns a view over the active Run's store.

    Raises if no `@step` is active (i.e. called from outside a step body).
    """
    span = current_span.get()
    if span is None:
        raise RuntimeError(
            "cairn() called outside a @step body — no current span"
        )
    if span.cairn_id is None:
        raise RuntimeError(
            "cairn() called before the step's cairn_id was computed — "
            "this should only happen if called before _resolve_args completes"
        )
    return Cairn(span.cairn_id, current_run().store)
