"""Semver-tagged record selection.

Pattern for picking records out of a `Cairn` by semver constraint:

    @step(memo=True, tags={"semver": "1.2.3"})
    async def predict(x: int) -> Output: ...

    @step
    async def consumer():
        prev = latest_matching(predict.cairn(x), ">=1.2.0,<2.0.0")
        if prev:
            return prev.result
        return await _heavy_work()

Optional dependency: `semver` (pip install semver). Imported lazily so
the rest of `cairns.patterns` works without it.
"""

from __future__ import annotations

from typing import Iterator

from cairns.core import Cairn, Record


def matching(cairn: Cairn, spec: str) -> Iterator[Record]:
    """Yield records whose `tags['semver']` satisfies `spec`. Newest-first.

    Records without a `semver` tag are skipped silently. Records whose
    tag is malformed (not parseable as semver) are also skipped.
    """
    try:
        from semver import match  # type: ignore[import-not-found]  # noqa: PLC0415
    except ImportError as e:
        raise ImportError(
            "install `semver` to use `cairns.patterns.semver` "
            "(pip install semver)"
        ) from e

    for record in cairn:
        v = record.tags.get("semver")
        if v is None:
            continue
        try:
            if match(v, spec):
                yield record
        except ValueError:
            # Malformed semver tag — skip rather than crash the iteration.
            continue


def latest_matching(cairn: Cairn, spec: str) -> Record | None:
    """Newest record whose `tags['semver']` satisfies `spec`."""
    return next(matching(cairn, spec), None)
