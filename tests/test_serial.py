"""Round-trip tests for the CAS serializer registry."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from cairn import run, step


class Analysis(BaseModel):
    sentiment: str
    score: float


class Item(BaseModel):
    name: str
    qty: int


class Wrapped(BaseModel):
    tag: str
    inner: Analysis


def test_pydantic_roundtrips_through_cache(tmp_path: Path) -> None:
    """A @step that returns a pydantic model returns the same model on cache hit."""
    store_path = str(tmp_path / ".cairn")

    @step(memo=True)
    async def analyze() -> Analysis:
        return Analysis(sentiment="positive", score=0.87)

    r1 = run(analyze, store_path=store_path)
    assert isinstance(r1, Analysis)
    assert r1.sentiment == "positive"
    assert r1.score == 0.87

    r2 = run(analyze, store_path=store_path)
    assert isinstance(r2, Analysis), f"cache hit returned {type(r2).__name__}"
    assert r2 == r1


def test_nested_pydantic_roundtrips(tmp_path: Path) -> None:
    """Pydantic-in-pydantic survives the round-trip."""
    store_path = str(tmp_path / ".cairn")

    @step(memo=True)
    async def wrap() -> Wrapped:
        return Wrapped(tag="t", inner=Analysis(sentiment="neg", score=0.1))

    r1 = run(wrap, store_path=store_path)
    r2 = run(wrap, store_path=store_path)
    assert isinstance(r2, Wrapped)
    assert isinstance(r2.inner, Analysis)
    assert r2 == r1


def test_tuple_distinct_from_list(tmp_path: Path) -> None:
    """Tuples round-trip as tuples, not lists."""
    store_path = str(tmp_path / ".cairn")

    @step(memo=True)
    async def pair() -> tuple[int, int]:
        return (1, 2)

    r1 = run(pair, store_path=store_path)
    r2 = run(pair, store_path=store_path)
    assert r1 == (1, 2)
    assert isinstance(r2, tuple)
    assert r2 == (1, 2)


def test_unregistered_type_falls_through_to_json(tmp_path: Path) -> None:
    """Plain JSON-native values still work with no serializer in play."""
    store_path = str(tmp_path / ".cairn")

    @step(memo=True)
    async def produce() -> dict[str, Any]:
        return {"a": 1, "b": [1, 2, 3], "c": {"d": "x"}}

    r1 = run(produce, store_path=store_path)
    r2 = run(produce, store_path=store_path)
    assert r1 == {"a": 1, "b": [1, 2, 3], "c": {"d": "x"}}
    assert r2 == r1


def test_missing_serializer_raises_on_read() -> None:
    """Reading a stone whose tag has no registered serializer raises clearly."""
    from cairn.core.serial import clear_serializers, from_jsonable

    clear_serializers()  # drops the pydantic default
    try:
        form = {"__cairn_serial__": "cairn.nonsense:Gone", "v": {"x": 1}}
        with pytest.raises((TypeError, ModuleNotFoundError, AttributeError)):
            from_jsonable(form)
    finally:
        # Reinstall defaults so later tests keep working.
        clear_serializers()


def test_list_of_pydantic_models(tmp_path: Path) -> None:
    """Lists containing pydantic models are walked and tagged per-element."""
    store_path = str(tmp_path / ".cairn")

    @step(memo=True)
    async def batch() -> list[Item]:
        return [Item(name="a", qty=1), Item(name="b", qty=2)]

    r1 = run(batch, store_path=store_path)
    r2 = run(batch, store_path=store_path)
    assert isinstance(r2, list)
    assert len(r2) == 2
    assert all(isinstance(x, Item) for x in r2)
    assert r2 == r1
