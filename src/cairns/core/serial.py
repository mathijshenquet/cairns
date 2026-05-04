"""Result serialization for the CAS.

Values flow between memory and disk as JSON. Types that don't round-trip
through plain JSON (Pydantic models, tuples, …) declare a `Serializer`
that turns them into a JSON-safe form on write and reconstructs them on
read.

On disk, a tagged value looks like::

    {"__cairn_serial__": "mymod.Analysis", "v": {"sentiment": "positive", ...}}

The tag is the value's fully-qualified type; on read we import the class
and walk the MRO via the active runtime's serializers. Untagged values
(plain JSON-native types) pass through unchanged, so old records keep
working.

Serializers live on `Runtime` instances. The active Run's runtime
is consulted; falls back to `default_runtime` when no Run is active.
Use `runtime.register_serializer(...)` to extend.
"""

from __future__ import annotations

import importlib
from typing import Any, Protocol, cast


_SERIAL_TAG = "__cairn_serial__"
_VALUE_KEY = "v"
_TUPLE_TAG = "__tuple__"


class Serializer(Protocol):
    """Bidirectional converter between a value and a JSON-safe form."""

    def to_jsonable(self, value: Any) -> Any: ...
    def from_jsonable(self, form: Any, cls: type) -> Any: ...


# ── walk ──


def _find(tp: type) -> Serializer | None:
    from .runtime import active_serializers  # noqa: PLC0415

    serializers = active_serializers()
    for base in tp.__mro__:
        s = serializers.get(base)
        if s is not None:
            return s
    return None


def _type_tag(tp: type) -> str:
    return f"{tp.__module__}:{tp.__qualname__}"


def _resolve_tag(tag: str) -> type:
    module_name, _, qualname = tag.partition(":")
    module = importlib.import_module(module_name)
    obj: Any = module
    for part in qualname.split("."):
        obj = getattr(obj, part)
    if not isinstance(obj, type):
        raise TypeError(f"serial tag {tag!r} did not resolve to a class")
    return obj


def to_jsonable(value: Any) -> Any:
    """Recursively convert `value` into a JSON-safe structure.

    - Scalars and `None` pass through.
    - A type with a registered serializer is tagged and wrapped.
    - Lists / dicts recurse by element. Tuples are tagged so round-trip
      preserves the tuple/list distinction (plain JSON loses it).
    - Anything else falls through and will raise at `json.dumps` time.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    tp: type = type(value)  # type: ignore[type-arg]
    ser = _find(tp)
    if ser is not None:
        return {_SERIAL_TAG: _type_tag(tp), _VALUE_KEY: ser.to_jsonable(value)}

    if isinstance(value, list):
        items = cast(list[Any], value)
        return [to_jsonable(x) for x in items]
    if isinstance(value, tuple):
        tup = cast(tuple[Any, ...], value)
        return {_SERIAL_TAG: _TUPLE_TAG, _VALUE_KEY: [to_jsonable(x) for x in tup]}
    if isinstance(value, dict):
        mapping = cast(dict[Any, Any], value)
        out: dict[str, Any] = {}
        for k, v in mapping.items():
            out[str(k)] = to_jsonable(v)
        return out
    return value


def from_jsonable(form: Any) -> Any:
    """Inverse of `to_jsonable`. Untagged input is returned as-is."""
    if isinstance(form, list):
        items = cast(list[Any], form)
        return [from_jsonable(x) for x in items]
    if isinstance(form, dict):
        mapping = cast(dict[str, Any], form)
        tag: Any = mapping.get(_SERIAL_TAG)
        if tag == _TUPLE_TAG:
            inner = cast(list[Any], mapping.get(_VALUE_KEY, []))
            return tuple(from_jsonable(x) for x in inner)
        if isinstance(tag, str):
            cls = _resolve_tag(tag)
            ser = _find(cls)
            if ser is None:
                raise TypeError(
                    f"No serializer registered for {tag!r}. "
                    f"Call `runtime.register_serializer({cls.__name__}, ...)` before loading."
                )
            return ser.from_jsonable(mapping[_VALUE_KEY], cls)
        return {k: from_jsonable(v) for k, v in mapping.items()}
    return form


# ── defaults ──


class _PydanticSerializer:
    """Round-trip Pydantic v2 models through their `.model_dump(mode='json')` form."""

    def to_jsonable(self, value: Any) -> Any:
        return value.model_dump(mode="json")  # type: ignore[no-any-return]

    def from_jsonable(self, form: Any, cls: type) -> Any:
        return cls.model_validate(form)  # type: ignore[attr-defined]


def install_defaults(runtime: Any) -> None:
    """Install Pydantic serializer on `runtime` (no-op if pydantic unavailable)."""
    try:
        from pydantic import BaseModel  # noqa: PLC0415
    except ImportError:
        return
    runtime.serializers[BaseModel] = _PydanticSerializer()
