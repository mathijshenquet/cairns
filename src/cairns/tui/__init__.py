"""Textual TUI for running and browsing Cairn pipelines.

The package is split for clarity:

- `render.py`   — trace → styled Text helpers
- `widgets.py`  — custom widgets (ChoicePanel, ConfirmPanel)
- `messages.py` — Message subclasses posted from the worker thread
- `sinks.py`    — TuiSink (events) and TuiInteractionSink (typed requests)
- `app.py`      — the unified `CairnsApp`

The public surface is `run_app` / `browse` plus the `CairnsApp` class.
"""

from __future__ import annotations

from typing import Any, Callable

from cairns.core import Handle

from .app import CairnsApp


def run_app(
    entry_fn: Callable[..., Handle[Any]],
    store_path: str = ".cairns",
    label: str = "main",
) -> None:
    app = CairnsApp(store_path, entry_fn=entry_fn, label=label)
    app.run()


def browse(store_path: str = ".cairns") -> None:
    app = CairnsApp(store_path)
    app.run()


__all__ = ["CairnsApp", "run_app", "browse"]
