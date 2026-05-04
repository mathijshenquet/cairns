"""Textual Message subclasses posted from the pipeline worker thread.

Three kinds:
- Pipeline lifecycle: `PipelineEvent` (each trace-log event), `PipelineDone`.
- Interaction requests: one Message per widget kind (input / choice / confirm),
  each carrying the args needed to mount a widget plus a concurrent.futures.Future
  that the main thread resolves on user interaction.
"""

from __future__ import annotations

import concurrent.futures
from typing import Any, Mapping

from textual.message import Message


class PipelineEvent(Message):
    """An event from the running pipeline, posted from the worker thread."""

    def __init__(self, event_dict: dict[str, Any]) -> None:
        super().__init__()
        self.event_dict = event_dict


class PipelineDone(Message):
    def __init__(self, result: Any = None, error: str | None = None) -> None:
        super().__init__()
        self.result = result
        self.error = error


class InputInteractionMessage(Message):
    """Pipeline is asking for free-form text."""

    def __init__(
        self,
        widget_id: int,
        prompt: str,
        default: str | None,
        placeholder: str | None,
        span_id: int | None,
        fut: concurrent.futures.Future[Any],
    ) -> None:
        super().__init__()
        self.widget_id = widget_id
        self.prompt = prompt
        self.default = default
        self.placeholder = placeholder
        self.span_id = span_id
        self.fut = fut


class ChoiceInteractionMessage(Message):
    """Pipeline is asking for a pick from named options."""

    def __init__(
        self,
        widget_id: int,
        prompt: str,
        options: Mapping[Any, str],
        default: Any,
        span_id: int | None,
        fut: concurrent.futures.Future[Any],
    ) -> None:
        super().__init__()
        self.widget_id = widget_id
        self.prompt = prompt
        self.options = options
        self.default = default
        self.span_id = span_id
        self.fut = fut


class ConfirmInteractionMessage(Message):
    """Pipeline is asking a yes/no question."""

    def __init__(
        self,
        widget_id: int,
        prompt: str,
        default: bool | None,
        span_id: int | None,
        fut: concurrent.futures.Future[Any],
    ) -> None:
        super().__init__()
        self.widget_id = widget_id
        self.prompt = prompt
        self.default = default
        self.span_id = span_id
        self.fut = fut
