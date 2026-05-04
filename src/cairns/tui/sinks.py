"""Thread-safe adapters bridging the pipeline worker to the Textual app.

Events and interaction requests both originate on the worker thread and
need to cross into the main Textual event loop. `TuiSink` converts core
`Event`s into `PipelineEvent` messages; `TuiInteractionSink` implements
the `InteractionSink` Protocol by posting typed request messages and
awaiting a `concurrent.futures.Future` that the main thread resolves.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import itertools
import time
from typing import TYPE_CHECKING, Any, Mapping, cast

from cairns.core import Event, event_to_dict

from .messages import (
    ChoiceInteractionMessage,
    ConfirmInteractionMessage,
    InputInteractionMessage,
    PipelineEvent,
)

if TYPE_CHECKING:
    from textual.app import App


_widget_ids = itertools.count(1)


def new_widget_id() -> int:
    return next(_widget_ids)


class TuiSink:
    """Event sink that posts events to a Textual app from any thread."""

    def __init__(self, app: App[Any]) -> None:
        self._app = app

    def emit(self, event: Event) -> None:
        event.ts = time.monotonic()
        d = event_to_dict(event)
        self._app.call_from_thread(self._app.post_message, PipelineEvent(d))


class TuiInteractionSink:
    """InteractionSink adapter that routes typed requests to the Textual app.

    The pipeline worker runs in its own thread + asyncio loop; Textual runs
    in the main thread. Each request posts a typed Message carrying a
    `concurrent.futures.Future`; the worker awaits it via `asyncio.wrap_future`,
    and the main thread resolves the future when the user interacts.
    """

    def __init__(self, app: App[Any]) -> None:
        self._app = app

    async def request_input(
        self,
        prompt: str,
        *,
        anchor_span: int | None,
        default: str | None = None,
        placeholder: str | None = None,
    ) -> str:
        fut: concurrent.futures.Future[Any] = concurrent.futures.Future()
        self._app.call_from_thread(
            self._app.post_message,
            InputInteractionMessage(
                new_widget_id(), prompt, default, placeholder, anchor_span, fut,
            ),
        )
        return cast(str, await asyncio.wrap_future(fut))

    async def request_choice(
        self,
        prompt: str,
        options: Mapping[Any, str],
        *,
        anchor_span: int | None,
        default: Any = None,
    ) -> Any:
        fut: concurrent.futures.Future[Any] = concurrent.futures.Future()
        self._app.call_from_thread(
            self._app.post_message,
            ChoiceInteractionMessage(
                new_widget_id(), prompt, options, default, anchor_span, fut,
            ),
        )
        return await asyncio.wrap_future(fut)

    async def request_confirm(
        self,
        prompt: str,
        *,
        anchor_span: int | None,
        default: bool | None = None,
    ) -> bool:
        fut: concurrent.futures.Future[Any] = concurrent.futures.Future()
        self._app.call_from_thread(
            self._app.post_message,
            ConfirmInteractionMessage(
                new_widget_id(), prompt, default, anchor_span, fut,
            ),
        )
        return cast(bool, await asyncio.wrap_future(fut))
