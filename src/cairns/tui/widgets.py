"""Custom widgets for interactive requests (choice / confirm panels)."""

from __future__ import annotations

from typing import Any, Mapping

from rich.text import Text
from textual import events
from textual.app import ComposeResult
from textual.containers import Container, Horizontal
from textual.message import Message
from textual.widgets import Static


class ChoicePanel(Container):
    """Side-by-side option panels with keyboard selection.

    Digit keys 1..N pick by position. Letter keys pick by first-char match
    on `str(key)`. Enter picks `default` if set.
    """

    can_focus = True

    DEFAULT_CSS = """
    ChoicePanel { height: auto; }
    ChoicePanel > Horizontal { height: auto; }
    ChoicePanel .choice-panel {
        border: round $surface-lighten-2;
        padding: 0 1;
        width: 1fr;
        height: auto;
    }
    ChoicePanel .choice-panel.-default {
        border: round $accent;
    }
    """

    class Chosen(Message):
        def __init__(self, panel: ChoicePanel, key: Any) -> None:
            super().__init__()
            self.panel = panel
            self.key = key

        @property
        def control(self) -> ChoicePanel:
            return self.panel

    def __init__(
        self,
        prompt: str,
        options: Mapping[Any, str],
        default: Any,
        widget_id_num: int,
    ) -> None:
        super().__init__(id=f"choice-{widget_id_num}")
        self._prompt = prompt
        self._options = dict(options)
        self._default = default
        self._keys = list(self._options.keys())

    def compose(self) -> ComposeResult:
        yield Static(Text(self._prompt, style="bold"))
        with Horizontal():
            for i, (k, v) in enumerate(self._options.items(), start=1):
                header = Text()
                header.append(f"[{i}] ", style="bold cyan")
                header.append(str(k), style="bold")
                if self._default is not None and k == self._default:
                    header.append("  (default)", style="dim")
                header.append("\n\n")
                header.append(v)
                classes = "choice-panel"
                if self._default is not None and k == self._default:
                    classes += " -default"
                yield Static(header, classes=classes)

    def on_key(self, event: events.Key) -> None:
        key = event.key
        if key.isdigit():
            i = int(key) - 1
            if 0 <= i < len(self._keys):
                self.post_message(self.Chosen(self, self._keys[i]))
                event.stop()
                return
        if key == "enter" and self._default is not None:
            self.post_message(self.Chosen(self, self._default))
            event.stop()
            return
        for k in self._keys:
            ks = str(k).lower()
            if ks and ks[0] == key.lower():
                self.post_message(self.Chosen(self, k))
                event.stop()
                return


class ConfirmPanel(Container):
    """Yes/No panel. y/Y → True, n/N → False, Enter → default."""

    can_focus = True

    DEFAULT_CSS = """
    ConfirmPanel { height: auto; }
    """

    class Answered(Message):
        def __init__(self, panel: ConfirmPanel, value: bool) -> None:
            super().__init__()
            self.panel = panel
            self.value = value

        @property
        def control(self) -> ConfirmPanel:
            return self.panel

    def __init__(
        self,
        prompt: str,
        default: bool | None,
        widget_id_num: int,
    ) -> None:
        super().__init__(id=f"confirm-{widget_id_num}")
        self._prompt = prompt
        self._default = default

    def compose(self) -> ComposeResult:
        hint = "[y/n]"
        if self._default is True:
            hint = "[Y/n]"
        elif self._default is False:
            hint = "[y/N]"
        yield Static(Text.assemble((self._prompt, "bold"), "  ", (hint, "dim")))

    def on_key(self, event: events.Key) -> None:
        k = event.key.lower()
        if k == "y":
            self.post_message(self.Answered(self, True))
            event.stop()
        elif k == "n":
            self.post_message(self.Answered(self, False))
            event.stop()
        elif k == "enter" and self._default is not None:
            self.post_message(self.Answered(self, self._default))
            event.stop()
