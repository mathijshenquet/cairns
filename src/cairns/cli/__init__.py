"""Cairns CLI.

Usage:
    cairns script.py [ENTRY]            Run a script (default action)
    cairns                              Browse runs interactively
    cairns list                         List all runs
    cairns show [RUN_ID]                Show trace (latest if omitted)
    cairns output PATH                  Show a cached output
    cairns gc [--before DATE]           Garbage collect
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from datetime import datetime, timezone
from typing import Any, Callable


def _store_path(args: argparse.Namespace) -> str:
    return getattr(args, "store", None) or ".cairns"


# ── Commands ──


def cmd_list(args: argparse.Namespace) -> None:
    from cairns.run import show_runs

    show_runs(_store_path(args))


def cmd_show(args: argparse.Namespace) -> None:
    from cairns.run import show_trace

    run_id: str | None = getattr(args, "run_id", None)
    show_trace(_store_path(args), run_id=run_id)


def cmd_output(args: argparse.Namespace) -> None:
    from cairns.run import show_output

    path: str = args.path
    if os.path.islink(path):
        path = str(os.path.realpath(path))
    show_output(path)


def cmd_gc(args: argparse.Namespace) -> None:
    from cairns.run import gc, list_runs

    store = _store_path(args)
    before: datetime | None = None
    if args.before:
        before = datetime.fromisoformat(args.before).replace(tzinfo=timezone.utc)

    keep_latest: bool = args.keep_latest

    # Show current state first
    runs = list_runs(store)
    if runs:
        from cairns.run import show_runs
        show_runs(store)

    removed_runs, removed_outputs = gc(store, before=before, keep_latest=keep_latest)

    if removed_runs:
        print(f"Removed {len(removed_runs)} run(s):")
        for r in removed_runs:
            print(f"  {r}")
    if removed_outputs:
        print(f"Removed {len(removed_outputs)} orphaned output(s)")
    if not removed_runs and not removed_outputs:
        print("Nothing to clean up.")


def cmd_run(script: str, entry_name: str, store: str, *, force: bool = False) -> None:
    from cairns.run import run as cairn_run

    # Load the script as a module
    script_dir = os.path.dirname(os.path.abspath(script))
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)

    spec = importlib.util.spec_from_file_location("__cairn_script__", script)
    if spec is None or spec.loader is None:
        print(f"Error: cannot load {script}", file=sys.stderr)
        sys.exit(1)
    module = importlib.util.module_from_spec(spec)
    sys.modules["__cairn_script__"] = module
    spec.loader.exec_module(module)

    # Find the entry point: try explicit name, then 'main', then script basename
    entry: Callable[..., Any] | None = getattr(module, entry_name, None)
    if entry is None and entry_name == "main":
        basename = os.path.splitext(os.path.basename(script))[0]
        entry = getattr(module, basename, None)
        if entry is not None:
            entry_name = basename
    if entry is None:
        candidates = [
            name for name in dir(module)
            if not name.startswith("_") and callable(getattr(module, name))
        ]
        print(f"Error: {script} has no function '{entry_name}'", file=sys.stderr)
        if candidates:
            print(f"Available functions: {', '.join(candidates)}", file=sys.stderr)
        sys.exit(1)

    # Build label from script path + entry name
    script_rel = os.path.relpath(script)
    script_module = os.path.splitext(script_rel)[0].replace(os.sep, ".")
    label = f"{script_module}:{entry_name}"

    # --force: remove previous runs for this entry point + GC orphaned outputs
    if force:
        from cairns.run import gc_outputs, list_runs, remove_run
        runs = [r for r in list_runs(store) if r.entry_name == label]
        for r in runs:
            remove_run(store, r.run_id)
        removed = gc_outputs(store)
        if runs or removed:
            print(f"Force: removed {len(runs)} run(s), {len(removed)} output(s)", file=sys.stderr)

    try:
        from cairns.tui import run_app
        run_app(entry, store_path=store, label=label)
    except ImportError:
        # Fallback to headless mode
        print(f"Running {script}:{entry_name}", file=sys.stderr)
        print(f"Store: {store}/\n", file=sys.stderr)
        try:
            # Entry is a @step or async function; calling it at top level
            # outside a run yields a deferred Handle that cairn_run consumes.
            result = cairn_run(entry(), store_path=store, label=label)
            print(f"\nResult: {result}", file=sys.stderr)
        except Exception as e:
            print(f"\nError: {e}", file=sys.stderr)
            sys.exit(1)


def cmd_browse(store: str) -> None:
    """Interactive run browser using Textual TUI."""
    try:
        from cairns.tui import browse
        browse(store)
    except ImportError:
        # Fallback: just list runs
        from cairns.run import show_runs
        show_runs(store)
        print("Install cairn[tui] for interactive browsing: uv pip install cairn[tui]")


# ── Main ──


def main() -> None:
    # Quick check: is the first arg a .py file? → run it directly
    if len(sys.argv) >= 2 and not sys.argv[1].startswith("-"):
        first_arg = sys.argv[1]
        if first_arg.endswith(".py") or os.path.isfile(first_arg):
            # cairns script.py [entry] [--store PATH] [--force]
            parser = argparse.ArgumentParser(prog="cairns")
            parser.add_argument("script", help="Python script to run")
            parser.add_argument("entry", nargs="?", default="main", help="Entry point function")
            parser.add_argument("--store", "-s", default=".cairns")
            parser.add_argument("--force", "-f", action="store_true", help="Clear cache for this entry point before running")
            args = parser.parse_args()
            cmd_run(args.script, args.entry, args.store, force=args.force)
            return

    # Otherwise: subcommand mode
    parser = argparse.ArgumentParser(
        prog="cairns",
        description="Compute graph orchestration with caching and observability",
    )
    parser.add_argument("--store", "-s", default=".cairns", help="Store path (default: .cairns)")
    subparsers = parser.add_subparsers(dest="command")

    # cairns list
    subparsers.add_parser("list", help="List all runs")

    # cairns show [RUN_ID]
    p_show = subparsers.add_parser("show", help="Show trace (latest if no run_id)")
    p_show.add_argument("run_id", nargs="?", default=None)

    # cairns output PATH
    p_output = subparsers.add_parser("output", help="Show a cached output")
    p_output.add_argument("path")

    # cairns gc
    p_gc = subparsers.add_parser("gc", help="Garbage collect")
    p_gc.add_argument("--before", help="Remove runs before this ISO date")
    p_gc.add_argument("--keep-latest", action="store_true", default=True)
    p_gc.add_argument("--no-keep-latest", dest="keep_latest", action="store_false")

    args = parser.parse_args()

    commands: dict[str, Callable[[argparse.Namespace], None]] = {
        "list": cmd_list,
        "show": cmd_show,
        "output": cmd_output,
        "gc": cmd_gc,
    }

    if args.command is None:
        # cairns with no args → interactive browser
        cmd_browse(args.store)
        return

    cmd = commands.get(args.command)
    if cmd is None:
        parser.print_help()
        sys.exit(1)
    cmd(args)


if __name__ == "__main__":
    main()
