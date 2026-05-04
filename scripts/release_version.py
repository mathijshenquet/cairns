#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["packaging>=24.0"]
# ///
"""Manage the cairns release version pinned in pyproject.toml.

Subcommands:
  print-next [--level=patch|minor|major|rc]
      Print the next release version. If the workspace version is already
      ahead of the latest v* tag, returns the workspace version (lets a
      manual minor/major bump take effect on the next push). Otherwise
      bumps the latest tag at the requested level.

  set <version>
      Write <version> into [project].version.

  bump <patch|minor|major|rc>
      Bump the workspace version at the given level and write it.
"""
from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

from packaging.version import InvalidVersion, Version

ROOT = pathlib.Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
PROJECT_SECTION = re.compile(r"(?ms)^\[project\]\s*\n(.*?)(?=^\[|\Z)")
VERSION_LINE = re.compile(r'(?m)^version = "([^"]+)"$')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_next = sub.add_parser("print-next")
    p_next.add_argument(
        "--level", choices=["patch", "minor", "major", "rc"], default="patch"
    )

    p_set = sub.add_parser("set")
    p_set.add_argument("version")

    p_bump = sub.add_parser("bump")
    p_bump.add_argument("level", choices=["patch", "minor", "major", "rc"])

    args = parser.parse_args()

    if args.cmd == "print-next":
        print(compute_next_version(args.level))
        return 0
    if args.cmd == "set":
        Version(args.version)
        write_version(args.version)
        return 0
    if args.cmd == "bump":
        new = bump_version(read_workspace_version(), args.level)
        write_version(new)
        print(new)
        return 0
    return 1


def compute_next_version(level: str) -> str:
    workspace = Version(read_workspace_version())
    latest = read_latest_tag_version()
    if latest is None or workspace > latest:
        return str(workspace)
    return bump_version(str(latest), level)


def bump_version(version: str, level: str) -> str:
    v = Version(version)
    major, minor, micro = v.major, v.minor, v.micro

    if level == "major":
        return f"{major + 1}.0.0"
    if level == "minor":
        return f"{major}.{minor + 1}.0"
    if level == "patch":
        if v.is_prerelease or v.is_devrelease:
            return f"{major}.{minor}.{micro}"
        return f"{major}.{minor}.{micro + 1}"
    if level == "rc":
        if v.pre and v.pre[0] == "rc":
            return f"{major}.{minor}.{micro}rc{v.pre[1] + 1}"
        return f"{major}.{minor}.{micro + 1}rc1"
    raise ValueError(f"unknown level: {level}")


def read_workspace_version() -> str:
    text = PYPROJECT.read_text()
    section = PROJECT_SECTION.search(text)
    if not section:
        raise SystemExit("no [project] section in pyproject.toml")
    match = VERSION_LINE.search(section.group(1))
    if not match:
        raise SystemExit("no version field in [project] section")
    return match.group(1)


def write_version(version: str) -> None:
    text = PYPROJECT.read_text()

    def replace_in_section(match: re.Match[str]) -> str:
        body = match.group(1)
        new_body, count = VERSION_LINE.subn(
            f'version = "{version}"', body, count=1
        )
        if count != 1:
            raise SystemExit("failed to update version in [project]")
        return f"[project]\n{new_body}"

    new_text, count = PROJECT_SECTION.subn(replace_in_section, text, count=1)
    if count != 1:
        raise SystemExit("could not locate [project] section")
    PYPROJECT.write_text(new_text)


def read_latest_tag_version() -> Version | None:
    result = subprocess.run(
        ["git", "tag", "--list", "v*", "--sort=-version:refname"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    for line in result.stdout.splitlines():
        tag = line.strip()
        if not tag:
            continue
        try:
            return Version(tag.removeprefix("v"))
        except InvalidVersion:
            continue
    return None


if __name__ == "__main__":
    raise SystemExit(main())
