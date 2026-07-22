#!/usr/bin/env python3
"""Check relative Markdown links in the Dure documentation tree."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


_LINK = re.compile(r"!?\[[^\]]*\]\((?P<target><[^>]+>|[^)\s]+)(?:\s+[^)]*)?\)")
_EXTERNAL_PREFIXES = ("#", "http://", "https://", "mailto:", "tel:", "data:")


def documentation_files(root: Path) -> list[Path]:
    """Return Markdown documentation for a repository root or Dure-only fixture.

    The repository keeps public policy documents at its root and the Dure project
    under ``dure/``.  Small test fixtures may instead place their README and docs
    directly under ``root``.
    """

    files = sorted(root.glob("*.md"))
    dure_root = root / "dure"
    if dure_root.is_dir():
        for path in (dure_root / "README.md", dure_root / "CHANGELOG.md"):
            if path.is_file():
                files.append(path)
        docs = dure_root / "docs"
    else:
        docs = root / "docs"
    if docs.is_dir():
        files.extend(sorted(docs.rglob("*.md")))
    return files


def check_relative_links(root: Path) -> list[str]:
    """Return one message for every missing relative Markdown link or image."""

    errors: list[str] = []
    for document in documentation_files(root):
        text = document.read_text(encoding="utf-8")
        for match in _LINK.finditer(text):
            target = match.group("target").strip("<>")
            if not target or target.startswith(_EXTERNAL_PREFIXES):
                continue
            target = target.split("#", maxsplit=1)[0]
            if not target:
                continue
            if not (document.parent / target).exists():
                line = text.count("\n", 0, match.start()) + 1
                errors.append(
                    f"{document.relative_to(root)}:{line}: missing relative link {target}"
                )
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="repository root containing root policy documents and dure/docs/",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()
    errors = check_relative_links(root)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"documentation links: OK ({len(documentation_files(root))} Markdown files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
