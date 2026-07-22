#!/usr/bin/env python3
"""Check relative Markdown links in the Dure documentation tree."""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path
from urllib.parse import unquote


_LINK = re.compile(r"!?\[[^\]]*\]\((?P<target><[^>]+>|[^)\s]+)(?:\s+[^)]*)?\)")
_EXTERNAL_PREFIXES = ("http://", "https://", "mailto:", "tel:", "data:")
_HEADING = re.compile(r"^#{1,6}\s+(?P<title>.+?)\s*#*\s*$", re.MULTILINE)
_DATED_DOCUMENT = re.compile(r"(?:기준일|작성일):\s*(?P<date>\d{4}-\d{2}-\d{2})")
_PACKAGE_VERSION = re.compile(r'^version\s*=\s*"(?P<version>[^"\s]+)"\s*$', re.MULTILINE)


def _dure_root(root: Path) -> Path:
    """Return the Dure project directory for a repository or fixture root."""

    return root / "dure" if (root / "dure").is_dir() else root


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


def _slug(title: str) -> str:
    """Return the conservative GitHub-style fragment used by Dure documents."""

    title = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", title)
    title = re.sub(r"[`*_~]", "", title).lower()
    title = re.sub(r"[^0-9a-z가-힣\s-]", "", title)
    return re.sub(r"[\s-]+", "-", title).strip("-")


def document_anchors(document: Path) -> tuple[set[str], list[str]]:
    """Return unique anchors and duplicate heading slugs for one document."""

    counts: dict[str, int] = {}
    duplicates: list[str] = []
    for match in _HEADING.finditer(document.read_text(encoding="utf-8")):
        slug = _slug(match.group("title"))
        if not slug:
            continue
        if slug in counts:
            duplicates.append(slug)
        counts[slug] = counts.get(slug, 0) + 1
    return set(counts), duplicates


def _line(document: Path, offset: int) -> int:
    return document.read_text(encoding="utf-8").count("\n", 0, offset) + 1


def check_relative_links(root: Path) -> list[str]:
    """Return errors for missing relative files and Markdown fragments."""

    errors: list[str] = []
    for document in documentation_files(root):
        text = document.read_text(encoding="utf-8")
        for match in _LINK.finditer(text):
            target = match.group("target").strip("<>")
            if not target or target.startswith(_EXTERNAL_PREFIXES):
                continue
            path_target, separator, fragment = target.partition("#")
            linked_path = document if not path_target else document.parent / path_target
            if not linked_path.exists():
                line = text.count("\n", 0, match.start()) + 1
                errors.append(
                    f"{document.relative_to(root)}:{line}: missing relative link {path_target}"
                )
                continue
            if separator and fragment and linked_path.suffix == ".md":
                anchors, _ = document_anchors(linked_path)
                anchor = _slug(unquote(fragment))
                if anchor not in anchors:
                    line = text.count("\n", 0, match.start()) + 1
                    errors.append(
                        f"{document.relative_to(root)}:{line}: missing anchor "
                        f"{fragment} in {linked_path.relative_to(root)}"
                    )
    return errors


def check_duplicate_headings(root: Path) -> list[str]:
    """Return errors for duplicate heading slugs in any checked Markdown file."""

    errors: list[str] = []
    for document in documentation_files(root):
        _, duplicates = document_anchors(document)
        for slug in duplicates:
            errors.append(f"{document.relative_to(root)}: duplicate heading anchor {slug}")
    return errors


def check_document_index(root: Path) -> list[str]:
    """Require the Dure index to link each top-level Dure documentation file."""

    docs_root = root / "dure" / "docs" if (root / "dure").is_dir() else root / "docs"
    index = docs_root / "README.md"
    if not index.is_file():
        return []
    linked = {
        Path(match.group("target").strip("<>").partition("#")[0]).as_posix()
        for match in _LINK.finditer(index.read_text(encoding="utf-8"))
    }
    errors: list[str] = []
    for document in sorted(docs_root.glob("*.md")):
        if document.name == "README.md":
            continue
        if document.name not in linked:
            errors.append(
                f"{index.relative_to(root)}: missing top-level document index entry {document.name}"
            )
    return errors


def check_document_freshness(
    root: Path, *, today: dt.date, max_age_days: int
) -> list[str]:
    """Flag explicitly dated documents whose stated review date is too old."""

    errors: list[str] = []
    for document in documentation_files(root):
        text = document.read_text(encoding="utf-8")
        for match in _DATED_DOCUMENT.finditer(text):
            recorded = dt.date.fromisoformat(match.group("date"))
            if (today - recorded).days > max_age_days:
                errors.append(
                    f"{document.relative_to(root)}:{_line(document, match.start())}: "
                    f"document date {recorded.isoformat()} is older than {max_age_days} days"
                )
    return errors


def check_release_documentation_contract(root: Path) -> list[str]:
    """Require the current package version to have aligned release documentation."""

    dure_root = _dure_root(root)
    pyproject = dure_root / "pyproject.toml"
    if not pyproject.is_file():
        return []
    match = _PACKAGE_VERSION.search(pyproject.read_text(encoding="utf-8"))
    if match is None:
        return [f"{pyproject.relative_to(root)}: missing project version"]
    version = match.group("version")
    evidence = dure_root / "docs" / "release-evidence" / f"v{version}.md"
    errors: list[str] = []
    if not evidence.is_file():
        errors.append(
            f"{pyproject.relative_to(root)}: missing current release evidence v{version}.md"
        )
    else:
        evidence_text = evidence.read_text(encoding="utf-8")
        if f"# v{version} 수용 증적" not in evidence_text:
            errors.append(
                f"{evidence.relative_to(root)}: heading does not identify v{version}"
            )
        if not re.search(r"`(?:PASSED|FAILED|NOT_RUN)`", evidence_text):
            errors.append(
                f"{evidence.relative_to(root)}: missing closed evidence status"
            )

    expected = (
        (dure_root / "CHANGELOG.md", f"## {version} — 소스 기준선"),
        (dure_root / "docs" / "roadmap.md", f"현재 릴리스 메타데이터: `{version}`"),
        (
            dure_root / "docs" / "release-evidence" / "README.md",
            f"현재 source metadata `{version}`",
        ),
    )
    for document, required_text in expected:
        if not document.is_file() or required_text not in document.read_text(encoding="utf-8"):
            errors.append(
                f"{document.relative_to(root)}: current release documentation must reference {version}"
            )
    return errors


def check_bootstrap_cli_contract(root: Path) -> list[str]:
    """Keep the Docker restart compatibility flag aligned with the CLI source."""

    dure_root = _dure_root(root)
    source = dure_root / "src" / "dure" / "cli.py"
    document = dure_root / "docs" / "cli-reference.md"
    if not source.is_file() or not document.is_file():
        return []
    source_text = source.read_text(encoding="utf-8")
    document_text = document.read_text(encoding="utf-8")
    cli_contract = "Compatibility flag; --apply already includes the required Docker restart"
    documentation_contract = "`--allow-docker-restart`는 이전 자동화와의 호환 플래그"
    if cli_contract in source_text and documentation_contract not in document_text:
        return [
            f"{document.relative_to(root)}: bootstrap Docker restart documentation "
            "does not match the CLI compatibility flag"
        ]
    return []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="repository root containing root policy documents and dure/docs/",
    )
    parser.add_argument(
        "--max-document-age-days",
        type=int,
        default=90,
        help="maximum age for an explicit 기준일/작성일 before the check fails",
    )
    parser.add_argument(
        "--today",
        type=dt.date.fromisoformat,
        default=dt.date.today(),
        help="override today as YYYY-MM-DD for deterministic checks",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()
    errors = (
        check_relative_links(root)
        + check_duplicate_headings(root)
        + check_document_index(root)
        + check_document_freshness(
            root, today=args.today, max_age_days=args.max_document_age_days
        )
        + check_release_documentation_contract(root)
        + check_bootstrap_cli_contract(root)
    )
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"documentation links: OK ({len(documentation_files(root))} Markdown files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
