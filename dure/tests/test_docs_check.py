from __future__ import annotations

import importlib.util
import datetime as dt
import tempfile
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPOSITORY_ROOT / "scripts" / "check_docs.py"
SPEC = importlib.util.spec_from_file_location("dure_docs_check", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
DOCS_CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DOCS_CHECK)


class DocumentationLinkCheckTests(unittest.TestCase):
    def test_accepts_existing_relative_markdown_and_image_links(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "docs" / "assets").mkdir(parents=True)
            (root / "docs" / "guide.md").write_text("# Guide\n", encoding="utf-8")
            (root / "docs" / "assets" / "diagram.png").write_bytes(b"png")
            (root / "README.md").write_text(
                "[guide](docs/guide.md)\n![diagram](docs/assets/diagram.png)\n",
                encoding="utf-8",
            )

            self.assertEqual(DOCS_CHECK.check_relative_links(root), [])

    def test_reports_a_missing_relative_link(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "docs").mkdir()
            (root / "README.md").write_text(
                "[missing](docs/missing.md)\n", encoding="utf-8"
            )

            errors = DOCS_CHECK.check_relative_links(root)

        self.assertEqual(len(errors), 1)
        self.assertIn("README.md:1", errors[0])
        self.assertIn("docs/missing.md", errors[0])

    def test_checks_root_policy_and_nested_dure_documentation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "dure" / "docs").mkdir(parents=True)
            (root / "README.md").write_text("# Root\n", encoding="utf-8")
            (root / "GOVERNANCE.md").write_text(
                "[missing](missing.md)\n", encoding="utf-8"
            )
            (root / "dure" / "README.md").write_text(
                "[guide](docs/guide.md)\n", encoding="utf-8"
            )
            (root / "dure" / "docs" / "guide.md").write_text(
                "# Guide\n", encoding="utf-8"
            )

            errors = DOCS_CHECK.check_relative_links(root)

        self.assertEqual(len(errors), 1)
        self.assertIn("GOVERNANCE.md:1", errors[0])
        self.assertIn("missing.md", errors[0])

    def test_reports_missing_anchor_and_duplicate_heading(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "docs").mkdir()
            (root / "README.md").write_text(
                "[missing](docs/guide.md#nope)\n", encoding="utf-8"
            )
            (root / "docs" / "guide.md").write_text(
                "# Same\n\n## Same\n", encoding="utf-8"
            )

            errors = DOCS_CHECK.check_relative_links(root) + DOCS_CHECK.check_duplicate_headings(root)

        self.assertEqual(len(errors), 2)
        self.assertIn("missing anchor nope", errors[0])
        self.assertIn("duplicate heading anchor same", errors[1])

    def test_reports_stale_explicit_document_date(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "README.md").write_text(
                "# Guide\n\n기준일: 2026-01-01\n", encoding="utf-8"
            )

            errors = DOCS_CHECK.check_document_freshness(
                root, today=dt.date(2026, 7, 22), max_age_days=90
            )

        self.assertEqual(len(errors), 1)
        self.assertIn("older than 90 days", errors[0])
