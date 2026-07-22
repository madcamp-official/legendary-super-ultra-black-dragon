from __future__ import annotations

import importlib.util
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
