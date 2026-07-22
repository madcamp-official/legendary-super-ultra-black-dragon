from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROVENANCE_TOOL = REPOSITORY_ROOT / "dure/scripts/release_provenance.py"
SPEC = importlib.util.spec_from_file_location("release_provenance", PROVENANCE_TOOL)
assert SPEC is not None and SPEC.loader is not None
release_provenance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_provenance)


class ReleaseProvenanceTests(unittest.TestCase):
    def _arguments(self, package: Path) -> dict[str, object]:
        return {
            "package": package,
            "version": "0.4.17",
            "tag": "v0.4.17",
            "source_repository": "https://github.com/madcamp-official/legendary-super-ultra-black-dragon",
            "source_commit": "a" * 40,
            "workflow_run_url": (
                "https://github.com/madcamp-official/legendary-super-ultra-black-dragon/actions/runs/123"
            ),
            "signing_key_fingerprint": "E1F952F8B23E7A1B884CB5A33EC5C8CAE53AFA01",
        }

    def test_create_and_verify_manifest_binds_the_exact_debian_package(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "dure_0.4.17_all.deb"
            package.write_bytes(b"official package bytes")
            arguments = self._arguments(package)

            manifest = release_provenance.create_manifest(**arguments)
            manifest_path = root / "release-provenance.json"
            release_provenance.write_manifest(manifest_path, manifest)
            verified = release_provenance.verify_manifest(
                manifest_path=manifest_path, **arguments
            )

        self.assertEqual(verified["release"]["tag"], "v0.4.17")
        self.assertEqual(verified["artifact"]["name"], "dure_0.4.17_all.deb")
        self.assertEqual(verified["apt"]["signing_key_fingerprint"], arguments["signing_key_fingerprint"])

    def test_verify_rejects_a_package_that_does_not_match_the_signed_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "dure_0.4.17_all.deb"
            package.write_bytes(b"official package bytes")
            arguments = self._arguments(package)
            manifest_path = root / "release-provenance.json"
            release_provenance.write_manifest(
                manifest_path, release_provenance.create_manifest(**arguments)
            )
            package.write_bytes(b"substituted package bytes")

            with self.assertRaisesRegex(
                release_provenance.ProvenanceError,
                "does not match the expected release package",
            ):
                release_provenance.verify_manifest(manifest_path=manifest_path, **arguments)

    def test_create_rejects_a_tag_that_does_not_match_the_debian_version(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "dure_0.4.17_all.deb"
            package.write_bytes(b"official package bytes")
            arguments = self._arguments(package)
            arguments["tag"] = "v0.4.18"

            with self.assertRaisesRegex(release_provenance.ProvenanceError, "must exactly match"):
                release_provenance.create_manifest(**arguments)

    def test_verify_rejects_extra_or_missing_claims(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "dure_0.4.17_all.deb"
            package.write_bytes(b"official package bytes")
            arguments = self._arguments(package)
            manifest_path = root / "release-provenance.json"
            manifest = release_provenance.create_manifest(**arguments)
            manifest["untrusted_mirror_note"] = "not part of the signed schema"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(
                release_provenance.ProvenanceError,
                "does not match the expected release package",
            ):
                release_provenance.verify_manifest(manifest_path=manifest_path, **arguments)
