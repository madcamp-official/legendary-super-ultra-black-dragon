from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    TestClient = None

from dure.control.api import create_app


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class ArtifactChunkOriginAPITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.chunk_root = root / "chunks"
        self.chunk_root.mkdir(mode=0o750)
        self.content = b"canonical-model-chunk"
        self.digest = hashlib.sha256(self.content).hexdigest()
        target = self.chunk_root / self.digest
        target.write_bytes(self.content)
        target.chmod(0o640)
        self.client = TestClient(
            create_app(
                database_url=f"sqlite:///{root / 'origin.db'}",
                admin_token="admin-secret",
                create_schema=True,
                artifact_chunk_root=self.chunk_root,
            )
        )

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def test_full_head_and_resume_are_exact_and_public(self):
        path = f"/chunks/sha256/{self.digest}"

        full = self.client.get(path, headers={"Accept-Encoding": "identity"})
        head = self.client.head(path, headers={"Accept-Encoding": "identity"})
        resumed = self.client.get(
            path,
            headers={"Accept-Encoding": "identity", "Range": "bytes=5-"},
        )

        self.assertEqual(full.status_code, 200)
        self.assertEqual(full.content, self.content)
        self.assertEqual(full.headers["content-length"], str(len(self.content)))
        self.assertEqual(full.headers["accept-ranges"], "bytes")
        self.assertEqual(
            full.headers["cache-control"], "public, max-age=31536000, immutable"
        )
        self.assertEqual(full.headers["x-content-type-options"], "nosniff")
        self.assertEqual(head.status_code, 200)
        self.assertEqual(head.content, b"")
        self.assertEqual(head.headers["content-length"], str(len(self.content)))
        self.assertEqual(resumed.status_code, 206)
        self.assertEqual(resumed.content, self.content[5:])
        self.assertEqual(
            resumed.headers["content-range"],
            f"bytes 5-{len(self.content) - 1}/{len(self.content)}",
        )

    def test_unknown_invalid_range_and_unsafe_file_fail_closed(self):
        path = f"/chunks/sha256/{self.digest}"

        self.assertEqual(self.client.get("/chunks/sha256/not-a-digest").status_code, 404)
        invalid = self.client.get(path, headers={"Range": "bytes=0-1"})
        beyond = self.client.get(path, headers={"Range": "bytes=999-"})
        self.assertEqual(invalid.status_code, 416)
        self.assertEqual(beyond.status_code, 416)

        target = self.chunk_root / self.digest
        target.unlink()
        os.symlink("missing", target)
        self.assertEqual(self.client.get(path).status_code, 404)
