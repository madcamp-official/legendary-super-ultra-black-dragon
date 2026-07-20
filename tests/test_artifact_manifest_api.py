from __future__ import annotations

import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import func, select

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    TestClient = None

from dure.control.api import create_app
from dure.control.models import (
    ArtifactChunk,
    ArtifactFileChunk,
    ArtifactManifest,
    ArtifactManifestFile,
    AuditEvent,
    Deployment,
    Task,
)
from dure.control.service import (
    ArtifactManifestConflictError,
    canonical_artifact_manifest_digest,
    create_model_artifact,
)


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _manifest() -> dict:
    return {
        "schema_version": 1,
        "files": [
            {
                "path": "weights/model.safetensors",
                "kind": "REGULAR",
                "size_bytes": 4,
                "sha256": _digest("d"),
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": 2,
                        "sha256": _digest("a"),
                    },
                    {
                        "ordinal": 1,
                        "offset_bytes": 2,
                        "length_bytes": 2,
                        "sha256": _digest("b"),
                    },
                ],
            },
            {
                "path": "config.json",
                "kind": "REGULAR",
                "size_bytes": 2,
                "sha256": _digest("c"),
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": 2,
                        "sha256": _digest("a"),
                    }
                ],
            },
        ],
    }


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class ArtifactManifestAPITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        database_url = (
            f"sqlite:///{Path(self.temporary.name) / 'artifact-manifest-api.db'}"
        )
        self.client = TestClient(
            create_app(
                database_url=database_url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.admin = {"Authorization": "Bearer admin-secret"}
        self.factory = self.client.app.state.session_factory

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def _artifact(self, *, manifest_digest: str | None = None):
        manifest = _manifest()
        with self.factory() as session:
            artifact = create_model_artifact(
                session,
                model_id="api-manifest-model",
                repository=f"Example/Manifest-{uuid.uuid4()}",
                revision="1" * 40,
                manifest_digest=(
                    manifest_digest
                    if manifest_digest is not None
                    else canonical_artifact_manifest_digest(manifest)
                ),
                quantization="awq",
                size_mib=1,
                default_max_model_len=1024,
                layer_count=1,
                license_id="apache-2.0",
            )
            return artifact.id, manifest

    def _manifest_counts(self) -> dict[str, int]:
        with self.factory() as session:
            return {
                model.__tablename__: session.scalar(
                    select(func.count()).select_from(model)
                )
                for model in (
                    ArtifactManifest,
                    ArtifactManifestFile,
                    ArtifactChunk,
                    ArtifactFileChunk,
                    Deployment,
                    Task,
                    AuditEvent,
                )
            }

    def test_register_is_canonical_idempotent_and_has_no_execution_side_effects(self):
        artifact_id, manifest = self._artifact()
        endpoint = f"/v1/admin/model-artifacts/{artifact_id}/manifest"
        before = self._manifest_counts()

        first = self.client.post(endpoint, headers=self.admin, json=manifest)

        self.assertEqual(first.status_code, 200, first.text)
        first_value = first.json()
        self.assertTrue(first_value["created"])
        self.assertEqual(
            first_value["manifest"]["digest"],
            canonical_artifact_manifest_digest(manifest),
        )
        self.assertEqual(first_value["manifest"]["total_size_bytes"], 6)
        self.assertEqual(first_value["manifest"]["file_count"], 2)
        self.assertEqual(first_value["manifest"]["chunk_count"], 3)
        self.assertEqual(
            [item["path"] for item in first_value["manifest"]["files"]],
            ["config.json", "weights/model.safetensors"],
        )
        after_first = self._manifest_counts()
        self.assertEqual(after_first["artifact_manifests"], 1)
        self.assertEqual(after_first["artifact_manifest_files"], 2)
        self.assertEqual(after_first["artifact_chunks"], 2)
        self.assertEqual(after_first["artifact_file_chunks"], 3)
        for unchanged in ("deployments", "tasks", "audit_events"):
            self.assertEqual(after_first[unchanged], before[unchanged])

        reordered = {
            "schema_version": 1,
            "files": list(reversed(manifest["files"])),
        }
        reordered["files"][1] = {
            **reordered["files"][1],
            "chunks": list(reversed(reordered["files"][1]["chunks"])),
        }
        second = self.client.post(endpoint, headers=self.admin, json=reordered)

        self.assertEqual(second.status_code, 200, second.text)
        self.assertFalse(second.json()["created"])
        self.assertEqual(second.json()["manifest"], first_value["manifest"])
        self.assertEqual(self._manifest_counts(), after_first)

        shown = self.client.get(endpoint, headers=self.admin)
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(shown.json()["manifest"], first_value["manifest"])
        self.assertEqual(self._manifest_counts(), after_first)

    def test_auth_unknown_fields_and_unknown_artifacts_fail_without_rows(self):
        artifact_id, manifest = self._artifact()
        endpoint = f"/v1/admin/model-artifacts/{artifact_id}/manifest"
        before = self._manifest_counts()

        self.assertEqual(self.client.post(endpoint, json=manifest).status_code, 401)
        secret = "manifest-origin-SUPERSECRET"
        invalid = {
            **manifest,
            "files": [
                {
                    **manifest["files"][0],
                    "origin_url": f"https://{secret}@example.invalid",
                    secret: "must-not-be-reflected",
                }
            ],
        }
        invalid_response = self.client.post(
            endpoint,
            headers=self.admin,
            json=invalid,
        )
        self.assertEqual(invalid_response.status_code, 422)
        self.assertNotIn(secret, invalid_response.text)
        self.assertEqual(
            invalid_response.json()["detail"],
            [
                {
                    "type": "request_validation",
                    "loc": ["request"],
                    "msg": "Request does not match the closed schema",
                }
            ],
        )
        unknown_endpoint = (
            f"/v1/admin/model-artifacts/{uuid.uuid4()}/manifest"
        )
        self.assertEqual(
            self.client.post(
                unknown_endpoint,
                headers=self.admin,
                json=manifest,
            ).status_code,
            404,
        )
        self.assertEqual(
            self.client.get(unknown_endpoint, headers=self.admin).status_code,
            404,
        )
        self.assertEqual(self._manifest_counts(), before)

    def test_openapi_keeps_the_closed_manifest_request_schema(self):
        document = self.client.get("/openapi.json").json()
        request_schema = document["paths"][
            "/v1/admin/model-artifacts/{artifact_id}/manifest"
        ]["post"]["requestBody"]["content"]["application/json"]["schema"]

        self.assertEqual(
            request_schema,
            {"$ref": "#/components/schemas/ArtifactManifestCreate"},
        )
        manifest_schema = document["components"]["schemas"][
            "ArtifactManifestCreate"
        ]
        self.assertFalse(manifest_schema["additionalProperties"])
        self.assertEqual(
            set(manifest_schema["required"]),
            {"schema_version", "files"},
        )
        self.assertEqual(
            document["paths"][
                "/v1/admin/model-artifacts/{artifact_id}/manifest"
            ]["post"]["responses"]["422"]["content"]["application/json"][
                "schema"
            ],
            {"$ref": "#/components/schemas/HTTPValidationError"},
        )

    def test_readback_failure_rolls_back_the_uncommitted_registration(self):
        artifact_id, manifest = self._artifact()
        endpoint = f"/v1/admin/model-artifacts/{artifact_id}/manifest"
        before = self._manifest_counts()

        with patch(
            "dure.control.api.artifact_manifest_dict",
            side_effect=ArtifactManifestConflictError(
                "stored artifact manifest is internally inconsistent"
            ),
        ):
            response = self.client.post(
                endpoint,
                headers=self.admin,
                json=manifest,
            )

        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self._manifest_counts(), before)

    def test_digest_mismatch_is_a_conflict_and_stores_nothing(self):
        artifact_id, manifest = self._artifact(manifest_digest=_digest("f"))
        endpoint = f"/v1/admin/model-artifacts/{artifact_id}/manifest"
        before = self._manifest_counts()

        response = self.client.post(endpoint, headers=self.admin, json=manifest)

        self.assertEqual(response.status_code, 409, response.text)
        self.assertIn("does not match", response.json()["detail"])
        self.assertEqual(self._manifest_counts(), before)


if __name__ == "__main__":
    unittest.main()
