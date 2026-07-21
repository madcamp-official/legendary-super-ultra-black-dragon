from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    TestClient = None

from sqlalchemy import func, select

from dure.control.api import create_app
from dure.control.models import (
    ArtifactCacheEvent,
    ArtifactManifest,
    Node,
    NodeArtifactCache,
    Task,
    utcnow,
)
from dure.control.service import claim_task, finish_task, save_heartbeat
from tests.helpers import profile


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class ArtifactCacheControlAPITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'cache-api.db'}"
        self.client = TestClient(
            create_app(
                database_url=url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.admin = {"Authorization": "Bearer admin-secret"}
        self.cache = {
            "id": "11111111-1111-4111-8111-111111111111",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "cache_kind": "FULL_SNAPSHOT",
            "cache_identity_digest": "sha256:" + "a" * 64,
            "status": "CORRUPT",
        }
        self.references = {"complete": True, "blocking_references": []}

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def _task_count(self) -> int:
        with self.client.app.state.session_factory() as session:
            return session.scalar(select(func.count()).select_from(Task)) or 0

    def test_list_show_and_verify_are_authenticated_and_read_only(self):
        self.assertEqual(
            self.client.get("/v1/admin/artifact-caches").status_code,
            401,
        )
        before = self._task_count()
        verification = {
            "cache": self.cache,
            "references": self.references,
            "eligible_for_quarantine": True,
        }
        with patch(
            "dure.control.api.list_artifact_caches",
            return_value=[self.cache],
        ), patch(
            "dure.control.api.artifact_cache_detail",
            return_value=self.cache,
        ), patch(
            "dure.control.api.verify_artifact_cache",
            return_value=verification,
        ):
            listed = self.client.get(
                "/v1/admin/artifact-caches", headers=self.admin
            )
            shown = self.client.get(
                f"/v1/admin/artifact-caches/{self.cache['id']}",
                headers=self.admin,
            )
            verified = self.client.get(
                f"/v1/admin/artifact-caches/{self.cache['id']}/verify",
                headers=self.admin,
            )

        self.assertEqual(listed.json(), {"caches": [self.cache]})
        self.assertEqual(shown.json(), {"cache": self.cache})
        self.assertEqual(verified.json(), verification)
        self.assertEqual(self._task_count(), before)
        self.assertEqual(
            self.client.post(
                f"/v1/admin/artifact-caches/{self.cache['id']}/verify",
                headers=self.admin,
            ).status_code,
            405,
        )

    def test_quarantine_defaults_to_preview_and_rejects_extra_fields(self):
        path = f"/v1/admin/artifact-caches/{self.cache['id']}/quarantine"
        with patch(
            "dure.control.api.prepare_or_apply_artifact_cache_quarantine",
            return_value=(self.cache, self.references, [], False),
        ) as quarantine:
            response = self.client.post(path, headers=self.admin, json={})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["tasks"], [])
        self.assertFalse(response.json()["changed"])
        quarantine.assert_called_once()
        self.assertFalse(quarantine.call_args.kwargs["apply"])
        for extra in ("path", "url", "command", "env", "docker_args"):
            with self.subTest(extra=extra):
                rejected = self.client.post(
                    path,
                    headers=self.admin,
                    json={"apply": False, extra: "/unsafe"},
                )
                self.assertEqual(rejected.status_code, 422)

    def test_explicit_apply_returns_only_the_dedicated_closed_task(self):
        task = SimpleNamespace(
            id="33333333-3333-4333-8333-333333333333",
            bulk_id="44444444-4444-4444-8444-444444444444",
            node_id=self.cache["node_id"],
            type="QUARANTINE_ARTIFACT_CACHE",
            status="QUEUED",
            deployment_id=None,
            operation_node_id=None,
            operation_attempt=None,
            payload={
                "node_id": self.cache["node_id"],
                "cache_kind": self.cache["cache_kind"],
                "cache_identity_digest": self.cache[
                    "cache_identity_digest"
                ],
            },
            attempts=0,
            lease_until=None,
            result=None,
            error=None,
        )
        with patch(
            "dure.control.api.prepare_or_apply_artifact_cache_quarantine",
            return_value=(self.cache, self.references, [task], True),
        ) as quarantine:
            response = self.client.post(
                f"/v1/admin/artifact-caches/{self.cache['id']}/quarantine",
                headers=self.admin,
                json={"apply": True},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["changed"])
        payload = response.json()["tasks"][0]["payload"]
        self.assertEqual(
            set(payload),
            {"node_id", "cache_kind", "cache_identity_digest"},
        )
        quarantine.assert_called_once()
        self.assertTrue(quarantine.call_args.kwargs["apply"])

    def test_general_task_api_cannot_create_quarantine_work(self):
        response = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [self.cache["node_id"]],
                "type": "QUARANTINE_ARTIFACT_CACHE",
                "options": {},
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("dedicated cache API", response.json()["detail"])


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class ArtifactCacheQuarantineIntegrationTests(unittest.TestCase):
    node_id = "55555555-5555-4555-8555-555555555555"
    cache_id = "66666666-6666-4666-8666-666666666666"
    digest = "sha256:" + "d" * 64

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'cache-integration.db'}"
        self.client = TestClient(
            create_app(
                database_url=url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.admin = {"Authorization": "Bearer admin-secret"}
        with self.client.app.state.session_factory() as session:
            session.add(
                Node(
                    id=self.node_id,
                    install_id="cache-quarantine-node",
                    display_name="cache-node",
                    hostname="cache-node",
                    agent_version="0.3.20",
                    approved=True,
                    last_seen=utcnow(),
                )
            )
            session.add(
                ArtifactManifest(
                    digest=self.digest,
                    schema_version=1,
                    model_artifact_id=None,
                    total_size_bytes=1,
                    file_count=1,
                    chunk_count=1,
                    canonical_json="{}",
                )
            )
            session.flush()
            session.add(
                NodeArtifactCache(
                    id=self.cache_id,
                    node_id=self.node_id,
                    cache_kind="FULL_SNAPSHOT",
                    cache_identity_digest=self.digest,
                    manifest_digest=self.digest,
                    source_manifest_digest=self.digest,
                    status="CORRUPT",
                    reason_code="PROBE_CORRUPT",
                    event_sequence=0,
                )
            )
            session.commit()

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def test_preview_is_read_only_and_apply_finishes_as_quarantined(self):
        path = f"/v1/admin/artifact-caches/{self.cache_id}/quarantine"
        before = self.client.get(
            f"/v1/admin/artifact-caches/{self.cache_id}", headers=self.admin
        ).json()["cache"]
        verified = self.client.get(
            f"/v1/admin/artifact-caches/{self.cache_id}/verify",
            headers=self.admin,
        )

        preview = self.client.post(path, headers=self.admin, json={})

        self.assertEqual(verified.status_code, 200)
        self.assertTrue(verified.json()["eligible_for_quarantine"])
        self.assertEqual(preview.status_code, 200)
        self.assertFalse(preview.json()["changed"])
        self.assertEqual(preview.json()["tasks"], [])
        with self.client.app.state.session_factory() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactCacheEvent)),
                0,
            )
        after = self.client.get(
            f"/v1/admin/artifact-caches/{self.cache_id}", headers=self.admin
        ).json()["cache"]
        self.assertEqual(after, before)

        applied = self.client.post(
            path,
            headers=self.admin,
            json={"apply": True},
        )

        self.assertEqual(applied.status_code, 200)
        self.assertTrue(applied.json()["changed"])
        task_id = applied.json()["tasks"][0]["id"]
        with self.client.app.state.session_factory() as session:
            task = claim_task(session, self.node_id)
            self.assertIsNotNone(task)
            self.assertEqual(task.id, task_id)
            accepted = finish_task(
                session,
                task,
                self.node_id,
                result={
                    "node_id": self.node_id,
                    "cache_kind": "FULL_SNAPSHOT",
                    "cache_identity_digest": self.digest,
                    "status": "QUARANTINED",
                },
                error=None,
            )
            self.assertTrue(accepted)
            cache = session.get(NodeArtifactCache, self.cache_id)
            self.assertEqual(cache.status, "QUARANTINED")
            self.assertEqual(cache.reason_code, "QUARANTINE_SUCCEEDED")

    def test_active_node_work_blocks_apply(self):
        with self.client.app.state.session_factory() as session:
            session.add(
                Task(
                    bulk_id="77777777-7777-4777-8777-777777777777",
                    node_id=self.node_id,
                    type="BENCHMARK",
                    payload={},
                )
            )
            session.commit()

        response = self.client.post(
            f"/v1/admin/artifact-caches/{self.cache_id}/quarantine",
            headers=self.admin,
            json={"apply": True},
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.json()["detail"]["code"],
            "ARTIFACT_CACHE_REFERENCED",
        )

    def test_failed_quarantine_preserves_cache_and_closes_the_request(self):
        applied = self.client.post(
            f"/v1/admin/artifact-caches/{self.cache_id}/quarantine",
            headers=self.admin,
            json={"apply": True},
        )
        self.assertEqual(applied.status_code, 200)

        with self.client.app.state.session_factory() as session:
            task = claim_task(session, self.node_id)
            self.assertIsNotNone(task)
            accepted = finish_task(
                session,
                task,
                self.node_id,
                result=None,
                error="raw local exception text",
            )
            self.assertTrue(accepted)
            cache = session.get(NodeArtifactCache, self.cache_id)
            task = session.get(Task, task.id)
            self.assertEqual(cache.status, "CORRUPT")
            self.assertEqual(cache.reason_code, "PROBE_CORRUPT")
            self.assertIsNone(cache.quarantine_request_id)
            self.assertEqual(
                task.error,
                "CACHE_QUARANTINE_EXECUTION_FAILED",
            )

    def test_only_a_complete_cache_scan_can_report_absence(self):
        incomplete = profile(self.node_id)
        incomplete.artifact_cache_observations = []
        incomplete.artifact_cache_scan_complete = False
        complete = profile(self.node_id)
        complete.artifact_cache_observations = []
        complete.artifact_cache_scan_complete = True

        with self.client.app.state.session_factory() as session:
            node = session.get(Node, self.node_id)
            save_heartbeat(session, node, {}, incomplete.to_dict())
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactCacheEvent)),
                0,
            )
            node = session.get(Node, self.node_id)
            save_heartbeat(session, node, {}, complete.to_dict())
            event = session.scalar(select(ArtifactCacheEvent))
            self.assertIsNotNone(event)
            self.assertEqual(event.reason_code, "PROBE_MISSING")


if __name__ == "__main__":
    unittest.main()
