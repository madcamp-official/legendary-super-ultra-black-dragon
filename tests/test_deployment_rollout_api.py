from __future__ import annotations

import tempfile
import unittest
import uuid
from datetime import timedelta
from pathlib import Path

from sqlalchemy import func, select

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    TestClient = None

from dure.control.api import create_app
from dure.control.models import (
    Deployment,
    DeploymentOperation,
    DeploymentOperationNode,
    Node,
    NodeCredential,
    Task,
    utcnow,
)
from dure.control.service import secret_hash


NODE_ID = "6a8c4f83-3d37-4fd6-a0a0-c3bf18a44aa1"
IMAGE = "registry.example/vllm@sha256:" + "a" * 64


def _plan(deployment_id: str, generation: int) -> dict:
    return {
        "deployment_id": deployment_id,
        "generation": generation,
        "image": IMAGE,
        "pipeline_parallel_size": 1,
        "tensor_parallel_size": 1,
        "ray_head_node_id": NODE_ID,
        "ray_head_address": "10.10.10.1:6379",
        "network_interface": "eth0",
        "assignments": [
            {
                "node_id": NODE_ID,
                "gpu_index": 0,
                "rank": 0,
                "pipeline_rank": 0,
                "layer_start": 0,
                "layer_end": 31,
                "role": "ray-head",
            }
        ],
    }


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class DeploymentRolloutAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'rollout-api.db'}"
        self.app = create_app(
            database_url=url,
            admin_token="admin-secret",
            create_schema=True,
        )
        self.client = TestClient(self.app)
        self.admin = {"Authorization": "Bearer admin-secret"}
        self.factory = self.app.state.session_factory
        self.engine = self.factory.kw["bind"]

    def tearDown(self) -> None:
        self.client.close()
        self.engine.dispose()
        self.temporary.cleanup()

    def _lineage(self, *, target_verified: bool = True) -> dict:
        target_id = str(uuid.uuid4())
        source_id = str(uuid.uuid4())
        target_plan = _plan(target_id, 1)
        source_plan = _plan(source_id, 2)
        with self.factory() as session:
            session.add(
                Node(
                    id=NODE_ID,
                    install_id="install-rollout-api",
                    display_name="rollout-api",
                    hostname="rollout-api",
                    agent_version="0.3.12",
                    approved=True,
                    last_seen=utcnow(),
                )
            )
            session.add(
                NodeCredential(
                    node_id=NODE_ID,
                    credential_hash=secret_hash("node-secret"),
                )
            )
            session.add_all(
                [
                    Deployment(
                        id=target_id,
                        lineage_id=target_id,
                        generation=1,
                        plan=target_plan,
                        accept_model_download=False,
                        pull_image=False,
                        status="VERIFIED",
                        verified_at=(
                            utcnow() - timedelta(hours=1)
                            if target_verified
                            else None
                        ),
                    ),
                    Deployment(
                        id=source_id,
                        lineage_id=target_id,
                        previous_generation_id=target_id,
                        generation=2,
                        plan=source_plan,
                        accept_model_download=False,
                        pull_image=False,
                        status="APPLIED",
                    ),
                ]
            )
            session.commit()
        return {
            "target_id": target_id,
            "source_id": source_id,
            "target_plan": target_plan,
            "source_plan": source_plan,
        }

    def test_generation_detail_preserves_legacy_fields_and_lists_lineage(
        self,
    ) -> None:
        lineage = self._lineage()

        response = self.client.get(
            f"/v1/admin/deployments/{lineage['source_id']}",
            headers=self.admin,
        )

        self.assertEqual(response.status_code, 200)
        deployment = response.json()["deployment"]
        self.assertTrue(
            {"id", "generation", "status", "plan"} <= set(deployment)
        )
        self.assertEqual(deployment["id"], lineage["source_id"])
        self.assertEqual(deployment["generation"], 2)
        self.assertEqual(deployment["status"], "APPLIED")
        self.assertEqual(deployment["plan"], lineage["source_plan"])
        self.assertEqual(deployment["lineage_id"], lineage["target_id"])
        self.assertEqual(
            deployment["previous_generation_id"], lineage["target_id"]
        )
        self.assertEqual(deployment["operations"], [])

        response = self.client.get(
            f"/v1/admin/deployments/{lineage['source_id']}/generations",
            headers=self.admin,
        )

        self.assertEqual(response.status_code, 200)
        generations = response.json()["generations"]
        self.assertEqual(
            [item["id"] for item in generations],
            [lineage["target_id"], lineage["source_id"]],
        )
        self.assertEqual([item["generation"] for item in generations], [1, 2])
        self.assertTrue(generations[0]["rollback_eligible"])
        self.assertFalse(generations[1]["rollback_eligible"])

    def test_rollback_prepare_creates_no_task_until_explicit_apply(self) -> None:
        lineage = self._lineage()
        endpoint = f"/v1/admin/deployments/{lineage['source_id']}/rollback"

        prepared = self.client.post(
            endpoint,
            headers=self.admin,
            json={"node_ids": [NODE_ID]},
        )

        self.assertEqual(prepared.status_code, 200)
        prepared_body = prepared.json()
        self.assertTrue(prepared_body["changed"])
        self.assertEqual(prepared_body["tasks"], [])
        self.assertEqual(prepared_body["operation"]["status"], "PREPARED")
        self.assertEqual(prepared_body["operation"]["phase"], "STOP_SOURCE")
        self.assertFalse(prepared_body["operation"]["serve"])
        self.assertEqual(
            {item["phase"] for item in prepared_body["operation"]["nodes"]},
            {"STOP_SOURCE", "START_TARGET", "VERIFY_TARGET"},
        )
        operation_id = prepared_body["operation"]["id"]
        with self.factory() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(DeploymentOperation)
                ),
                1,
            )
            self.assertEqual(
                session.get(Deployment, lineage["source_id"]).status,
                "APPLIED",
            )

        repeated = self.client.post(
            endpoint,
            headers=self.admin,
            json={"node_ids": [NODE_ID]},
        )
        self.assertEqual(repeated.status_code, 200)
        self.assertFalse(repeated.json()["changed"])
        self.assertEqual(repeated.json()["tasks"], [])
        self.assertEqual(repeated.json()["operation"]["id"], operation_id)

        applied = self.client.post(
            endpoint,
            headers=self.admin,
            json={"node_ids": [NODE_ID], "apply": True},
        )

        self.assertEqual(applied.status_code, 200)
        applied_body = applied.json()
        self.assertTrue(applied_body["changed"])
        self.assertEqual(applied_body["operation"]["id"], operation_id)
        self.assertEqual(applied_body["operation"]["status"], "QUEUED")
        self.assertEqual(len(applied_body["tasks"]), 1)
        self.assertEqual(applied_body["tasks"][0]["type"], "STOP_DEPLOYMENT")
        stop_node = next(
            item
            for item in applied_body["operation"]["nodes"]
            if item["phase"] == "STOP_SOURCE"
        )
        self.assertEqual(stop_node["status"], "QUEUED")
        self.assertEqual(len(stop_node["tasks"]), 1)

        detail = self.client.get(
            f"/v1/admin/deployments/{lineage['source_id']}",
            headers=self.admin,
        ).json()["deployment"]
        self.assertEqual(detail["status"], "ROLLING_BACK")
        self.assertEqual(detail["operations"][0]["id"], operation_id)

    def test_rollback_request_is_strict_bounded_and_closed(self) -> None:
        lineage = self._lineage()
        endpoint = f"/v1/admin/deployments/{lineage['source_id']}/rollback"
        invalid_bodies = [
            {"node_ids": [NODE_ID], "apply": "true"},
            {"node_ids": [NODE_ID], "serve": 1},
            {"node_ids": []},
            {"node_ids": [str(uuid.uuid4()) for _ in range(65)]},
        ]
        for field, value in (
            ("target_id", lineage["target_id"]),
            ("plan", lineage["target_plan"]),
            ("accept_model_download", True),
            ("pull_image", True),
        ):
            invalid_bodies.append({"node_ids": [NODE_ID], field: value})

        for body in invalid_bodies:
            with self.subTest(body=body):
                response = self.client.post(
                    endpoint,
                    headers=self.admin,
                    json=body,
                )
                self.assertEqual(response.status_code, 422)

        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)),
                0,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(DeploymentOperation)
                ),
                0,
            )

    def test_task_creation_rejects_top_level_extra_fields_without_writes(
        self,
    ) -> None:
        lineage = self._lineage()
        base = {
            "node_ids": [NODE_ID],
            "type": "APPLY_DEPLOYMENT",
            "deployment_id": lineage["source_id"],
            "options": {"serve": False},
        }
        for field, value in (
            ("command", ["id"]),
            ("docker_args", ["--privileged"]),
            ("apply", True),
        ):
            with self.subTest(field=field):
                response = self.client.post(
                    "/v1/admin/tasks",
                    headers=self.admin,
                    json={**base, field: value},
                )
                self.assertEqual(response.status_code, 422, response.text)

        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)),
                0,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(DeploymentOperation)
                ),
                0,
            )

    def test_rollout_errors_are_structured_for_not_found_and_conflict(self) -> None:
        missing = self.client.post(
            f"/v1/admin/deployments/{uuid.uuid4()}/rollback",
            headers=self.admin,
            json={"node_ids": [NODE_ID]},
        )
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(
            missing.json()["detail"],
            {
                "code": "DEPLOYMENT_GENERATION_NOT_FOUND",
                "message": "deployment generation not found",
                "details": {},
            },
        )

        lineage = self._lineage(target_verified=False)
        endpoint = f"/v1/admin/deployments/{lineage['source_id']}/rollback"
        invalid_node = self.client.post(
            endpoint,
            headers=self.admin,
            json={"node_ids": ["not-a-canonical-uuid"]},
        )
        self.assertEqual(invalid_node.status_code, 409)
        self.assertEqual(
            invalid_node.json()["detail"]["code"],
            "ROLLBACK_NODE_SET_INVALID",
        )
        self.assertEqual(invalid_node.json()["detail"]["details"], {})

        unverified = self.client.post(
            endpoint,
            headers=self.admin,
            json={"node_ids": [NODE_ID]},
        )
        self.assertEqual(unverified.status_code, 409)
        self.assertEqual(
            unverified.json()["detail"],
            {
                "code": "ROLLBACK_TARGET_NOT_VERIFIED",
                "message": "rollback target has no full verification evidence",
                "details": {},
            },
        )

        missing_detail = self.client.get(
            f"/v1/admin/deployments/{uuid.uuid4()}",
            headers=self.admin,
        )
        self.assertEqual(missing_detail.status_code, 404)
        self.assertEqual(
            missing_detail.json()["detail"]["code"],
            "DEPLOYMENT_GENERATION_NOT_FOUND",
        )

    def test_rollout_admin_routes_require_authentication(self) -> None:
        lineage = self._lineage()
        self.assertEqual(
            self.client.get(
                f"/v1/admin/deployments/{lineage['source_id']}"
            ).status_code,
            401,
        )
        self.assertEqual(
            self.client.get(
                f"/v1/admin/deployments/{lineage['source_id']}/generations"
            ).status_code,
            401,
        )
        self.assertEqual(
            self.client.post(
                f"/v1/admin/deployments/{lineage['source_id']}/rollback",
                json={"node_ids": [NODE_ID]},
            ).status_code,
            401,
        )

    def test_task_creation_and_claim_conflicts_are_structured(self) -> None:
        lineage = self._lineage()
        created = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [NODE_ID],
                "type": "APPLY_DEPLOYMENT",
                "deployment_id": lineage["source_id"],
                "options": {"serve": False},
            },
        )
        self.assertEqual(created.status_code, 200, created.text)
        task_value = created.json()["tasks"][0]
        self.assertIsNotNone(task_value["operation_node_id"])
        self.assertEqual(task_value["operation_attempt"], 1)

        conflict = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [NODE_ID],
                "type": "STOP_DEPLOYMENT",
                "deployment_id": lineage["source_id"],
                "options": {},
            },
        )
        self.assertEqual(conflict.status_code, 409, conflict.text)
        self.assertEqual(
            conflict.json()["detail"]["code"],
            "DEPLOYMENT_OPERATION_ACTIVE",
        )

        with self.factory() as session:
            operation_node = session.get(
                DeploymentOperationNode, task_value["operation_node_id"]
            )
            operation_node.status = "FAILED"
            session.commit()

        claim = self.client.post(
            "/v1/agent/tasks/claim",
            headers={"Authorization": "Bearer node-secret"},
        )
        self.assertEqual(claim.status_code, 409, claim.text)
        self.assertEqual(
            claim.json()["detail"]["code"],
            "DEPLOYMENT_OPERATION_TASK_CONFLICT",
        )
        with self.factory() as session:
            task = session.get(Task, task_value["id"])
            self.assertEqual(task.status, "QUEUED")
            self.assertEqual(task.attempts, 0)
            self.assertIsNone(task.lease_until)
