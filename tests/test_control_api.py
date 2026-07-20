from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    TestClient = None

from dure.control.api import create_app
from dure.planner import build_plan
from tests.helpers import profile


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class ControlAPITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'api.db'}"
        self.client = TestClient(create_app(database_url=url, admin_token="admin-secret", create_schema=True))
        self.admin = {"Authorization": "Bearer admin-secret"}

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def test_enroll_heartbeat_list_and_revoke(self):
        created = self.client.post("/v1/admin/enrollments", headers=self.admin, json={"expires_in_seconds": 3600})
        self.assertEqual(created.status_code, 200)
        claimed = self.client.post("/v1/enrollments/claim", json={
            "token": created.json()["token"], "install_id": "install-api-1234",
            "agent_version": "0.2.0", "profile": profile("api-node").to_dict(),
        })
        self.assertEqual(claimed.status_code, 200)
        node_id = claimed.json()["node_id"]
        agent_headers = {"Authorization": f"Bearer {claimed.json()['credential']}"}
        heartbeat = self.client.post("/v1/agent/heartbeat", headers=agent_headers, json={"state": {"phase": "READY", "role": "gpu-worker"}})
        self.assertEqual(heartbeat.status_code, 200)
        nodes = self.client.get("/v1/admin/nodes", headers=self.admin).json()["nodes"]
        self.assertEqual(nodes[0]["id"], node_id)
        self.assertEqual(nodes[0]["phase"], "READY")
        self.assertEqual(nodes[0]["connectivity"], "online")
        self.assertEqual(self.client.post(f"/v1/admin/nodes/{node_id}/revoke", headers=self.admin).status_code, 200)
        self.assertEqual(self.client.post("/v1/agent/heartbeat", headers=agent_headers, json={"state": {}}).status_code, 401)

    def test_admin_auth_is_required(self):
        self.assertEqual(self.client.get("/v1/admin/nodes").status_code, 401)
        self.assertEqual(self.client.get("/v1/admin/inventory").status_code, 401)

    def test_inventory_returns_profiles_for_capacity_diagnosis(self):
        joined_profile = profile("inventory-node")
        joined = self.client.post(
            "/v1/nodes/join",
            json={
                "install_id": "install-inventory-node",
                "agent_version": "0.3.3",
                "profile": joined_profile.to_dict(),
            },
        ).json()
        self.client.post(f"/v1/admin/nodes/{joined['node_id']}/approve", headers=self.admin)

        response = self.client.get("/v1/admin/inventory", headers=self.admin)

        self.assertEqual(response.status_code, 200)
        node = response.json()["nodes"][0]
        self.assertEqual(node["id"], joined["node_id"])
        self.assertEqual(node["agent_version"], "0.3.3")
        self.assertEqual(node["profile"]["cpu_count"], joined_profile.cpu_count)
        self.assertIn("profile_updated_at", node)

    def test_tokenless_join_heartbeats_pending_then_admin_approves(self):
        joined = self.client.post("/v1/nodes/join", json={
            "install_id": "install-open-join",
            "agent_version": "0.3.0",
            "profile": profile("open-node").to_dict(),
        })
        self.assertEqual(joined.status_code, 200)
        self.assertEqual(joined.json()["status"], "pending")
        node_id = joined.json()["node_id"]
        agent_headers = {"Authorization": f"Bearer {joined.json()['credential']}"}
        heartbeat = self.client.post(
            "/v1/agent/heartbeat", headers=agent_headers, json={"state": {"phase": "DISCOVERED"}}
        )
        self.assertEqual(heartbeat.status_code, 200)
        self.assertFalse(heartbeat.json()["approved"])
        claim = self.client.post("/v1/agent/tasks/claim", headers=agent_headers)
        self.assertEqual(claim.json(), {"task": None, "status": "pending"})
        approved = self.client.post(f"/v1/admin/nodes/{node_id}/approve", headers=self.admin)
        self.assertEqual(approved.status_code, 200)
        nodes = self.client.get("/v1/admin/nodes", headers=self.admin).json()["nodes"]
        self.assertTrue(nodes[0]["approved"])

    def test_deployment_bulk_task_claim_and_complete(self):
        enrollment = self.client.post("/v1/admin/enrollments", headers=self.admin, json={}).json()
        claimed = self.client.post("/v1/enrollments/claim", json={
            "token": enrollment["token"], "install_id": "install-tasks-1234",
            "agent_version": "0.2.0", "profile": profile("task-node").to_dict(),
        }).json()
        node_id = claimed["node_id"]
        agent_headers = {"Authorization": f"Bearer {claimed['credential']}"}
        planned_profile = profile(node_id)
        plan = build_plan([planned_profile], image="registry/vllm@sha256:" + "f" * 64)
        response = self.client.post("/v1/admin/deployments", headers=self.admin, json={"plan": plan.to_dict()})
        self.assertEqual(response.status_code, 200)
        queued = self.client.post("/v1/admin/tasks", headers=self.admin, json={
            "node_ids": [node_id, "missing"], "type": "APPLY_DEPLOYMENT",
            "deployment_id": plan.deployment_id, "options": {"serve": False},
        })
        self.assertEqual(queued.status_code, 200)
        self.assertIn("missing", queued.json()["errors"])
        task = self.client.post("/v1/agent/tasks/claim", headers=agent_headers).json()["task"]
        self.assertEqual(task["type"], "APPLY_DEPLOYMENT")
        completed = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete", headers=agent_headers, json={"result": {"ok": True}}
        )
        self.assertEqual(completed.status_code, 200)
        detail = self.client.get(f"/v1/admin/tasks/{task['id']}", headers=self.admin).json()["task"]
        self.assertEqual(detail["status"], "SUCCEEDED")
