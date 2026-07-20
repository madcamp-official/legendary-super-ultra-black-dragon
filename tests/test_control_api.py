from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    TestClient = None

from dure.control.api import create_app
from dure.control.models import Node
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
        heartbeat = self.client.post(
            "/v1/agent/heartbeat",
            headers=agent_headers,
            json={
                "state": {"phase": "READY", "role": "gpu-worker"},
                "agent_version": "0.3.12",
            },
        )
        self.assertEqual(heartbeat.status_code, 200)
        nodes = self.client.get("/v1/admin/nodes", headers=self.admin).json()["nodes"]
        self.assertEqual(nodes[0]["id"], node_id)
        self.assertEqual(nodes[0]["agent_version"], "0.3.12")
        self.assertEqual(nodes[0]["phase"], "READY")
        self.assertEqual(nodes[0]["connectivity"], "online")
        invalid_version = self.client.post(
            "/v1/agent/heartbeat",
            headers=agent_headers,
            json={"state": {}, "agent_version": "development"},
        )
        self.assertEqual(invalid_version.status_code, 422)
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
        self.assertEqual(nodes[0]["agent_version"], "0.3.0")

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
        with self.client.app.state.session_factory() as session:
            node = session.get(Node, node_id)
            node.desired_state = None
            session.commit()
        node_view = self.client.get(
            f"/v1/admin/nodes/{node_id}", headers=self.admin
        ).json()["node"]
        self.assertEqual(node_view["desired_state"], "APPLY_DEPLOYMENT")
        task = self.client.post("/v1/agent/tasks/claim", headers=agent_headers).json()["task"]
        self.assertEqual(task["type"], "APPLY_DEPLOYMENT")
        completed = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete", headers=agent_headers, json={"result": {"ok": True}}
        )
        self.assertEqual(completed.status_code, 200)
        detail = self.client.get(f"/v1/admin/tasks/{task['id']}", headers=self.admin).json()["task"]
        self.assertEqual(detail["status"], "SUCCEEDED")
        node_view = self.client.get(
            f"/v1/admin/nodes/{node_id}", headers=self.admin
        ).json()["node"]
        self.assertIsNone(node_view["desired_state"])

    def test_model_registry_requires_admin_and_rejects_extra_execution_fields(self):
        artifact_body = {
            "model_id": "qwen-test-awq",
            "repository": "Qwen/Test-AWQ",
            "revision": "a" * 40,
            "manifest_digest": "sha256:" + "b" * 64,
            "quantization": "awq",
            "size_mib": 8192,
            "default_max_model_len": 8192,
            "layer_count": 32,
            "license_id": "apache-2.0",
        }
        self.assertEqual(
            self.client.post("/v1/admin/model-artifacts", json=artifact_body).status_code,
            401,
        )
        for key, value in (
            ("command", "id"),
            ("docker_args", ["--privileged"]),
            ("env", {"TOKEN": "secret"}),
            ("mounts", ["/etc:/host"]),
            ("host_path", "/etc"),
        ):
            with self.subTest(key=key):
                unsafe = dict(artifact_body, **{key: value})
                self.assertEqual(
                    self.client.post(
                        "/v1/admin/model-artifacts", headers=self.admin, json=unsafe
                    ).status_code,
                    422,
                )
        created = self.client.post(
            "/v1/admin/model-artifacts", headers=self.admin, json=artifact_body
        )
        self.assertEqual(created.status_code, 200)
        duplicate = self.client.post(
            "/v1/admin/model-artifacts", headers=self.admin, json=artifact_body
        )
        self.assertEqual(duplicate.status_code, 409)
        unsafe_runtime = self.client.post(
            "/v1/admin/runtime-releases",
            headers=self.admin,
            json={
                "version": "unsafe",
                "image": "--privileged@sha256:" + "c" * 64,
                "vllm_version": "0.9.0",
                "cuda_version": "12.8",
                "gpu_architectures": ["ampere"],
            },
        )
        self.assertEqual(unsafe_runtime.status_code, 400)

    def test_model_registry_api_requires_evidence_for_active_transition(self):
        artifact = self.client.post(
            "/v1/admin/model-artifacts",
            headers=self.admin,
            json={
                "model_id": "qwen-test-awq",
                "repository": "Qwen/Test-AWQ",
                "revision": "a" * 40,
                "manifest_digest": "sha256:" + "b" * 64,
                "quantization": "awq",
                "size_mib": 8192,
                "default_max_model_len": 8192,
                "layer_count": 32,
                "license_id": "apache-2.0",
            },
        )
        self.assertEqual(artifact.status_code, 200)
        runtime = self.client.post(
            "/v1/admin/runtime-releases",
            headers=self.admin,
            json={
                "version": "test",
                "image": "registry.example/vllm@sha256:" + "c" * 64,
                "vllm_version": "0.9.0",
                "cuda_version": "12.8",
                "gpu_architectures": ["ampere"],
            },
        )
        self.assertEqual(runtime.status_code, 200)
        release = self.client.post(
            "/v1/admin/model-releases",
            headers=self.admin,
            json={
                "artifact_id": artifact.json()["artifact"]["id"],
                "runtime_id": runtime.json()["runtime"]["id"],
                "quality_rank": 10,
            },
        )
        self.assertEqual(release.status_code, 200)
        release_id = release.json()["release"]["id"]
        placement = self.client.post(
            f"/v1/admin/model-releases/{release_id}/placements",
            headers=self.admin,
            json={
                "profile_id": "single-24g",
                "topology": "single-gpu",
                "node_count": 1,
                "min_gpu_memory_mib": 24576,
                "min_disk_free_mib": 16384,
                "pipeline_parallel_size": 1,
                "tensor_parallel_size": 1,
                "requires_network_evidence": False,
                "requires_nccl": False,
                "min_bandwidth_mbps": None,
                "max_rtt_ms": None,
                "max_packet_loss_pct": None,
                "max_ttft_p95_ms": 1000.0,
                "max_tpot_p95_ms": 100.0,
                "max_e2e_p95_ms": 5000.0,
                "min_success_rate": 0.99,
                "min_vram_headroom_pct": 10.0,
                "min_throughput_tps": 10.0,
            },
        )
        self.assertEqual(placement.status_code, 200)

        validated = self.client.post(
            f"/v1/admin/model-releases/{release_id}/transition",
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(validated.status_code, 200)
        active = self.client.post(
            f"/v1/admin/model-releases/{release_id}/transition",
            headers=self.admin,
            json={"status": "ACTIVE"},
        )
        self.assertEqual(active.status_code, 409)
        self.assertEqual(active.json()["detail"]["code"], "BENCHMARK_GATE_FAILED")
        self.assertEqual(
            active.json()["detail"]["details"]["placements"][0]["code"],
            "EVIDENCE_MISSING",
        )
        listed = self.client.get("/v1/admin/model-releases", headers=self.admin)
        self.assertEqual(listed.json()["releases"][0]["status"], "VALIDATED")
        self.assertEqual(listed.json()["releases"][0]["placements"][0]["profile_id"], "single-24g")
