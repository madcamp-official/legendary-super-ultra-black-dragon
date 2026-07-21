from __future__ import annotations

import copy
import unittest

from dure.activation import ActivationSpec, ActivationWorkflow
from dure.artifact_manifest import parse_artifact_manifest


NODE_ID = "11111111-1111-4111-8111-111111111111"
ARTIFACT_ID = "22222222-2222-4222-8222-222222222222"
RUNTIME_ID = "33333333-3333-4333-8333-333333333333"
RELEASE_ID = "44444444-4444-4444-8444-444444444444"
PLACEMENT_ID = "55555555-5555-4555-8555-555555555555"
BENCHMARK_ID = "66666666-6666-4666-8666-666666666666"
DEPLOYMENT_ID = "77777777-7777-4777-8777-777777777777"
PREPARATION_ID = "88888888-8888-4888-8888-888888888888"


def _manifest() -> dict:
    return {
        "schema_version": 1,
        "files": [
            {
                "path": "config.json",
                "kind": "REGULAR",
                "size_bytes": 2,
                "sha256": "sha256:" + "a" * 64,
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": 2,
                        "sha256": "sha256:" + "b" * 64,
                    }
                ],
            }
        ],
    }


def _spec_document() -> dict:
    manifest = _manifest()
    return {
        "schema_version": 1,
        "artifact": {
            "model_id": "qwen-test-awq",
            "repository": "Qwen/Qwen-Test-AWQ",
            "revision": "a" * 40,
            "manifest_digest": parse_artifact_manifest(manifest).digest,
            "quantization": "awq",
            "size_mib": 1024,
            "default_max_model_len": 8192,
            "layer_count": 32,
            "license_id": "apache-2.0",
        },
        "manifest": manifest,
        "runtime": {
            "version": "vllm-0.9.0",
            "image": "registry.example/vllm@sha256:" + "c" * 64,
            "vllm_version": "0.9.0",
            "cuda_version": "12.4",
            "gpu_architectures": ["ampere"],
        },
        "release": {"quality_rank": 10},
        "placement": {
            "profile_id": "single-24g",
            "topology": "single-gpu",
            "node_count": 1,
            "min_gpu_memory_mib": 24000,
            "min_disk_free_mib": 4096,
            "pipeline_parallel_size": 1,
            "tensor_parallel_size": 1,
            "requires_network_evidence": False,
            "requires_nccl": False,
            "min_bandwidth_mbps": None,
            "max_rtt_ms": None,
            "max_packet_loss_pct": None,
            "max_ttft_p95_ms": 2000.0,
            "max_tpot_p95_ms": 200.0,
            "max_e2e_p95_ms": 10000.0,
            "min_success_rate": 0.95,
            "min_vram_headroom_pct": 5.0,
            "min_throughput_tps": 1.0,
        },
        "benchmark": {
            "workload_id": "quality-eval",
            "dure_commit": "d" * 40,
            "attempt": 1,
        },
    }


def _inventory() -> dict:
    return {
        "nodes": [
            {
                "id": NODE_ID,
                "agent_version": "0.3.26",
                "approved": True,
                "connectivity": "online",
                "profile": {
                    "disk_free_mib": 50000,
                    "gpus": [
                        {"healthy": True, "memory_mib": 24576}
                    ],
                    "runtime": {
                        "engine": "docker",
                        "engine_ready": True,
                        "nvidia_runtime": True,
                    },
                    "installed_models": [],
                },
            }
        ]
    }


class FakeActivationClient:
    def __init__(self):
        self.calls = []
        self.release_status = "DRAFT"
        self.task_counter = 0
        self.selected_release_id = RELEASE_ID

    def _task(self, task_id):
        return {"id": task_id, "status": "SUCCEEDED", "error": None}

    def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if (method, path) == ("GET", "/v1/admin/inventory"):
            return _inventory()
        if (method, path) == ("POST", "/v1/admin/tasks"):
            self.task_counter += 1
            task_id = f"task-{self.task_counter}"
            return {"tasks": [self._task(task_id)], "errors": {}, "bulk_id": task_id}
        if method == "GET" and path.startswith("/v1/admin/tasks/"):
            return {"task": self._task(path.rsplit("/", 1)[-1])}
        if (method, path) == ("GET", "/v1/admin/model-artifacts"):
            return {"artifacts": []}
        if (method, path) == ("POST", "/v1/admin/model-artifacts"):
            return {"artifact": {"id": ARTIFACT_ID, **payload}}
        if method == "POST" and path.endswith("/manifest"):
            return {"manifest": {"digest": parse_artifact_manifest(payload).digest}}
        if (method, path) == ("GET", "/v1/admin/runtime-releases"):
            return {"runtimes": []}
        if (method, path) == ("POST", "/v1/admin/runtime-releases"):
            return {"runtime": {"id": RUNTIME_ID, **payload}}
        if (method, path) == ("GET", "/v1/admin/model-releases"):
            return {"releases": []}
        if (method, path) == ("POST", "/v1/admin/model-releases"):
            return {
                "release": {
                    "id": RELEASE_ID,
                    "status": "DRAFT",
                    "quality_rank": payload["quality_rank"],
                    "placements": [],
                }
            }
        if method == "POST" and path.endswith("/placements"):
            return {"placement": {"id": PLACEMENT_ID, "release_id": RELEASE_ID, **payload}}
        if method == "POST" and path.endswith("/transition"):
            self.release_status = "VALIDATED"
            return {"release": {"id": RELEASE_ID, "status": self.release_status}}
        if (method, path) == ("POST", "/v1/admin/benchmark-runs/prepare"):
            return {"benchmark_run": {"id": BENCHMARK_ID, "status": "PREPARED"}}
        if method == "POST" and path.endswith("/apply") and "benchmark-runs" in path:
            return {"task": self._task("benchmark-task")}
        if method == "GET" and "/benchmark-runs/" in path:
            return {"benchmark_run": {"id": BENCHMARK_ID, "status": "SUCCEEDED"}}
        if method == "POST" and path.endswith("/promote"):
            self.release_status = "ACTIVE"
            return {"release": {"id": RELEASE_ID, "status": "ACTIVE"}}
        if (method, path) == ("POST", "/v1/admin/deployment-recommendations"):
            return {
                "recommendation": {
                    "id": "sha256:" + "e" * 64,
                    "selected": {
                        "node_ids": [NODE_ID],
                        "model_release_id": self.selected_release_id,
                        "placement_id": PLACEMENT_ID,
                    },
                }
            }
        if method == "POST" and path.endswith("/accept"):
            return {"deployment": {"id": DEPLOYMENT_ID}}
        if method == "POST" and path.endswith("/prepare"):
            return {"preparation": {"id": PREPARATION_ID, "status": "PREPARED"}}
        if method == "GET" and "/deployment-preparations/" in path:
            return {"preparation": {"id": PREPARATION_ID, "status": "SUCCEEDED"}}
        raise AssertionError(f"unexpected request: {method} {path} {payload}")


class ActivationSpecTests(unittest.TestCase):
    def test_closed_spec_validates_manifest_and_rejects_multinode_automation(self):
        spec = ActivationSpec.from_dict(_spec_document())

        self.assertEqual(spec.manifest, parse_artifact_manifest(_manifest()).document)
        self.assertRegex(spec.digest, r"^sha256:[0-9a-f]{64}$")

        invalid = copy.deepcopy(_spec_document())
        invalid["placement"].update(
            topology="pipeline",
            node_count=3,
            pipeline_parallel_size=3,
            requires_network_evidence=True,
            requires_nccl=True,
        )
        with self.assertRaisesRegex(ValueError, "single-gpu"):
            ActivationSpec.from_dict(invalid)


class ActivationWorkflowTests(unittest.TestCase):
    def test_preview_is_read_only_and_selects_an_eligible_node(self):
        client = FakeActivationClient()
        workflow = ActivationWorkflow(client)

        result = workflow.preview(ActivationSpec.from_dict(_spec_document()), node_ids=None)

        self.assertFalse(result["apply"])
        self.assertEqual(result["benchmark_node_id"], NODE_ID)
        self.assertEqual(client.calls, [("GET", "/v1/admin/inventory", None)])

    def test_preview_rejects_agents_without_the_packaged_benchmark(self):
        client = FakeActivationClient()
        original_request = client.request

        def request(method, path, payload=None):
            value = original_request(method, path, payload)
            if (method, path) == ("GET", "/v1/admin/inventory"):
                value["nodes"][0]["agent_version"] = "0.3.25"
            return value

        client.request = request

        with self.assertRaisesRegex(ValueError, "no node eligible"):
            ActivationWorkflow(client).preview(
                ActivationSpec.from_dict(_spec_document()), node_ids=None
            )

    def test_apply_runs_registry_benchmark_recommend_prepare_apply_and_verify(self):
        client = FakeActivationClient()
        messages = []
        workflow = ActivationWorkflow(
            client,
            sleeper=lambda _seconds: None,
            reporter=messages.append,
        )

        result = workflow.apply(ActivationSpec.from_dict(_spec_document()), node_ids=None)

        self.assertEqual(result["status"], "READY")
        self.assertEqual(result["deployment_id"], DEPLOYMENT_ID)
        self.assertEqual(result["node_ids"], [NODE_ID])
        paths = [(method, path) for method, path, _payload in client.calls]
        self.assertIn(("POST", f"/v1/admin/model-releases/{RELEASE_ID}/promote"), paths)
        self.assertIn(("POST", "/v1/admin/deployment-recommendations"), paths)
        task_payloads = [
            payload
            for method, path, payload in client.calls
            if (method, path) == ("POST", "/v1/admin/tasks")
        ]
        self.assertIn("APPLY_DEPLOYMENT", {item["type"] for item in task_payloads})
        self.assertIn("VERIFY", {item["type"] for item in task_payloads})
        benchmark_apply = next(
            payload
            for method, path, payload in client.calls
            if method == "POST" and "/benchmark-runs/" in path and path.endswith("/apply")
        )
        self.assertEqual(
            benchmark_apply,
            {"apply": True, "prepare_model": True, "pull_image": True},
        )

    def test_apply_never_deploys_a_different_recommended_release(self):
        client = FakeActivationClient()
        client.selected_release_id = "99999999-9999-4999-8999-999999999999"
        workflow = ActivationWorkflow(client, sleeper=lambda _seconds: None)

        with self.assertRaisesRegex(RuntimeError, "different release"):
            workflow.apply(ActivationSpec.from_dict(_spec_document()), node_ids=None)

        self.assertFalse(
            any(
                method == "POST" and path.endswith("/accept")
                for method, path, _payload in client.calls
            )
        )


if __name__ == "__main__":
    unittest.main()
