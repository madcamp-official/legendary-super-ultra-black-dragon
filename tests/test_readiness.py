import copy
import json
import unittest
from unittest.mock import MagicMock, patch

from dure.pipeline_runtime import (
    RAY_COMPONENT,
    pipeline_contract_detail,
    strict_runtime_contract_digest,
)
from dure.readiness import PIPELINE_SNAPSHOT_SCRIPT, ReadinessVerifier
from dure.runtime import DEPLOYMENT_IDENTITY_FORMAT

from .helpers import FakeRunner, strict_pipeline_fixture


def _response(status=200, body=b""):
    value = MagicMock()
    value.status = status
    value.read.return_value = body
    value.__enter__.return_value = value
    value.__exit__.return_value = False
    return value


class ReadinessTests(unittest.TestCase):
    def test_api_requires_health_and_a_served_model(self):
        model_body = json.dumps({"data": [{"id": "qwen-test"}]}).encode()
        with patch(
            "dure.readiness.urllib.request.urlopen",
            side_effect=[_response(), _response(body=model_body)],
        ):
            result = ReadinessVerifier().api("http://127.0.0.1:8000")

        self.assertTrue(result.ok, result.detail)
        self.assertIn("qwen-test", result.detail)

    def test_strict_api_requires_the_exact_planned_served_model(self):
        plan, head, _ = strict_pipeline_fixture()
        verifier = ReadinessVerifier(node_id=head.node_id)
        wrong_body = json.dumps({"data": [{"id": "other-model"}]}).encode()
        with patch.object(
            verifier, "_container_identity", return_value=(None, "container")
        ), patch(
            "dure.readiness.urllib.request.urlopen",
            side_effect=[_response(), _response(body=wrong_body)],
        ):
            wrong = verifier.api(plan=plan)
        self.assertFalse(wrong.ok)

        expected_body = json.dumps(
            {"data": [{"id": plan.model.model_id}]}
        ).encode()
        with patch.object(
            verifier, "_container_identity", return_value=(None, "container")
        ), patch(
            "dure.readiness.urllib.request.urlopen",
            side_effect=[_response(), _response(body=expected_body)],
        ):
            expected = verifier.api(plan=plan)
        self.assertTrue(expected.ok, expected.detail)

    @staticmethod
    def pipeline_snapshot(*, actors=True):
        actor_values = (
            [
                {
                    "actor_id": "actor-head",
                    "class_name": "RayWorkerWrapper",
                    "node_id": "ray-head",
                    "state": "ALIVE",
                },
                {
                    "actor_id": "actor-worker",
                    "class_name": "vllm.executor.ray_utils.RayWorkerWrapper",
                    "node_id": "ray-worker",
                    "state": "ALIVE",
                },
            ]
            if actors
            else []
        )
        return {
            "schema_version": 1,
            "vllm_version": "0.9.0",
            "nodes": [
                {
                    "node_id": "ray-head",
                    "runtime_address": "192.168.0.10",
                    "gpu": 1.0,
                    "alive": True,
                    "dure_node_resources": {
                        "dure_node_11111111111141118111111111111111": 1.0
                    },
                },
                {
                    "node_id": "ray-worker",
                    "runtime_address": "192.168.0.11",
                    "gpu": 1,
                    "alive": True,
                    "dure_node_resources": {
                        "dure_node_22222222222242228222222222222222": 1
                    },
                },
            ],
            "actors": actor_values,
        }

    def test_pipeline_contract_uses_bounded_fixed_snapshot_and_canonical_detail(self):
        plan, head, _ = strict_pipeline_fixture()
        assignment = plan.assignments[0]
        name = f"dure-ray-{plan.deployment_id}"
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        identity = "\t".join(
            str(item)
            for item in (
                "container-id",
                "running",
                plan.deployment_id,
                plan.generation,
                assignment.node_id,
                plan.execution_backend,
                assignment.pipeline_rank,
                assignment.expected_runtime_rank,
                "ray-node",
                strict_runtime_contract_digest(plan, assignment, RAY_COMPONENT),
            )
        )

        def response(command):
            if command[-2] == PIPELINE_SNAPSHOT_SCRIPT:
                return (0, json.dumps(self.pipeline_snapshot()), "")
            return None

        runner = FakeRunner(
            responses={inspect: (0, identity, "")},
            response_factory=response,
        )

        result = ReadinessVerifier(
            runner, node_id=head.node_id
        ).pipeline_rank_contract(
            plan, assignment, head, require_actors=True
        )

        self.assertTrue(result.ok, result.detail)
        self.assertEqual(result.name, "pipeline-rank-contract")
        self.assertEqual(result.detail, pipeline_contract_detail(plan, assignment))
        decoded = json.loads(result.detail)
        self.assertEqual(
            set(decoded),
            {
                "schema_version",
                "backend",
                "vllm_version",
                "node_id",
                "runtime_address",
                "pipeline_rank",
                "runtime_rank",
                "ordered_bindings",
            },
        )
        self.assertEqual(len(runner.limited_output_calls), 1)
        command, limit = runner.limited_output_calls[0]
        self.assertEqual(command[-2], PIPELINE_SNAPSHOT_SCRIPT)
        self.assertEqual(command[-1], "192.168.0.10:6379")
        self.assertEqual(limit, 256 * 1024)

    def test_pipeline_snapshot_rejects_missing_extra_duplicate_and_bad_actor_topology(self):
        plan, _, _ = strict_pipeline_fixture()
        baseline = self.pipeline_snapshot()
        invalid = {}

        missing = copy.deepcopy(baseline)
        missing["nodes"].pop()
        invalid["missing-node"] = missing

        extra = copy.deepcopy(baseline)
        extra["nodes"].append(
            {
                "node_id": "ray-extra",
                "runtime_address": "192.168.0.12",
                "gpu": 1,
                "alive": True,
                "dure_node_resources": {
                    "dure_node_44444444444444448444444444444444": 1
                },
            }
        )
        invalid["extra-node"] = extra

        duplicate = copy.deepcopy(baseline)
        duplicate["nodes"][1]["runtime_address"] = "192.168.0.10"
        invalid["duplicate-address"] = duplicate

        wrong_gpu = copy.deepcopy(baseline)
        wrong_gpu["nodes"][1]["gpu"] = 2
        invalid["two-gpus"] = wrong_gpu

        wrong_uuid = copy.deepcopy(baseline)
        wrong_uuid["nodes"][1]["dure_node_resources"] = {
            "dure_node_11111111111141118111111111111111": 1
        }
        invalid["swapped-dure-uuid"] = wrong_uuid

        wrong_version = copy.deepcopy(baseline)
        wrong_version["vllm_version"] = "0.9.1"
        invalid["wrong-vllm"] = wrong_version

        missing_actor = copy.deepcopy(baseline)
        missing_actor["actors"].pop()
        invalid["missing-actor"] = missing_actor

        duplicate_actor = copy.deepcopy(baseline)
        duplicate_actor["actors"][1]["node_id"] = "ray-head"
        invalid["duplicate-actor-node"] = duplicate_actor

        for label, snapshot in invalid.items():
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    ReadinessVerifier._validate_pipeline_snapshot(
                        plan, snapshot, require_actors=True
                    )

    def test_pipeline_contract_rejects_swapped_local_address_before_docker(self):
        plan, head, _ = strict_pipeline_fixture()
        head.network.addresses = ["192.168.0.11"]
        runner = FakeRunner()

        result = ReadinessVerifier(
            runner, node_id=head.node_id
        ).pipeline_rank_contract(
            plan, plan.assignments[0], head, require_actors=False
        )

        self.assertFalse(result.ok)
        self.assertIn("runtime_address", result.detail)
        self.assertEqual(runner.calls, [])


if __name__ == "__main__":
    unittest.main()
