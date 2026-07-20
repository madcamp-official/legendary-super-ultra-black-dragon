import copy
import json
import unittest
from unittest.mock import MagicMock, patch

from dure.pipeline_runtime import (
    RAY_COMPONENT,
    pipeline_contract_detail,
    stage_cache_identity,
    stage_identity_labels,
    strict_runtime_contract_digest,
)
from dure.models import CheckResult
from dure.readiness import PIPELINE_SNAPSHOT_SCRIPT, ReadinessVerifier
from dure.runtime import DEPLOYMENT_IDENTITY_FORMAT

from .helpers import (
    FakeRunner,
    strict_pipeline_fixture,
    strict_stage_pipeline_fixture,
)


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

    def test_stage_pipeline_contract_binds_rank_artifact_and_cache_identity(self):
        plan, head, _ = strict_stage_pipeline_fixture()
        assignment = plan.assignments[0]
        name = f"dure-ray-{plan.deployment_id}"
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        labels = stage_identity_labels(plan, assignment)
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
                RAY_COMPONENT,
                strict_runtime_contract_digest(plan, assignment, RAY_COMPONENT),
                labels["dure.cache-kind"],
                labels["dure.stage-variant"],
                labels["dure.stage-manifest"],
                labels["dure.stage-cache-identity"],
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
        with patch("dure.readiness.validate_strict_stage_cache") as validate_cache:
            result = ReadinessVerifier(
                runner, node_id=head.node_id
            ).pipeline_rank_contract(
                plan, assignment, head, require_actors=True
            )

        self.assertTrue(result.ok, result.detail)
        validate_cache.assert_called_once_with(plan, assignment)
        detail = json.loads(result.detail)
        self.assertEqual(
            detail["stage_artifact"],
            {
                "artifact_set_digest": plan.stage_artifact.artifact_set_digest,
                "contract_identity_digest": (
                    plan.stage_artifact.contract_identity_digest
                ),
                "source_manifest_digest": (
                    plan.stage_artifact.source_manifest_digest
                ),
                "loader_format": "VLLM_SHARDED_STATE_V1",
                "stage_manifest_digest": assignment.stage_manifest_digest,
                "stage_tensor_keys_digest": assignment.stage_tensor_keys_digest,
                "stage_cache_identity_digest": stage_cache_identity(
                    plan, assignment
                ).cache_identity_digest,
            },
        )
        for binding, expected_assignment in zip(
            detail["ordered_bindings"], plan.assignments, strict=True
        ):
            self.assertEqual(
                binding["stage_manifest_digest"],
                expected_assignment.stage_manifest_digest,
            )
            self.assertEqual(
                binding["stage_tensor_keys_digest"],
                expected_assignment.stage_tensor_keys_digest,
            )
            self.assertEqual(
                binding["stage_cache_identity_digest"],
                stage_cache_identity(
                    plan, expected_assignment
                ).cache_identity_digest,
            )

    def test_stage_pipeline_contract_rejects_cache_failure_before_docker(self):
        plan, head, _ = strict_stage_pipeline_fixture()
        assignment = plan.assignments[0]
        runner = FakeRunner()

        with patch(
            "dure.readiness.validate_strict_stage_cache",
            side_effect=ValueError("assigned STAGE cache failed integrity validation"),
        ):
            result = ReadinessVerifier(
                runner, node_id=head.node_id
            ).pipeline_rank_contract(
                plan, assignment, head, require_actors=False
            )

        self.assertFalse(result.ok)
        self.assertIn("integrity", result.detail)
        self.assertEqual(runner.calls, [])
        self.assertEqual(runner.limited_output_calls, [])

    def test_stage_pipeline_contract_rejects_swapped_container_identity(self):
        plan, head, _ = strict_stage_pipeline_fixture()
        assignment = plan.assignments[0]
        name = f"dure-ray-{plan.deployment_id}"
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        labels = stage_identity_labels(plan, assignment)
        swapped = "\t".join(
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
                RAY_COMPONENT,
                strict_runtime_contract_digest(plan, assignment, RAY_COMPONENT),
                labels["dure.cache-kind"],
                labels["dure.stage-variant"],
                "sha256:" + "9" * 64,
                labels["dure.stage-cache-identity"],
            )
        )
        runner = FakeRunner(responses={inspect: (0, swapped, "")})

        with patch("dure.readiness.validate_strict_stage_cache"):
            result = ReadinessVerifier(
                runner, node_id=head.node_id
            ).pipeline_rank_contract(
                plan, assignment, head, require_actors=False
            )

        self.assertFalse(result.ok)
        self.assertIn("identity", result.detail.lower())
        self.assertEqual(runner.limited_output_calls, [])
        self.assertFalse(any(call[:2] == ("docker", "exec") for call in runner.calls))

    def test_stage_one_shot_contract_revalidates_cache_on_every_call(self):
        plan, head, _ = strict_stage_pipeline_fixture()
        assignment = plan.assignments[0]

        def response(command):
            if command[-2] == PIPELINE_SNAPSHOT_SCRIPT:
                return (0, json.dumps(self.pipeline_snapshot()), "")
            return None

        verifier = ReadinessVerifier(
            FakeRunner(response_factory=response), node_id=head.node_id
        )
        with patch.object(
            verifier, "_container_identity", return_value=(None, "container-id")
        ), patch(
            "dure.readiness.validate_strict_stage_cache"
        ) as validate_cache:
            first = verifier.pipeline_rank_contract(
                plan, assignment, head, require_actors=True
            )
            second = verifier.pipeline_rank_contract(
                plan, assignment, head, require_actors=True
            )

        self.assertTrue(first.ok, first.detail)
        self.assertTrue(second.ok, second.detail)
        self.assertEqual(validate_cache.call_count, 2)
        validate_cache.assert_any_call(plan, assignment)

    def test_stage_wait_hashes_before_polling_and_after_topology_success(self):
        plan, head, _ = strict_stage_pipeline_fixture()
        assignment = plan.assignments[0]
        verifier = ReadinessVerifier(FakeRunner(), node_id=head.node_id)
        waiting = CheckResult(
            "pipeline-rank-contract", False, "actors are not ready"
        )
        ready = CheckResult(
            "pipeline-rank-contract",
            True,
            pipeline_contract_detail(plan, assignment),
        )

        with patch(
            "dure.readiness.validate_strict_stage_cache"
        ) as validate_cache, patch.object(
            verifier,
            "_pipeline_rank_contract",
            side_effect=[waiting, ready],
        ) as poll, patch(
            "dure.readiness.time.sleep"
        ):
            result = verifier.wait_pipeline_rank_contract(
                plan,
                assignment,
                head,
                require_actors=True,
                timeout=10,
                interval=0,
            )

        self.assertTrue(result.ok, result.detail)
        self.assertEqual(validate_cache.call_count, 2)
        self.assertEqual(poll.call_count, 2)
        self.assertTrue(
            all(
                call.kwargs["stage_cache_prevalidated"]
                for call in poll.call_args_list
            )
        )

    def test_stage_wait_fails_closed_when_final_cache_revalidation_fails(self):
        plan, head, _ = strict_stage_pipeline_fixture()
        assignment = plan.assignments[0]
        verifier = ReadinessVerifier(FakeRunner(), node_id=head.node_id)
        ready = CheckResult(
            "pipeline-rank-contract",
            True,
            pipeline_contract_detail(plan, assignment),
        )

        with patch(
            "dure.readiness.validate_strict_stage_cache",
            side_effect=[None, ValueError("stage cache changed after topology")],
        ) as validate_cache, patch.object(
            verifier, "_pipeline_rank_contract", return_value=ready
        ) as poll:
            result = verifier.wait_pipeline_rank_contract(
                plan,
                assignment,
                head,
                require_actors=True,
                timeout=10,
                interval=0,
            )

        self.assertFalse(result.ok)
        self.assertIn("changed after topology", result.detail)
        self.assertEqual(validate_cache.call_count, 2)
        poll.assert_called_once()

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
