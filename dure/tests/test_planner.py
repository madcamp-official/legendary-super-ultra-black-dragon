import copy
import itertools
import unittest

from dure.model_cache import MODEL_CACHE_KIND_FULL_SNAPSHOT
from dure.models import (
    VLLM_RAY_PP_BACKEND,
    VLLM_RAY_PP_RUNTIME_VERSION,
    DeploymentPlan,
    ModelSpec,
    NodeAssignment,
)
from dure.planner import (
    StrictRayPPTopologyError,
    build_plan,
    classify_node,
    recommend_local_model,
    strict_vllm_ray_pp_order,
)

from .helpers import profile, strict_stage_pipeline_fixture


class PlannerTests(unittest.TestCase):
    def _strict_plan(self) -> DeploymentPlan:
        assignments = [
            NodeAssignment(
                node_id="00000000-0000-4000-8000-000000000010",
                gpu_index=0,
                rank=0,
                pipeline_rank=0,
                layer_start=0,
                layer_end=1,
                role="ray-head",
                expected_runtime_rank=0,
                runtime_address="10.0.0.9",
            ),
            NodeAssignment(
                node_id="00000000-0000-4000-8000-000000000020",
                gpu_index=0,
                rank=1,
                pipeline_rank=1,
                layer_start=2,
                layer_end=3,
                expected_runtime_rank=1,
                runtime_address="10.0.0.11",
            ),
            NodeAssignment(
                node_id="00000000-0000-4000-8000-000000000030",
                gpu_index=0,
                rank=2,
                pipeline_rank=2,
                layer_start=4,
                layer_end=5,
                expected_runtime_rank=2,
                runtime_address="10.0.0.2",
            ),
        ]
        return DeploymentPlan(
            deployment_id="00000000-0000-4000-8000-000000000001",
            generation=1,
            model=ModelSpec(
                model_id="test-model",
                repository="TestOrg/TestModel",
                quantization="awq",
                checkpoint_gib=8.0,
                min_gpu_memory_gib=8.0,
                default_max_model_len=8192,
                layer_count=6,
            ),
            image="registry.example/vllm@sha256:" + "a" * 64,
            pipeline_parallel_size=3,
            tensor_parallel_size=1,
            ray_head_node_id=assignments[0].node_id,
            ray_head_address="10.0.0.9:6379",
            network_interface="ens3",
            model_revision="b" * 40,
            model_path="/var/lib/dure/models/test-model",
            assignments=assignments,
            execution_backend=VLLM_RAY_PP_BACKEND,
            runtime_vllm_version=VLLM_RAY_PP_RUNTIME_VERSION,
            model_cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )

    def test_cpu_node_is_utility(self):
        node = profile("cpu-1", gpu_memory_mib=None)
        node.cpu_count = 4
        node.memory_mib = 3800

        role, capabilities = classify_node(node)

        self.assertEqual(role, "utility")
        self.assertIn("utility-controller", capabilities)
        self.assertIn("artifact-cache", capabilities)
        self.assertNotIn("gpu-worker", capabilities)
        self.assertIsNone(recommend_local_model(node))

    def test_three_24g_gpus_select_72b_pipeline(self):
        nodes = [
            profile("camp-7", address="192.168.0.228"),
            profile("camp-9", address="192.168.0.83"),
            profile("camp-8", address="192.168.0.84"),
        ]

        plan = build_plan(nodes)

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.model.model_id, "qwen2.5-72b-awq")
        self.assertEqual(plan.pipeline_parallel_size, 3)
        self.assertEqual(plan.tensor_parallel_size, 1)
        self.assertEqual(plan.world_size, 3)
        self.assertEqual(plan.ray_head_address, "192.168.0.228:6379")
        self.assertEqual(
            [(item.layer_start, item.layer_end) for item in plan.assignments],
            [(0, 26), (27, 53), (54, 79)],
        )

    def test_non_contiguous_gpu_index_is_supported(self):
        node = profile("gpu-2", gpu_index=2)
        plan = build_plan([node], model_id="qwen2.5-32b-awq")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.assignments[0].gpu_index, 2)

    def test_72b_requires_three_eligible_gpus(self):
        with self.assertRaisesRegex(ValueError, "requires 3"):
            build_plan([profile("one")], model_id="qwen2.5-72b-awq")

    def test_duplicate_node_profiles_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate node"):
            build_plan([profile("same"), profile("same")])

    def test_auto_selection_is_deterministic_and_rejects_22g_for_72b(self):
        nodes = [
            profile("node-c", gpu_memory_mib=22528),
            profile("node-a", gpu_memory_mib=22528),
            profile("node-b", gpu_memory_mib=22528),
        ]

        forward = build_plan(nodes)
        reverse = build_plan(list(reversed(nodes)))

        self.assertIsNotNone(forward)
        self.assertIsNotNone(reverse)
        assert forward is not None and reverse is not None
        self.assertNotEqual(forward.model.model_id, "qwen2.5-72b-awq")
        self.assertEqual(forward.model.model_id, reverse.model.model_id)
        self.assertEqual(
            [item.node_id for item in forward.assignments],
            [item.node_id for item in reverse.assignments],
        )

    def test_explicit_model_preserves_profile_order_and_plan_json(self):
        nodes = [profile("node-c"), profile("node-a"), profile("node-b")]

        plan = build_plan(nodes, model_id="qwen2.5-72b-awq")

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.ray_head_node_id, "node-c")
        self.assertEqual(
            [item.node_id for item in plan.assignments],
            ["node-c", "node-a", "node-b"],
        )
        serialized = plan.to_dict()
        self.assertNotIn("execution_backend", serialized)
        self.assertNotIn("runtime_vllm_version", serialized)
        self.assertNotIn("model_cache_kind", serialized)
        for assignment in serialized["assignments"]:
            self.assertNotIn("expected_runtime_rank", assignment)
            self.assertNotIn("runtime_address", assignment)
        self.assertEqual(DeploymentPlan.from_dict(serialized).to_dict(), serialized)

    def test_strict_ray_pp_contract_round_trips(self):
        serialized = self._strict_plan().to_dict()

        self.assertEqual(serialized["execution_backend"], VLLM_RAY_PP_BACKEND)
        self.assertEqual(
            serialized["runtime_vllm_version"], VLLM_RAY_PP_RUNTIME_VERSION
        )
        self.assertEqual(
            serialized["model_cache_kind"], MODEL_CACHE_KIND_FULL_SNAPSHOT
        )
        self.assertEqual(
            [item["expected_runtime_rank"] for item in serialized["assignments"]],
            [0, 1, 2],
        )
        self.assertEqual(DeploymentPlan.from_dict(serialized).to_dict(), serialized)

    def test_strict_stage_contract_round_trips_without_changing_full_wire(self):
        plan, _head, _worker = strict_stage_pipeline_fixture()

        serialized = plan.to_dict()

        self.assertEqual(serialized["model_cache_kind"], "STAGE")
        self.assertEqual(
            serialized["model_path"], "/var/lib/dure/models/stages"
        )
        self.assertEqual(
            serialized["stage_artifact"]["loader_format"],
            "VLLM_SHARDED_STATE_V1",
        )
        self.assertEqual(
            [item["stage_manifest_digest"] for item in serialized["assignments"]],
            ["sha256:" + "2" * 64, "sha256:" + "3" * 64],
        )
        self.assertEqual(DeploymentPlan.from_dict(serialized).to_dict(), serialized)

        full = self._strict_plan().to_dict()
        self.assertNotIn("stage_artifact", full)
        self.assertTrue(
            all("stage_manifest_digest" not in item for item in full["assignments"])
        )

    def test_strict_stage_contract_rejects_open_or_mismatched_identity(self):
        plan, _head, _worker = strict_stage_pipeline_fixture()
        serialized = plan.to_dict()

        unknown = copy.deepcopy(serialized)
        unknown["stage_artifact"]["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "closed wire schema"):
            DeploymentPlan.from_dict(unknown)

        wrong_loader = copy.deepcopy(serialized)
        wrong_loader["stage_artifact"]["loader_format"] = "auto"
        with self.assertRaisesRegex(ValueError, "loader contract"):
            DeploymentPlan.from_dict(wrong_loader)

        wrong_runtime = copy.deepcopy(serialized)
        wrong_runtime["stage_artifact"]["runtime_image"] = (
            "registry.example/other@sha256:" + "9" * 64
        )
        with self.assertRaisesRegex(ValueError, "contract identity"):
            DeploymentPlan.from_dict(wrong_runtime)

        inconsistent_contract = copy.deepcopy(serialized)
        inconsistent_contract["stage_artifact"]["source_manifest_digest"] = (
            "sha256:" + "8" * 64
        )
        with self.assertRaisesRegex(ValueError, "contract identity"):
            DeploymentPlan.from_dict(inconsistent_contract)

        missing_rank = copy.deepcopy(serialized)
        missing_rank["assignments"][1].pop("stage_manifest_digest")
        with self.assertRaisesRegex(ValueError, "assignment identity"):
            DeploymentPlan.from_dict(missing_rank)

        user_path = copy.deepcopy(serialized)
        user_path["model_path"] = "/tmp/operator-stage"
        with self.assertRaisesRegex(ValueError, "fixed Dure stage root"):
            DeploymentPlan.from_dict(user_path)

    def test_strict_ray_pp_contract_rejects_unknown_backend_and_public_address(self):
        unknown = self._strict_plan().to_dict()
        unknown["execution_backend"] = "VLLM_UNKNOWN_V1"
        with self.assertRaisesRegex(ValueError, "unknown execution backend"):
            DeploymentPlan.from_dict(unknown)

        public = self._strict_plan().to_dict()
        public["assignments"][2]["runtime_address"] = "203.0.113.30"
        with self.assertRaisesRegex(ValueError, "private IPv4"):
            DeploymentPlan.from_dict(public)

        unsupported_quantization = self._strict_plan().to_dict()
        unsupported_quantization["model"]["quantization"] = "gptq"
        with self.assertRaisesRegex(ValueError, "requires AWQ quantization"):
            DeploymentPlan.from_dict(unsupported_quantization)

    def test_strict_ray_pp_contract_rejects_duplicate_rank_gap_and_swap(self):
        duplicate = self._strict_plan().to_dict()
        duplicate["assignments"][2]["runtime_address"] = "10.0.0.11"
        with self.assertRaisesRegex(ValueError, "runtime addresses must be unique"):
            DeploymentPlan.from_dict(duplicate)

        rank_gap = self._strict_plan().to_dict()
        rank_gap["assignments"][1]["expected_runtime_rank"] = 2
        with self.assertRaisesRegex(ValueError, "runtime ranks must be contiguous"):
            DeploymentPlan.from_dict(rank_gap)

        swapped = self._strict_plan().to_dict()
        swapped["assignments"][1]["runtime_address"] = "10.0.0.2"
        swapped["assignments"][2]["runtime_address"] = "10.0.0.11"
        with self.assertRaisesRegex(ValueError, "ordered by runtime address"):
            DeploymentPlan.from_dict(swapped)

    def test_strict_ray_pp_contract_rejects_topologies_outside_two_or_three_nodes(self):
        four_stage = self._strict_plan().to_dict()
        four_stage["pipeline_parallel_size"] = 4
        four_stage["world_size"] = 4
        with self.assertRaisesRegex(ValueError, "exactly 2 or 3"):
            DeploymentPlan.from_dict(four_stage)

        nodes = [
            profile(
                f"00000000-0000-4000-8000-{index:012d}",
                address=f"10.0.1.{index}",
            )
            for index in range(1, 5)
        ]
        with self.assertRaisesRegex(ValueError, "exactly two or three"):
            strict_vllm_ray_pp_order(nodes, head_node_id=nodes[0].node_id)

    def test_strict_ray_pp_order_is_input_permutation_invariant(self):
        head = profile(
            "00000000-0000-4000-8000-000000000010", address="10.0.0.9"
        )
        worker_a = profile(
            "00000000-0000-4000-8000-000000000020", address="10.0.0.2"
        )
        worker_b = profile(
            "00000000-0000-4000-8000-000000000030", address="10.0.0.11"
        )
        expected = [head.node_id, worker_b.node_id, worker_a.node_id]

        for permutation in itertools.permutations([head, worker_a, worker_b]):
            with self.subTest(order=[item.node_id for item in permutation]):
                bindings = strict_vllm_ray_pp_order(
                    list(permutation), head_node_id=head.node_id
                )
                self.assertEqual(
                    [item.profile.node_id for item in bindings], expected
                )

    def test_strict_ray_pp_order_rejects_ambiguous_or_unbound_default_addresses(self):
        head = profile(
            "00000000-0000-4000-8000-000000000010", address="10.0.0.9"
        )
        worker = profile(
            "00000000-0000-4000-8000-000000000020", address="10.0.0.10"
        )
        cases = (
            ([], "DEFAULT_INTERFACE_ADDRESS_REQUIRED"),
            (["10.0.0.10", "10.0.0.11"], "PRIVATE_IPV4_AMBIGUOUS"),
            (["10.0.0.11"], "DEFAULT_INTERFACE_ADDRESS_MISMATCH"),
        )
        for addresses, reason in cases:
            with self.subTest(addresses=addresses):
                changed = copy.deepcopy(worker)
                changed.network.default_interface_addresses = addresses
                with self.assertRaises(StrictRayPPTopologyError) as raised:
                    strict_vllm_ray_pp_order(
                        [head, changed], head_node_id=head.node_id
                    )
                self.assertEqual(raised.exception.reason, reason)

    def test_strict_ray_pp_order_rejects_negative_gpu_index(self):
        head = profile(
            "00000000-0000-4000-8000-000000000010", address="10.0.0.9"
        )
        worker = profile(
            "00000000-0000-4000-8000-000000000020", address="10.0.0.10"
        )
        worker.gpus[0].index = -1

        with self.assertRaises(StrictRayPPTopologyError) as raised:
            strict_vllm_ray_pp_order([head, worker], head_node_id=head.node_id)

        self.assertEqual(raised.exception.reason, "GPU_INDEX_INVALID")

    def test_strict_metadata_cannot_be_smuggled_into_legacy_plan(self):
        plan = build_plan([profile("legacy")], model_id="qwen2.5-32b-awq")
        self.assertIsNotNone(plan)
        assert plan is not None
        changed = copy.deepcopy(plan)
        changed.assignments[0].runtime_address = "10.0.0.1"

        with self.assertRaisesRegex(ValueError, "legacy deployment plan"):
            changed.to_dict()


if __name__ == "__main__":
    unittest.main()
