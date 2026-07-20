import unittest

from dure.models import DeploymentPlan
from dure.planner import build_plan, classify_node, recommend_local_model

from .helpers import profile


class PlannerTests(unittest.TestCase):
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
        self.assertEqual(DeploymentPlan.from_dict(plan.to_dict()).to_dict(), plan.to_dict())


if __name__ == "__main__":
    unittest.main()
