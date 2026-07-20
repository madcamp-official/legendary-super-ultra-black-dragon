import unittest

from dure.models import InstalledModelProfile
from dure.selector import InventoryNode, recommend_model

from .helpers import profile


def evaluation(result, model_id):
    return next(item for item in result.evaluations if item.model_id == model_id)


class SelectorTests(unittest.TestCase):
    def test_inventory_order_does_not_change_recommendation(self):
        nodes = [
            InventoryNode(profile("node-c"), network_verified=True),
            InventoryNode(profile("node-a"), network_verified=True),
            InventoryNode(profile("node-b"), network_verified=True),
        ]

        forward = recommend_model(nodes)
        reverse = recommend_model(list(reversed(nodes)))

        self.assertEqual(forward.to_dict(), reverse.to_dict())
        self.assertEqual(forward.selected_model_id, "qwen2.5-72b-awq")
        self.assertEqual(forward.selected_node_ids, ("node-a", "node-b", "node-c"))

    def test_three_22g_nodes_do_not_qualify_for_72b(self):
        nodes = [
            InventoryNode(profile(f"node-{index}", gpu_memory_mib=22528), network_verified=True)
            for index in range(3)
        ]

        result = recommend_model(nodes)
        candidate = evaluation(result, "qwen2.5-72b-awq")

        self.assertFalse(candidate.feasible)
        self.assertIn("GPU_MEMORY", {item.code for item in candidate.rejections})
        self.assertNotEqual(result.selected_model_id, "qwen2.5-72b-awq")

    def test_untrusted_or_stale_nodes_are_rejected(self):
        nodes = [
            InventoryNode(profile("pending"), approved=False),
            InventoryNode(profile("offline"), online=False),
            InventoryNode(profile("stale"), profile_fresh=False),
        ]

        result = recommend_model(nodes, model_id="qwen2.5-7b-awq")
        candidate = result.evaluations[0]

        self.assertIsNone(result.selected_model_id)
        self.assertEqual({item.code for item in candidate.rejections}, {"NODE_STATUS", "NODE_COUNT"})

    def test_multinode_candidate_requires_network_evidence(self):
        nodes = [InventoryNode(profile(f"node-{index}")) for index in range(3)]

        result = recommend_model(nodes, model_id="qwen2.5-72b-awq")

        self.assertIsNone(result.selected_model_id)
        self.assertIn("NETWORK_EVIDENCE", {item.code for item in result.evaluations[0].rejections})

    def test_disk_and_runtime_failures_are_explained(self):
        disk_node = profile("disk")
        disk_node.disk_free_mib = 1024
        runtime_node = profile("runtime")
        runtime_node.runtime.nvidia_runtime = False

        result = recommend_model(
            [InventoryNode(disk_node), InventoryNode(runtime_node)],
            model_id="qwen2.5-7b-awq",
        )

        codes = {item.code for item in result.evaluations[0].rejections}
        self.assertIn("DISK_SPACE", codes)
        self.assertIn("RUNTIME", codes)

    def test_complete_local_artifact_breaks_node_tie(self):
        cached = profile("node-z")
        cached.installed_models.append(
            InstalledModelProfile(
                source="huggingface",
                model_id="Qwen/Qwen2.5-32B-Instruct-AWQ",
                complete=True,
            )
        )

        result = recommend_model(
            [InventoryNode(profile("node-a")), InventoryNode(cached)],
            model_id="qwen2.5-32b-awq",
        )

        self.assertEqual(result.selected_node_ids, ("node-z",))

    def test_duplicate_node_ids_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate inventory"):
            recommend_model([InventoryNode(profile("same")), InventoryNode(profile("same"))])


if __name__ == "__main__":
    unittest.main()
