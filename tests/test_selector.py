import unittest
from dataclasses import replace

from dure.catalog import ModelCatalog, STATIC_CATALOG
from dure.models import GPUProfile, InstalledModelProfile
from dure.selector import InventoryNode, recommend_model

from .helpers import profile


def evaluation(result, model_id):
    return next(item for item in result.evaluations if item.model_id == model_id)


def inventory(node_profile, *, node_id=None, approved=True, online=True, fresh=True, network=False):
    return InventoryNode(
        node_id=node_id or node_profile.node_id,
        profile=node_profile,
        approved=approved,
        online=online,
        profile_fresh=fresh,
        network_verified=network,
    )


class SelectorTests(unittest.TestCase):
    def test_inventory_order_does_not_change_recommendation(self):
        nodes = [
            inventory(profile("node-c"), network=True),
            inventory(profile("node-a"), network=True),
            inventory(profile("node-b"), network=True),
        ]

        forward = recommend_model(nodes)
        reverse = recommend_model(list(reversed(nodes)))

        self.assertEqual(forward.to_dict(), reverse.to_dict())
        self.assertEqual(forward.selected_model_id, "qwen2.5-72b-awq")
        self.assertEqual(forward.selected_node_ids, ("node-a", "node-b", "node-c"))

    def test_three_22g_nodes_do_not_qualify_for_72b(self):
        nodes = [
            inventory(profile(f"node-{index}", gpu_memory_mib=22528), network=True)
            for index in range(3)
        ]

        result = recommend_model(nodes)
        candidate = evaluation(result, "qwen2.5-72b-awq")

        self.assertFalse(candidate.feasible)
        self.assertIn("GPU_MEMORY", {item.code for item in candidate.rejections})
        self.assertNotEqual(result.selected_model_id, "qwen2.5-72b-awq")

    def test_untrusted_or_stale_nodes_are_rejected(self):
        nodes = [
            inventory(profile("pending"), approved=False),
            inventory(profile("offline"), online=False),
            inventory(profile("stale"), fresh=False),
        ]

        result = recommend_model(nodes, model_id="qwen2.5-7b-awq")
        candidate = result.evaluations[0]

        self.assertIsNone(result.selected_model_id)
        self.assertEqual(
            {item.code for item in candidate.rejections},
            {"NODE_PENDING", "NODE_OFFLINE", "PROFILE_STALE", "NODE_COUNT"},
        )

    def test_multinode_candidate_requires_network_evidence(self):
        nodes = [inventory(profile(f"node-{index}")) for index in range(3)]

        result = recommend_model(nodes, model_id="qwen2.5-72b-awq")

        self.assertIsNone(result.selected_model_id)
        self.assertIn("NETWORK_EVIDENCE", {item.code for item in result.evaluations[0].rejections})

    def test_disk_and_runtime_failures_are_explained(self):
        disk_node = profile("disk")
        disk_node.disk_free_mib = 1024
        runtime_node = profile("runtime")
        runtime_node.runtime.nvidia_runtime = False

        result = recommend_model(
            [inventory(disk_node), inventory(runtime_node)],
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
                revision="a" * 40,
                quantization="awq",
                complete=True,
            )
        )
        entry = replace(
            STATIC_CATALOG.entry("qwen2.5-32b-awq"), artifact_revision="a" * 40
        )
        catalog = ModelCatalog("test", "test", (entry,))

        result = recommend_model(
            [inventory(profile("node-a")), inventory(cached)],
            catalog=catalog,
            model_id="qwen2.5-32b-awq",
        )

        self.assertEqual(result.selected_node_ids, ("node-z",))

    def test_duplicate_node_ids_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate inventory"):
            recommend_model([inventory(profile("same")), inventory(profile("same"))])

    def test_server_uuid_is_independent_from_profile_hostname(self):
        first = profile("shared-host")
        second = profile("shared-host")

        result = recommend_model(
            [
                inventory(first, node_id="00000000-0000-0000-0000-000000000002"),
                inventory(second, node_id="00000000-0000-0000-0000-000000000001"),
            ],
            model_id="qwen2.5-32b-awq",
        )

        self.assertEqual(
            result.selected_node_ids,
            ("00000000-0000-0000-0000-000000000001",),
        )

    def test_nested_inventory_order_is_canonicalized(self):
        first = profile("node-a")
        first.gpus.append(
            GPUProfile(
                index=1,
                name="NVIDIA RTX A6000",
                uuid="GPU-second",
                driver_version="610.43.02",
                memory_mib=24576,
                compute_capability="8.6",
            )
        )
        first.network.addresses.append("10.0.0.2")
        first.issues.extend(["z", "a"])
        second = profile("node-a")
        second.gpus = list(reversed(first.gpus))
        second.network.addresses = list(reversed(first.network.addresses))
        second.issues = list(reversed(first.issues))

        left = recommend_model([inventory(first)])
        right = recommend_model([inventory(second)])

        self.assertEqual(left.inventory_fingerprint, right.inventory_fingerprint)

    def test_driver_compute_and_engine_failures_are_distinct(self):
        no_driver = profile("driver", driver="")
        old_gpu = profile("compute")
        old_gpu.gpus[0].compute_capability = "7.0"
        wrong_engine = profile("engine")
        wrong_engine.runtime.engine = "podman"

        result = recommend_model(
            [inventory(no_driver), inventory(old_gpu), inventory(wrong_engine)],
            model_id="qwen2.5-7b-awq",
        )

        codes = {item.code for item in result.evaluations[0].rejections}
        self.assertIn("GPU_DRIVER", codes)
        self.assertIn("COMPUTE_CAPABILITY", codes)
        self.assertIn("RUNTIME", codes)

    def test_wrong_quantization_cache_is_not_preferred(self):
        wrong_cache = profile("node-z")
        wrong_cache.installed_models.append(
            InstalledModelProfile(
                source="huggingface",
                model_id="Qwen/Qwen2.5-32B-Instruct-AWQ",
                revision="a" * 40,
                quantization="gptq",
                complete=True,
            )
        )
        entry = replace(
            STATIC_CATALOG.entry("qwen2.5-32b-awq"), artifact_revision="a" * 40
        )

        result = recommend_model(
            [inventory(profile("node-a")), inventory(wrong_cache)],
            catalog=ModelCatalog("test", "test", (entry,)),
        )

        self.assertEqual(result.selected_node_ids, ("node-a",))


if __name__ == "__main__":
    unittest.main()
