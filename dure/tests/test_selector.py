import unittest
from dataclasses import replace

from dure.catalog import (
    ModelCatalog,
    NetworkEvidenceBinding,
    STATIC_CATALOG,
    StageArtifactDelivery,
    StageRankDelivery,
)
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


def network_evidence(evidence_id, *node_ids):
    return NetworkEvidenceBinding(
        evidence_id=evidence_id,
        evidence_digest="sha256:" + (evidence_id[-1] * 64),
        node_ids=tuple(node_ids),
        registered_at="2026-07-21T00:00:00Z",
    )


def stage_delivery(character: str, *, rank_size_bytes: int = 1024) -> StageArtifactDelivery:
    return StageArtifactDelivery(
        artifact_set_digest="sha256:" + character * 64,
        contract_identity_digest="sha256:" + "c" * 64,
        source_manifest_digest="sha256:" + "d" * 64,
        runtime_image="registry.example/vllm@sha256:" + "e" * 64,
        vllm_version="0.9.0",
        exporter_build_digest="sha256:" + "f" * 64,
        architecture="Qwen2ForCausalLM",
        quantization="awq",
        tensor_parallel_size=1,
        pipeline_parallel_size=3,
        loader_format="VLLM_SHARDED_STATE_V1",
        ranks=tuple(
            StageRankDelivery(
                rank=rank,
                pipeline_rank=rank,
                tensor_rank=0,
                manifest_digest="sha256:" + str(rank + 1) * 64,
                tensor_key_count=10 + rank,
                tensor_keys_digest="sha256:" + str(rank + 4) * 64,
                weight_size_bytes=rank_size_bytes - 1,
                total_size_bytes=rank_size_bytes,
                file_count=5,
            )
            for rank in range(3)
        ),
    )


class SelectorTests(unittest.TestCase):
    def _delivery_catalog(
        self,
        *,
        stage: StageArtifactDelivery,
    ) -> ModelCatalog:
        base = STATIC_CATALOG.entry("qwen2.5-72b-awq")
        evidence = replace(
            network_evidence("evidence-a", "node-a", "node-b", "node-c"),
            rank_node_ids=("node-a", "node-c", "node-b"),
        )
        full = replace(
            base,
            candidate_id="release:placement",
            network_evidence=(evidence,),
        )
        staged = replace(
            full,
            candidate_id=f"release:placement:STAGE:{stage.artifact_set_digest}",
            stage_artifact=stage,
        )
        return ModelCatalog("test", "test", (full, staged))

    def test_stage_uses_rank_bytes_before_full_snapshot_disk_gate(self):
        nodes = []
        for node_id in ("node-a", "node-b", "node-c"):
            value = profile(node_id)
            value.disk_free_mib = 256
            nodes.append(inventory(value))
        catalog = self._delivery_catalog(stage=stage_delivery("a"))

        result = recommend_model(nodes, catalog=catalog)

        self.assertIn(":STAGE:", result.selected_candidate_id)
        self.assertEqual(result.selected_node_ids, ("node-a", "node-b", "node-c"))
        staged, full = result.evaluations
        self.assertTrue(staged.feasible)
        self.assertEqual(staged.rank_node_ids, ("node-a", "node-c", "node-b"))
        self.assertFalse(full.feasible)
        self.assertIn("DISK_SPACE", {item.code for item in full.rejections})

    def test_full_snapshot_is_an_independent_fallback_when_stage_disk_fails(self):
        nodes = []
        for node_id in ("node-a", "node-b", "node-c"):
            value = profile(node_id)
            value.disk_free_mib = 60000
            nodes.append(inventory(value))
        huge_rank = 40000 * 1024 * 1024
        catalog = self._delivery_catalog(
            stage=stage_delivery("a", rank_size_bytes=huge_rank)
        )

        result = recommend_model(nodes, catalog=catalog)

        self.assertEqual(result.selected_candidate_id, "release:placement")
        staged = result.evaluations[0]
        self.assertFalse(staged.feasible)
        self.assertIn(
            "STAGE_DISK_SPACE", {item.code for item in staged.rejections}
        )
        self.assertTrue(result.evaluations[1].feasible)

    def test_stage_and_full_failures_remain_separate_and_explained(self):
        nodes = []
        for node_id in ("node-a", "node-b", "node-c"):
            value = profile(node_id)
            value.disk_free_mib = 100
            nodes.append(inventory(value))
        catalog = self._delivery_catalog(
            stage=stage_delivery("a", rank_size_bytes=100 * 1024 * 1024)
        )

        result = recommend_model(nodes, catalog=catalog)

        self.assertIsNone(result.selected_candidate_id)
        self.assertIn(
            "STAGE_DISK_SPACE",
            {item.code for item in result.evaluations[0].rejections},
        )
        self.assertIn(
            "DISK_SPACE",
            {item.code for item in result.evaluations[1].rejections},
        )

    def test_stage_variant_digest_is_the_deterministic_tie_break(self):
        nodes = [
            inventory(profile(node_id))
            for node_id in ("node-a", "node-b", "node-c")
        ]
        first_catalog = self._delivery_catalog(stage=stage_delivery("a"))
        base_full = next(
            item for item in first_catalog.entries if item.stage_artifact is None
        )
        stage_a = next(
            item for item in first_catalog.entries if item.stage_artifact is not None
        )
        stage_b_value = stage_delivery("b")
        stage_b = replace(
            stage_a,
            candidate_id=(
                "release:placement:STAGE:" + stage_b_value.artifact_set_digest
            ),
            stage_artifact=stage_b_value,
        )

        forward = recommend_model(
            nodes,
            catalog=ModelCatalog("test", "test", (stage_b, base_full, stage_a)),
        )
        reverse = recommend_model(
            list(reversed(nodes)),
            catalog=ModelCatalog("test", "test", (stage_a, base_full, stage_b)),
        )

        self.assertEqual(forward.to_dict(), reverse.to_dict())
        self.assertTrue(forward.selected_candidate_id.endswith("sha256:" + "a" * 64))

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
        self.assertNotIn(
            "network_evidence_id",
            evaluation(forward, "qwen2.5-72b-awq").to_dict(),
        )

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

    def test_exact_network_evidence_selects_its_complete_node_set(self):
        entry = replace(
            STATIC_CATALOG.entry("qwen2.5-72b-awq"),
            network_evidence=(
                network_evidence("evidence-a", "node-d", "node-b", "node-c"),
            ),
        )
        result = recommend_model(
            [
                inventory(profile("node-a")),
                inventory(profile("node-b")),
                inventory(profile("node-c")),
                inventory(profile("node-d")),
            ],
            catalog=ModelCatalog("test", "test", (entry,)),
        )

        candidate = result.evaluations[0]
        self.assertTrue(candidate.feasible)
        self.assertEqual(result.selected_node_ids, ("node-b", "node-c", "node-d"))
        self.assertEqual(candidate.network_evidence_id, "evidence-a")
        self.assertEqual(candidate.network_evidence_digest, "sha256:" + ("a" * 64))
        self.assertEqual(
            candidate.to_dict()["network_evidence_registered_at"],
            "2026-07-21T00:00:00Z",
        )

    def test_exact_network_evidence_groups_are_not_combined(self):
        first = network_evidence("evidence-a", "node-a", "node-b", "node-c")
        second = network_evidence("evidence-b", "node-b", "node-c", "node-d")
        entry = replace(
            STATIC_CATALOG.entry("qwen2.5-72b-awq"),
            network_evidence=(first, second),
        )
        undersized = profile("node-c", gpu_memory_mib=22528)

        result = recommend_model(
            [
                inventory(profile("node-a"), network=True),
                inventory(profile("node-b"), network=True),
                inventory(undersized, network=True),
                inventory(profile("node-d"), network=True),
            ],
            catalog=ModelCatalog("test", "test", (entry,)),
        )

        candidate = result.evaluations[0]
        self.assertFalse(candidate.feasible)
        self.assertEqual(candidate.node_ids, ())
        self.assertIsNone(candidate.network_evidence_id)
        self.assertIn("NETWORK_EVIDENCE", {item.code for item in candidate.rejections})

    def test_exact_network_evidence_choice_is_order_independent(self):
        first = network_evidence("evidence-a", "node-a", "node-c", "node-d")
        second = network_evidence("evidence-b", "node-b", "node-c", "node-d")
        entry = STATIC_CATALOG.entry("qwen2.5-72b-awq")
        nodes = [
            inventory(profile("node-a")),
            inventory(profile("node-b")),
            inventory(profile("node-c")),
            inventory(profile("node-d")),
        ]

        forward = recommend_model(
            nodes,
            catalog=ModelCatalog(
                "test", "test", (replace(entry, network_evidence=(second, first)),)
            ),
        )
        reverse = recommend_model(
            list(reversed(nodes)),
            catalog=ModelCatalog(
                "test", "test", (replace(entry, network_evidence=(first, second)),)
            ),
        )

        self.assertEqual(forward.to_dict(), reverse.to_dict())
        self.assertEqual(forward.selected_node_ids, ("node-a", "node-c", "node-d"))
        self.assertEqual(forward.evaluations[0].network_evidence_id, "evidence-a")

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
