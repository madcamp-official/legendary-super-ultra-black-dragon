from __future__ import annotations

import hashlib
import random
import unittest
from dataclasses import replace

from dure.fleet_scheduler import (
    DEFAULT_MAX_CANDIDATES,
    MAX_SAFE_RECURSIVE_CANDIDATES,
    FleetDeploymentCandidate,
    FleetGpuBinding,
    FleetSchedulingError,
    FleetSchedulingLimitError,
    schedule_fleet,
)
from dure.models import GPUProfile
from dure.profile_generator import generate_auto_placement_profile_specs
from dure.resource_pool import (
    FLEET_MODEL_IDS,
    FLEET_TENSOR_PARALLEL_SIZE,
    GpuSlot,
    build_gpu_pool_snapshot,
)
from dure.selector import InventoryNode

from .helpers import profile


EXPECTED_MODEL_IDS = frozenset(
    {
        "qwen2.5-7b-awq",
        "qwen2.5-14b-awq",
        "qwen2.5-32b-awq",
        "qwen2.5-72b-awq",
    }
)


def inventory_node(
    node_id: str,
    memory_mib: int,
    *,
    extra_gpu_memory_mib: tuple[int, ...] = (),
) -> InventoryNode:
    node_profile = profile(node_id, gpu_memory_mib=memory_mib)
    for offset, extra_memory_mib in enumerate(extra_gpu_memory_mib, start=1):
        node_profile.gpus.append(
            GPUProfile(
                index=offset,
                name=f"test-gpu-{extra_memory_mib}",
                uuid=f"GPU-{node_id}-{offset:02d}",
                driver_version=node_profile.gpus[0].driver_version,
                memory_mib=extra_memory_mib,
                compute_capability=node_profile.gpus[0].compute_capability,
            )
        )
    return InventoryNode.local(node_profile)


def deployment_candidate(
    candidate_id: str,
    model_id: str,
    slots: tuple[GpuSlot, ...],
    *,
    quality: float,
    throughput: float,
) -> FleetDeploymentCandidate:
    return FleetDeploymentCandidate(
        candidate_id=candidate_id,
        model_id=model_id,
        placement_profile_id=f"profile-{candidate_id}",
        evidence_id=f"evidence-{candidate_id}",
        evidence_digest=(
            "sha256:" + hashlib.sha256(candidate_id.encode("utf-8")).hexdigest()
        ),
        bindings=tuple(
            FleetGpuBinding(
                node_id=slot.node_id,
                gpu_index=slot.gpu_index,
                gpu_uuid=slot.gpu_uuid,
                rank=rank,
            )
            for rank, slot in enumerate(slots)
        ),
        tensor_parallel_size=1,
        pipeline_parallel_size=len(slots),
        quality_score=quality,
        throughput_tps=throughput,
    )


def slots_by_node(nodes: list[InventoryNode]) -> dict[str, GpuSlot]:
    snapshot = build_gpu_pool_snapshot(nodes)
    return {slot.node_id: slot for slot in snapshot.selected_slots}


class FleetScaleAcceptanceTests(unittest.TestCase):
    def test_heterogeneous_pool_selects_exactly_one_best_gpu_per_node(self):
        memory_by_node = {
            "node-low": 4096,
            "node-8g": 8192,
            "node-12g": 12288,
            "node-24g": 24576,
            "node-48g": 49152,
            "node-80g": 81920,
        }
        nodes = [
            inventory_node(node_id, memory_mib)
            for node_id, memory_mib in memory_by_node.items()
        ]
        nodes.append(
            inventory_node(
                "node-multi",
                8192,
                extra_gpu_memory_mib=(24576, 81920, 49152),
            )
        )

        forward = build_gpu_pool_snapshot(nodes)
        reverse = build_gpu_pool_snapshot(reversed(nodes))

        self.assertEqual(forward.to_dict(), reverse.to_dict())
        self.assertEqual(len(forward.nodes), 7)
        self.assertEqual(len(forward.selected_slots), 7)
        selected = {slot.node_id: slot for slot in forward.selected_slots}
        for node_id, memory_mib in memory_by_node.items():
            self.assertEqual(selected[node_id].memory_mib, memory_mib)
        self.assertEqual(selected["node-multi"].memory_mib, 81920)
        self.assertEqual(selected["node-multi"].gpu_index, 2)
        self.assertEqual(selected["node-multi"].gpu_uuid, "GPU-node-multi-02")
        self.assertEqual(
            len({slot.node_id for slot in forward.selected_slots}),
            len(forward.selected_slots),
        )

    def test_model_and_topology_contract_is_closed_and_tp_is_always_one(self):
        self.assertEqual(FLEET_MODEL_IDS, EXPECTED_MODEL_IDS)
        self.assertEqual(FLEET_TENSOR_PARALLEL_SIZE, 1)

        specs = {
            model_id: generate_auto_placement_profile_specs(model_id)
            for model_id in sorted(EXPECTED_MODEL_IDS)
        }
        self.assertEqual(
            {
                model_id: tuple(spec.pipeline_parallel_size for spec in values)
                for model_id, values in specs.items()
            },
            {
                "qwen2.5-7b-awq": (1,),
                "qwen2.5-14b-awq": (1,),
                "qwen2.5-32b-awq": (1,),
                "qwen2.5-72b-awq": (1, 2, 3),
            },
        )
        for model_id, values in specs.items():
            for spec in values:
                self.assertEqual(spec.model_id, model_id)
                self.assertEqual(spec.tensor_parallel_size, 1)
                self.assertEqual(spec.node_count, spec.pipeline_parallel_size)

        with self.assertRaisesRegex(ValueError, "Fleet allowlist"):
            generate_auto_placement_profile_specs("outside-model")

    def test_scheduler_rejects_model_tp_and_per_node_gpu_contract_violations(self):
        slot = slots_by_node([inventory_node("node-a", 81920)])["node-a"]
        valid = deployment_candidate(
            "valid",
            "qwen2.5-7b-awq",
            (slot,),
            quality=7,
            throughput=20,
        )
        duplicate_node_binding = FleetGpuBinding(
            node_id=slot.node_id,
            gpu_index=slot.gpu_index + 1,
            gpu_uuid="GPU-node-a-second",
            rank=1,
        )
        cases = (
            (replace(valid, model_id="outside-model"), "FLEET_MODEL_NOT_ALLOWED"),
            (replace(valid, tensor_parallel_size=2), "FLEET_TP_UNSUPPORTED"),
            (
                replace(
                    valid,
                    bindings=(valid.bindings[0], duplicate_node_binding),
                    pipeline_parallel_size=2,
                ),
                "FLEET_BINDING_DUPLICATE",
            ),
        )

        for value, expected_code in cases:
            with self.subTest(expected_code=expected_code):
                with self.assertRaises(FleetSchedulingError) as raised:
                    schedule_fleet([value])
                self.assertEqual(raised.exception.code, expected_code)

    def test_five_nodes_place_one_72b_and_one_small_model(self):
        nodes = [
            inventory_node("node-24a", 24576),
            inventory_node("node-24b", 24576),
            inventory_node("node-24c", 24576),
            inventory_node("node-12", 12288),
            inventory_node("node-low", 4096),
        ]
        pool = build_gpu_pool_snapshot(nodes)
        by_node = {slot.node_id: slot for slot in pool.selected_slots}
        values = [
            deployment_candidate(
                "72b-main",
                "qwen2.5-72b-awq",
                tuple(by_node[node_id] for node_id in ("node-24a", "node-24b", "node-24c")),
                quality=72,
                throughput=7,
            ),
            deployment_candidate(
                "14b-small",
                "qwen2.5-14b-awq",
                (by_node["node-12"],),
                quality=14,
                throughput=12,
            ),
            deployment_candidate(
                "7b-alternative",
                "qwen2.5-7b-awq",
                (by_node["node-12"],),
                quality=7,
                throughput=20,
            ),
        ]

        result = schedule_fleet(
            values,
            available_node_ids=[node.node_id for node in pool.nodes],
        )

        self.assertEqual(
            {item.candidate_id for item in result.selected},
            {"72b-main", "14b-small"},
        )
        self.assertEqual(result.score.utilized_node_count, 4)
        self.assertNotIn("node-low", result.used_node_ids)
        self.assertEqual(
            {item.candidate_id: item.code for item in result.rejections},
            {"7b-alternative": "RESOURCE_CONFLICT"},
        )
        self.assertTrue(result.search_complete)
        self.assertFalse(result.search_limit_reached)

    def test_six_nodes_place_two_disjoint_72b_replicas_deterministically(self):
        nodes = [
            inventory_node(f"node-{index}", 24576)
            for index in range(1, 7)
        ]
        by_node = slots_by_node(nodes)
        first = deployment_candidate(
            "72b-a",
            "qwen2.5-72b-awq",
            tuple(by_node[f"node-{index}"] for index in range(1, 4)),
            quality=72,
            throughput=7,
        )
        second = deployment_candidate(
            "72b-b",
            "qwen2.5-72b-awq",
            tuple(by_node[f"node-{index}"] for index in range(4, 7)),
            quality=72,
            throughput=7,
        )
        overlapping = deployment_candidate(
            "72b-overlap",
            "qwen2.5-72b-awq",
            tuple(by_node[f"node-{index}"] for index in (2, 3, 4)),
            quality=72,
            throughput=6,
        )
        single_gpu_alternatives = [
            deployment_candidate(
                f"32b-{index}",
                "qwen2.5-32b-awq",
                (by_node[f"node-{index}"],),
                quality=32,
                throughput=20,
            )
            for index in range(1, 7)
        ]
        values = [first, second, overlapping, *single_gpu_alternatives]

        forward = schedule_fleet(values)
        reverse = schedule_fleet(reversed(values))

        self.assertEqual(forward.to_dict(), reverse.to_dict())
        self.assertEqual(
            {item.candidate_id for item in forward.selected},
            {"72b-a", "72b-b"},
        )
        selected_bindings = [
            binding
            for candidate in forward.selected
            for binding in candidate.bindings
        ]
        self.assertEqual(
            len(selected_bindings),
            len({binding.node_id for binding in selected_bindings}),
        )
        self.assertEqual(
            len(selected_bindings),
            len({binding.gpu_uuid for binding in selected_bindings}),
        )
        for candidate in forward.selected:
            self.assertEqual(candidate.tensor_parallel_size, 1)
            self.assertEqual(
                len(candidate.bindings), candidate.pipeline_parallel_size
            )

    def test_one_and_128_node_pools_have_no_product_node_count_branch(self):
        one_node = inventory_node("single-node", 8192)
        one_slot = slots_by_node([one_node])[one_node.node_id]
        one = schedule_fleet(
            [
                deployment_candidate(
                    "single-7b",
                    "qwen2.5-7b-awq",
                    (one_slot,),
                    quality=7,
                    throughput=20,
                )
            ]
        )
        self.assertEqual(one.used_node_ids, ("single-node",))

        memory_sizes = (8192, 12288, 24576, 49152, 81920)
        nodes = [
            inventory_node(
                f"scale-node-{index:03d}",
                memory_sizes[index % len(memory_sizes)],
            )
            for index in range(128)
        ]
        snapshot = build_gpu_pool_snapshot(nodes)
        model_by_memory = {
            8192: ("qwen2.5-7b-awq", 7),
            12288: ("qwen2.5-14b-awq", 14),
            24576: ("qwen2.5-32b-awq", 32),
            49152: ("qwen2.5-72b-awq", 72),
            81920: ("qwen2.5-72b-awq", 72),
        }
        values = []
        for slot in snapshot.selected_slots:
            model_id, quality = model_by_memory[slot.memory_mib]
            values.append(
                deployment_candidate(
                    f"scale-{slot.node_id}",
                    model_id,
                    (slot,),
                    quality=quality,
                    throughput=quality,
                )
            )
        shuffled = list(values)
        random.Random(20260721).shuffle(shuffled)

        forward = schedule_fleet(
            shuffled,
            available_node_ids=[node.node_id for node in nodes],
        )
        reverse = schedule_fleet(
            reversed(shuffled),
            available_node_ids=reversed([node.node_id for node in nodes]),
        )

        self.assertEqual(forward.to_dict(), reverse.to_dict())
        self.assertEqual(len(snapshot.selected_slots), 128)
        self.assertEqual(len(forward.selected), 128)
        self.assertEqual(forward.score.utilized_node_count, 128)
        self.assertTrue(forward.search_complete)
        self.assertFalse(forward.search_limit_reached)
        self.assertEqual(
            {item.model_id for item in forward.selected}, EXPECTED_MODEL_IDS
        )
        self.assertTrue(
            all(item.tensor_parallel_size == 1 for item in forward.selected)
        )

    def test_low_vram_is_not_a_pool_error_and_reasons_remain_structured(self):
        low = inventory_node("node-low", 4096)
        pending = replace(inventory_node("node-pending", 24576), approved=False)
        runtime = inventory_node("node-runtime", 24576)
        runtime.profile.runtime.engine_ready = False
        occupied = inventory_node("node-occupied", 24576)
        usable = inventory_node("node-usable", 8192)

        snapshot = build_gpu_pool_snapshot(
            [low, pending, runtime, occupied, usable],
            occupied_node_ids=["node-occupied"],
            occupancy_reasons={"node-occupied": "FLEET_RESERVATION"},
        )
        by_node = {node.node_id: node for node in snapshot.nodes}

        self.assertIsNotNone(by_node["node-low"].selected_gpu)
        self.assertIsNone(by_node["node-low"].unavailable_reason)
        self.assertEqual(by_node["node-pending"].unavailable_reason, "NODE_PENDING")
        self.assertEqual(
            by_node["node-runtime"].unavailable_reason, "RUNTIME_UNAVAILABLE"
        )
        self.assertEqual(
            by_node["node-occupied"].unavailable_reason, "NODE_OCCUPIED"
        )
        self.assertEqual(
            by_node["node-occupied"].occupancy_reason, "FLEET_RESERVATION"
        )
        for node_id in ("node-pending", "node-runtime", "node-occupied"):
            serialized = by_node[node_id].to_dict()
            self.assertIsInstance(serialized["unavailable_reason"], str)
            self.assertIsNone(serialized["selected_gpu"])

        available_slots = {
            slot.node_id: slot for slot in snapshot.selected_slots
        }
        result = schedule_fleet(
            [
                deployment_candidate(
                    "usable-7b",
                    "qwen2.5-7b-awq",
                    (available_slots["node-usable"],),
                    quality=7,
                    throughput=20,
                )
            ],
            available_node_ids=available_slots,
        )
        self.assertEqual(result.used_node_ids, ("node-usable",))
        self.assertNotIn("node-low", result.used_node_ids)

    def test_candidate_and_search_limits_are_bounded_and_observable(self):
        self.assertEqual(DEFAULT_MAX_CANDIDATES, MAX_SAFE_RECURSIVE_CANDIDATES)
        slots = slots_by_node(
            [
                inventory_node(f"limit-node-{index:03d}", 8192)
                for index in range(DEFAULT_MAX_CANDIDATES + 1)
            ]
        )
        values = [
            deployment_candidate(
                f"limit-{index:03d}",
                "qwen2.5-7b-awq",
                (slots[f"limit-node-{index:03d}"],),
                quality=7,
                throughput=20,
            )
            for index in range(DEFAULT_MAX_CANDIDATES + 1)
        ]

        accepted_boundary = schedule_fleet(values[:DEFAULT_MAX_CANDIDATES])
        self.assertEqual(
            len(accepted_boundary.selected), DEFAULT_MAX_CANDIDATES
        )
        self.assertTrue(accepted_boundary.search_complete)

        with self.assertRaises(FleetSchedulingLimitError) as raised:
            schedule_fleet(values)
        self.assertEqual(raised.exception.code, "FLEET_CANDIDATE_LIMIT")
        with self.assertRaisesRegex(ValueError, "safe recursive search limit"):
            schedule_fleet(
                values[:1],
                max_candidates=MAX_SAFE_RECURSIVE_CANDIDATES + 1,
            )

        bounded = schedule_fleet(values[:8], max_search_states=1)
        self.assertEqual(bounded.explored_states, 1)
        self.assertFalse(bounded.search_complete)
        self.assertTrue(bounded.search_limit_reached)
        self.assertEqual(len(bounded.selected), 8)


if __name__ == "__main__":
    unittest.main()
