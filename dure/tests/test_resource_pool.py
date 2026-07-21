from __future__ import annotations

import copy
import unittest
from dataclasses import replace

from dure.models import GPUProfile
from dure.resource_pool import build_gpu_pool_snapshot
from dure.selector import InventoryNode

from .helpers import profile


class GpuPoolSnapshotTests(unittest.TestCase):
    def inventory(self, node_id: str, *, memory_mib: int = 24576) -> InventoryNode:
        return InventoryNode.local(profile(node_id, gpu_memory_mib=memory_mib))

    def test_selects_exactly_one_gpu_by_memory_then_uuid(self):
        node = self.inventory("node-a", memory_mib=24576)
        same_memory = copy.deepcopy(node.profile.gpus[0])
        same_memory.index = 2
        same_memory.uuid = "GPU-000-node-a"
        smaller = GPUProfile(
            index=3,
            name="smaller",
            uuid="GPU-smaller",
            driver_version="610.43.02",
            memory_mib=12288,
            compute_capability="8.6",
        )
        node.profile.gpus.extend([same_memory, smaller])

        snapshot = build_gpu_pool_snapshot([node])

        self.assertEqual(len(snapshot.selected_slots), 1)
        self.assertEqual(snapshot.selected_slots[0].gpu_index, 2)
        self.assertEqual(snapshot.selected_slots[0].gpu_uuid, "GPU-000-node-a")

    def test_snapshot_is_permutation_invariant_and_has_no_node_limit(self):
        nodes = [self.inventory(f"node-{index:04d}") for index in range(128)]

        forward = build_gpu_pool_snapshot(nodes)
        reverse = build_gpu_pool_snapshot(reversed(nodes))

        self.assertEqual(forward.to_dict(), reverse.to_dict())
        self.assertEqual(len(forward.selected_slots), 128)

    def test_unavailable_and_occupied_nodes_keep_structured_reasons(self):
        pending = self.inventory("pending")
        pending = replace(pending, approved=False)
        occupied = self.inventory("occupied")
        low_runtime = self.inventory("runtime")
        low_runtime.profile.runtime.engine_ready = False

        snapshot = build_gpu_pool_snapshot(
            [pending, occupied, low_runtime],
            occupied_node_ids=["occupied"],
            occupancy_reasons={"occupied": "FLEET_RESERVATION"},
            network_zones={"occupied": "zone-a"},
        )

        reasons = {node.node_id: node.unavailable_reason for node in snapshot.nodes}
        self.assertEqual(reasons["pending"], "NODE_PENDING")
        self.assertEqual(reasons["occupied"], "NODE_OCCUPIED")
        self.assertEqual(reasons["runtime"], "RUNTIME_UNAVAILABLE")
        self.assertEqual(snapshot.selected_slots, ())
        occupied_snapshot = next(
            node for node in snapshot.nodes if node.node_id == "occupied"
        )
        self.assertEqual(occupied_snapshot.occupancy_reason, "FLEET_RESERVATION")
        self.assertEqual(occupied_snapshot.network_zone, "zone-a")

    def test_duplicate_gpu_identity_is_unavailable(self):
        node = self.inventory("node-a")
        duplicate = copy.deepcopy(node.profile.gpus[0])
        duplicate.index = 2
        node.profile.gpus.append(duplicate)

        snapshot = build_gpu_pool_snapshot([node])

        self.assertEqual(
            snapshot.nodes[0].unavailable_reason, "GPU_IDENTITY_DUPLICATE"
        )
        self.assertEqual(snapshot.selected_slots, ())

    def test_duplicate_nodes_are_rejected(self):
        node = self.inventory("duplicate")

        with self.assertRaisesRegex(ValueError, "duplicate inventory"):
            build_gpu_pool_snapshot([node, node])


if __name__ == "__main__":
    unittest.main()
