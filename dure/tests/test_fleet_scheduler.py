from __future__ import annotations

import hashlib
import random
import unittest
from dataclasses import replace

from dure.fleet_scheduler import (
    FleetDeploymentCandidate,
    FleetGpuBinding,
    FleetSchedulingError,
    FleetSchedulingLimitError,
    schedule_fleet,
)


def candidate(
    candidate_id: str,
    model_id: str,
    nodes: tuple[str, ...],
    *,
    quality: float,
    throughput: float,
    cache_hits: int = 0,
    zone: str = "zone-a",
    zone_penalty: float = 0.0,
    imbalance: float = 0.0,
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
                node_id=node_id,
                gpu_index=0,
                gpu_uuid=f"GPU-{node_id}",
                rank=rank,
            )
            for rank, node_id in enumerate(nodes)
        ),
        tensor_parallel_size=1,
        pipeline_parallel_size=len(nodes),
        quality_score=quality,
        throughput_tps=throughput,
        cache_hit_count=cache_hits,
        network_zone=zone,
        zone_penalty=zone_penalty,
        imbalance_score=imbalance,
    )


class FleetSchedulerTests(unittest.TestCase):
    def test_selects_multiple_disjoint_deployments_without_node_reuse(self):
        candidates = [
            candidate(
                "candidate-a",
                "qwen2.5-72b-awq",
                ("node-01", "node-02", "node-03"),
                quality=72,
                throughput=30,
            ),
            candidate(
                "candidate-b",
                "qwen2.5-72b-awq",
                ("node-04", "node-05", "node-06"),
                quality=72,
                throughput=30,
            ),
            candidate(
                "candidate-c",
                "qwen2.5-32b-awq",
                ("node-07",),
                quality=32,
                throughput=20,
            ),
            candidate(
                "candidate-d",
                "qwen2.5-32b-awq",
                ("node-08",),
                quality=32,
                throughput=20,
            ),
            candidate(
                "candidate-e",
                "qwen2.5-72b-awq",
                ("node-01", "node-04", "node-07"),
                quality=72,
                throughput=20,
                imbalance=5,
            ),
            *[
                candidate(
                    f"candidate-small-{index}",
                    "qwen2.5-32b-awq",
                    (f"node-{index:02d}",),
                    quality=32,
                    throughput=20,
                )
                for index in range(1, 7)
            ],
        ]

        result = schedule_fleet(candidates)

        self.assertEqual(
            {item.candidate_id for item in result.selected},
            {"candidate-a", "candidate-b", "candidate-c", "candidate-d"},
        )
        self.assertEqual(result.score.utilized_node_count, 8)
        self.assertEqual(len(result.used_node_ids), len(set(result.used_node_ids)))
        rejected = {item.candidate_id: item for item in result.rejections}
        self.assertEqual(rejected["candidate-e"].code, "RESOURCE_CONFLICT")

    def test_minimum_replica_policy_precedes_quality(self):
        large = candidate(
            "candidate-a",
            "qwen2.5-72b-awq",
            ("node-a", "node-b", "node-c"),
            quality=1000,
            throughput=1000,
        )
        small = [
            candidate(
                f"candidate-{suffix}",
                "qwen2.5-7b-awq",
                (node_id,),
                quality=1,
                throughput=1,
            )
            for suffix, node_id in zip("bcd", ("node-a", "node-b", "node-c"))
        ]

        result = schedule_fleet(
            [large, *small],
            minimum_replicas={"qwen2.5-7b-awq": 3},
        )

        self.assertEqual(
            {item.candidate_id for item in result.selected},
            {"candidate-b", "candidate-c", "candidate-d"},
        )
        self.assertTrue(result.score.all_minimum_replicas_met)
        self.assertEqual(result.unmet_minimum_replicas, ())

    def test_quality_first_prefers_one_72b_over_many_lower_quality_models(self):
        large = candidate(
            "candidate-large",
            "qwen2.5-72b-awq",
            ("node-a", "node-b", "node-c"),
            quality=72,
            throughput=5,
        )
        smaller = [
            candidate(
                f"candidate-small-{node_id}",
                "qwen2.5-32b-awq",
                (node_id,),
                quality=32,
                throughput=100,
            )
            for node_id in ("node-a", "node-b", "node-c")
        ]

        result = schedule_fleet([large, *smaller])

        self.assertEqual(
            [item.candidate_id for item in result.selected],
            ["candidate-large"],
        )
        self.assertEqual(result.score.quality_vector, (72.0,))

    def test_score_order_and_uuid_tie_break_are_deterministic(self):
        higher_throughput = candidate(
            "candidate-z",
            "qwen2.5-14b-awq",
            ("node-z",),
            quality=14,
            throughput=20,
        )
        lower_throughput = candidate(
            "candidate-a",
            "qwen2.5-14b-awq",
            ("node-a",),
            quality=14,
            throughput=10,
        )
        # Force the alternatives to conflict by GPU UUID while their node UUIDs differ.
        lower_throughput = replace(
            lower_throughput,
            bindings=(replace(lower_throughput.bindings[0], gpu_uuid="GPU-node-z"),),
        )
        result = schedule_fleet([lower_throughput, higher_throughput])
        self.assertEqual(result.selected[0].candidate_id, "candidate-z")

        tied_a = replace(lower_throughput, throughput_tps=20)
        forward = schedule_fleet([higher_throughput, tied_a]).to_dict()
        reverse = schedule_fleet([tied_a, higher_throughput]).to_dict()
        self.assertEqual(forward, reverse)
        self.assertEqual(forward["selected"][0]["candidate_id"], "candidate-a")

    def test_candidate_contract_fails_closed(self):
        valid = candidate(
            "candidate-a",
            "qwen2.5-7b-awq",
            ("node-a",),
            quality=7,
            throughput=10,
        )
        cases = (
            (replace(valid, model_id="unknown"), "FLEET_MODEL_NOT_ALLOWED"),
            (replace(valid, tensor_parallel_size=2), "FLEET_TP_UNSUPPORTED"),
            (replace(valid, pipeline_parallel_size=2), "FLEET_BINDING_INVALID"),
            (
                replace(
                    valid,
                    bindings=(valid.bindings[0], valid.bindings[0]),
                    pipeline_parallel_size=2,
                ),
                "FLEET_BINDING_DUPLICATE",
            ),
            (replace(valid, evidence_digest="sha256:bad"), "FLEET_EVIDENCE_INVALID"),
        )
        for invalid, code in cases:
            with self.subTest(code=code):
                with self.assertRaises(FleetSchedulingError) as raised:
                    schedule_fleet([invalid])
                self.assertEqual(raised.exception.code, code)

        conflicting_evidence = replace(
            valid,
            candidate_id="candidate-b",
            bindings=(
                replace(
                    valid.bindings[0],
                    node_id="node-b",
                    gpu_uuid="GPU-node-b",
                ),
            ),
        )
        with self.assertRaises(FleetSchedulingError) as raised:
            schedule_fleet([valid, conflicting_evidence])
        self.assertEqual(raised.exception.code, "FLEET_EVIDENCE_CONFLICT")

    def test_large_node_pool_has_no_product_node_limit(self):
        candidates = [
            candidate(
                f"candidate-{index:03x}",
                "qwen2.5-7b-awq",
                (f"node-{index:04d}",),
                quality=7,
                throughput=1,
            )
            for index in range(200)
        ]
        shuffled = list(candidates)
        random.Random(20260721).shuffle(shuffled)

        result = schedule_fleet(
            shuffled,
            available_node_ids=[f"node-{index:04d}" for index in range(1000)],
        )

        self.assertEqual(len(result.selected), 200)
        self.assertEqual(result.score.utilized_node_count, 200)
        self.assertLess(result.explored_states, 1000)

    def test_hundred_heterogeneous_nodes_choose_best_single_gpu_profile(self):
        candidates = []
        for index in range(100):
            node_id = f"node-{index:04d}"
            for model_id, quality in (
                ("qwen2.5-7b-awq", 7),
                ("qwen2.5-14b-awq", 14),
                ("qwen2.5-32b-awq", 32),
            ):
                candidates.append(
                    candidate(
                        f"candidate-{index:04d}-{quality}",
                        model_id,
                        (node_id,),
                        quality=quality,
                        throughput=quality,
                    )
                )

        result = schedule_fleet(candidates)

        self.assertEqual(len(result.selected), 100)
        self.assertEqual(
            {item.model_id for item in result.selected},
            {"qwen2.5-32b-awq"},
        )
        self.assertLess(result.explored_states, 10_000)

    def test_candidate_and_search_limits_are_explicit(self):
        values = [
            candidate(
                f"candidate-{index}",
                "qwen2.5-7b-awq",
                (f"node-{index}",),
                quality=7,
                throughput=1,
            )
            for index in range(3)
        ]
        with self.assertRaises(FleetSchedulingLimitError) as candidate_limit:
            schedule_fleet(values, max_candidates=2)
        self.assertEqual(candidate_limit.exception.code, "FLEET_CANDIDATE_LIMIT")

        bounded = schedule_fleet(values, max_search_states=1)
        self.assertEqual(len(bounded.selected), 3)
        self.assertFalse(bounded.search_complete)
        self.assertTrue(bounded.search_limit_reached)
        self.assertEqual(bounded.explored_states, 1)

    def test_dense_overlapping_candidates_return_deterministic_bounded_result(self):
        values = [
            candidate(
                f"candidate-{index:03d}",
                "qwen2.5-72b-awq",
                (
                    f"node-{index:03d}",
                    f"node-{(index + 1) % 100:03d}",
                    f"node-{(index + 2) % 100:03d}",
                ),
                quality=72,
                throughput=10,
            )
            for index in range(100)
        ]

        forward = schedule_fleet(values, max_search_states=500)
        reverse = schedule_fleet(list(reversed(values)), max_search_states=500)

        self.assertEqual(forward.to_dict(), reverse.to_dict())
        self.assertTrue(forward.search_limit_reached)
        self.assertFalse(forward.search_complete)
        self.assertGreater(len(forward.selected), 0)
        bound_nodes = [
            binding.node_id
            for deployment in forward.selected
            for binding in deployment.bindings
        ]
        self.assertEqual(len(bound_nodes), len(set(bound_nodes)))

    def test_bounded_fallback_preserves_a_feasible_minimum_replica_witness(self):
        nodes = [f"node-{index:03d}" for index in range(100)]
        good_bindings = [
            (nodes[index], nodes[33 + 2 * index], nodes[34 + 2 * index])
            for index in range(33)
        ]
        values = []
        for block in range(11):
            bad_nodes = tuple(nodes[3 * block : 3 * block + 3])
            for variant in range(6):
                values.append(
                    candidate(
                        f"bad-{block:02d}-{variant}",
                        "qwen2.5-72b-awq",
                        bad_nodes,
                        quality=72,
                        throughput=10,
                    )
                )
        for index, binding_nodes in enumerate(good_bindings):
            values.append(
                candidate(
                    f"good-{index:02d}",
                    "qwen2.5-72b-awq",
                    binding_nodes,
                    quality=72,
                    throughput=10,
                )
            )

        result = schedule_fleet(
            values,
            minimum_replicas={"qwen2.5-72b-awq": 33},
            available_node_ids=nodes,
            max_search_states=1,
        )

        self.assertTrue(result.score.all_minimum_replicas_met)
        self.assertEqual(result.score.fulfilled_minimum_replicas, 33)
        self.assertEqual(len(result.selected), 33)
        self.assertFalse(result.search_complete)

    def test_reserve_and_locality_break_later_ties(self):
        reserve = candidate(
            "candidate-r",
            "qwen2.5-7b-awq",
            ("node-reserve",),
            quality=7,
            throughput=10,
            cache_hits=1,
        )
        ordinary = candidate(
            "candidate-o",
            "qwen2.5-7b-awq",
            ("node-ordinary",),
            quality=7,
            throughput=10,
            cache_hits=0,
        )
        ordinary = replace(
            ordinary,
            bindings=(replace(ordinary.bindings[0], gpu_uuid="GPU-shared"),),
        )
        reserve = replace(
            reserve,
            bindings=(replace(reserve.bindings[0], gpu_uuid="GPU-shared"),),
        )

        result = schedule_fleet(
            [reserve, ordinary],
            available_node_ids=["node-reserve", "node-ordinary"],
            reserve_node_ids=["node-reserve"],
        )

        self.assertEqual(result.selected[0].candidate_id, "candidate-o")
        self.assertTrue(result.score.reserve_policy_met)

    def test_imbalance_then_cache_then_zone_break_ties(self):
        baseline = candidate(
            "candidate-a",
            "qwen2.5-7b-awq",
            ("node-a",),
            quality=7,
            throughput=10,
        )
        competing = candidate(
            "candidate-b",
            "qwen2.5-7b-awq",
            ("node-b",),
            quality=7,
            throughput=10,
        )
        competing = replace(
            competing,
            bindings=(replace(competing.bindings[0], gpu_uuid="GPU-node-a"),),
        )

        lower_imbalance = replace(baseline, imbalance_score=1, cache_hit_count=1)
        result = schedule_fleet([lower_imbalance, competing])
        self.assertEqual(result.selected[0].candidate_id, "candidate-b")

        cached = replace(baseline, cache_hit_count=1, zone_penalty=10)
        result = schedule_fleet([cached, competing])
        self.assertEqual(result.selected[0].candidate_id, "candidate-a")

        same_cache = replace(competing, cache_hit_count=1, zone_penalty=1)
        result = schedule_fleet([replace(cached, zone_penalty=2), same_cache])
        self.assertEqual(result.selected[0].candidate_id, "candidate-b")


if __name__ == "__main__":
    unittest.main()
