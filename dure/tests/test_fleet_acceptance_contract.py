from __future__ import annotations

import copy
import unittest

from dure.control.fleet_acceptance import (
    FleetAcceptanceError,
    _assert_plan_candidate_identity,
    _candidate_bindings,
)
from dure.control.fleet import _plan_contract_digest
from dure.models import (
    DeploymentPlan,
    ModelSpec,
    NodeAssignment,
    StageArtifactBinding,
)
from dure.stage_cache import stage_contract_identity_digest


def _digest(character: str) -> str:
    return "sha256:" + character * 64


class FleetAcceptanceContractTests(unittest.TestCase):
    def _stage_candidate_and_plan(self):
        nodes = [
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        ]
        runtime_image = "registry.example/vllm@sha256:" + "1" * 64
        source_manifest = _digest("2")
        exporter_digest = _digest("3")
        stage = StageArtifactBinding(
            artifact_set_digest=_digest("4"),
            contract_identity_digest=stage_contract_identity_digest(
                source_manifest_digest=source_manifest,
                runtime_image=runtime_image,
                vllm_version="0.9.0",
                exporter_build_digest=exporter_digest,
                architecture="Qwen2ForCausalLM",
                quantization="awq",
                tensor_parallel_size=1,
                pipeline_parallel_size=2,
                loader_format="VLLM_SHARDED_STATE_V1",
            ),
            source_manifest_digest=source_manifest,
            runtime_image=runtime_image,
            vllm_version="0.9.0",
            exporter_build_digest=exporter_digest,
            architecture="Qwen2ForCausalLM",
            quantization="awq",
            tensor_parallel_size=1,
            pipeline_parallel_size=2,
            loader_format="VLLM_SHARDED_STATE_V1",
        )
        bindings = [
            {
                "node_id": node_id,
                "gpu_index": 0,
                "gpu_uuid": f"GPU-stage-{rank}",
                "rank": rank,
            }
            for rank, node_id in enumerate(nodes)
        ]
        stage_ranks = [
            {
                "rank": rank,
                "manifest_digest": _digest(str(5 + rank)),
                "tensor_keys_digest": _digest(str(7 + rank)),
            }
            for rank in range(2)
        ]
        candidate = {
            "candidate_id": _digest("9"),
            "model_id": "qwen2.5-72b-awq",
            "artifact_repository": "Qwen/Qwen2.5-72B-Instruct-AWQ",
            "artifact_revision": "a" * 40,
            "artifact_manifest_digest": source_manifest,
            "quantization": "awq",
            "runtime_image": runtime_image,
            "execution_backend": "VLLM_RAY_PP_V1",
            "runtime_vllm_version": "0.9.0",
            "placement_id": "33333333-3333-4333-8333-333333333333",
            "tensor_parallel_size": 1,
            "pipeline_parallel_size": 2,
            "bindings": bindings,
            "gpu_bindings": copy.deepcopy(bindings),
            "rank_node_ids": nodes,
            "model_cache_kind": "STAGE",
            "stage_artifact": stage.to_dict(),
            "stage_ranks": stage_ranks,
            "stage_node_bindings": [
                {"node_id": nodes[rank], **stage_ranks[rank]}
                for rank in range(2)
            ],
        }
        deployment_id = "44444444-4444-4444-8444-444444444444"
        plan = DeploymentPlan(
            deployment_id=deployment_id,
            generation=1,
            model=ModelSpec(
                model_id=candidate["model_id"],
                repository=candidate["artifact_repository"],
                quantization="awq",
                checkpoint_gib=40.0,
                min_gpu_memory_gib=24.0,
                default_max_model_len=8192,
                layer_count=28,
            ),
            image=runtime_image,
            pipeline_parallel_size=2,
            tensor_parallel_size=1,
            ray_head_node_id=nodes[0],
            ray_head_address="10.0.0.2:6379",
            network_interface="eth0",
            model_revision=candidate["artifact_revision"],
            model_path="/var/lib/dure/models/stages",
            assignments=[
                NodeAssignment(
                    node_id=nodes[rank],
                    gpu_index=0,
                    gpu_uuid=f"GPU-stage-{rank}",
                    rank=rank,
                    pipeline_rank=rank,
                    layer_start=rank * 14,
                    layer_end=rank * 14 + 13,
                    role="ray-head" if rank == 0 else "ray-worker",
                    expected_runtime_rank=rank,
                    runtime_address=f"10.0.0.{rank + 2}",
                    stage_manifest_digest=stage_ranks[rank][
                        "manifest_digest"
                    ],
                    stage_tensor_keys_digest=stage_ranks[rank][
                        "tensor_keys_digest"
                    ],
                )
                for rank in range(2)
            ],
            execution_backend="VLLM_RAY_PP_V1",
            runtime_vllm_version="0.9.0",
            model_cache_kind="STAGE",
            stage_artifact=stage,
        ).to_dict()
        candidate["plan_contract_digest"] = _plan_contract_digest(plan)
        return candidate, bindings, deployment_id, plan

    def test_stage_candidate_and_plan_preserve_exact_rank_identity(self):
        candidate, bindings, deployment_id, plan = (
            self._stage_candidate_and_plan()
        )

        self.assertEqual(_candidate_bindings(candidate), bindings)
        _assert_plan_candidate_identity(
            plan=plan,
            candidate=candidate,
            bindings=bindings,
            deployment_id=deployment_id,
            generation=1,
        )

    def test_stage_rank_or_runtime_drift_is_rejected(self):
        candidate, bindings, deployment_id, plan = (
            self._stage_candidate_and_plan()
        )
        changed_candidate = copy.deepcopy(candidate)
        changed_candidate["stage_node_bindings"][1][
            "manifest_digest"
        ] = _digest("0")
        with self.assertRaises(FleetAcceptanceError):
            _candidate_bindings(changed_candidate)

        changed_plan = copy.deepcopy(plan)
        changed_plan["image"] = "registry.example/other@sha256:" + "f" * 64
        with self.assertRaises(FleetAcceptanceError) as raised:
            _assert_plan_candidate_identity(
                plan=changed_plan,
                candidate=candidate,
                bindings=bindings,
                deployment_id=deployment_id,
                generation=1,
            )
        self.assertEqual(
            raised.exception.code,
            "FLEET_GENERATION_PLAN_INVALID",
        )

    def test_every_valid_runtime_plan_field_is_bound_by_the_contract_digest(self):
        candidate, bindings, deployment_id, plan = (
            self._stage_candidate_and_plan()
        )
        mutations = (
            ("network_interface", "ens5"),
            ("max_model_len", 4096),
            ("gpu_memory_utilization", 0.85),
        )
        for field, value in mutations:
            with self.subTest(field=field):
                changed = copy.deepcopy(plan)
                changed[field] = value
                with self.assertRaises(FleetAcceptanceError) as raised:
                    _assert_plan_candidate_identity(
                        plan=changed,
                        candidate=candidate,
                        bindings=bindings,
                        deployment_id=deployment_id,
                        generation=1,
                    )
                self.assertEqual(
                    raised.exception.code,
                    "FLEET_GENERATION_IDENTITY_MISMATCH",
                )


if __name__ == "__main__":
    unittest.main()
