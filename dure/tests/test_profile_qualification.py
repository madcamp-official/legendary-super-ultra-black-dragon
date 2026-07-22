from __future__ import annotations

import copy
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import func, select

from dure.control.benchmark import promote_model_release
from dure.control.db import Base, make_engine, make_session_factory
from dure.control.fleet import FleetEvaluationError, evaluate_fleet_schedule
from dure.control.fleet_acceptance import (
    FleetAcceptanceError,
    accept_fleet_recommendation,
)
from dure.control.fleet_recommendation import recommend_fleet
from dure.control.models import (
    Deployment,
    DeploymentRecommendationRecord,
    FleetRecommendationRecord,
    FleetRecord,
    FleetResourceReservation,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    ProfileQualificationEvidence,
    ProfileQualificationRun,
    Task,
    TaskStatus,
    TaskType,
    utcnow,
)
from dure.control.qualification import (
    QUALIFICATION_STEPS,
    ProfileQualificationError,
    _validate_steps,
    activate_validated_profile,
    cancel_profile_qualification,
    prepare_profile_qualification,
    register_profile_qualification_evidence,
    validate_profile_qualification_evidence,
)
from dure.control.recommendation import evaluate_deployment_recommendation
from dure.control.service import (
    BenchmarkRunError,
    apply_benchmark_run,
    create_model_artifact,
    create_model_release,
    create_runtime_release,
    generate_auto_placement_profiles,
    prepare_benchmark_run,
    transition_model_release,
)

from .helpers import profile


def _passing_steps() -> list[dict]:
    return [
        {"step_id": step, "status": "PASSED", "failure_code": None}
        for step in QUALIFICATION_STEPS
    ]


def _passing_metrics(run: dict, *, multi_node: bool) -> dict:
    workload = run["workload"]
    return {
        "model_load_seconds": 30.0,
        "request_count": 200,
        "restart_count": 1,
        "max_model_len": run["max_model_len"],
        "concurrency": run["max_concurrency"],
        "input_tokens": workload["input_tokens"],
        "output_tokens": workload["output_tokens"],
        "warmup_requests": workload["warmup_requests"],
        "ttft_p95_ms": 100.0,
        "tpot_p95_ms": 20.0,
        "e2e_p95_ms": 1000.0,
        "throughput_tps": 20.0,
        "success_rate": 1.0,
        "vram_headroom_pct": 20.0,
        "network_bandwidth_mbps": 20000.0 if multi_node else None,
        "network_rtt_ms": 0.5 if multi_node else None,
        "packet_loss_pct": 0.0 if multi_node else None,
        "nccl_all_reduce_ok": True if multi_node else None,
    }


class ProfileQualificationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.engine = make_engine(
            f"sqlite:///{Path(self.temporary.name) / 'qualification.db'}"
        )
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self):
        self.engine.dispose()
        self.temporary.cleanup()

    def _release(self, session, model_id: str):
        size_mib, context, layers, manifest_digit, quality_rank = {
            "qwen2.5-7b-awq": (4916, 8192, 28, "b", 7),
            "qwen2.5-14b-awq": (9728, 8192, 48, "e", 14),
            "qwen2.5-32b-awq": (19968, 4096, 64, "d", 32),
            "qwen2.5-72b-awq": (39670, 8192, 80, "f", 72),
        }[model_id]
        artifact = create_model_artifact(
            session,
            model_id=model_id,
            repository=f"Qwen/{model_id}",
            revision="a" * 40,
            manifest_digest="sha256:" + manifest_digit * 64,
            quantization="awq",
            size_mib=size_mib,
            default_max_model_len=context,
            layer_count=layers,
            license_id="apache-2.0",
        )
        runtime = create_runtime_release(
            session,
            version=f"vllm-{model_id}",
            image=f"registry.example/{model_id}@sha256:" + "c" * 64,
            vllm_version="0.9.0",
            cuda_version="12.8",
            gpu_architectures=["ampere"],
        )
        release = create_model_release(
            session,
            artifact_id=artifact.id,
            runtime_id=runtime.id,
            quality_rank=quality_rank,
        )
        generate_auto_placement_profiles(
            session, release_id=release.id, apply=True
        )
        return release

    def _nodes(self, session, count: int) -> list[str]:
        now = utcnow()
        node_ids = []
        for index in range(count):
            node = Node(
                install_id=f"qualification-{index}-{uuid.uuid4()}",
                display_name=f"qualification-{index}",
                hostname=f"qualification-{index}",
                agent_version="0.4.2",
                approved=True,
                last_seen=now,
            )
            session.add(node)
            session.flush()
            observed = profile(
                f"agent-reported-{index}",
                address=f"10.20.0.{index + 10}",
            ).to_dict()
            session.add(
                NodeProfileRecord(
                    node_id=node.id,
                    profile=observed,
                    updated_at=now,
                )
            )
            node_ids.append(node.id)
        session.commit()
        return sorted(node_ids)

    def _qualify(
        self,
        session,
        placement: PlacementProfileRecord,
        node_ids: list[str],
        *,
        purpose: str = "PRIMARY",
    ):
        run, created = prepare_profile_qualification(
            session,
            request_id=str(uuid.uuid4()),
            placement_id=placement.id,
            node_ids=node_ids,
            purpose=purpose,
            apply=True,
        )
        self.assertTrue(created)
        evidence, _, created = register_profile_qualification_evidence(
            session,
            run_id=run["id"],
            steps=_passing_steps(),
            metrics=_passing_metrics(
                run,
                multi_node=placement.node_count > 1,
            ),
            executor_image=(
                "registry.example/qualification@sha256:" + "d" * 64
            ),
            dure_commit="e" * 40,
        )
        self.assertTrue(created)
        return evidence

    def test_fleet_scheduler_combines_disjoint_exact_evidence_sets(self):
        with self.factory() as session:
            large_release = self._release(session, "qwen2.5-72b-awq")
            small_release = self._release(session, "qwen2.5-32b-awq")
            large = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == large_release.id,
                    PlacementProfileRecord.pipeline_parallel_size == 3,
                )
            )
            small = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == small_release.id,
                )
            )
            node_ids = self._nodes(session, 9)

            self._qualify(session, large, node_ids[0:3])
            activate_validated_profile(session, large.id)
            self._qualify(
                session,
                large,
                node_ids[3:6],
                purpose="SUPPLEMENTARY",
            )
            self._qualify(session, small, [node_ids[6]])
            activate_validated_profile(session, small.id)
            self._qualify(
                session,
                small,
                [node_ids[7]],
                purpose="SUPPLEMENTARY",
            )
            low_profile = session.get(NodeProfileRecord, node_ids[8])
            changed = copy.deepcopy(low_profile.profile)
            changed["gpus"][0]["memory_mib"] = 4096
            low_profile.profile = changed
            low_profile.updated_at = utcnow()
            session.commit()

            for release in (large_release, small_release):
                transition_model_release(session, release.id, "VALIDATED")
                promote_model_release(session, release.id)

            result = evaluate_fleet_schedule(
                session,
                node_ids=node_ids,
                all_online=False,
            )
            reversed_result = evaluate_fleet_schedule(
                session,
                node_ids=list(reversed(node_ids)),
                all_online=False,
            )

            self.assertEqual(result, reversed_result)
            selected = result["schedule"]["selected"]
            self.assertEqual(
                [item["model_id"] for item in selected],
                [
                    "qwen2.5-72b-awq",
                    "qwen2.5-72b-awq",
                    "qwen2.5-32b-awq",
                    "qwen2.5-32b-awq",
                ],
            )
            self.assertEqual(
                len(result["schedule"]["used_node_ids"]),
                8,
            )
            self.assertEqual(
                len(result["schedule"]["used_gpu_uuids"]),
                8,
            )
            self.assertEqual(
                result["unassigned_nodes"],
                [
                    {
                        "node_id": node_ids[8],
                        "reason": "NO_VALIDATED_CANDIDATE",
                        "occupancy_reason": None,
                        "candidate_ids": [],
                        "candidate_rejection_codes": [],
                    }
                ],
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)),
                0,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                0,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(
                        DeploymentRecommendationRecord
                    )
                ),
                0,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(
                        FleetRecommendationRecord
                    )
                ),
                0,
            )

            zoned_result = evaluate_fleet_schedule(
                session,
                node_ids=node_ids,
                all_online=False,
                network_zones={
                    node_id: (
                        "zone-a"
                        if index in {0, 1, 2, 6, 7}
                        else "zone-b"
                        if index in {3, 5}
                        else "zone-c"
                    )
                    for index, node_id in enumerate(node_ids)
                },
            )
            self.assertNotEqual(
                result["selected_candidate_ids"],
                zoned_result["selected_candidate_ids"],
            )
            large_candidates = sorted(
                (
                    item
                    for item in zoned_result["candidates"]
                    if item["model_id"] == "qwen2.5-72b-awq"
                ),
                key=lambda item: item["rank_node_ids"],
            )
            self.assertEqual(large_candidates[0]["network_zone"], "zone-a")
            self.assertEqual(large_candidates[0]["zone_penalty"], 0.0)
            self.assertIsNone(large_candidates[1]["network_zone"])
            self.assertEqual(large_candidates[1]["zone_penalty"], 1.0)

            with self.assertRaises(FleetEvaluationError) as invalid_zone:
                evaluate_fleet_schedule(
                    session,
                    node_ids=node_ids,
                    all_online=False,
                    network_zones={node_ids[0]: ""},
                )
            self.assertEqual(
                invalid_zone.exception.code,
                "FLEET_NETWORK_ZONE_INVALID",
            )

            stored = recommend_fleet(
                session,
                node_ids=node_ids,
                all_online=False,
            )
            stored_evaluation = stored["recommendation"]["evaluation"]
            self.assertEqual(stored_evaluation, result)
            self.assertEqual(
                len(stored_evaluation["schedule"]["selected"]),
                4,
            )
            self.assertTrue(
                all(
                    item["evidence_id"]
                    and item["gpu_bindings"]
                    for item in stored_evaluation["schedule"]["selected"]
                )
            )
            selected_candidate_ids = sorted(
                item["candidate_id"]
                for item in stored_evaluation["schedule"]["selected"]
            )
            self.assertEqual(
                selected_candidate_ids,
                stored_evaluation["selected_candidate_ids"],
            )
            self.assertTrue(
                all(
                    candidate_id.startswith("sha256:")
                    and len(candidate_id) == 71
                    for candidate_id in selected_candidate_ids
                )
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(
                        FleetRecommendationRecord
                    )
                ),
                1,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)),
                0,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                0,
            )

            overlapping = recommend_fleet(
                session,
                node_ids=node_ids,
                all_online=False,
                minimum_replicas={"qwen2.5-72b-awq": 2},
            )
            self.assertNotEqual(
                stored["recommendation"]["id"],
                overlapping["recommendation"]["id"],
            )

            stale_node_id = stored_evaluation["schedule"]["selected"][0][
                "bindings"
            ][0]["node_id"]
            stale_profile = session.get(NodeProfileRecord, stale_node_id)
            original_profile = copy.deepcopy(stale_profile.profile)
            original_updated_at = stale_profile.updated_at
            changed = copy.deepcopy(original_profile)
            changed["gpus"][0]["memory_mib"] -= 1
            stale_profile.profile = changed
            stale_profile.updated_at = utcnow()
            session.commit()

            with self.assertRaises(FleetAcceptanceError) as stale:
                accept_fleet_recommendation(
                    session,
                    stored["recommendation"]["id"],
                )
            self.assertEqual(stale.exception.code, "FLEET_RECOMMENDATION_STALE")
            for model in (
                FleetRecord,
                Deployment,
                FleetResourceReservation,
                Task,
            ):
                self.assertEqual(
                    session.scalar(select(func.count()).select_from(model)),
                    0,
                )

            stale_profile = session.get(NodeProfileRecord, stale_node_id)
            stale_profile.profile = original_profile
            stale_profile.updated_at = original_updated_at
            session.commit()

            accepted = accept_fleet_recommendation(
                session,
                stored["recommendation"]["id"],
            )
            self.assertTrue(accepted["created"])
            fleet = accepted["fleet"]
            self.assertEqual(fleet["status"], "ACCEPTED")
            self.assertEqual(
                fleet["source_recommendation_id"],
                stored["recommendation"]["id"],
            )
            self.assertEqual(len(fleet["deployments"]), 4)
            self.assertEqual(len(fleet["reservations"]), 8)
            self.assertEqual(
                sorted(
                    item["fleet_candidate_id"]
                    for item in fleet["deployments"]
                ),
                selected_candidate_ids,
            )
            self.assertTrue(
                all(
                    item["generation"] == 1
                    and item["status"] == "CREATED"
                    and item["plan"]["tensor_parallel_size"] == 1
                    and item["plan"]["generation"] == 1
                    for item in fleet["deployments"]
                )
            )
            reservation_node_ids = [
                item["node_id"] for item in fleet["reservations"]
            ]
            reservation_gpu_uuids = [
                item["gpu_uuid"] for item in fleet["reservations"]
            ]
            self.assertEqual(
                len(reservation_node_ids), len(set(reservation_node_ids))
            )
            self.assertEqual(
                len(reservation_gpu_uuids), len(set(reservation_gpu_uuids))
            )
            self.assertTrue(
                all(item["released_at"] is None for item in fleet["reservations"])
            )

            expected_bindings = {
                (
                    item["node_id"],
                    item["gpu_index"],
                    item["gpu_uuid"],
                    item["rank"],
                )
                for candidate in stored_evaluation["schedule"]["selected"]
                for item in candidate["bindings"]
            }
            self.assertEqual(
                {
                    (
                        item["node_id"],
                        item["gpu_index"],
                        item["gpu_uuid"],
                        item["rank"],
                    )
                    for item in fleet["reservations"]
                },
                expected_bindings,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(FleetRecord)),
                1,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                4,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(FleetResourceReservation)
                ),
                8,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)),
                0,
            )

            repeated = accept_fleet_recommendation(
                session,
                stored["recommendation"]["id"],
            )
            self.assertFalse(repeated["created"])
            self.assertEqual(repeated["fleet"], fleet)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(FleetRecord)),
                1,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                4,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(FleetResourceReservation)
                ),
                8,
            )

            tampered_deployment = session.get(
                Deployment, fleet["deployments"][0]["id"]
            )
            original_plan = copy.deepcopy(tampered_deployment.plan)
            changed_plan = copy.deepcopy(original_plan)
            changed_plan["image"] = (
                "registry.example/tampered@sha256:" + "9" * 64
            )
            tampered_deployment.plan = changed_plan
            session.commit()
            with self.assertRaises(FleetAcceptanceError) as tampered:
                accept_fleet_recommendation(
                    session,
                    stored["recommendation"]["id"],
                )
            self.assertEqual(
                tampered.exception.code,
                "FLEET_GENERATION_IDENTITY_MISMATCH",
            )
            tampered_deployment = session.get(
                Deployment, tampered_deployment.id
            )
            tampered_deployment.plan = original_plan
            session.commit()

            with patch(
                "dure.control.fleet_acceptance.evaluate_fleet_recommendation",
                return_value=overlapping["recommendation"],
            ):
                with self.assertRaises(FleetAcceptanceError) as conflict:
                    accept_fleet_recommendation(
                        session,
                        overlapping["recommendation"]["id"],
                    )
            self.assertEqual(conflict.exception.code, "FLEET_RESOURCE_CONFLICT")
            self.assertEqual(
                session.scalar(select(func.count()).select_from(FleetRecord)),
                1,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                4,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(FleetResourceReservation)
                ),
                8,
            )

    def test_exact_gpu_evidence_validates_activates_and_feeds_recommendation(self):
        with self.factory() as session:
            release = self._release(session, "qwen2.5-72b-awq")
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id,
                    PlacementProfileRecord.pipeline_parallel_size == 3,
                )
            )
            node_ids = self._nodes(session, 3)
            request_id = str(uuid.uuid4())

            preview, created = prepare_profile_qualification(
                session,
                request_id=request_id,
                placement_id=placement.id,
                node_ids=list(reversed(node_ids)),
                apply=False,
            )

            self.assertFalse(created)
            self.assertEqual(preview["status"], "QUALIFYING")
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(ProfileQualificationRun)
                ),
                0,
            )
            run, created = prepare_profile_qualification(
                session,
                request_id=request_id,
                placement_id=placement.id,
                node_ids=node_ids,
                apply=True,
            )
            self.assertTrue(created)
            self.assertEqual(len(run["gpu_bindings"]), 3)
            self.assertEqual(run["workload"]["output_tokens"], 32)
            self.assertEqual(run["workload"]["minimum_request_count"], 2)
            self.assertTrue(
                all(binding["gpu_uuid"].startswith("GPU-") for binding in run["gpu_bindings"])
            )
            # Qualification may legitimately populate caches and consume disk.
            # Keep the exact node/GPU binding frozen, but do not make dynamic
            # capacity fields invalidate evidence produced by that same run.
            dynamic = session.get(NodeProfileRecord, node_ids[0])
            changed_profile = copy.deepcopy(dynamic.profile)
            changed_profile["disk_free_mib"] -= 1
            dynamic.profile = changed_profile
            dynamic.updated_at = utcnow()
            session.commit()
            evidence, stored_run, created = register_profile_qualification_evidence(
                session,
                run_id=request_id,
                steps=_passing_steps(),
                metrics=_passing_metrics(run, multi_node=True),
                executor_image="registry.example/qualification@sha256:" + "d" * 64,
                dure_commit="e" * 40,
            )

            self.assertTrue(created)
            self.assertEqual(evidence.status, "PASSED")
            self.assertEqual(stored_run.status, "PASSED")
            session.refresh(placement)
            self.assertEqual(placement.status, "VALIDATED")
            self.assertEqual(placement.qualification_evidence_id, evidence.id)
            activated, changed = activate_validated_profile(session, placement.id)
            self.assertTrue(changed)
            self.assertEqual(activated.status, "ACTIVE")
            _, changed = activate_validated_profile(session, placement.id)
            self.assertFalse(changed)

            transition_model_release(session, release.id, "VALIDATED")
            promoted, evidence_ids, changed = promote_model_release(
                session, release.id
            )
            self.assertTrue(changed)
            self.assertEqual(promoted.status, "ACTIVE")
            self.assertEqual(evidence_ids, [evidence.id])
            promoted, evidence_ids, changed = promote_model_release(
                session, release.id
            )
            self.assertFalse(changed)
            self.assertEqual(promoted.status, "ACTIVE")
            self.assertEqual(evidence_ids, [evidence.id])

            response, _ = evaluate_deployment_recommendation(
                session,
                node_ids=node_ids,
                all_online=False,
                objective="quality-first",
            )
            recommendation = response["recommendation"]
            self.assertIsNotNone(recommendation["selected"])
            self.assertEqual(
                recommendation["selected"]["network_evidence_id"], evidence.id
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)), 0
            )

            record = session.get(NodeProfileRecord, node_ids[0])
            record.updated_at = record.updated_at.replace(year=2000)
            session.commit()
            replay, created = prepare_profile_qualification(
                session,
                request_id=request_id,
                placement_id=placement.id,
                node_ids=list(reversed(node_ids)),
                apply=True,
            )
            self.assertFalse(created)
            self.assertEqual(replay["status"], "PASSED")

    def test_single_node_recommendation_uses_only_the_qualified_node(self):
        with self.factory() as session:
            release = self._release(session, "qwen2.5-7b-awq")
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id
                )
            )
            node_ids = self._nodes(session, 2)
            qualified_node_id = node_ids[1]
            run, created = prepare_profile_qualification(
                session,
                request_id=str(uuid.uuid4()),
                placement_id=placement.id,
                node_ids=[qualified_node_id],
                apply=True,
            )
            self.assertTrue(created)
            evidence, _, created = register_profile_qualification_evidence(
                session,
                run_id=run["id"],
                steps=_passing_steps(),
                metrics=_passing_metrics(run, multi_node=False),
                executor_image=(
                    "registry.example/qualification@sha256:" + "d" * 64
                ),
                dure_commit="e" * 40,
            )
            self.assertTrue(created)
            activate_validated_profile(session, placement.id)
            transition_model_release(session, release.id, "VALIDATED")
            promote_model_release(session, release.id)

            response, _ = evaluate_deployment_recommendation(
                session,
                node_ids=node_ids,
                all_online=False,
                objective="quality-first",
            )

            selected = response["recommendation"]["selected"]
            self.assertIsNotNone(selected)
            self.assertEqual(selected["node_ids"], [qualified_node_id])
            self.assertEqual(selected["network_evidence_id"], evidence.id)

    def test_failed_step_returns_profile_to_draft(self):
        with self.factory() as session:
            release = self._release(session, "qwen2.5-7b-awq")
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id
                )
            )
            node_id = self._nodes(session, 1)[0]
            request_id = str(uuid.uuid4())
            run, _ = prepare_profile_qualification(
                session,
                request_id=request_id,
                placement_id=placement.id,
                node_ids=[node_id],
                apply=True,
            )
            steps = _passing_steps()
            steps[4] = {
                "step_id": "MODEL_LOAD",
                "status": "FAILED",
                "failure_code": "MODEL_LOAD_FAILED",
            }

            evidence, run, _ = register_profile_qualification_evidence(
                session,
                run_id=request_id,
                steps=steps,
                metrics=_passing_metrics(run, multi_node=False),
                executor_image="registry.example/qualification@sha256:" + "d" * 64,
                dure_commit="e" * 40,
            )

            self.assertEqual(evidence.status, "FAILED")
            self.assertEqual(run.failure_code, "MODEL_LOAD_FAILED")
            session.refresh(placement)
            self.assertEqual(placement.status, "DRAFT")
            with self.assertRaisesRegex(ProfileQualificationError, "VALIDATED"):
                activate_validated_profile(session, placement.id)

    def test_gpu_uuid_drift_blocks_evidence_until_explicit_cancel(self):
        with self.factory() as session:
            release = self._release(session, "qwen2.5-7b-awq")
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id
                )
            )
            node_id = self._nodes(session, 1)[0]
            request_id = str(uuid.uuid4())
            run, _ = prepare_profile_qualification(
                session,
                request_id=request_id,
                placement_id=placement.id,
                node_ids=[node_id],
                apply=True,
            )
            record = session.get(NodeProfileRecord, node_id)
            changed = copy.deepcopy(record.profile)
            changed["gpus"][0]["uuid"] = "GPU-replaced-device"
            record.profile = changed
            record.updated_at = utcnow()
            session.commit()

            with self.assertRaisesRegex(ProfileQualificationError, "changed"):
                register_profile_qualification_evidence(
                    session,
                    run_id=request_id,
                    steps=_passing_steps(),
                    metrics=_passing_metrics(run, multi_node=False),
                    executor_image=(
                        "registry.example/qualification@sha256:" + "d" * 64
                    ),
                    dure_commit="e" * 40,
                )
            run, changed = cancel_profile_qualification(session, request_id)
            self.assertTrue(changed)
            self.assertEqual(run.status, "CANCELED")
            session.refresh(placement)
            self.assertEqual(placement.status, "DRAFT")
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(ProfileQualificationEvidence)
                ),
                0,
            )

    def test_busy_node_and_wrong_frozen_workload_are_rejected(self):
        with self.factory() as session:
            release = self._release(session, "qwen2.5-7b-awq")
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id
                )
            )
            node_id = self._nodes(session, 1)[0]
            task = Task(
                bulk_id=str(uuid.uuid4()),
                node_id=node_id,
                type=TaskType.PREPARE_MODEL.value,
                status=TaskStatus.QUEUED.value,
                payload={},
            )
            session.add(task)
            session.commit()

            with self.assertRaisesRegex(ProfileQualificationError, "eligible"):
                prepare_profile_qualification(
                    session,
                    request_id=str(uuid.uuid4()),
                    placement_id=placement.id,
                    node_ids=[node_id],
                    apply=False,
                )

            task.status = TaskStatus.SUCCEEDED.value
            session.commit()
            request_id = str(uuid.uuid4())
            run, _ = prepare_profile_qualification(
                session,
                request_id=request_id,
                placement_id=placement.id,
                node_ids=[node_id],
                apply=True,
            )
            metrics = _passing_metrics(run, multi_node=False)
            metrics["max_model_len"] -= 1
            with self.assertRaisesRegex(ValueError, "frozen workload"):
                register_profile_qualification_evidence(
                    session,
                    run_id=request_id,
                    steps=_passing_steps(),
                    metrics=metrics,
                    executor_image=(
                        "registry.example/qualification@sha256:" + "d" * 64
                    ),
                    dure_commit="e" * 40,
                )
            stored = session.get(ProfileQualificationRun, request_id)
            self.assertEqual(stored.status, "QUALIFYING")

    def test_active_qualification_reserves_nodes_until_canceled(self):
        with self.factory() as session:
            first_release = self._release(session, "qwen2.5-7b-awq")
            second_release = self._release(session, "qwen2.5-14b-awq")
            first_placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == first_release.id
                )
            )
            second_placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == second_release.id
                )
            )
            node_id = self._nodes(session, 1)[0]
            first_request_id = str(uuid.uuid4())
            _, created = prepare_profile_qualification(
                session,
                request_id=first_request_id,
                placement_id=first_placement.id,
                node_ids=[node_id],
                apply=True,
            )
            self.assertTrue(created)

            with self.assertRaises(ProfileQualificationError) as blocked:
                prepare_profile_qualification(
                    session,
                    request_id=str(uuid.uuid4()),
                    placement_id=second_placement.id,
                    node_ids=[node_id],
                    apply=True,
                )
            self.assertEqual(blocked.exception.code, "QUALIFICATION_NODE_INELIGIBLE")
            self.assertEqual(
                blocked.exception.details["nodes"],
                [
                    {
                        "node_id": node_id,
                        "reason": "NODE_OCCUPIED",
                    }
                ],
            )

            _, changed = cancel_profile_qualification(session, first_request_id)
            self.assertTrue(changed)
            _, created = prepare_profile_qualification(
                session,
                request_id=str(uuid.uuid4()),
                placement_id=second_placement.id,
                node_ids=[node_id],
                apply=True,
            )
            self.assertTrue(created)

    def test_active_qualification_blocks_benchmark_task_creation(self):
        with self.factory() as session:
            release = self._release(session, "qwen2.5-7b-awq")
            release.status = "VALIDATED"
            session.commit()
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id
                )
            )
            node_id = self._nodes(session, 1)[0]
            qualification_id = str(uuid.uuid4())
            prepare_profile_qualification(
                session,
                request_id=qualification_id,
                placement_id=placement.id,
                node_ids=[node_id],
                apply=True,
            )
            benchmark, _ = prepare_benchmark_run(
                session,
                request_id=str(uuid.uuid4()),
                release_id=release.id,
                placement_id=placement.id,
                node_ids=[node_id],
                workload_id="short-chat-1k-128",
                dure_commit="f" * 40,
            )

            with self.assertRaises(BenchmarkRunError) as blocked:
                apply_benchmark_run(session, benchmark.request_id)
            self.assertEqual(blocked.exception.code, "BENCHMARK_NODE_BUSY")
            self.assertEqual(
                blocked.exception.details,
                {
                    "node_id": node_id,
                    "qualification_run_id": qualification_id,
                },
            )
            self.assertEqual(
                session.scalar(
                    select(func.count())
                    .select_from(Task)
                    .where(Task.type == TaskType.BENCHMARK.value)
                ),
                0,
            )

    def test_supplementary_pass_preserves_primary_activation_evidence(self):
        with self.factory() as session:
            release = self._release(session, "qwen2.5-7b-awq")
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id
                )
            )
            primary_node, supplementary_node = self._nodes(session, 2)
            primary_run, _ = prepare_profile_qualification(
                session,
                request_id=str(uuid.uuid4()),
                placement_id=placement.id,
                node_ids=[primary_node],
                apply=True,
            )
            primary_evidence, _, _ = register_profile_qualification_evidence(
                session,
                run_id=primary_run["id"],
                steps=_passing_steps(),
                metrics=_passing_metrics(primary_run, multi_node=False),
                executor_image=(
                    "registry.example/qualification@sha256:" + "d" * 64
                ),
                dure_commit="e" * 40,
            )

            supplementary_run, created = prepare_profile_qualification(
                session,
                request_id=str(uuid.uuid4()),
                placement_id=placement.id,
                node_ids=[supplementary_node],
                apply=True,
                purpose="SUPPLEMENTARY",
            )
            self.assertTrue(created)
            self.assertEqual(supplementary_run["purpose"], "SUPPLEMENTARY")
            session.refresh(placement)
            self.assertEqual(placement.status, "VALIDATED")
            supplementary_evidence, stored_run, _ = (
                register_profile_qualification_evidence(
                    session,
                    run_id=supplementary_run["id"],
                    steps=_passing_steps(),
                    metrics=_passing_metrics(
                        supplementary_run, multi_node=False
                    ),
                    executor_image=(
                        "registry.example/qualification@sha256:" + "d" * 64
                    ),
                    dure_commit="f" * 40,
                )
            )

            session.refresh(placement)
            self.assertEqual(placement.status, "VALIDATED")
            self.assertEqual(
                placement.qualification_evidence_id, primary_evidence.id
            )
            self.assertNotEqual(supplementary_evidence.id, primary_evidence.id)
            activated, changed = activate_validated_profile(
                session, placement.id
            )
            self.assertTrue(changed)
            self.assertEqual(activated.status, "ACTIVE")
            validate_profile_qualification_evidence(
                session,
                placement=placement,
                evidence=supplementary_evidence,
                run=stored_run,
                require_primary=False,
            )
            with self.assertRaises(ProfileQualificationError) as primary_only:
                validate_profile_qualification_evidence(
                    session,
                    placement=placement,
                    evidence=supplementary_evidence,
                    run=stored_run,
                )
            self.assertEqual(
                primary_only.exception.code, "QUALIFICATION_EVIDENCE_INVALID"
            )

    def test_supplementary_failure_and_cancel_do_not_downgrade_active_profile(self):
        with self.factory() as session:
            release = self._release(session, "qwen2.5-7b-awq")
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id
                )
            )
            primary_node, failed_node, canceled_node = self._nodes(session, 3)
            primary_run, _ = prepare_profile_qualification(
                session,
                request_id=str(uuid.uuid4()),
                placement_id=placement.id,
                node_ids=[primary_node],
                apply=True,
            )
            primary_evidence, _, _ = register_profile_qualification_evidence(
                session,
                run_id=primary_run["id"],
                steps=_passing_steps(),
                metrics=_passing_metrics(primary_run, multi_node=False),
                executor_image=(
                    "registry.example/qualification@sha256:" + "d" * 64
                ),
                dure_commit="e" * 40,
            )
            activate_validated_profile(session, placement.id)

            failed_run, _ = prepare_profile_qualification(
                session,
                request_id=str(uuid.uuid4()),
                placement_id=placement.id,
                node_ids=[failed_node],
                apply=True,
                purpose="SUPPLEMENTARY",
            )
            canceled_run, _ = prepare_profile_qualification(
                session,
                request_id=str(uuid.uuid4()),
                placement_id=placement.id,
                node_ids=[canceled_node],
                apply=True,
                purpose="SUPPLEMENTARY",
            )
            failed_steps = _passing_steps()
            failed_steps[4] = {
                "step_id": "MODEL_LOAD",
                "status": "FAILED",
                "failure_code": "MODEL_LOAD_FAILED",
            }
            register_profile_qualification_evidence(
                session,
                run_id=failed_run["id"],
                steps=failed_steps,
                metrics=_passing_metrics(failed_run, multi_node=False),
                executor_image=(
                    "registry.example/qualification@sha256:" + "d" * 64
                ),
                dure_commit="f" * 40,
            )
            cancel_profile_qualification(session, canceled_run["id"])

            session.refresh(placement)
            self.assertEqual(placement.status, "ACTIVE")
            self.assertEqual(
                placement.qualification_evidence_id, primary_evidence.id
            )
            with self.assertRaises(ProfileQualificationError) as wrong_purpose:
                prepare_profile_qualification(
                    session,
                    request_id=str(uuid.uuid4()),
                    placement_id=placement.id,
                    node_ids=[failed_node],
                    apply=False,
                )
            self.assertEqual(
                wrong_purpose.exception.code, "QUALIFICATION_PROFILE_STATE"
            )

    def test_failure_code_must_match_its_step(self):
        steps = _passing_steps()
        steps[0] = {
            "step_id": "STATIC_COMPATIBILITY",
            "status": "FAILED",
            "failure_code": "MODEL_LOAD_FAILED",
        }
        with self.assertRaisesRegex(ValueError, "canonical failure_code"):
            _validate_steps(steps)


if __name__ == "__main__":
    unittest.main()
