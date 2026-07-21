from __future__ import annotations

import copy
import tempfile
import unittest
import uuid
from pathlib import Path

from sqlalchemy import func, select

from dure.control.benchmark import promote_model_release
from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import (
    Deployment,
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
        size_mib, context, layers, manifest_digit = {
            "qwen2.5-7b-awq": (4916, 8192, 28, "b"),
            "qwen2.5-14b-awq": (9728, 8192, 48, "e"),
            "qwen2.5-72b-awq": (39670, 8192, 80, "f"),
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
            quality_rank=72 if "72b" in model_id else 7,
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
                agent_version="0.3.28",
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
            self.assertTrue(
                all(binding["gpu_uuid"].startswith("GPU-") for binding in run["gpu_bindings"])
            )
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
