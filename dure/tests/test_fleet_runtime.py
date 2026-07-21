from __future__ import annotations

import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import ANY, patch

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from dure.control import fleet_runtime as fleet_runtime_module
from dure.control import preparation as preparation_module
from dure.control import service as service_module
from dure.control.api import create_app
from dure.control.benchmark import promote_model_release
from dure.control.db import Base, make_engine, make_session_factory
from dure.control.fleet_acceptance import accept_fleet_recommendation
from dure.control.fleet_recommendation import recommend_fleet
from dure.control.fleet_runtime import (
    apply_fleet,
    fleet_operation_is_current,
    prepare_fleet,
    recompute_fleet_runtime_status,
    sync_fleet_operation_status,
)
from dure.control.models import (
    ArtifactPreparation,
    Deployment,
    DeploymentOperation,
    DeploymentOperationNode,
    FleetDeploymentRuntime,
    FleetRecord,
    FleetResourceReservation,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    Task,
    utcnow,
)
from dure.control.preparation import (
    ArtifactPreparationError,
    prepare_deployment_artifacts,
)
from dure.control.qualification import (
    QUALIFICATION_STEPS,
    activate_validated_profile,
    prepare_profile_qualification,
    register_profile_qualification_evidence,
)
from dure.control.rollout import (
    DeploymentRolloutConflictError,
    claim_operation_task,
    finish_operation_task,
)
from dure.control.recommendation import (
    RecommendationGenerationConflictError,
    accept_deployment_recommendation,
    recommend_deployment,
)
from dure.control.service import (
    canonical_artifact_manifest_digest,
    claim_task,
    create_model_artifact,
    create_model_release,
    create_runtime_release,
    create_tasks,
    finish_task,
    generate_auto_placement_profiles,
    register_artifact_manifest,
    transition_model_release,
)
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from dure.models import DeploymentPlan, VLLM_RAY_PP_BACKEND
from dure.pipeline_runtime import pipeline_contract_detail
from dure.task import TaskStatus, TaskType

from .helpers import profile
from .test_artifact_manifest_api import _manifest


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


class FleetRuntimeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        database = Path(self.temporary.name) / "fleet-runtime.db"
        self.database_url = f"sqlite:///{database}"
        self.engine = make_engine(self.database_url)
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temporary.cleanup()

    def _nodes(self, session, count: int, key: str) -> list[str]:
        now = utcnow()
        node_ids: list[str] = []
        for index in range(count):
            node = Node(
                install_id=f"fleet-runtime-{key}-{index}-{uuid.uuid4()}",
                display_name=f"fleet-runtime-{key}-{index}",
                hostname=f"fleet-runtime-{key}-{index}",
                agent_version="0.3.32",
                approved=True,
                last_seen=now,
            )
            session.add(node)
            session.flush()
            observed = profile(
                f"fleet-runtime-{key}-{index}",
                address=f"10.62.{count}.{index + 10}",
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

    def _accepted_fleet(
        self,
        *,
        node_count: int = 2,
        model_id: str = "qwen2.5-7b-awq",
        pipeline_parallel_size: int = 1,
        key: str = "default",
        include_standalone_recommendation: bool = False,
    ) -> dict:
        manifest = _manifest()
        manifest_digest = canonical_artifact_manifest_digest(manifest)
        with self.factory() as session:
            artifact = create_model_artifact(
                session,
                model_id=model_id,
                repository=f"Qwen/{model_id}-{key}",
                revision="a" * 40,
                manifest_digest=manifest_digest,
                quantization="awq",
                size_mib={
                    "qwen2.5-7b-awq": 4916,
                    "qwen2.5-72b-awq": 39670,
                }[model_id],
                default_max_model_len=8192,
                layer_count={
                    "qwen2.5-7b-awq": 28,
                    "qwen2.5-72b-awq": 80,
                }[model_id],
                license_id="apache-2.0",
            )
            register_artifact_manifest(
                session,
                artifact_id=artifact.id,
                manifest=manifest,
            )
            runtime = create_runtime_release(
                session,
                version=f"vllm-fleet-runtime-{key}",
                image=(
                    f"registry.example/{model_id}@sha256:"
                    + canonical_artifact_manifest_digest(
                        {"schema_version": 1, "files": manifest["files"]}
                    ).removeprefix("sha256:")
                ),
                vllm_version="0.9.0",
                cuda_version="12.8",
                gpu_architectures=["ampere"],
            )
            release = create_model_release(
                session,
                artifact_id=artifact.id,
                runtime_id=runtime.id,
                quality_rank=72 if model_id == "qwen2.5-72b-awq" else 7,
            )
            generate_auto_placement_profiles(
                session,
                release_id=release.id,
                apply=True,
            )
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id,
                    PlacementProfileRecord.pipeline_parallel_size
                    == pipeline_parallel_size,
                )
            )
            self.assertIsNotNone(placement)
            node_ids = self._nodes(session, node_count, key)

            qualification_sets = (
                [node_ids]
                if pipeline_parallel_size > 1
                else [[node_id] for node_id in node_ids]
            )
            evidence_ids: list[str] = []
            for index, exact_nodes in enumerate(qualification_sets):
                run, created = prepare_profile_qualification(
                    session,
                    request_id=str(uuid.uuid4()),
                    placement_id=placement.id,
                    node_ids=exact_nodes,
                    purpose="PRIMARY" if index == 0 else "SUPPLEMENTARY",
                    apply=True,
                )
                self.assertTrue(created)
                evidence, _, created = register_profile_qualification_evidence(
                    session,
                    run_id=run["id"],
                    steps=_passing_steps(),
                    metrics=_passing_metrics(
                        run,
                        multi_node=pipeline_parallel_size > 1,
                    ),
                    executor_image=(
                        "registry.example/qualification@sha256:" + "d" * 64
                    ),
                    dure_commit="e" * 40,
                )
                self.assertTrue(created)
                evidence_ids.append(evidence.id)
                if index == 0:
                    activated, changed = activate_validated_profile(
                        session, placement.id
                    )
                    self.assertTrue(changed)
                    self.assertEqual(activated.status, "ACTIVE")

            transition_model_release(session, release.id, "VALIDATED")
            promoted, promoted_evidence, changed = promote_model_release(
                session, release.id
            )
            self.assertTrue(changed)
            self.assertEqual(promoted.status, "ACTIVE")
            self.assertEqual(promoted_evidence, [evidence_ids[0]])

            standalone_recommendation_id = None
            if include_standalone_recommendation:
                standalone = recommend_deployment(
                    session,
                    node_ids=node_ids,
                    all_online=False,
                )
                self.assertIsNotNone(
                    standalone["recommendation"]["selected"]
                )
                standalone_recommendation_id = standalone[
                    "recommendation"
                ]["id"]

            recommendation = recommend_fleet(
                session,
                node_ids=node_ids,
                all_online=False,
            )
            selected = recommendation["recommendation"]["evaluation"][
                "schedule"
            ]["selected"]
            self.assertEqual(
                len(selected),
                1 if pipeline_parallel_size > 1 else node_count,
            )
            accepted = accept_fleet_recommendation(
                session,
                recommendation["recommendation"]["id"],
            )
            return {
                "fleet_id": accepted["fleet"]["id"],
                "accepted": accepted,
                "recommendation_id": recommendation["recommendation"]["id"],
                "standalone_recommendation_id": (
                    standalone_recommendation_id
                ),
                "node_ids": node_ids,
                "manifest": manifest,
            }

    @staticmethod
    def _preparation_result(task: Task, manifest: dict) -> dict:
        payload = task.payload
        result = {
            "preparation_id": payload["preparation_id"],
            "preparation_node_id": payload["preparation_node_id"],
            "attempt_id": payload["attempt_id"],
            "attempt_no": payload["attempt_no"],
            "deployment_id": payload["deployment_id"],
            "generation": payload["generation"],
            "node_id": payload["node_id"],
            "stage": (
                "MODEL"
                if task.type == TaskType.PREPARE_MODEL.value
                else "IMAGE"
            ),
            "reused": False,
        }
        if task.type == TaskType.PREPARE_MODEL.value:
            result.update(
                model_id=payload["model_id"],
                manifest_digest=payload["manifest_digest"],
                cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
                verification_version=MODEL_CACHE_VERIFICATION_VERSION,
                bytes_verified=sum(
                    item["size_bytes"] for item in manifest["files"]
                ),
                file_count=len(manifest["files"]),
            )
        else:
            result.update(
                runtime_image=payload["runtime_image"],
                image_id=payload["runtime_image"].rsplit("@", 1)[1],
            )
        return result

    def _complete_preparation(self, context: dict) -> None:
        with self.factory() as session:
            for node_id in context["node_ids"]:
                while True:
                    task = claim_task(session, node_id)
                    if task is None:
                        break
                    self.assertIn(
                        task.type,
                        {
                            TaskType.PREPARE_MODEL.value,
                            TaskType.PREPARE_IMAGE.value,
                        },
                    )
                    self.assertTrue(
                        finish_task(
                            session,
                            task,
                            node_id,
                            result=self._preparation_result(
                                task, context["manifest"]
                            ),
                            error=None,
                        )
                    )

    @staticmethod
    def _operation_result(task: Task) -> dict:
        plan = DeploymentPlan.from_dict(task.payload["plan"])
        assignment = plan.assignment_for(task.node_id)
        assert assignment is not None
        strict = plan.execution_backend == VLLM_RAY_PP_BACKEND
        names = {
            "host-gpu",
            "container-gpu",
            "pipeline-rank-contract" if strict else "ray-cluster",
        }
        if task.type in {
            TaskType.APPLY_DEPLOYMENT.value,
            TaskType.START_DEPLOYMENT.value,
            TaskType.RESTART_DEPLOYMENT.value,
        }:
            names.update(
                {
                    "node-profile",
                    "deployment-plan",
                    (
                        "stage-cache"
                        if plan.model_cache_kind != MODEL_CACHE_KIND_FULL_SNAPSHOT
                        else "model"
                    ),
                    "container-image",
                    "ray-container",
                }
            )
            if (
                task.payload.get("serve") is True
                and plan.ray_head_node_id == task.node_id
            ):
                names.update({"vllm-api-start", "vllm-api"})
        elif task.type == TaskType.VERIFY.value:
            if (
                task.payload.get("api") is True
                and plan.ray_head_node_id == task.node_id
            ):
                names.add("vllm-api")
        else:  # pragma: no cover - helper is called only for rollout phases
            raise AssertionError(task.type)

        checks = []
        for name in sorted(names):
            detail = (
                pipeline_contract_detail(plan, assignment)
                if name == "pipeline-rank-contract"
                else "verified"
            )
            checks.append(
                {
                    "name": name,
                    "ok": True,
                    "detail": detail,
                    "blocking": True,
                }
            )
        result = {"checks": checks}
        if task.type == TaskType.VERIFY.value:
            result["ok"] = True
        return result

    @staticmethod
    def _phase_tasks(
        session,
        operation_id: str,
        phase: str,
    ) -> list[Task]:
        return list(
            session.scalars(
                select(Task)
                .join(
                    DeploymentOperationNode,
                    DeploymentOperationNode.id == Task.operation_node_id,
                )
                .where(
                    DeploymentOperationNode.operation_id == operation_id,
                    DeploymentOperationNode.phase == phase,
                )
                .order_by(Task.node_id, Task.id)
            )
        )

    def _finish_phase(
        self,
        session,
        operation_id: str,
        phase: str,
        *,
        failure_node_id: str | None = None,
    ) -> None:
        tasks = self._phase_tasks(session, operation_id, phase)
        self.assertTrue(tasks, phase)
        for expected in tasks:
            claimed = claim_task(session, expected.node_id)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.id, expected.id)
            error = (
                "TASK_EXECUTION_FAILED"
                if expected.node_id == failure_node_id
                else None
            )
            self.assertTrue(
                finish_task(
                    session,
                    claimed,
                    claimed.node_id,
                    result=(
                        None
                        if error is not None
                        else self._operation_result(claimed)
                    ),
                    error=error,
                )
            )

    def _prepare_successfully(self, context: dict) -> dict:
        with self.factory() as session:
            result = prepare_fleet(session, context["fleet_id"])
            self.assertTrue(all(item["changed"] for item in result["actions"]))
        self._complete_preparation(context)
        with self.factory() as session:
            runtimes = list(
                session.scalars(
                    select(FleetDeploymentRuntime)
                    .where(
                        FleetDeploymentRuntime.fleet_id
                        == context["fleet_id"]
                    )
                    .order_by(FleetDeploymentRuntime.deployment_id)
                )
            )
            self.assertTrue(runtimes)
            self.assertTrue(all(item.status == "PREPARED" for item in runtimes))
            return {item.deployment_id: item.id for item in runtimes}

    def test_accept_creates_one_runtime_record_for_every_deployment(self) -> None:
        context = self._accepted_fleet(node_count=2, key="accept")

        with self.factory() as session:
            deployments = list(
                session.scalars(
                    select(Deployment).where(
                        Deployment.fleet_id == context["fleet_id"]
                    )
                )
            )
            runtimes = list(
                session.scalars(
                    select(FleetDeploymentRuntime).where(
                        FleetDeploymentRuntime.fleet_id
                        == context["fleet_id"]
                    )
                )
            )
            self.assertEqual(len(deployments), 2)
            self.assertEqual(
                sorted(item.deployment_id for item in runtimes),
                sorted(item.id for item in deployments),
            )
            self.assertTrue(all(item.status == "ACCEPTED" for item in runtimes))
            self.assertEqual(
                [item["deployment_id"] for item in context["accepted"]["fleet"]["runtime"]],
                sorted(item.id for item in deployments),
            )

            repeated = accept_fleet_recommendation(
                session, context["recommendation_id"]
            )
            self.assertFalse(repeated["created"])
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(FleetDeploymentRuntime)
                ),
                2,
            )

    def test_stale_multi_runtime_recompute_reads_every_committed_transition(
        self,
    ) -> None:
        context = self._accepted_fleet(node_count=2, key="stale-aggregate")
        session_b = self.factory()
        try:
            stale_runtimes = list(
                session_b.scalars(
                    select(FleetDeploymentRuntime)
                    .where(
                        FleetDeploymentRuntime.fleet_id
                        == context["fleet_id"]
                    )
                    .order_by(FleetDeploymentRuntime.deployment_id)
                )
            )
            stale_fleet = session_b.get(
                FleetRecord, context["fleet_id"]
            )
            self.assertEqual(len(stale_runtimes), 2)
            self.assertTrue(
                all(item.status == "ACCEPTED" for item in stale_runtimes)
            )
            self.assertEqual(stale_fleet.status, "ACCEPTED")
            session_b.commit()

            with self.factory() as session_a:
                runtime_a = session_a.get(
                    FleetDeploymentRuntime, stale_runtimes[0].id
                )
                runtime_a.status = "ACTIVE"
                self.assertEqual(
                    recompute_fleet_runtime_status(
                        session_a, context["fleet_id"]
                    ),
                    "APPLYING",
                )
                session_a.commit()

            stale_runtimes[1].status = "ACTIVE"
            self.assertEqual(
                recompute_fleet_runtime_status(
                    session_b, context["fleet_id"]
                ),
                "ACTIVE",
            )
            session_b.commit()

            with self.factory() as verification:
                persisted = list(
                    verification.scalars(
                        select(FleetDeploymentRuntime)
                        .where(
                            FleetDeploymentRuntime.fleet_id
                            == context["fleet_id"]
                        )
                        .order_by(FleetDeploymentRuntime.deployment_id)
                    )
                )
                fleet = verification.get(
                    FleetRecord, context["fleet_id"]
                )
                self.assertEqual(
                    [item.status for item in persisted],
                    ["ACTIVE", "ACTIVE"],
                )
                self.assertEqual(fleet.status, "ACTIVE")
        finally:
            session_b.close()

    def test_generic_prepare_and_task_routes_cannot_bypass_fleet_runtime(self) -> None:
        context = self._accepted_fleet(node_count=1, key="closed")
        with self.factory() as session:
            deployment = session.scalar(
                select(Deployment).where(
                    Deployment.fleet_id == context["fleet_id"]
                )
            )
            with self.assertRaises(ArtifactPreparationError) as preparation:
                prepare_deployment_artifacts(
                    session,
                    deployment.id,
                    request_id=str(uuid.uuid4()),
                    apply=True,
                )
            self.assertEqual(
                preparation.exception.code, "FLEET_RUNTIME_NOT_AVAILABLE"
            )
            with self.assertRaises(DeploymentRolloutConflictError) as tasks:
                create_tasks(
                    session,
                    node_ids=context["node_ids"],
                    task_type=TaskType.APPLY_DEPLOYMENT,
                    deployment_id=deployment.id,
                    options={"serve": True},
                )
            self.assertEqual(tasks.exception.code, "FLEET_RUNTIME_NOT_AVAILABLE")
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

    def test_prepare_is_deterministic_and_idempotent(self) -> None:
        context = self._accepted_fleet(node_count=2, key="prepare-idempotent")
        with self.factory() as session:
            first = prepare_fleet(session, context["fleet_id"])
            first_rows = list(
                session.scalars(
                    select(FleetDeploymentRuntime)
                    .where(
                        FleetDeploymentRuntime.fleet_id
                        == context["fleet_id"]
                    )
                    .order_by(FleetDeploymentRuntime.deployment_id)
                )
            )
            first_ids = [item.preparation_id for item in first_rows]
            first_task_ids = sorted(
                task_id
                for action in first["actions"]
                for task_id in action["task_ids"]
            )

            second = prepare_fleet(session, context["fleet_id"])
            second_rows = list(
                session.scalars(
                    select(FleetDeploymentRuntime)
                    .where(
                        FleetDeploymentRuntime.fleet_id
                        == context["fleet_id"]
                    )
                    .order_by(FleetDeploymentRuntime.deployment_id)
                )
            )
            self.assertEqual(
                [item.preparation_id for item in second_rows], first_ids
            )
            self.assertEqual(
                sorted(task.id for task in session.scalars(select(Task))),
                first_task_ids,
            )
            self.assertTrue(
                all(action["changed"] is False for action in second["actions"])
            )
            self.assertTrue(all(item.status == "PREPARING" for item in second_rows))

    def test_internal_apply_binds_exact_nodes_serve_and_pending_verify(self) -> None:
        context = self._accepted_fleet(
            node_count=3,
            model_id="qwen2.5-72b-awq",
            pipeline_parallel_size=3,
            key="exact-apply",
        )
        self._prepare_successfully(context)

        with self.factory() as session:
            result = apply_fleet(session, context["fleet_id"])
            self.assertEqual(len(result["actions"]), 1)
            action = result["actions"][0]
            self.assertTrue(action["changed"])
            operation = session.get(
                DeploymentOperation, action["operation_id"]
            )
            runtime = session.scalar(
                select(FleetDeploymentRuntime).where(
                    FleetDeploymentRuntime.fleet_id == context["fleet_id"]
                )
            )
            deployment = session.get(Deployment, runtime.deployment_id)
            expected_nodes = sorted(
                item["node_id"] for item in deployment.plan["assignments"]
            )
            self.assertEqual(operation.node_ids, expected_nodes)
            self.assertEqual(expected_nodes, context["node_ids"])
            self.assertTrue(operation.serve)
            self.assertTrue(operation.api)
            self.assertEqual(runtime.current_operation_id, operation.id)
            self.assertEqual(runtime.status, "APPLYING")
            verify_rows = list(
                session.scalars(
                    select(DeploymentOperationNode)
                    .where(
                        DeploymentOperationNode.operation_id == operation.id,
                        DeploymentOperationNode.phase == "VERIFY",
                    )
                    .order_by(DeploymentOperationNode.node_id)
                )
            )
            self.assertEqual(
                [item.node_id for item in verify_rows], expected_nodes
            )
            self.assertTrue(
                all(
                    item.status == "PENDING" and item.attempt_count == 0
                    for item in verify_rows
                )
            )
            apply_tasks = self._phase_tasks(session, operation.id, "APPLY")
            self.assertEqual(
                sorted(item.node_id for item in apply_tasks), expected_nodes
            )
            self.assertTrue(
                all(item.payload["serve"] is False for item in apply_tasks)
            )

    def test_stale_session_apply_is_an_idempotent_noop(self) -> None:
        context = self._accepted_fleet(node_count=1, key="stale-session")
        self._prepare_successfully(context)

        session_b = self.factory()
        try:
            stale_runtime = session_b.scalar(
                select(FleetDeploymentRuntime).where(
                    FleetDeploymentRuntime.fleet_id == context["fleet_id"]
                )
            )
            stale_fleet = session_b.get(FleetRecord, context["fleet_id"])
            self.assertEqual(stale_runtime.status, "PREPARED")
            self.assertEqual(stale_fleet.status, "PREPARED")
            session_b.commit()

            with self.factory() as session_a:
                first = apply_fleet(session_a, context["fleet_id"])
                self.assertEqual(len(first["actions"]), 1)
                self.assertTrue(first["actions"][0]["changed"])
                operation_id = first["actions"][0]["operation_id"]

            repeated = apply_fleet(session_b, context["fleet_id"])
            self.assertEqual(
                repeated["actions"],
                [
                    {
                        "deployment_id": stale_runtime.deployment_id,
                        "changed": False,
                        "status": "APPLYING",
                        "reason": "OPERATION_ALREADY_ACTIVE",
                    }
                ],
            )

            with self.factory() as verification:
                runtime = verification.get(
                    FleetDeploymentRuntime, stale_runtime.id
                )
                fleet = verification.get(FleetRecord, context["fleet_id"])
                operation = verification.get(
                    DeploymentOperation, operation_id
                )
                self.assertEqual(runtime.status, "APPLYING")
                self.assertEqual(runtime.current_operation_id, operation_id)
                self.assertIsNone(runtime.failure_phase)
                self.assertIsNone(runtime.failure_code)
                self.assertEqual(fleet.status, "APPLYING")
                self.assertEqual(
                    (operation.status, operation.phase), ("QUEUED", "APPLY")
                )
                self.assertEqual(
                    verification.scalar(
                        select(func.count()).select_from(DeploymentOperation)
                    ),
                    1,
                )
                self.assertEqual(
                    verification.scalar(
                        select(func.count()).select_from(Task).where(
                            Task.type == TaskType.APPLY_DEPLOYMENT.value
                        )
                    ),
                    1,
                )
        finally:
            session_b.close()

    def test_apply_failure_cas_does_not_overwrite_a_concurrent_success(self) -> None:
        context = self._accepted_fleet(node_count=1, key="apply-cas")
        self._prepare_successfully(context)
        original_create_tasks = service_module.create_tasks
        original_record_failure = fleet_runtime_module._record_runtime_failure
        producer_calls = 0
        concurrent_result: dict | None = None

        def interleaved_create_tasks(*args, **kwargs):
            nonlocal producer_calls
            producer_calls += 1
            if producer_calls == 1:
                raise DeploymentRolloutConflictError(
                    "synthetic first producer failure",
                    code="SYNTHETIC_APPLY_FAILURE",
                )
            return original_create_tasks(*args, **kwargs)

        def advance_before_failure_record(*args, **kwargs):
            nonlocal concurrent_result
            with self.factory() as session_b:
                concurrent_result = apply_fleet(
                    session_b, context["fleet_id"]
                )
            return original_record_failure(*args, **kwargs)

        with self.factory() as session_a, patch(
            "dure.control.service.create_tasks",
            side_effect=interleaved_create_tasks,
        ), patch(
            "dure.control.fleet_runtime._record_runtime_failure",
            side_effect=advance_before_failure_record,
        ):
            failed_producer = apply_fleet(session_a, context["fleet_id"])

        self.assertIsNotNone(concurrent_result)
        self.assertTrue(concurrent_result["actions"][0]["changed"])
        operation_id = concurrent_result["actions"][0]["operation_id"]
        self.assertEqual(
            failed_producer["actions"],
            [
                {
                    "deployment_id": concurrent_result["actions"][0][
                        "deployment_id"
                    ],
                    "changed": False,
                    "status": "APPLYING",
                    "reason": "RUNTIME_ADVANCED",
                    "operation_id": operation_id,
                }
            ],
        )

        with self.factory() as session:
            runtime = session.scalar(
                select(FleetDeploymentRuntime).where(
                    FleetDeploymentRuntime.fleet_id == context["fleet_id"]
                )
            )
            fleet = session.get(FleetRecord, context["fleet_id"])
            operation = session.get(DeploymentOperation, operation_id)
            tasks = list(
                session.scalars(
                    select(Task).where(
                        Task.type == TaskType.APPLY_DEPLOYMENT.value
                    )
                )
            )
            self.assertEqual(runtime.status, "APPLYING")
            self.assertEqual(runtime.current_operation_id, operation_id)
            self.assertIsNone(runtime.failure_phase)
            self.assertIsNone(runtime.failure_code)
            self.assertEqual(fleet.status, "APPLYING")
            self.assertEqual(
                (operation.status, operation.phase), ("QUEUED", "APPLY")
            )
            self.assertEqual(len(tasks), 1)

    def test_prepare_failure_cas_does_not_overwrite_concurrent_progress(self) -> None:
        context = self._accepted_fleet(node_count=1, key="prepare-cas")
        original_prepare = preparation_module.prepare_deployment_artifacts
        original_record_failure = fleet_runtime_module._record_runtime_failure
        producer_calls = 0
        concurrent_result: dict | None = None

        def interleaved_prepare(*args, **kwargs):
            nonlocal producer_calls
            producer_calls += 1
            if producer_calls == 1:
                raise ArtifactPreparationError(
                    "synthetic first producer failure",
                    code="SYNTHETIC_PREPARE_FAILURE",
                )
            return original_prepare(*args, **kwargs)

        def advance_before_failure_record(*args, **kwargs):
            nonlocal concurrent_result
            with self.factory() as session_b:
                concurrent_result = prepare_fleet(
                    session_b, context["fleet_id"]
                )
            return original_record_failure(*args, **kwargs)

        with self.factory() as session_a, patch(
            "dure.control.preparation.prepare_deployment_artifacts",
            side_effect=interleaved_prepare,
        ), patch(
            "dure.control.fleet_runtime._record_runtime_failure",
            side_effect=advance_before_failure_record,
        ):
            failed_producer = prepare_fleet(
                session_a, context["fleet_id"]
            )

        self.assertIsNotNone(concurrent_result)
        self.assertTrue(concurrent_result["actions"][0]["changed"])
        preparation_id = concurrent_result["actions"][0]["preparation_id"]
        self.assertEqual(
            failed_producer["actions"],
            [
                {
                    "deployment_id": concurrent_result["actions"][0][
                        "deployment_id"
                    ],
                    "changed": False,
                    "status": "PREPARING",
                    "reason": "RUNTIME_ADVANCED",
                }
            ],
        )

        with self.factory() as session:
            runtime = session.scalar(
                select(FleetDeploymentRuntime).where(
                    FleetDeploymentRuntime.fleet_id == context["fleet_id"]
                )
            )
            fleet = session.get(FleetRecord, context["fleet_id"])
            preparation = session.get(
                ArtifactPreparation, preparation_id
            )
            tasks = list(
                session.scalars(
                    select(Task).where(
                        Task.type == TaskType.PREPARE_MODEL.value
                    )
                )
            )
            self.assertEqual(runtime.status, "PREPARING")
            self.assertEqual(runtime.preparation_id, preparation_id)
            self.assertIsNone(runtime.failure_phase)
            self.assertIsNone(runtime.failure_code)
            self.assertEqual(fleet.status, "PREPARING")
            self.assertIsNotNone(preparation)
            self.assertEqual(len(tasks), 1)

    def test_stale_prepare_cannot_regress_an_applying_runtime(self) -> None:
        context = self._accepted_fleet(node_count=1, key="stale-prepare")
        self._prepare_successfully(context)

        stale_session = self.factory()
        try:
            stale_runtime = stale_session.scalar(
                select(FleetDeploymentRuntime).where(
                    FleetDeploymentRuntime.fleet_id == context["fleet_id"]
                )
            )
            self.assertEqual(stale_runtime.status, "PREPARED")
            stale_session.commit()

            with self.factory() as applying_session:
                applied = apply_fleet(
                    applying_session, context["fleet_id"]
                )
                self.assertTrue(applied["actions"][0]["changed"])
                operation_id = applied["actions"][0]["operation_id"]

            repeated = prepare_fleet(stale_session, context["fleet_id"])
            self.assertEqual(
                repeated["actions"],
                [
                    {
                        "deployment_id": stale_runtime.deployment_id,
                        "changed": False,
                        "status": "APPLYING",
                        "reason": "PREPARATION_ALREADY_TERMINAL",
                    }
                ],
            )

            with self.factory() as verification:
                runtime = verification.get(
                    FleetDeploymentRuntime, stale_runtime.id
                )
                fleet = verification.get(FleetRecord, context["fleet_id"])
                operation = verification.get(
                    DeploymentOperation, operation_id
                )
                self.assertEqual(runtime.status, "APPLYING")
                self.assertEqual(runtime.current_operation_id, operation_id)
                self.assertEqual(fleet.status, "APPLYING")
                self.assertEqual(
                    (operation.status, operation.phase), ("QUEUED", "APPLY")
                )
        finally:
            stale_session.close()

    def test_standalone_recommendation_cannot_extend_a_fleet_lineage(self) -> None:
        context = self._accepted_fleet(
            node_count=1,
            key="lineage-boundary",
            include_standalone_recommendation=True,
        )
        recommendation_id = context["standalone_recommendation_id"]
        self.assertIsNotNone(recommendation_id)

        with self.factory() as session:
            deployment = session.scalar(
                select(Deployment).where(
                    Deployment.fleet_id == context["fleet_id"]
                )
            )
            self.assertEqual(deployment.generation, 1)
            with self.assertRaises(
                RecommendationGenerationConflictError
            ) as raised:
                accept_deployment_recommendation(
                    session,
                    recommendation_id,
                    previous_generation_id=deployment.id,
                )
            self.assertEqual(
                raised.exception.code,
                "FLEET_LINEAGE_EXTENSION_FORBIDDEN",
            )
            session.rollback()
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(Deployment).where(
                        Deployment.lineage_id == deployment.lineage_id,
                        Deployment.generation == 2,
                    )
                ),
                0,
            )
            deployment_id = deployment.id

        client = TestClient(
            create_app(
                database_url=self.database_url,
                admin_token="admin-secret",
                create_schema=False,
            )
        )
        try:
            response = client.post(
                f"/v1/admin/deployment-recommendations/{recommendation_id}/accept",
                headers={"Authorization": "Bearer admin-secret"},
                json={"previous_generation_id": deployment_id},
            )
            self.assertEqual(response.status_code, 409, response.text)
            self.assertEqual(
                response.json()["detail"]["code"],
                "FLEET_LINEAGE_EXTENSION_FORBIDDEN",
            )
        finally:
            client.close()

        with self.factory() as session:
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(Deployment).where(
                        Deployment.lineage_id == deployment_id,
                        Deployment.generation == 2,
                    )
                ),
                0,
            )

    def test_apply_start_api_verify_api_verify_reaches_active(self) -> None:
        context = self._accepted_fleet(
            node_count=3,
            model_id="qwen2.5-72b-awq",
            pipeline_parallel_size=3,
            key="lifecycle",
        )
        self._prepare_successfully(context)
        with self.factory() as session:
            operation_id = apply_fleet(session, context["fleet_id"])["actions"][0][
                "operation_id"
            ]

            self._finish_phase(session, operation_id, "APPLY")
            operation = session.get(DeploymentOperation, operation_id)
            runtime = session.scalar(
                select(FleetDeploymentRuntime).where(
                    FleetDeploymentRuntime.fleet_id == context["fleet_id"]
                )
            )
            self.assertEqual(operation.phase, "START_API")
            self.assertEqual(runtime.status, "APPLYING")

            self._finish_phase(session, operation_id, "START_API")
            session.refresh(operation)
            session.refresh(runtime)
            self.assertEqual(operation.phase, "VERIFY_API")
            self.assertEqual(runtime.status, "VERIFYING")

            self._finish_phase(session, operation_id, "VERIFY_API")
            session.refresh(operation)
            session.refresh(runtime)
            self.assertEqual(operation.phase, "VERIFY")
            self.assertEqual(runtime.status, "VERIFYING")

            self._finish_phase(session, operation_id, "VERIFY")
            session.refresh(operation)
            session.refresh(runtime)
            deployment = session.get(Deployment, runtime.deployment_id)
            fleet = session.get(FleetRecord, context["fleet_id"])
            self.assertEqual((operation.status, operation.phase), ("SUCCEEDED", "COMPLETE"))
            self.assertEqual(runtime.status, "ACTIVE")
            self.assertEqual(deployment.status, "VERIFIED")
            self.assertIsNotNone(deployment.verified_at)
            self.assertEqual(fleet.status, "ACTIVE")

    def test_preparation_failure_isolated_and_keeps_reservations(self) -> None:
        context = self._accepted_fleet(node_count=2, key="prepare-failure")
        original_prepare = preparation_module.prepare_deployment_artifacts
        producer_calls = 0

        def fail_first_preparation(*args, **kwargs):
            nonlocal producer_calls
            producer_calls += 1
            if producer_calls == 1:
                raise ArtifactPreparationError(
                    "synthetic isolated preparation failure",
                    code="SYNTHETIC_PREPARE_FAILURE",
                )
            return original_prepare(*args, **kwargs)

        with self.factory() as session, patch(
            "dure.control.preparation.prepare_deployment_artifacts",
            side_effect=fail_first_preparation,
        ):
            result = prepare_fleet(session, context["fleet_id"])
            self.assertEqual(
                [item["status"] for item in result["actions"]],
                ["PREPARE_FAILED", "PREPARING"],
            )
            runtimes = list(
                session.scalars(
                    select(FleetDeploymentRuntime)
                    .where(
                        FleetDeploymentRuntime.fleet_id
                        == context["fleet_id"]
                    )
                    .order_by(FleetDeploymentRuntime.deployment_id)
                )
            )
            self.assertEqual(
                [item.status for item in runtimes],
                ["PREPARE_FAILED", "PREPARING"],
            )
            fleet = session.get(FleetRecord, context["fleet_id"])
            self.assertEqual(fleet.status, "PARTIAL_FAILED")
            reservations = list(
                session.scalars(
                    select(FleetResourceReservation).where(
                        FleetResourceReservation.fleet_id
                        == context["fleet_id"]
                    )
                )
            )
            self.assertTrue(all(item.released_at is None for item in reservations))
            self.assertFalse(
                session.scalar(
                    select(Task.id).where(
                        Task.type.in_(
                            {
                                TaskType.STOP_DEPLOYMENT.value,
                                TaskType.RESTART_DEPLOYMENT.value,
                            }
                        )
                    )
                )
            )

    def test_apply_failure_isolated_from_sibling_and_keeps_reservations(self) -> None:
        context = self._accepted_fleet(node_count=2, key="apply-failure")
        self._prepare_successfully(context)
        original_create_tasks = service_module.create_tasks
        producer_calls = 0

        def fail_first_apply(*args, **kwargs):
            nonlocal producer_calls
            producer_calls += 1
            if producer_calls == 1:
                raise DeploymentRolloutConflictError(
                    "synthetic isolated apply failure",
                    code="SYNTHETIC_APPLY_FAILURE",
                )
            return original_create_tasks(*args, **kwargs)

        with self.factory() as session, patch(
            "dure.control.service.create_tasks",
            side_effect=fail_first_apply,
        ):
            result = apply_fleet(session, context["fleet_id"])
            self.assertEqual(
                [item["status"] for item in result["actions"]],
                ["APPLY_FAILED", "APPLYING"],
            )
            runtimes = list(
                session.scalars(
                    select(FleetDeploymentRuntime)
                    .where(
                        FleetDeploymentRuntime.fleet_id
                        == context["fleet_id"]
                    )
                    .order_by(FleetDeploymentRuntime.deployment_id)
                )
            )
            self.assertEqual(
                [item.status for item in runtimes],
                ["APPLY_FAILED", "APPLYING"],
            )
            self.assertEqual(
                session.get(FleetRecord, context["fleet_id"]).status,
                "APPLYING",
            )
            reservations = list(
                session.scalars(
                    select(FleetResourceReservation).where(
                        FleetResourceReservation.fleet_id
                        == context["fleet_id"]
                    )
                )
            )
            self.assertTrue(all(item.released_at is None for item in reservations))
            self.assertFalse(
                session.scalar(
                    select(Task.id).where(
                        Task.type.in_(
                            {
                                TaskType.STOP_DEPLOYMENT.value,
                                TaskType.RESTART_DEPLOYMENT.value,
                            }
                        )
                    )
                )
            )

    def test_verify_failure_does_not_stop_or_roll_back_active_sibling(self) -> None:
        context = self._accepted_fleet(node_count=2, key="verify-failure")
        self._prepare_successfully(context)
        with self.factory() as session:
            actions = apply_fleet(session, context["fleet_id"])["actions"]
            operations = [
                session.get(DeploymentOperation, item["operation_id"])
                for item in actions
            ]
            operations.sort(key=lambda item: item.deployment_id)

            for phase in ("APPLY", "START_API", "VERIFY_API", "VERIFY"):
                self._finish_phase(session, operations[1].id, phase)
            successful = session.get(DeploymentOperation, operations[1].id)
            self.assertEqual(successful.status, "SUCCEEDED")

            for phase in ("APPLY", "START_API", "VERIFY_API"):
                self._finish_phase(session, operations[0].id, phase)
            verify_tasks = self._phase_tasks(session, operations[0].id, "VERIFY")
            self.assertEqual(len(verify_tasks), 1)
            self._finish_phase(
                session,
                operations[0].id,
                "VERIFY",
                failure_node_id=verify_tasks[0].node_id,
            )

            runtimes = list(
                session.scalars(
                    select(FleetDeploymentRuntime)
                    .where(
                        FleetDeploymentRuntime.fleet_id
                        == context["fleet_id"]
                    )
                    .order_by(FleetDeploymentRuntime.deployment_id)
                )
            )
            self.assertEqual(
                [item.status for item in runtimes],
                ["VERIFY_FAILED", "ACTIVE"],
            )
            self.assertEqual(
                session.get(FleetRecord, context["fleet_id"]).status,
                "PARTIAL_FAILED",
            )
            reservations = list(
                session.scalars(
                    select(FleetResourceReservation).where(
                        FleetResourceReservation.fleet_id
                        == context["fleet_id"]
                    )
                )
            )
            self.assertTrue(all(item.released_at is None for item in reservations))
            self.assertFalse(
                session.scalar(
                    select(Task.id).where(
                        Task.type.in_(
                            {
                                TaskType.STOP_DEPLOYMENT.value,
                                TaskType.RESTART_DEPLOYMENT.value,
                            }
                        )
                    )
                )
            )

    def test_stale_operation_cannot_claim_finish_or_project_runtime_state(self) -> None:
        context = self._accepted_fleet(node_count=1, key="stale-operation")
        self._prepare_successfully(context)
        with self.factory() as session:
            action = apply_fleet(session, context["fleet_id"])["actions"][0]
            stale = session.get(DeploymentOperation, action["operation_id"])
            task = self._phase_tasks(session, stale.id, "APPLY")[0]
            runtime = session.scalar(
                select(FleetDeploymentRuntime).where(
                    FleetDeploymentRuntime.fleet_id == context["fleet_id"]
                )
            )
            replacement = DeploymentOperation(
                id=str(uuid.uuid4()),
                request_digest="sha256:" + "9" * 64,
                lineage_id=stale.lineage_id,
                deployment_id=stale.deployment_id,
                kind="APPLY",
                status="QUEUED",
                phase="APPLY",
                node_ids=list(stale.node_ids),
                serve=True,
                api=True,
                active_lineage_id=None,
            )
            session.add(replacement)
            session.flush()
            runtime.current_operation_id = replacement.id
            session.commit()

            task.status = TaskStatus.RUNNING.value
            task.attempts = 1
            self.assertFalse(claim_operation_task(session, task, task.node_id))
            self.assertFalse(
                finish_operation_task(
                    session,
                    task,
                    task.node_id,
                    result=self._operation_result(task),
                    error=None,
                )
            )
            self.assertFalse(fleet_operation_is_current(session, stale))
            self.assertFalse(sync_fleet_operation_status(session, stale))
            session.refresh(runtime)
            self.assertEqual(runtime.current_operation_id, replacement.id)
            self.assertEqual(runtime.status, "APPLYING")


class FleetRuntimeAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        database_url = f"sqlite:///{Path(self.temporary.name) / 'fleet-api.db'}"
        self.client = TestClient(
            create_app(
                database_url=database_url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.admin = {"Authorization": "Bearer admin-secret"}

    def tearDown(self) -> None:
        self.client.close()
        self.temporary.cleanup()

    def test_prepare_and_apply_require_auth_and_strict_empty_body(self) -> None:
        fleet_id = str(uuid.uuid4())
        for action in ("prepare", "apply"):
            with self.subTest(action=action):
                unauthorized = self.client.post(
                    f"/v1/admin/fleets/{fleet_id}/{action}", json={}
                )
                self.assertEqual(unauthorized.status_code, 401)

                target = f"dure.control.api.{action}_fleet"
                service_result = {"fleet_id": fleet_id, "actions": []}
                with patch(target, return_value=service_result) as service, patch(
                    "dure.control.api.show_fleet",
                    return_value={"fleet": {"id": fleet_id}},
                ) as show:
                    strict = self.client.post(
                        f"/v1/admin/fleets/{fleet_id}/{action}",
                        headers=self.admin,
                        json={"apply": True},
                    )
                    self.assertEqual(strict.status_code, 422, strict.text)
                    service.assert_not_called()

                    accepted = self.client.post(
                        f"/v1/admin/fleets/{fleet_id}/{action}",
                        headers=self.admin,
                        json={},
                    )
                    self.assertEqual(accepted.status_code, 200, accepted.text)
                    self.assertEqual(
                        accepted.json(),
                        {
                            "fleet_id": fleet_id,
                            "actions": [],
                            "fleet": {"id": fleet_id},
                        },
                    )
                    service.assert_called_once_with(ANY, fleet_id)
                    show.assert_called_once_with(ANY, fleet_id)

    def test_prepare_and_apply_return_structured_not_found(self) -> None:
        fleet_id = str(uuid.uuid4())
        for action in ("prepare", "apply"):
            with self.subTest(action=action):
                response = self.client.post(
                    f"/v1/admin/fleets/{fleet_id}/{action}",
                    headers=self.admin,
                    json={},
                )
                self.assertEqual(response.status_code, 404, response.text)
                self.assertEqual(
                    response.json()["detail"]["code"], "FLEET_NOT_FOUND"
                )


if __name__ == "__main__":
    unittest.main()
