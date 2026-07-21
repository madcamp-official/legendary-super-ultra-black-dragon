from __future__ import annotations

import copy
import json
import tempfile
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Callable

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import Deployment, Node, Task, utcnow
from dure.control.rollout import (
    DeploymentRolloutConflictError,
    DeploymentRolloutError,
    attach_deployment_bulk_operation,
    claim_operation_task,
    finish_operation_task,
    prepare_or_apply_rollback,
    valid_deployment_task_success_result,
)
from dure.control.service import (
    claim_enrollment,
    create_enrollment,
    create_tasks,
    finish_task,
    save_deployment,
)
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
)
from dure.models import (
    DeploymentPlan,
    ModelSpec,
    NodeAssignment,
    VLLM_RAY_PP_BACKEND,
    VLLM_RAY_PP_RUNTIME_VERSION,
)
from dure.pipeline_runtime import pipeline_contract_detail
from dure.stage_cache import stage_contract_identity_digest
from dure.task import TaskStatus, TaskType

from tests.helpers import profile


NODE_A = "6a8c4f83-3d37-4fd6-a0a0-c3bf18a44aa1"
NODE_B = "6a8c4f83-3d37-4fd6-a0a0-c3bf18a44aa2"
IMAGE = "registry.example/vllm@sha256:" + "a" * 64


def _strict_plan(
    deployment_id: str,
    *,
    node_ids: tuple[str, str] = (NODE_A, NODE_B),
    generation: int = 1,
) -> DeploymentPlan:
    return DeploymentPlan(
        deployment_id=deployment_id,
        generation=generation,
        model=ModelSpec(
            model_id="strict-test-model",
            repository="example/strict-test-model",
            quantization="awq",
            checkpoint_gib=10.0,
            min_gpu_memory_gib=8.0,
            default_max_model_len=4096,
            layer_count=4,
        ),
        image=IMAGE,
        pipeline_parallel_size=2,
        tensor_parallel_size=1,
        ray_head_node_id=node_ids[0],
        ray_head_address="10.10.10.1:6379",
        network_interface="ens3",
        model_revision="a" * 40,
        model_path="/var/lib/dure/models/sha256-" + "b" * 64,
        assignments=[
            NodeAssignment(
                node_id=node_ids[0],
                gpu_index=0,
                rank=0,
                pipeline_rank=0,
                expected_runtime_rank=0,
                runtime_address="10.10.10.1",
                layer_start=0,
                layer_end=1,
                role="ray-head",
            ),
            NodeAssignment(
                node_id=node_ids[1],
                gpu_index=0,
                rank=1,
                pipeline_rank=1,
                expected_runtime_rank=1,
                runtime_address="10.10.10.2",
                layer_start=2,
                layer_end=3,
                role="ray-worker",
            ),
        ],
        execution_backend=VLLM_RAY_PP_BACKEND,
        runtime_vllm_version=VLLM_RAY_PP_RUNTIME_VERSION,
        model_cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
    )


def _strict_stage_plan(deployment_id: str) -> DeploymentPlan:
    plan = _strict_plan(deployment_id).to_dict()
    source_manifest_digest = "sha256:" + "c" * 64
    exporter_build_digest = "sha256:" + "d" * 64
    contract_identity_digest = stage_contract_identity_digest(
        source_manifest_digest=source_manifest_digest,
        runtime_image=IMAGE,
        vllm_version=VLLM_RAY_PP_RUNTIME_VERSION,
        exporter_build_digest=exporter_build_digest,
        architecture="Qwen2ForCausalLM",
        quantization="awq",
        tensor_parallel_size=1,
        pipeline_parallel_size=2,
        loader_format="VLLM_SHARDED_STATE_V1",
    )
    plan.update(
        model_path="/var/lib/dure/models/stages",
        model_cache_kind=MODEL_CACHE_KIND_STAGE,
        stage_artifact={
            "artifact_set_digest": "sha256:" + "e" * 64,
            "contract_identity_digest": contract_identity_digest,
            "source_manifest_digest": source_manifest_digest,
            "runtime_image": IMAGE,
            "vllm_version": VLLM_RAY_PP_RUNTIME_VERSION,
            "exporter_build_digest": exporter_build_digest,
            "architecture": "Qwen2ForCausalLM",
            "quantization": "awq",
            "tensor_parallel_size": 1,
            "pipeline_parallel_size": 2,
            "loader_format": "VLLM_SHARDED_STATE_V1",
        },
    )
    for rank, assignment in enumerate(plan["assignments"]):
        assignment["stage_manifest_digest"] = (
            "sha256:" + str(rank + 4) * 64
        )
        assignment["stage_tensor_keys_digest"] = (
            "sha256:" + str(rank + 6) * 64
        )
    return DeploymentPlan.from_dict(plan)


def _rank_detail(plan: dict, node_id: str) -> str:
    if plan.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE:
        typed_plan = DeploymentPlan.from_dict(plan)
        assignment = typed_plan.assignment_for(node_id)
        assert assignment is not None
        return pipeline_contract_detail(typed_plan, assignment)
    bindings = [
        {
            "node_id": assignment["node_id"],
            "runtime_address": assignment["runtime_address"],
            "pipeline_rank": assignment["pipeline_rank"],
            "runtime_rank": assignment["expected_runtime_rank"],
        }
        for assignment in sorted(
            plan["assignments"],
            key=lambda item: item["expected_runtime_rank"],
        )
    ]
    current = next(item for item in bindings if item["node_id"] == node_id)
    return json.dumps(
        {
            "schema_version": 1,
            "backend": VLLM_RAY_PP_BACKEND,
            "vllm_version": VLLM_RAY_PP_RUNTIME_VERSION,
            **current,
            "ordered_bindings": bindings,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _verify_result(plan: dict, node_id: str) -> dict:
    checks = [
        {
            "name": name,
            "ok": True,
            "detail": "verified",
            "blocking": True,
        }
        for name in ("host-gpu", "container-gpu")
    ]
    checks.append(
        {
            "name": "pipeline-rank-contract",
            "ok": True,
            "detail": _rank_detail(plan, node_id),
            "blocking": True,
        }
    )
    if node_id == plan["ray_head_node_id"]:
        checks.append(
            {
                "name": "vllm-api",
                "ok": True,
                "detail": "verified",
                "blocking": True,
            }
        )
    return {"checks": checks, "ok": True}


def _apply_result(
    plan: dict, node_id: str, *, model_check: str = "model"
) -> dict:
    checks = [
        {
            "name": name,
            "ok": True,
            "detail": "verified",
            "blocking": True,
        }
        for name in (
            "node-profile",
            "deployment-plan",
            model_check,
            "container-image",
            "ray-container",
            "host-gpu",
            "container-gpu",
        )
    ]
    checks.append(
        {
            "name": "pipeline-rank-contract",
            "ok": True,
            "detail": _rank_detail(plan, node_id),
            "blocking": True,
        }
    )
    return {"checks": checks}


def _rank_check(result: dict) -> dict:
    return next(
        check
        for check in result["checks"]
        if check["name"] == "pipeline-rank-contract"
    )


def _set_nested_boolean_rank(result: dict) -> None:
    check = _rank_check(result)
    detail = json.loads(check["detail"])
    detail["ordered_bindings"][0]["runtime_rank"] = False
    check["detail"] = json.dumps(
        detail, sort_keys=True, separators=(",", ":")
    )


def _add_duplicate_backend_key(result: dict) -> None:
    check = _rank_check(result)
    check["detail"] = check["detail"].replace(
        '{"backend":',
        '{"backend":"VLLM_RAY_PP_V1","backend":',
        1,
    )


class StrictRayControlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.engine = make_engine(
            f"sqlite:///{Path(self.temporary.name) / 'strict-control.db'}"
        )
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temporary.cleanup()

    def _enroll(self, session, *, name: str, address: str, version: str):
        _, token = create_enrollment(session, timedelta(hours=1))
        return claim_enrollment(
            session,
            token=token,
            install_id=f"install-{name}",
            profile=profile(name, address=address).to_dict(),
            agent_version=version,
        )[0]

    def _add_nodes(self, session) -> None:
        for node_id in (NODE_A, NODE_B):
            session.add(
                Node(
                    id=node_id,
                    install_id=f"install-{node_id}",
                    display_name=node_id,
                    hostname=node_id,
                    agent_version="0.3.18",
                    approved=True,
                    last_seen=utcnow(),
                )
            )
        session.commit()

    def _operation_task(self, session, plan: dict, *, suffix: str) -> Task:
        deployment_id = plan["deployment_id"]
        deployment = Deployment(
            id=deployment_id,
            lineage_id=deployment_id,
            generation=1,
            plan=plan,
            accept_model_download=False,
            pull_image=False,
        )
        task = Task(
            bulk_id=f"bulk-{suffix}",
            node_id=NODE_A,
            type=TaskType.VERIFY.value,
            deployment_id=deployment_id,
            payload={"plan": plan, "generation": 1, "api": False},
        )
        session.add_all([deployment, task])
        session.flush()
        attach_deployment_bulk_operation(
            session,
            deployment=deployment,
            task_type=TaskType.VERIFY,
            tasks=[task],
            options={"api": True},
        )
        session.commit()
        task.status = TaskStatus.RUNNING.value
        task.attempts = 1
        self.assertTrue(claim_operation_task(session, task, NODE_A))
        session.commit()
        return task

    def _apply_operation_task(self, session, plan: dict, *, suffix: str) -> Task:
        deployment_id = plan["deployment_id"]
        deployment = Deployment(
            id=deployment_id,
            lineage_id=deployment_id,
            generation=1,
            plan=plan,
            accept_model_download=False,
            pull_image=False,
        )
        tasks = [
            Task(
                bulk_id=f"bulk-{suffix}",
                node_id=node_id,
                type=TaskType.APPLY_DEPLOYMENT.value,
                deployment_id=deployment_id,
                payload={"plan": plan, "generation": 1, "serve": False},
            )
            for node_id in (NODE_A, NODE_B)
        ]
        session.add_all([deployment, *tasks])
        session.flush()
        attach_deployment_bulk_operation(
            session,
            deployment=deployment,
            task_type=TaskType.APPLY_DEPLOYMENT,
            tasks=tasks,
            options={"serve": True},
        )
        session.commit()
        task = tasks[0]
        task.status = TaskStatus.RUNNING.value
        task.attempts = 1
        self.assertTrue(claim_operation_task(session, task, NODE_A))
        session.commit()
        return task

    def test_strict_start_results_require_cache_kind_specific_check(self) -> None:
        plans = (
            (
                _strict_plan(str(uuid.uuid4())).to_dict(),
                "model",
                "stage-cache",
            ),
            (
                _strict_stage_plan(str(uuid.uuid4())).to_dict(),
                "stage-cache",
                "model",
            ),
        )
        for plan, required_check, wrong_check in plans:
            for task_type in (
                TaskType.APPLY_DEPLOYMENT.value,
                TaskType.START_DEPLOYMENT.value,
                TaskType.RESTART_DEPLOYMENT.value,
            ):
                with self.subTest(
                    cache_kind=plan["model_cache_kind"],
                    task_type=task_type,
                ):
                    task = Task(
                        node_id=NODE_B,
                        type=task_type,
                        deployment_id=plan["deployment_id"],
                        payload={"plan": plan, "serve": False},
                    )
                    valid = _apply_result(
                        plan, NODE_B, model_check=required_check
                    )
                    self.assertTrue(
                        valid_deployment_task_success_result(
                            task,
                            valid,
                            operation_kind="APPLY",
                            operation_phase="APPLY",
                        )
                    )

                    wrong = _apply_result(
                        plan, NODE_B, model_check=wrong_check
                    )
                    self.assertFalse(
                        valid_deployment_task_success_result(
                            task,
                            wrong,
                            operation_kind="APPLY",
                            operation_phase="APPLY",
                        )
                    )

                    extra = copy.deepcopy(valid)
                    extra["checks"].append(
                        {
                            "name": wrong_check,
                            "ok": True,
                            "detail": "verified",
                            "blocking": True,
                        }
                    )
                    self.assertFalse(
                        valid_deployment_task_success_result(
                            task,
                            extra,
                            operation_kind="APPLY",
                            operation_phase="APPLY",
                        )
                    )

    def test_strict_deployment_requires_exact_uuid_nodes_and_new_agents(self) -> None:
        with self.factory() as session:
            node_a = self._enroll(
                session,
                name="node-a",
                address="10.10.10.1",
                version="0.3.17",
            )
            node_b = self._enroll(
                session,
                name="node-b",
                address="10.10.10.2",
                version="0.3.18",
            )
            invalid_image_plan = _strict_plan(str(uuid.uuid4())).to_dict()
            invalid_image_plan["image"] += ":tag"
            with self.assertRaisesRegex(ValueError, "OCI sha256 digest"):
                save_deployment(
                    session,
                    invalid_image_plan,
                    accept_model_download=False,
                    pull_image=False,
                )

            unknown_plan = _strict_plan(str(uuid.uuid4())).to_dict()
            unknown_plan["assignments"][0]["node_id"] = str(uuid.uuid4())
            unknown_plan["ray_head_node_id"] = unknown_plan["assignments"][0]["node_id"]
            with self.assertRaisesRegex(ValueError, "server-issued node UUIDs"):
                save_deployment(
                    session,
                    unknown_plan,
                    accept_model_download=False,
                    pull_image=False,
                )

            plan = _strict_plan(
                str(uuid.uuid4()), node_ids=(node_a.id, node_b.id)
            )
            deployment = save_deployment(
                session,
                plan.to_dict(),
                accept_model_download=False,
                pull_image=False,
            )
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                create_tasks(
                    session,
                    node_ids=[node_a.id, node_b.id],
                    task_type=TaskType.VERIFY,
                    deployment_id=deployment.id,
                    options={"api": True},
                )
            self.assertEqual(context.exception.code, "DEPLOYMENT_STRICT_AGENT_TOO_OLD")

            session.get(Node, node_a.id).agent_version = "0.3.18"
            session.commit()
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                create_tasks(
                    session,
                    node_ids=[node_a.id],
                    task_type=TaskType.VERIFY,
                    deployment_id=deployment.id,
                    options={"api": True},
                )
            self.assertEqual(context.exception.code, "DEPLOYMENT_STRICT_NODE_SET_MISMATCH")

            with self.assertRaises(DeploymentRolloutConflictError) as context:
                create_tasks(
                    session,
                    node_ids=[node_a.id, node_b.id, node_b.id],
                    task_type=TaskType.VERIFY,
                    deployment_id=deployment.id,
                    options={"api": True},
                )
            self.assertEqual(context.exception.code, "DEPLOYMENT_STRICT_NODE_SET_MISMATCH")

            session.get(Node, node_b.id).approved = False
            session.commit()
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                create_tasks(
                    session,
                    node_ids=[node_a.id, node_b.id],
                    task_type=TaskType.VERIFY,
                    deployment_id=deployment.id,
                    options={"api": True},
                )
            self.assertEqual(context.exception.code, "DEPLOYMENT_STRICT_NODE_UNAVAILABLE")
            session.get(Node, node_b.id).approved = True
            session.commit()

            session.get(Node, node_b.id).last_seen = utcnow() - timedelta(
                seconds=31
            )
            session.commit()
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                create_tasks(
                    session,
                    node_ids=[node_a.id, node_b.id],
                    task_type=TaskType.VERIFY,
                    deployment_id=deployment.id,
                    options={"api": True},
                )
            self.assertEqual(
                context.exception.code, "DEPLOYMENT_STRICT_NODE_UNAVAILABLE"
            )
            session.get(Node, node_b.id).last_seen = utcnow()
            session.commit()

            with self.assertRaises(DeploymentRolloutConflictError) as context:
                create_tasks(
                    session,
                    node_ids=[node_a.id, node_b.id],
                    task_type=TaskType.VERIFY,
                    deployment_id=deployment.id,
                    options={"api": False},
                )
            self.assertEqual(
                context.exception.code,
                "DEPLOYMENT_STRICT_RUNTIME_ATTESTATION_REQUIRED",
            )

            _bulk_id, tasks, errors = create_tasks(
                session,
                node_ids=[node_a.id, node_b.id],
                task_type=TaskType.VERIFY,
                deployment_id=deployment.id,
                options={"api": True},
            )
            self.assertEqual(len(tasks), 2)
            self.assertFalse(errors)

    def test_strict_rank_evidence_is_exact_and_unknown_backend_is_closed(self) -> None:
        with self.factory() as session:
            self._add_nodes(session)
            valid_plan = _strict_plan(str(uuid.uuid4())).to_dict()
            valid_task = self._operation_task(session, valid_plan, suffix="valid")
            self.assertTrue(
                finish_operation_task(
                    session,
                    valid_task,
                    NODE_A,
                    result=_verify_result(valid_plan, NODE_A),
                    error=None,
                )
            )
            session.commit()
            self.assertEqual(valid_task.status, TaskStatus.SUCCEEDED.value)

            variants: list[tuple[str, Callable[[dict], None]]] = [
                (
                    "missing",
                    lambda result: result.update(
                        checks=[
                            check
                            for check in result["checks"]
                            if check["name"] != "pipeline-rank-contract"
                        ]
                    ),
                ),
                (
                    "duplicate",
                    lambda result: result["checks"].append(
                        copy.deepcopy(_rank_check(result))
                    ),
                ),
                (
                    "swapped",
                    lambda result: _rank_check(result).update(
                        detail=_rank_check(result)["detail"].replace(
                            '"runtime_rank":0', '"runtime_rank":1', 1
                        )
                    ),
                ),
                (
                    "nonblocking",
                    lambda result: _rank_check(result).update(blocking=False),
                ),
                (
                    "failed-nonblocking-required-check",
                    lambda result: result["checks"][0].update(
                        ok=False, blocking=False
                    ),
                ),
                ("nested-boolean-rank", _set_nested_boolean_rank),
                ("duplicate-json-key", _add_duplicate_backend_key),
                (
                    "unknown-extra-check",
                    lambda result: result["checks"].append(
                        {
                            "name": "agent-note",
                            "ok": True,
                            "detail": "unexpected",
                            "blocking": False,
                        }
                    ),
                ),
                (
                    "oversized-detail",
                    lambda result: result["checks"][0].update(
                        detail="x" * 8193
                    ),
                ),
            ]
            for suffix, mutate in variants:
                with self.subTest(suffix=suffix):
                    plan = _strict_plan(str(uuid.uuid4())).to_dict()
                    task = self._operation_task(session, plan, suffix=suffix)
                    result = _verify_result(plan, NODE_A)
                    mutate(result)
                    self.assertTrue(
                        finish_operation_task(
                            session,
                            task,
                            NODE_A,
                            result=result,
                            error=None,
                        )
                    )
                    session.commit()
                    self.assertEqual(task.status, TaskStatus.FAILED.value)
                    self.assertEqual(task.error, "TASK_RESULT_INVALID")

            unknown_plan = _strict_plan(str(uuid.uuid4())).to_dict()
            unknown_plan["execution_backend"] = "UNSUPPORTED_BACKEND"
            deployment_id = unknown_plan["deployment_id"]
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan=unknown_plan,
                accept_model_download=False,
                pull_image=False,
            )
            task = Task(
                bulk_id="bulk-unknown",
                node_id=NODE_A,
                type=TaskType.VERIFY.value,
                deployment_id=deployment_id,
                payload={"plan": unknown_plan, "generation": 1, "api": False},
            )
            session.add_all([deployment, task])
            session.flush()
            with self.assertRaises(DeploymentRolloutError) as context:
                attach_deployment_bulk_operation(
                    session,
                    deployment=deployment,
                    task_type=TaskType.VERIFY,
                    tasks=[task],
                    options={"api": False},
                )
            self.assertEqual(context.exception.code, "ROLLBACK_PLAN_INVALID")

    def test_strict_apply_never_accepts_the_legacy_empty_result_shape(self) -> None:
        with self.factory() as session:
            self._add_nodes(session)
            plan = _strict_plan(str(uuid.uuid4())).to_dict()
            task = self._apply_operation_task(session, plan, suffix="empty-apply")

            self.assertTrue(
                finish_operation_task(
                    session,
                    task,
                    NODE_A,
                    result={},
                    error=None,
                )
            )
            session.commit()

            self.assertEqual(task.status, TaskStatus.FAILED.value)
            self.assertEqual(task.error, "TASK_RESULT_INVALID")

    def test_strict_staged_apply_accepts_ray_phase_then_queues_head_api(self) -> None:
        with self.factory() as session:
            self._add_nodes(session)
            plan = _strict_plan(str(uuid.uuid4())).to_dict()
            deployment = save_deployment(
                session,
                plan,
                accept_model_download=False,
                pull_image=False,
            )
            _bulk_id, tasks, errors = create_tasks(
                session,
                node_ids=[NODE_A, NODE_B],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={"serve": True},
            )
            self.assertFalse(errors)
            self.assertEqual(len(tasks), 2)
            self.assertTrue(all(task.payload["serve"] is False for task in tasks))

            initial_ids = {task.id for task in tasks}
            for task in tasks:
                task.status = TaskStatus.RUNNING.value
                task.attempts = 1
                self.assertTrue(claim_operation_task(session, task, task.node_id))
                session.commit()
                self.assertTrue(
                    finish_operation_task(
                        session,
                        task,
                        task.node_id,
                        result=_apply_result(plan, task.node_id),
                        error=None,
                    )
                )
                session.commit()

            followups = [
                task
                for task in session.query(Task).all()
                if task.id not in initial_ids
            ]
            self.assertEqual(len(followups), 1)
            self.assertEqual(
                followups[0].type, TaskType.START_DEPLOYMENT.value
            )
            self.assertEqual(followups[0].node_id, NODE_A)
            self.assertIs(followups[0].payload["serve"], True)

    def test_direct_strict_lifecycle_tasks_use_the_closed_result_schema(self) -> None:
        with self.factory() as session:
            self._add_nodes(session)
            for task_type in (
                TaskType.START_DEPLOYMENT,
                TaskType.RESTART_DEPLOYMENT,
                TaskType.STOP_DEPLOYMENT,
            ):
                with self.subTest(task_type=task_type.value):
                    plan = _strict_plan(str(uuid.uuid4())).to_dict()
                    deployment = Deployment(
                        id=plan["deployment_id"],
                        lineage_id=plan["deployment_id"],
                        generation=1,
                        plan=plan,
                        accept_model_download=False,
                        pull_image=False,
                    )
                    payload = {"plan": plan, "generation": 1}
                    if task_type != TaskType.STOP_DEPLOYMENT:
                        payload["serve"] = True
                    task = Task(
                        bulk_id=f"bulk-direct-{task_type.value}",
                        node_id=NODE_A,
                        type=task_type.value,
                        deployment_id=deployment.id,
                        payload=payload,
                        status=TaskStatus.RUNNING.value,
                    )
                    session.add_all([deployment, task])
                    session.commit()

                    self.assertTrue(
                        finish_task(
                            session,
                            task,
                            NODE_A,
                            result={},
                            error=None,
                        )
                    )
                    self.assertEqual(task.status, TaskStatus.FAILED.value)
                    self.assertEqual(task.error, "TASK_RESULT_INVALID")

            plan = _strict_plan(str(uuid.uuid4())).to_dict()
            deployment = Deployment(
                id=plan["deployment_id"],
                lineage_id=plan["deployment_id"],
                generation=1,
                plan=plan,
                accept_model_download=False,
                pull_image=False,
            )
            task = Task(
                bulk_id="bulk-direct-valid-stop",
                node_id=NODE_A,
                type=TaskType.STOP_DEPLOYMENT.value,
                deployment_id=deployment.id,
                payload={"plan": plan, "generation": 1},
                status=TaskStatus.RUNNING.value,
            )
            session.add_all([deployment, task])
            session.commit()
            result = {
                "checks": [
                    {
                        "name": "deployment-stop",
                        "ok": True,
                        "detail": "stopped",
                        "blocking": True,
                    }
                ]
            }
            self.assertTrue(
                finish_task(
                    session,
                    task,
                    NODE_A,
                    result=result,
                    error=None,
                )
            )
            self.assertEqual(task.status, TaskStatus.SUCCEEDED.value)
            self.assertIsNone(task.error)

    def test_strict_rollback_requires_strict_capable_agents(self) -> None:
        with self.factory() as session:
            self._add_nodes(session)
            for node_id in (NODE_A, NODE_B):
                session.get(Node, node_id).agent_version = "0.3.17"

            target_id = str(uuid.uuid4())
            source_id = str(uuid.uuid4())
            target = Deployment(
                id=target_id,
                lineage_id=target_id,
                generation=1,
                plan=_strict_plan(target_id, generation=1).to_dict(),
                accept_model_download=False,
                pull_image=False,
                status="VERIFIED",
                verified_at=utcnow() - timedelta(hours=1),
            )
            source = Deployment(
                id=source_id,
                lineage_id=target_id,
                previous_generation_id=target_id,
                generation=2,
                plan=_strict_plan(source_id, generation=2).to_dict(),
                accept_model_download=False,
                pull_image=False,
                status="APPLIED",
            )
            session.add_all([target, source])
            session.commit()

            with self.assertRaises(DeploymentRolloutError) as context:
                prepare_or_apply_rollback(
                    session,
                    source_id,
                    [NODE_A, NODE_B],
                    apply=False,
                    serve=False,
                )
            self.assertEqual(context.exception.code, "ROLLBACK_AGENT_TOO_OLD")
            session.rollback()

            for node_id in (NODE_A, NODE_B):
                session.get(Node, node_id).agent_version = "0.3.18"
            session.commit()
            with self.assertRaises(DeploymentRolloutError) as context:
                prepare_or_apply_rollback(
                    session,
                    source_id,
                    [NODE_A, NODE_B],
                    apply=False,
                    serve=False,
                )
            self.assertEqual(
                context.exception.code,
                "ROLLBACK_STRICT_API_ATTESTATION_REQUIRED",
            )
            session.rollback()

            operation, tasks, changed = prepare_or_apply_rollback(
                session,
                source_id,
                [NODE_A, NODE_B],
                apply=False,
                serve=True,
            )
            self.assertEqual(operation.status, "PREPARED")
            self.assertFalse(tasks)
            self.assertTrue(changed)

    def test_strict_rollback_allows_different_model_and_stage_identity(self) -> None:
        with self.factory() as session:
            self._add_nodes(session)
            for node_id in (NODE_A, NODE_B):
                session.get(Node, node_id).agent_version = "0.3.19"

            for source_cache_kind in (
                MODEL_CACHE_KIND_FULL_SNAPSHOT,
                MODEL_CACHE_KIND_STAGE,
            ):
                with self.subTest(source_cache_kind=source_cache_kind):
                    target_id = str(uuid.uuid4())
                    source_id = str(uuid.uuid4())
                    target_plan = _strict_stage_plan(target_id).to_dict()
                    if source_cache_kind == MODEL_CACHE_KIND_STAGE:
                        source_plan = _strict_stage_plan(source_id).to_dict()
                        source_manifest_digest = "sha256:" + "1" * 64
                        exporter_build_digest = "sha256:" + "2" * 64
                        stage_artifact = source_plan["stage_artifact"]
                        stage_artifact.update(
                            {
                                "artifact_set_digest": "sha256:" + "3" * 64,
                                "source_manifest_digest": source_manifest_digest,
                                "exporter_build_digest": exporter_build_digest,
                                "contract_identity_digest": (
                                    stage_contract_identity_digest(
                                        source_manifest_digest=(
                                            source_manifest_digest
                                        ),
                                        runtime_image=stage_artifact[
                                            "runtime_image"
                                        ],
                                        vllm_version=stage_artifact[
                                            "vllm_version"
                                        ],
                                        exporter_build_digest=(
                                            exporter_build_digest
                                        ),
                                        architecture=stage_artifact[
                                            "architecture"
                                        ],
                                        quantization=stage_artifact[
                                            "quantization"
                                        ],
                                        tensor_parallel_size=stage_artifact[
                                            "tensor_parallel_size"
                                        ],
                                        pipeline_parallel_size=stage_artifact[
                                            "pipeline_parallel_size"
                                        ],
                                        loader_format=stage_artifact[
                                            "loader_format"
                                        ],
                                    )
                                ),
                            }
                        )
                        for rank, assignment in enumerate(
                            source_plan["assignments"]
                        ):
                            assignment["stage_manifest_digest"] = (
                                "sha256:" + str(rank + 8) * 64
                            )
                            assignment["stage_tensor_keys_digest"] = (
                                "sha256:" + ("a" if rank == 0 else "b") * 64
                            )
                    else:
                        source_plan = _strict_plan(
                            source_id, generation=2
                        ).to_dict()
                        source_plan["model_path"] = (
                            "/var/lib/dure/models/sha256-" + "f" * 64
                        )

                    source_plan["generation"] = 2
                    source_plan["model"]["model_id"] = "strict-new-model"
                    source_plan["model"]["repository"] = (
                        "example/strict-new-model"
                    )
                    source_plan["model"]["layer_count"] = 6
                    source_plan["model_revision"] = "b" * 40
                    source_plan["assignments"][0]["layer_start"] = 0
                    source_plan["assignments"][0]["layer_end"] = 2
                    source_plan["assignments"][1]["layer_start"] = 3
                    source_plan["assignments"][1]["layer_end"] = 5
                    # Both immutable plans remain valid in isolation; only
                    # their artifact/model identity and model-specific layer
                    # ranges differ.
                    DeploymentPlan.from_dict(target_plan)
                    DeploymentPlan.from_dict(source_plan)

                    target = Deployment(
                        id=target_id,
                        lineage_id=target_id,
                        generation=1,
                        plan=target_plan,
                        accept_model_download=False,
                        pull_image=False,
                        status="VERIFIED",
                        verified_at=utcnow() - timedelta(hours=1),
                    )
                    source = Deployment(
                        id=source_id,
                        lineage_id=target_id,
                        previous_generation_id=target_id,
                        generation=2,
                        plan=source_plan,
                        accept_model_download=False,
                        pull_image=False,
                        status="APPLIED",
                    )
                    session.add_all([target, source])
                    session.commit()

                    operation, tasks, changed = prepare_or_apply_rollback(
                        session,
                        source_id,
                        [NODE_A, NODE_B],
                        apply=False,
                        serve=True,
                    )

                    self.assertEqual(operation.status, "PREPARED")
                    self.assertFalse(tasks)
                    self.assertTrue(changed)
                    self.assertEqual(
                        target.plan["model_cache_kind"],
                        MODEL_CACHE_KIND_STAGE,
                    )
                    self.assertNotEqual(
                        target.plan["model"], source.plan["model"]
                    )
                    if source_cache_kind == MODEL_CACHE_KIND_STAGE:
                        self.assertNotEqual(
                            target.plan["stage_artifact"],
                            source.plan["stage_artifact"],
                        )

    def test_strict_rollback_rejects_changed_runtime_topology(self) -> None:
        with self.factory() as session:
            self._add_nodes(session)
            target_id = str(uuid.uuid4())
            source_id = str(uuid.uuid4())
            target_plan = _strict_plan(target_id, generation=1).to_dict()
            source_plan = _strict_plan(source_id, generation=2).to_dict()
            source_plan["assignments"][1]["runtime_address"] = "10.10.10.3"
            # The source remains a valid strict plan, but it is a different
            # runtime topology from the direct rollback target.
            DeploymentPlan.from_dict(source_plan)
            target = Deployment(
                id=target_id,
                lineage_id=target_id,
                generation=1,
                plan=target_plan,
                accept_model_download=False,
                pull_image=False,
                status="VERIFIED",
                verified_at=utcnow() - timedelta(hours=1),
            )
            source = Deployment(
                id=source_id,
                lineage_id=target_id,
                previous_generation_id=target_id,
                generation=2,
                plan=source_plan,
                accept_model_download=False,
                pull_image=False,
                status="APPLIED",
            )
            session.add_all([target, source])
            session.commit()

            with self.assertRaises(DeploymentRolloutError) as rejected:
                prepare_or_apply_rollback(
                    session,
                    source_id,
                    [NODE_A, NODE_B],
                    apply=False,
                    serve=True,
                )

            self.assertEqual(
                rejected.exception.code, "ROLLBACK_TOPOLOGY_UNSUPPORTED"
            )


if __name__ == "__main__":
    unittest.main()
