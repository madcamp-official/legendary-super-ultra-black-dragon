from __future__ import annotations

import tempfile
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import func, select

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import (
    AuditEvent,
    Deployment,
    DeploymentOperation,
    DeploymentOperationNode,
    Node,
    Task,
    utcnow,
)
from dure.control.rollout import (
    DeploymentRolloutConflictError,
    DeploymentRolloutError,
    attach_deployment_bulk_operation,
    cancel_operation_task,
    claim_operation_task,
    deployment_generation_detail,
    deployment_lineage_generations,
    finish_operation_task,
    prepare_or_apply_rollback,
)
from dure.task import TaskStatus, TaskType


NODE_A = "6a8c4f83-3d37-4fd6-a0a0-c3bf18a44aa1"
NODE_B = "6a8c4f83-3d37-4fd6-a0a0-c3bf18a44aa2"
IMAGE = "registry.example/vllm@sha256:" + "a" * 64


def _plan(
    deployment_id: str,
    generation: int,
    node_ids: list[str],
    *,
    layer_end: int,
) -> dict:
    assignments = [
        {
            "node_id": node_id,
            "gpu_index": 0,
            "rank": rank,
            "pipeline_rank": rank,
            "layer_start": 0 if rank == 0 else rank * 10,
            "layer_end": layer_end if len(node_ids) == 1 else (rank + 1) * 10 - 1,
            "role": "ray-head" if rank == 0 else "ray-worker",
        }
        for rank, node_id in enumerate(node_ids)
    ]
    return {
        "deployment_id": deployment_id,
        "generation": generation,
        "image": IMAGE,
        "pipeline_parallel_size": len(node_ids),
        "tensor_parallel_size": 1,
        "ray_head_node_id": node_ids[0],
        "ray_head_address": "10.10.10.1:6379",
        "network_interface": "eth0",
        "assignments": assignments,
    }


def _check(name: str, *, ok: bool = True, blocking: bool = True) -> dict:
    return {
        "name": name,
        "ok": ok,
        "detail": "verified" if ok else "waiting for peers",
        "blocking": blocking,
    }


def _check_result(task: Task | None = None) -> dict:
    if task is None or task.type == TaskType.STOP_DEPLOYMENT.value:
        names = ["deployment-stop"]
    elif task.type == TaskType.VERIFY.value:
        names = ["host-gpu", "container-gpu", "ray-cluster"]
        plan = task.payload.get("plan", {})
        if (
            task.payload.get("api") is True
            and plan.get("ray_head_node_id") == task.node_id
        ):
            names.append("vllm-api")
    else:
        names = [
            "node-profile",
            "deployment-plan",
            "model",
            "container-image",
            "ray-container",
            "host-gpu",
            "container-gpu",
            "ray-cluster",
        ]
        plan = task.payload.get("plan", {})
        if (
            task.payload.get("serve") is True
            and plan.get("ray_head_node_id") == task.node_id
        ):
            names.extend(["vllm-api-start", "vllm-api"])
    result = {"checks": [_check(name) for name in names]}
    if task is not None and task.type == TaskType.VERIFY.value:
        result["ok"] = True
    return result


class DeploymentRolloutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.engine = make_engine(
            f"sqlite:///{Path(self.temporary.name) / 'rollout.db'}"
        )
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temporary.cleanup()

    def _node(
        self,
        session,
        node_id: str,
        *,
        approved: bool = True,
        agent_version: str = "0.3.12",
        age_seconds: int = 0,
    ) -> Node:
        node = Node(
            id=node_id,
            install_id=f"install-{node_id}",
            display_name=node_id,
            hostname=node_id,
            agent_version=agent_version,
            approved=approved,
            last_seen=utcnow() - timedelta(seconds=age_seconds),
        )
        session.add(node)
        return node

    def _lineage(self, session, node_ids: list[str]) -> tuple[Deployment, Deployment]:
        target_id = str(uuid.uuid4())
        source_id = str(uuid.uuid4())
        target = Deployment(
            id=target_id,
            lineage_id=target_id,
            previous_generation_id=None,
            generation=1,
            plan=_plan(target_id, 1, node_ids, layer_end=31),
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
            plan=_plan(source_id, 2, node_ids, layer_end=31),
            accept_model_download=False,
            pull_image=False,
            status="APPLIED",
        )
        session.add_all([target, source])
        session.commit()
        return target, source

    def _claim(self, session, task: Task) -> None:
        task.status = TaskStatus.RUNNING.value
        task.attempts += 1
        self.assertTrue(claim_operation_task(session, task, task.node_id))
        session.commit()

    def _finish(self, session, task: Task, *, verify: bool = False) -> None:
        self.assertTrue(
            finish_operation_task(
                session,
                task,
                task.node_id,
                result=_check_result(task),
                error=None,
            )
        )
        session.commit()

    def _tasks(self, session, operation_id: str, task_type: TaskType) -> list[Task]:
        return list(
            session.scalars(
                select(Task)
                .where(
                    Task.bulk_id == operation_id,
                    Task.type == task_type.value,
                )
                .order_by(Task.created_at, Task.id)
            )
        )

    def _phase_tasks(self, session, operation_id: str, phase: str) -> list[Task]:
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
                .order_by(Task.created_at, Task.id)
            )
        )

    def test_rollback_prepare_is_idempotent_and_apply_is_explicit(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            target, source = self._lineage(session, [NODE_A])

            operation, tasks, changed = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=False, serve=True
            )

            self.assertTrue(changed)
            self.assertEqual(operation.status, "PREPARED")
            self.assertFalse(tasks)
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)
            repeated, tasks, changed = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=False, serve=True
            )
            self.assertEqual(repeated.id, operation.id)
            self.assertFalse(changed)
            self.assertFalse(tasks)
            applied, tasks, changed = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=True, serve=True
            )
            self.assertEqual(applied.id, operation.id)
            self.assertTrue(changed)
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0].type, TaskType.STOP_DEPLOYMENT.value)
            self.assertEqual(
                set(tasks[0].payload), {"plan", "generation"}
            )
            self.assertEqual(source.status, "ROLLING_BACK")
            self.assertEqual(target.status, "ROLLBACK_TARGET_PENDING")
            audit_count = session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.target == operation.id)
            )
            self.assertEqual(audit_count, 1)

    def test_rollback_stages_stop_start_verify_and_projects_generation_detail(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            target, source = self._lineage(session, [NODE_A])
            source.verified_at = utcnow() - timedelta(minutes=5)
            session.commit()
            old_verified_at = target.verified_at
            operation, stop_tasks, _ = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=True, serve=True
            )

            stop = stop_tasks[0]
            self._claim(session, stop)
            self._finish(session, stop)
            start_tasks = self._tasks(
                session, operation.id, TaskType.START_DEPLOYMENT
            )
            self.assertEqual(len(start_tasks), 1)
            self.assertEqual(
                set(start_tasks[0].payload), {"plan", "generation", "serve"}
            )
            self.assertFalse(start_tasks[0].payload["serve"])

            start = start_tasks[0]
            self._claim(session, start)
            self._finish(session, start)
            verify_tasks = self._tasks(session, operation.id, TaskType.VERIFY)
            self.assertEqual(len(verify_tasks), 1)
            self.assertEqual(verify_tasks[0].payload["api"], False)

            verify = verify_tasks[0]
            self._claim(session, verify)
            self._finish(session, verify, verify=True)
            api_start_tasks = list(
                session.scalars(
                    select(Task)
                    .join(
                        DeploymentOperationNode,
                        DeploymentOperationNode.id == Task.operation_node_id,
                    )
                    .where(
                        DeploymentOperationNode.operation_id == operation.id,
                        DeploymentOperationNode.phase == "START_API",
                    )
                )
            )
            self.assertEqual(len(api_start_tasks), 1)
            self.assertTrue(api_start_tasks[0].payload["serve"])
            self._claim(session, api_start_tasks[0])
            self._finish(session, api_start_tasks[0])
            api_verify_tasks = list(
                session.scalars(
                    select(Task)
                    .join(
                        DeploymentOperationNode,
                        DeploymentOperationNode.id == Task.operation_node_id,
                    )
                    .where(
                        DeploymentOperationNode.operation_id == operation.id,
                        DeploymentOperationNode.phase == "VERIFY_API",
                    )
                )
            )
            self.assertEqual(len(api_verify_tasks), 1)
            self.assertTrue(api_verify_tasks[0].payload["api"])
            self._claim(session, api_verify_tasks[0])
            self._finish(session, api_verify_tasks[0], verify=True)
            session.refresh(operation)
            session.refresh(source)
            session.refresh(target)
            self.assertEqual((operation.status, operation.phase), ("SUCCEEDED", "COMPLETE"))
            self.assertIsNone(operation.active_lineage_id)
            self.assertEqual(source.status, "ROLLED_BACK")
            self.assertIsNone(source.verified_at)
            self.assertEqual(target.status, "VERIFIED")
            self.assertNotEqual(target.verified_at, old_verified_at)

            detail = deployment_generation_detail(session, target.id)
            self.assertEqual(detail["id"], target.id)
            self.assertTrue(detail["rollback_eligible"])
            self.assertEqual(detail["operations"][0]["kind"], "ROLLBACK")
            phase_nodes = detail["operations"][0]["nodes"]
            self.assertEqual(
                {item["phase"] for item in phase_nodes},
                {
                    "STOP_SOURCE",
                    "START_TARGET",
                    "VERIFY_TARGET",
                    "START_API",
                    "VERIFY_API",
                },
            )
            self.assertTrue(all(item["status"] == "SUCCEEDED" for item in phase_nodes))
            self.assertTrue(all(len(item["tasks"]) == 1 for item in phase_nodes))
            lineage = deployment_lineage_generations(session, source.id)
            self.assertEqual([item["generation"] for item in lineage], [1, 2])
            audit_count = session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.target == operation.id)
            )
            self.assertEqual(audit_count, 2)
            same, tasks, changed = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=True, serve=True
            )
            self.assertEqual(same.id, operation.id)
            self.assertFalse(tasks)
            self.assertFalse(changed)
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                prepare_or_apply_rollback(
                    session, source.id, [NODE_A], apply=False, serve=False
                )
            self.assertEqual(
                context.exception.code, "ROLLBACK_SOURCE_ALREADY_ROLLED_BACK"
            )
            session.rollback()

    def test_partial_stop_blocks_next_phase_and_retry_fences_late_completion(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            self._node(session, NODE_B)
            _target, source = self._lineage(session, [NODE_A, NODE_B])
            operation, stop_tasks, _ = prepare_or_apply_rollback(
                session,
                source.id,
                [NODE_B, NODE_A],
                apply=True,
                serve=False,
            )
            by_node = {task.node_id: task for task in stop_tasks}
            failed = by_node[NODE_A]
            self._claim(session, failed)
            self.assertTrue(
                finish_operation_task(
                    session,
                    failed,
                    failed.node_id,
                    result=None,
                    error="runtime detail must not become a failure code",
                )
            )
            session.commit()
            session.refresh(operation)
            self.assertEqual(operation.status, "PARTIAL_FAILED")
            self.assertFalse(
                self._tasks(session, operation.id, TaskType.START_DEPLOYMENT)
            )
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                prepare_or_apply_rollback(
                    session,
                    source.id,
                    [NODE_A, NODE_B],
                    apply=True,
                    serve=False,
                )
            self.assertEqual(context.exception.code, "ROLLBACK_PHASE_IN_PROGRESS")
            session.rollback()

            succeeded = by_node[NODE_B]
            self._claim(session, succeeded)
            self._finish(session, succeeded)
            session.refresh(operation)
            self.assertEqual(operation.status, "PARTIAL_FAILED")
            self.assertFalse(
                self._tasks(session, operation.id, TaskType.START_DEPLOYMENT)
            )

            _same, retry_tasks, changed = prepare_or_apply_rollback(
                session,
                source.id,
                [NODE_A, NODE_B],
                apply=True,
                serve=False,
            )
            self.assertTrue(changed)
            self.assertEqual(len(retry_tasks), 1)
            retry = retry_tasks[0]
            self.assertEqual(retry.node_id, NODE_A)
            self.assertEqual(retry.operation_attempt, 2)
            record = session.get(DeploymentOperationNode, retry.operation_node_id)
            self.assertEqual(record.attempt_count, 2)
            self.assertEqual(record.failure_code, None)
            # Attempt 1 cannot overwrite the retried node's current progress.
            self.assertFalse(
                finish_operation_task(
                    session,
                    failed,
                    failed.node_id,
                    result=_check_result(),
                    error=None,
                )
            )
            session.rollback()

            retry = session.get(Task, retry.id)
            self._claim(session, retry)
            self._finish(session, retry)
            self.assertEqual(
                len(self._tasks(session, operation.id, TaskType.START_DEPLOYMENT)),
                2,
            )
            failed_record = session.get(
                DeploymentOperationNode, failed.operation_node_id
            )
            self.assertIsNone(failed_record.failure_code)

    def test_rollback_rechecks_target_cache_after_stop_and_retries_without_network(self) -> None:
        from dure.control.preparation import ArtifactPreparationError

        with self.factory() as session:
            self._node(session, NODE_A)
            target, source = self._lineage(session, [NODE_A])
            gate = {"blocked": False}

            def effective_plan(
                _session,
                deployment,
                *,
                require_prepared=True,
                lock_ready_caches=True,
            ):
                if (
                    gate["blocked"]
                    and deployment.id == target.id
                    and require_prepared
                ):
                    raise ArtifactPreparationError(
                        "target cache is no longer ready",
                        code="DEPLOYMENT_ARTIFACT_CACHE_NOT_READY",
                    )
                return deployment.plan

            with patch(
                "dure.control.preparation.effective_deployment_plan",
                side_effect=effective_plan,
            ):
                operation, stop_tasks, changed = prepare_or_apply_rollback(
                    session,
                    source.id,
                    [NODE_A],
                    apply=True,
                    serve=False,
                )
                self.assertTrue(changed)
                self.assertEqual(len(stop_tasks), 1)
                self._claim(session, stop_tasks[0])

                # The cache can disappear or become corrupt while the source
                # is stopping. The second gate must fail before START_TARGET.
                gate["blocked"] = True
                self._finish(session, stop_tasks[0])
                session.refresh(operation)
                session.refresh(source)
                session.refresh(target)
                self.assertEqual(operation.phase, "START_TARGET")
                self.assertEqual(operation.status, "FAILED")
                self.assertEqual(source.status, "ROLLBACK_FAILED")
                self.assertEqual(target.status, "ROLLBACK_FAILED")
                self.assertFalse(
                    self._phase_tasks(session, operation.id, "START_TARGET")
                )
                start_record = session.scalar(
                    select(DeploymentOperationNode).where(
                        DeploymentOperationNode.operation_id == operation.id,
                        DeploymentOperationNode.phase == "START_TARGET",
                        DeploymentOperationNode.node_id == NODE_A,
                    )
                )
                self.assertIsNotNone(start_record)
                self.assertEqual(
                    start_record.failure_code,
                    "ROLLBACK_TARGET_CACHE_NOT_READY",
                )

                # Restoring exact READY evidence permits retrying only the
                # blocked phase. Rollback never prepares, downloads, or pulls.
                gate["blocked"] = False
                same, retry_tasks, retried = prepare_or_apply_rollback(
                    session,
                    source.id,
                    [NODE_A],
                    apply=True,
                    serve=False,
                )
                self.assertEqual(same.id, operation.id)
                self.assertTrue(retried)
                self.assertEqual(len(retry_tasks), 1)
                self.assertEqual(
                    retry_tasks[0].type, TaskType.START_DEPLOYMENT.value
                )
                self.assertNotIn(
                    "accept_model_download", retry_tasks[0].payload
                )
                self.assertNotIn("pull_image", retry_tasks[0].payload)
                self.assertFalse(
                    self._tasks(session, operation.id, TaskType.PREPARE_MODEL)
                )
                self.assertFalse(
                    self._tasks(session, operation.id, TaskType.PREPARE_IMAGE)
                )

    def test_rollback_denies_unverified_mismatched_or_unsafe_inputs(self) -> None:
        with self.factory() as session:
            node = self._node(session, NODE_A)
            target, source = self._lineage(session, [NODE_A])

            for apply, serve in ((1, False), (False, "false")):
                with self.subTest(apply=apply, serve=serve):
                    with self.assertRaises(DeploymentRolloutError) as context:
                        prepare_or_apply_rollback(
                            session,
                            source.id,
                            [NODE_A],
                            apply=apply,
                            serve=serve,
                        )
                    self.assertEqual(context.exception.code, "ROLLBACK_REQUEST_INVALID")
                    session.rollback()

            target = session.get(Deployment, target.id)
            target.status = "ROLLED_BACK"
            session.commit()
            with self.assertRaises(DeploymentRolloutError) as context:
                prepare_or_apply_rollback(
                    session, source.id, [NODE_A], apply=False, serve=False
                )
            self.assertEqual(context.exception.code, "ROLLBACK_TARGET_NOT_VERIFIED")
            session.rollback()
            target = session.get(Deployment, target.id)
            target.status = "VERIFIED"
            session.commit()

            target.verified_at = None
            session.commit()
            with self.assertRaises(DeploymentRolloutError) as context:
                prepare_or_apply_rollback(
                    session, source.id, [NODE_A], apply=False, serve=False
                )
            self.assertEqual(context.exception.code, "ROLLBACK_TARGET_NOT_VERIFIED")
            session.rollback()
            target = session.get(Deployment, target.id)
            target.verified_at = utcnow()
            node = session.get(Node, NODE_A)
            node.agent_version = "0.3.11"
            session.commit()
            with self.assertRaises(DeploymentRolloutError) as context:
                prepare_or_apply_rollback(
                    session, source.id, [NODE_A], apply=False, serve=False
                )
            self.assertEqual(context.exception.code, "ROLLBACK_AGENT_TOO_OLD")
            session.rollback()
            node = session.get(Node, NODE_A)
            node.agent_version = "0.3.12"
            source = session.get(Deployment, source.id)
            source.plan = dict(source.plan)
            source.plan["assignments"] = [dict(source.plan["assignments"][0])]
            source.plan["assignments"][0]["layer_end"] += 1
            session.commit()
            with self.assertRaises(DeploymentRolloutError) as context:
                prepare_or_apply_rollback(
                    session, source.id, [NODE_A], apply=False, serve=False
                )
            self.assertEqual(context.exception.code, "ROLLBACK_TOPOLOGY_UNSUPPORTED")

    def test_rollback_validation_is_all_or_none_before_task_creation(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            self._node(session, NODE_B, age_seconds=31)
            _target, source = self._lineage(session, [NODE_A, NODE_B])
            with self.assertRaises(DeploymentRolloutError) as context:
                prepare_or_apply_rollback(
                    session,
                    source.id,
                    [NODE_A, NODE_B],
                    apply=True,
                    serve=False,
                )
            self.assertEqual(context.exception.code, "ROLLBACK_NODE_OFFLINE")
            session.rollback()
            self.assertEqual(
                session.scalar(select(func.count()).select_from(DeploymentOperation)),
                0,
            )
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)

    def test_rollback_rejects_changed_network_topology_before_task_creation(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            _target, source = self._lineage(session, [NODE_A])
            source.plan = dict(source.plan)
            source.plan["ray_head_address"] = "10.10.10.99:6379"
            session.commit()

            with self.assertRaises(DeploymentRolloutError) as context:
                prepare_or_apply_rollback(
                    session, source.id, [NODE_A], apply=True, serve=False
                )

            self.assertEqual(context.exception.code, "ROLLBACK_TOPOLOGY_UNSUPPORTED")
            session.rollback()
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(DeploymentOperation)
                ),
                0,
            )

    def test_rollback_rejects_type_confused_legacy_topology(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            _target, source = self._lineage(session, [NODE_A])
            source_plan = dict(source.plan)
            source_plan["assignments"] = [
                dict(item) for item in source.plan["assignments"]
            ]
            # Python considers False == 0.  Stored JSON topology comparison
            # must still fail closed when the wire types differ.
            source_plan["assignments"][0]["gpu_index"] = False
            source.plan = source_plan
            session.commit()

            with self.assertRaises(DeploymentRolloutError) as context:
                prepare_or_apply_rollback(
                    session, source.id, [NODE_A], apply=True, serve=False
                )

            self.assertEqual(
                context.exception.code, "ROLLBACK_TOPOLOGY_UNSUPPORTED"
            )
            session.rollback()
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(DeploymentOperation)
                ),
                0,
            )
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)

    def test_multiple_prepares_are_allowed_but_only_one_can_be_applied(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            _target, source = self._lineage(session, [NODE_A])
            first, _, _ = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=False, serve=False
            )
            second, tasks, changed = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=False, serve=True
            )
            self.assertNotEqual(first.id, second.id)
            self.assertTrue(changed)
            self.assertFalse(tasks)
            self.assertIsNone(first.active_lineage_id)
            self.assertIsNone(second.active_lineage_id)
            first, tasks, changed = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=True, serve=False
            )
            self.assertTrue(changed)
            self.assertEqual(len(tasks), 1)
            self.assertEqual(first.active_lineage_id, first.lineage_id)
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                prepare_or_apply_rollback(
                    session, source.id, [NODE_A], apply=True, serve=True
                )
            self.assertEqual(context.exception.code, "DEPLOYMENT_OPERATION_ACTIVE")
            self.assertEqual(context.exception.details["operation_id"], first.id)

    def test_queued_cache_quarantine_blocks_rollback_activation(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            _target, source = self._lineage(session, [NODE_A])
            quarantine = Task(
                bulk_id=str(uuid.uuid4()),
                node_id=NODE_A,
                type=TaskType.QUARANTINE_ARTIFACT_CACHE.value,
                payload={
                    "node_id": NODE_A,
                    "cache_kind": "FULL_SNAPSHOT",
                    "cache_identity_digest": "sha256:" + "b" * 64,
                },
            )
            session.add(quarantine)
            session.commit()

            with self.assertRaises(DeploymentRolloutConflictError) as context:
                prepare_or_apply_rollback(
                    session,
                    source.id,
                    [NODE_A],
                    apply=True,
                    serve=False,
                )

            self.assertEqual(context.exception.code, "DEPLOYMENT_MUTATION_ACTIVE")
            self.assertEqual(context.exception.details["task_id"], quarantine.id)
            session.rollback()
            active = list(
                session.scalars(
                    select(Task).where(
                        Task.status.in_(
                            {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
                        )
                    )
                )
            )
            self.assertEqual([task.type for task in active], [
                TaskType.QUARANTINE_ARTIFACT_CACHE.value
            ])

    def test_generic_apply_and_verify_are_linked_and_full_verify_qualifies(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            deployment_id = str(uuid.uuid4())
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan=_plan(deployment_id, 1, [NODE_A], layer_end=31),
                accept_model_download=True,
                pull_image=True,
                status="CREATED",
            )
            session.add(deployment)
            session.commit()

            apply_task = Task(
                bulk_id=str(uuid.uuid4()),
                node_id=NODE_A,
                type=TaskType.APPLY_DEPLOYMENT.value,
                deployment_id=deployment.id,
                payload={
                    "plan": deployment.plan,
                    "generation": deployment.generation,
                    "serve": True,
                    "accept_model_download": True,
                    "pull_image": True,
                },
            )
            session.add(apply_task)
            operation = attach_deployment_bulk_operation(
                session,
                deployment=deployment,
                task_type=TaskType.APPLY_DEPLOYMENT,
                tasks=[apply_task],
                options={"serve": False},
            )
            session.commit()
            self.assertIsNotNone(operation)
            self.assertEqual(
                set(apply_task.payload),
                {
                    "plan",
                    "generation",
                    "serve",
                    "accept_model_download",
                    "pull_image",
                },
            )
            self.assertTrue(apply_task.payload["accept_model_download"])
            self.assertTrue(apply_task.payload["pull_image"])
            self.assertFalse(apply_task.payload["serve"])
            self.assertEqual(apply_task.operation_attempt, 1)
            self._claim(session, apply_task)
            # This preserves the legacy generic completion response contract.
            self.assertTrue(
                finish_operation_task(
                    session,
                    apply_task,
                    NODE_A,
                    result={"ok": True},
                    error=None,
                )
            )
            session.commit()
            session.refresh(deployment)
            self.assertEqual(deployment.status, "APPLIED")
            self.assertIsNone(deployment.verified_at)

            verify_task = Task(
                bulk_id=str(uuid.uuid4()),
                node_id=NODE_A,
                type=TaskType.VERIFY.value,
                deployment_id=deployment.id,
                payload={"plan": deployment.plan, "generation": 1, "api": False},
            )
            session.add(verify_task)
            verify_operation = attach_deployment_bulk_operation(
                session,
                deployment=deployment,
                task_type=TaskType.VERIFY,
                tasks=[verify_task],
                options={"api": False},
            )
            session.commit()
            self._claim(session, verify_task)
            self._finish(session, verify_task, verify=True)
            session.refresh(deployment)
            session.refresh(verify_operation)
            self.assertEqual(deployment.status, "VERIFIED")
            self.assertIsNotNone(deployment.verified_at)
            self.assertEqual(verify_operation.status, "SUCCEEDED")
            self.assertIsNone(verify_operation.active_lineage_id)

    def test_generic_apply_accepts_nonblocking_failed_check(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            deployment_id = str(uuid.uuid4())
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan=_plan(deployment_id, 1, [NODE_A], layer_end=31),
                accept_model_download=False,
                pull_image=False,
                status="CREATED",
            )
            session.add(deployment)
            session.commit()
            task = Task(
                bulk_id=str(uuid.uuid4()),
                node_id=NODE_A,
                type=TaskType.APPLY_DEPLOYMENT.value,
                deployment_id=deployment.id,
                payload={},
            )
            session.add(task)
            operation = attach_deployment_bulk_operation(
                session,
                deployment=deployment,
                task_type=TaskType.APPLY_DEPLOYMENT,
                tasks=[task],
                options={"serve": False},
            )
            session.commit()
            self._claim(session, task)
            result = _check_result(task)
            ray_check = next(
                item for item in result["checks"] if item["name"] == "ray-cluster"
            )
            ray_check.update(ok=False, blocking=False, detail="waiting for peers")
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
            session.refresh(operation)
            session.refresh(deployment)
            self.assertEqual(operation.status, "SUCCEEDED")
            self.assertEqual(deployment.status, "APPLIED")

    def test_generic_operation_cancel_projects_terminal_failure_once(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            deployment_id = str(uuid.uuid4())
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan=_plan(deployment_id, 1, [NODE_A], layer_end=31),
                accept_model_download=False,
                pull_image=False,
                status="CREATED",
            )
            session.add(deployment)
            session.commit()
            task = Task(
                bulk_id=str(uuid.uuid4()),
                node_id=NODE_A,
                type=TaskType.APPLY_DEPLOYMENT.value,
                deployment_id=deployment.id,
                payload={},
            )
            session.add(task)
            operation = attach_deployment_bulk_operation(
                session,
                deployment=deployment,
                task_type=TaskType.APPLY_DEPLOYMENT,
                tasks=[task],
                options={"serve": False},
            )
            session.commit()
            self.assertTrue(cancel_operation_task(session, task))
            session.commit()
            session.refresh(task)
            session.refresh(operation)
            session.refresh(deployment)
            record = session.get(DeploymentOperationNode, task.operation_node_id)
            self.assertEqual(task.status, TaskStatus.CANCELED.value)
            self.assertEqual(record.status, "CANCELED")
            self.assertEqual(record.failure_code, "TASK_CANCELED")
            self.assertEqual(operation.status, "FAILED")
            self.assertIsNone(operation.active_lineage_id)
            self.assertEqual(deployment.status, "APPLY_FAILED")
            audit_count = session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.target == operation.id)
            )
            self.assertEqual(audit_count, 2)

    def test_operation_cancel_hook_rejects_a_running_task(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            deployment_id = str(uuid.uuid4())
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan=_plan(deployment_id, 1, [NODE_A], layer_end=31),
                accept_model_download=False,
                pull_image=False,
                status="CREATED",
            )
            session.add(deployment)
            session.commit()
            task = Task(
                bulk_id=str(uuid.uuid4()),
                node_id=NODE_A,
                type=TaskType.APPLY_DEPLOYMENT.value,
                deployment_id=deployment.id,
                payload={},
            )
            session.add(task)
            operation = attach_deployment_bulk_operation(
                session,
                deployment=deployment,
                task_type=TaskType.APPLY_DEPLOYMENT,
                tasks=[task],
                options={"serve": False},
            )
            session.commit()
            self._claim(session, task)

            self.assertFalse(cancel_operation_task(session, task))
            session.rollback()

            session.refresh(task)
            session.refresh(operation)
            record = session.get(
                DeploymentOperationNode, task.operation_node_id
            )
            self.assertEqual(task.status, TaskStatus.RUNNING.value)
            self.assertEqual(record.status, "RUNNING")
            self.assertEqual(operation.status, "RUNNING")
            self.assertEqual(operation.active_lineage_id, deployment.id)

    def test_subset_verify_succeeds_without_creating_rollback_evidence(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            self._node(session, NODE_B)
            deployment_id = str(uuid.uuid4())
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan=_plan(deployment_id, 1, [NODE_A, NODE_B], layer_end=31),
                accept_model_download=False,
                pull_image=False,
                status="APPLIED",
            )
            session.add(deployment)
            session.commit()
            task = Task(
                bulk_id=str(uuid.uuid4()),
                node_id=NODE_A,
                type=TaskType.VERIFY.value,
                deployment_id=deployment.id,
                payload={},
            )
            session.add(task)
            operation = attach_deployment_bulk_operation(
                session,
                deployment=deployment,
                task_type=TaskType.VERIFY,
                tasks=[task],
                options={"api": False},
            )
            session.commit()
            self._claim(session, task)
            self._finish(session, task, verify=True)
            session.refresh(deployment)
            session.refresh(operation)
            self.assertEqual(operation.status, "SUCCEEDED")
            self.assertEqual(deployment.status, "PARTIALLY_VERIFIED")
            self.assertIsNone(deployment.verified_at)

    def test_multinode_serving_apply_stages_api_on_the_head_only(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            self._node(session, NODE_B)
            deployment_id = str(uuid.uuid4())
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan=_plan(
                    deployment_id, 1, [NODE_A, NODE_B], layer_end=31
                ),
                accept_model_download=False,
                pull_image=False,
                status="CREATED",
            )
            session.add(deployment)
            tasks = [
                Task(
                    bulk_id=str(uuid.uuid4()),
                    node_id=node_id,
                    type=TaskType.APPLY_DEPLOYMENT.value,
                    deployment_id=deployment.id,
                    payload={},
                )
                for node_id in (NODE_A, NODE_B)
            ]
            session.add_all(tasks)
            operation = attach_deployment_bulk_operation(
                session,
                deployment=deployment,
                task_type=TaskType.APPLY_DEPLOYMENT,
                tasks=tasks,
                options={"serve": True},
            )
            session.commit()

            self.assertTrue(all(task.payload["serve"] is False for task in tasks))
            self._claim(session, tasks[0])
            self._finish(session, tasks[0])
            self.assertFalse(self._phase_tasks(session, operation.id, "START_API"))
            self._claim(session, tasks[1])
            self._finish(session, tasks[1])

            api_start = self._phase_tasks(session, operation.id, "START_API")
            self.assertEqual(len(api_start), 1)
            self.assertEqual(api_start[0].node_id, NODE_A)
            self.assertTrue(api_start[0].payload["serve"])
            self._claim(session, api_start[0])
            self._finish(session, api_start[0])

            api_verify = self._phase_tasks(session, operation.id, "VERIFY_API")
            self.assertEqual(len(api_verify), 1)
            self.assertEqual(api_verify[0].node_id, NODE_A)
            self.assertTrue(api_verify[0].payload["api"])
            self._claim(session, api_verify[0])
            self._finish(session, api_verify[0])

            session.refresh(operation)
            session.refresh(deployment)
            self.assertEqual((operation.status, operation.phase), ("SUCCEEDED", "COMPLETE"))
            self.assertEqual(deployment.status, "APPLIED")
            self.assertIsNone(deployment.verified_at)

    def test_serving_apply_rejects_a_subset_without_adding_the_head(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            self._node(session, NODE_B)
            deployment_id = str(uuid.uuid4())
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan=_plan(
                    deployment_id, 1, [NODE_A, NODE_B], layer_end=31
                ),
                accept_model_download=False,
                pull_image=False,
                status="CREATED",
            )
            session.add(deployment)
            task = Task(
                bulk_id=str(uuid.uuid4()),
                node_id=NODE_B,
                type=TaskType.APPLY_DEPLOYMENT.value,
                deployment_id=deployment.id,
                payload={},
            )
            session.add(task)
            with self.assertRaises(DeploymentRolloutError) as context:
                attach_deployment_bulk_operation(
                    session,
                    deployment=deployment,
                    task_type=TaskType.APPLY_DEPLOYMENT,
                    tasks=[task],
                    options={"serve": True},
                )
            self.assertEqual(
                context.exception.code,
                "DEPLOYMENT_OPERATION_NODE_SET_MISMATCH",
            )
            session.rollback()
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(DeploymentOperation)
                ),
                0,
            )

    def test_verify_evidence_missing_or_duplicate_checks_fails_terminally(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            for variant in ("missing", "duplicate"):
                with self.subTest(variant=variant):
                    deployment_id = str(uuid.uuid4())
                    deployment = Deployment(
                        id=deployment_id,
                        lineage_id=deployment_id,
                        generation=1,
                        plan=_plan(deployment_id, 1, [NODE_A], layer_end=31),
                        accept_model_download=False,
                        pull_image=False,
                        status="VERIFIED",
                        verified_at=utcnow(),
                    )
                    task = Task(
                        bulk_id=str(uuid.uuid4()),
                        node_id=NODE_A,
                        type=TaskType.VERIFY.value,
                        deployment_id=deployment.id,
                        payload={},
                    )
                    session.add_all([deployment, task])
                    operation = attach_deployment_bulk_operation(
                        session,
                        deployment=deployment,
                        task_type=TaskType.VERIFY,
                        tasks=[task],
                        options={"api": False},
                    )
                    session.commit()
                    self._claim(session, task)
                    result = _check_result(task)
                    if variant == "missing":
                        result["checks"] = [
                            item
                            for item in result["checks"]
                            if item["name"] != "container-gpu"
                        ]
                    else:
                        result["checks"].append(dict(result["checks"][0]))
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
                    session.refresh(task)
                    session.refresh(operation)
                    session.refresh(deployment)
                    record = session.get(
                        DeploymentOperationNode, task.operation_node_id
                    )
                    self.assertEqual(task.status, TaskStatus.FAILED.value)
                    self.assertEqual(task.error, "TASK_RESULT_INVALID")
                    self.assertEqual(record.failure_code, "TASK_RESULT_INVALID")
                    self.assertEqual(operation.status, "FAILED")
                    self.assertIsNone(operation.active_lineage_id)
                    self.assertEqual(deployment.status, "VERIFY_FAILED")
                    self.assertIsNone(deployment.verified_at)

    def test_rollback_api_start_nonblocking_failure_retries_same_phase(self) -> None:
        with self.factory() as session:
            self._node(session, NODE_A)
            _target, source = self._lineage(session, [NODE_A])
            operation, stop_tasks, _ = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=True, serve=True
            )
            self._claim(session, stop_tasks[0])
            self._finish(session, stop_tasks[0])
            start = self._phase_tasks(session, operation.id, "START_TARGET")[0]
            self._claim(session, start)
            self._finish(session, start)
            verify = self._phase_tasks(session, operation.id, "VERIFY_TARGET")[0]
            self._claim(session, verify)
            self._finish(session, verify)
            api_start = self._phase_tasks(session, operation.id, "START_API")[0]
            self._claim(session, api_start)
            invalid = _check_result(api_start)
            failed_api = next(
                item
                for item in invalid["checks"]
                if item["name"] == "vllm-api"
            )
            failed_api.update(ok=False, blocking=False, detail="not ready")
            self.assertTrue(
                finish_operation_task(
                    session,
                    api_start,
                    NODE_A,
                    result=invalid,
                    error=None,
                )
            )
            session.commit()
            session.refresh(operation)
            self.assertEqual(operation.status, "FAILED")
            self.assertEqual(operation.phase, "START_API")
            self.assertFalse(self._phase_tasks(session, operation.id, "VERIFY_API"))

            _same, retries, changed = prepare_or_apply_rollback(
                session, source.id, [NODE_A], apply=True, serve=True
            )
            self.assertTrue(changed)
            self.assertEqual(len(retries), 1)
            self.assertEqual(retries[0].operation_attempt, 2)
            self.assertEqual(retries[0].type, TaskType.START_DEPLOYMENT.value)


if __name__ == "__main__":
    unittest.main()
