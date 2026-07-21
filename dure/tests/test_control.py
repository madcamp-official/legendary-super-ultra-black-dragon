from __future__ import annotations

import copy
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import (
    Deployment,
    DeploymentOperation,
    DeploymentOperationNode,
    EnrollmentToken,
    NodeCredential,
    Task,
    TaskStatus,
    TaskType,
    utcnow,
)
from dure.control.rollout import DeploymentRolloutConflictError
from dure.control.service import (
    authenticate_node,
    claim_enrollment,
    claim_task,
    cancel_task,
    create_enrollment,
    create_tasks,
    extend_task,
    finish_task,
    join_node,
    node_status,
    approve_node,
    revoke_node,
    rotate_node_credential,
    save_deployment,
)
from dure.planner import build_plan

from tests.helpers import profile


class ControlServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'control.db'}"
        self.engine = make_engine(url)
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self):
        self.engine.dispose()
        self.temporary.cleanup()

    def enroll(self, session, name="node-a"):
        _, token = create_enrollment(session, timedelta(hours=1))
        node, credential = claim_enrollment(
            session, token=token, install_id=f"install-{name}",
            profile=profile(name).to_dict(), agent_version="0.2.0",
        )
        return node, credential

    def deployment(self, session, *, name="node-a", download=True, pull=True):
        node, _ = self.enroll(session, name=name)
        plan = build_plan(
            [profile(name)],
            image="registry.example/vllm@sha256:" + "a" * 64,
        )
        deployment = save_deployment(
            session,
            plan.to_dict(),
            accept_model_download=download,
            pull_image=pull,
        )
        return node, deployment

    def test_enrollment_is_hashed_one_time_and_revocable(self):
        with self.factory() as session:
            record, token = create_enrollment(session, timedelta(hours=1))
            self.assertNotEqual(record.token_hash, token)
            node, credential = claim_enrollment(
                session, token=token, install_id="install-12345678",
                profile=profile("alpha").to_dict(), agent_version="0.2.0",
            )
            self.assertIsNotNone(authenticate_node(session, credential))
            with self.assertRaises(ValueError):
                claim_enrollment(session, token=token, install_id="install-other1", profile=profile("beta").to_dict(), agent_version="0.2.0")
            self.assertTrue(revoke_node(session, node.id))
            self.assertIsNone(authenticate_node(session, credential))
            stored = session.scalar(select(NodeCredential).where(NodeCredential.node_id == node.id))
            self.assertNotEqual(stored.credential_hash, credential)
            replacement = rotate_node_credential(session, node.id)
            self.assertIsNotNone(authenticate_node(session, replacement))

    def test_expired_enrollment_is_rejected(self):
        with self.factory() as session:
            record, token = create_enrollment(session, timedelta(hours=1))
            record.expires_at = utcnow() - timedelta(seconds=1)
            session.commit()
            with self.assertRaises(ValueError):
                claim_enrollment(session, token=token, install_id="install-expired", profile=profile("old").to_dict(), agent_version="0.2.0")

    def test_tokenless_join_is_pending_until_approved(self):
        with self.factory() as session:
            node, credential = join_node(
                session,
                install_id="install-tokenless",
                profile=profile("pending-node").to_dict(),
                agent_version="0.3.0",
            )
            self.assertFalse(node.approved)
            self.assertEqual(authenticate_node(session, credential).id, node.id)
            _, tasks, errors = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.PROBE,
                deployment_id=None,
                options={},
            )
            self.assertFalse(tasks)
            self.assertEqual(errors[node.id], "unknown, pending, or revoked node")
            self.assertTrue(approve_node(session, node.id))
            _, tasks, errors = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.PROBE,
                deployment_id=None,
                options={},
            )
            self.assertEqual(len(tasks), 1)
            self.assertFalse(errors)

    def test_connectivity_thresholds(self):
        now = utcnow()
        self.assertEqual(node_status(now - timedelta(seconds=10), now), "online")
        self.assertEqual(node_status(now - timedelta(seconds=40), now), "offline")
        self.assertEqual(node_status(now - timedelta(seconds=100), now), "stale")

    def test_tasks_lease_retry_complete_and_partial_bulk(self):
        with self.factory() as session:
            node, _ = self.enroll(session)
            bulk, tasks, errors = create_tasks(
                session, node_ids=[node.id, "missing"], task_type=TaskType.PROBE,
                deployment_id=None, options={},
            )
            self.assertTrue(bulk)
            self.assertEqual(errors, {"missing": "unknown, pending, or revoked node"})
            claimed = claim_task(session, node.id)
            self.assertEqual(claimed.id, tasks[0].id)
            self.assertEqual(claimed.attempts, 1)
            claimed.lease_until = utcnow() - timedelta(seconds=1)
            session.commit()
            claimed_again = claim_task(session, node.id)
            self.assertEqual(claimed_again.id, claimed.id)
            self.assertEqual(claimed_again.attempts, 2)
            self.assertTrue(finish_task(session, claimed_again, node.id, result={"ok": True}, error=None))
            self.assertEqual(claimed_again.status, TaskStatus.SUCCEEDED.value)
            self.assertTrue(finish_task(session, claimed_again, node.id, result={"ok": True}, error=None))

    def test_deployment_normalizes_legacy_hostname_and_rejects_unpinned_image(self):
        with self.factory() as session:
            node, _ = self.enroll(session)
            plan = build_plan([profile("node-a")], image="registry.example/vllm@sha256:" + "a" * 64)
            deployment = save_deployment(session, plan.to_dict(), accept_model_download=True, pull_image=True)
            self.assertEqual(deployment.plan["assignments"][0]["node_id"], node.id)
            other = build_plan([profile("other")], image="vllm/vllm-openai:latest")
            with self.assertRaises(ValueError):
                save_deployment(session, other.to_dict(), accept_model_download=False, pull_image=False)

    def test_task_payload_rejects_arbitrary_options(self):
        with self.factory() as session:
            node, _ = self.enroll(session)
            with self.assertRaises(ValueError):
                create_tasks(session, node_ids=[node.id], task_type=TaskType.PROBE, deployment_id=None, options={"command": "id"})

    def test_queued_cache_quarantine_blocks_new_deployment_work(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            quarantine = Task(
                bulk_id="77777777-7777-4777-8777-777777777777",
                node_id=node.id,
                type=TaskType.QUARANTINE_ARTIFACT_CACHE.value,
                payload={
                    "node_id": node.id,
                    "cache_kind": "FULL_SNAPSHOT",
                    "cache_identity_digest": "sha256:" + "b" * 64,
                },
            )
            session.add(quarantine)
            session.commit()

            with self.assertRaises(DeploymentRolloutConflictError) as raised:
                create_tasks(
                    session,
                    node_ids=[node.id],
                    task_type=TaskType.APPLY_DEPLOYMENT,
                    deployment_id=deployment.id,
                    options={"serve": False},
                )

            self.assertEqual(raised.exception.code, "DEPLOYMENT_NODE_TASK_ACTIVE")
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

    def test_queued_cache_quarantine_blocks_new_deployment_record(self):
        with self.factory() as session:
            node, _ = self.enroll(session)
            plan = build_plan(
                [profile("node-a")],
                image="registry.example/vllm@sha256:" + "a" * 64,
            )
            session.add(
                Task(
                    bulk_id="99999999-9999-4999-8999-999999999999",
                    node_id=node.id,
                    type=TaskType.QUARANTINE_ARTIFACT_CACHE.value,
                    payload={
                        "node_id": node.id,
                        "cache_kind": "FULL_SNAPSHOT",
                        "cache_identity_digest": "sha256:" + "b" * 64,
                    },
                )
            )
            session.commit()

            with self.assertRaisesRegex(
                ValueError,
                "active artifact cache quarantine",
            ):
                save_deployment(
                    session,
                    plan.to_dict(),
                    accept_model_download=False,
                    pull_image=False,
                )

            session.rollback()
            self.assertIsNone(session.get(Deployment, plan.deployment_id))

    def test_deployment_apply_tracks_operation_and_preserves_payload(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            bulk_id, tasks, errors = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={"serve": False},
            )
            self.assertTrue(bulk_id)
            self.assertFalse(errors)
            self.assertEqual(len(tasks), 1)
            task = tasks[0]
            self.assertEqual(
                task.payload,
                {
                    "serve": False,
                    "plan": deployment.plan,
                    "generation": deployment.generation,
                    "accept_model_download": True,
                    "pull_image": True,
                },
            )
            self.assertIsNotNone(task.operation_node_id)
            self.assertEqual(task.operation_attempt, 1)
            operation = session.scalar(select(DeploymentOperation))
            self.assertEqual(operation.kind, "APPLY")
            self.assertEqual(operation.status, "QUEUED")
            self.assertEqual(operation.active_lineage_id, deployment.lineage_id)

            claimed = claim_task(session, node.id)
            self.assertEqual(claimed.id, task.id)
            operation_node = session.get(
                DeploymentOperationNode, task.operation_node_id
            )
            self.assertEqual(operation_node.status, "RUNNING")
            self.assertTrue(
                finish_task(
                    session,
                    claimed,
                    node.id,
                    result={"ok": True},
                    error=None,
                )
            )
            session.refresh(operation)
            session.refresh(operation_node)
            session.refresh(deployment)
            self.assertEqual(operation.status, "SUCCEEDED")
            self.assertIsNone(operation.active_lineage_id)
            self.assertEqual(operation_node.status, "SUCCEEDED")
            self.assertEqual(deployment.status, "APPLIED")

    def test_deployment_task_options_require_strict_booleans_atomically(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            with self.assertRaisesRegex(ValueError, "strict boolean"):
                create_tasks(
                    session,
                    node_ids=[node.id],
                    task_type=TaskType.APPLY_DEPLOYMENT,
                    deployment_id=deployment.id,
                    options={"serve": 1},
                )
            self.assertEqual(session.query(Task).count(), 0)
            self.assertEqual(session.query(DeploymentOperation).count(), 0)
            session.refresh(node)
            self.assertIsNone(node.desired_state)

    def test_active_operation_blocks_another_lineage_mutation_atomically(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            _, tasks, _ = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={"serve": False},
            )
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                create_tasks(
                    session,
                    node_ids=[node.id],
                    task_type=TaskType.STOP_DEPLOYMENT,
                    deployment_id=deployment.id,
                    options={},
                )
            self.assertEqual(context.exception.code, "DEPLOYMENT_OPERATION_ACTIVE")
            self.assertEqual(session.query(Task).count(), 1)
            session.refresh(node)
            self.assertEqual(node.desired_state, TaskType.APPLY_DEPLOYMENT.value)
            self.assertEqual(tasks[0].status, TaskStatus.QUEUED.value)

    def test_legacy_queued_mutation_blocks_new_lineage_mutation(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            legacy = Task(
                bulk_id="legacy-bulk",
                node_id=node.id,
                type=TaskType.START_DEPLOYMENT.value,
                deployment_id=deployment.id,
                payload={},
            )
            session.add(legacy)
            session.commit()
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                create_tasks(
                    session,
                    node_ids=[node.id],
                    task_type=TaskType.VERIFY,
                    deployment_id=deployment.id,
                    options={"api": False},
                )
            self.assertEqual(context.exception.code, "DEPLOYMENT_MUTATION_ACTIVE")
            self.assertEqual(context.exception.details["task_id"], legacy.id)
            self.assertEqual(session.query(Task).count(), 1)

    def test_claim_hook_rejection_rolls_back_task_lease(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            _, tasks, _ = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={},
            )
            task = tasks[0]
            operation_node = session.get(
                DeploymentOperationNode, task.operation_node_id
            )
            operation_node.status = "FAILED"
            session.commit()
            with self.assertRaises(DeploymentRolloutConflictError) as context:
                claim_task(session, node.id)
            self.assertEqual(
                context.exception.code, "DEPLOYMENT_OPERATION_TASK_CONFLICT"
            )
            session.refresh(task)
            self.assertEqual(task.status, TaskStatus.QUEUED.value)
            self.assertEqual(task.attempts, 0)
            self.assertIsNone(task.lease_until)

    def test_late_operation_attempt_cannot_finish_current_progress(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            _, tasks, _ = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={},
            )
            task = claim_task(session, node.id)
            operation_node = session.get(
                DeploymentOperationNode, task.operation_node_id
            )
            operation = session.get(
                DeploymentOperation, operation_node.operation_id
            )
            operation_node.attempt_count = 2
            session.commit()
            self.assertFalse(
                finish_task(
                    session,
                    task,
                    node.id,
                    result={"ok": True},
                    error=None,
                )
            )
            session.refresh(task)
            session.refresh(operation_node)
            session.refresh(operation)
            self.assertEqual(task.status, TaskStatus.RUNNING.value)
            self.assertIsNone(task.result)
            self.assertEqual(operation_node.status, "RUNNING")
            self.assertEqual(operation.status, "RUNNING")

    def test_expired_operation_lease_fails_instead_of_reclaiming_same_attempt(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            _, tasks, _ = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={},
            )
            task = claim_task(session, node.id, lease_seconds=-1)
            operation_node = session.get(
                DeploymentOperationNode, task.operation_node_id
            )
            operation = session.get(
                DeploymentOperation, operation_node.operation_id
            )

            self.assertIsNone(claim_task(session, node.id))

            session.refresh(task)
            session.refresh(operation_node)
            session.refresh(operation)
            session.refresh(deployment)
            self.assertEqual(task.status, TaskStatus.FAILED.value)
            self.assertEqual(task.error, "TASK_LEASE_EXPIRED")
            self.assertEqual(task.attempts, 1)
            self.assertEqual(operation_node.status, "FAILED")
            self.assertEqual(
                operation_node.failure_code, "TASK_LEASE_EXPIRED"
            )
            self.assertEqual(operation.status, "FAILED")
            self.assertIsNone(operation.active_lineage_id)
            self.assertEqual(deployment.status, "APPLY_FAILED")
            self.assertFalse(
                finish_task(
                    session,
                    task,
                    node.id,
                    result={"ok": True},
                    error=None,
                )
            )

    def test_operation_task_cancel_is_atomic_and_idempotent(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            _, tasks, _ = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={},
            )
            task = tasks[0]
            operation_node = session.get(
                DeploymentOperationNode, task.operation_node_id
            )
            operation = session.get(
                DeploymentOperation, operation_node.operation_id
            )
            self.assertTrue(cancel_task(session, task))
            self.assertTrue(cancel_task(session, task))
            session.refresh(task)
            session.refresh(operation_node)
            session.refresh(operation)
            session.refresh(deployment)
            self.assertEqual(task.status, TaskStatus.CANCELED.value)
            self.assertEqual(operation_node.status, "CANCELED")
            self.assertEqual(operation.status, "FAILED")
            self.assertIsNone(operation.active_lineage_id)
            self.assertEqual(deployment.status, "APPLY_FAILED")

    def test_running_operation_task_cannot_be_canceled(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            _, tasks, _ = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={},
            )
            task = claim_task(session, node.id)
            operation_node = session.get(
                DeploymentOperationNode, task.operation_node_id
            )
            operation = session.get(
                DeploymentOperation, operation_node.operation_id
            )
            self.assertFalse(cancel_task(session, task))
            session.refresh(task)
            session.refresh(operation_node)
            session.refresh(operation)
            session.refresh(deployment)
            self.assertEqual(task.status, TaskStatus.RUNNING.value)
            self.assertEqual(operation_node.status, "RUNNING")
            self.assertEqual(operation.status, "RUNNING")
            self.assertEqual(operation.active_lineage_id, deployment.lineage_id)
            self.assertEqual(deployment.status, "APPLYING")

    def test_expired_operation_task_can_be_reconciled_by_admin_cancel(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            _, _tasks, _ = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={},
            )
            task = claim_task(session, node.id, lease_seconds=-1)
            operation_node = session.get(
                DeploymentOperationNode, task.operation_node_id
            )

            self.assertTrue(cancel_task(session, task))
            self.assertTrue(cancel_task(session, task))

            session.refresh(task)
            session.refresh(operation_node)
            self.assertEqual(task.status, TaskStatus.FAILED.value)
            self.assertEqual(task.error, "TASK_LEASE_EXPIRED")
            self.assertEqual(operation_node.status, "FAILED")
            self.assertEqual(
                operation_node.failure_code, "TASK_LEASE_EXPIRED"
            )

    def test_task_heartbeat_cannot_revive_expired_or_stale_operation_attempt(self):
        with self.factory() as session:
            node, deployment = self.deployment(session)
            create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={},
            )
            task = claim_task(session, node.id, lease_seconds=-1)
            expired_lease = task.lease_until

            self.assertFalse(extend_task(session, task, node.id))
            session.refresh(task)
            self.assertEqual(task.status, TaskStatus.RUNNING.value)
            self.assertEqual(
                task.lease_until.replace(tzinfo=None),
                expired_lease.replace(tzinfo=None),
            )

            task.lease_until = utcnow() + timedelta(minutes=5)
            operation_node = session.get(
                DeploymentOperationNode, task.operation_node_id
            )
            operation_node.attempt_count += 1
            session.commit()
            current_lease = task.lease_until

            self.assertFalse(extend_task(session, task, node.id))
            session.refresh(task)
            self.assertEqual(
                task.lease_until.replace(tzinfo=None),
                current_lease.replace(tzinfo=None),
            )

    def test_task_heartbeat_reads_time_after_bound_rows_are_locked(self):
        with self.factory() as session:
            node, _deployment = self.deployment(session)
            create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.PROBE,
                deployment_id=None,
                options={},
            )
            task = claim_task(session, node.id)
            before_wait = utcnow()
            after_wait = before_wait + timedelta(seconds=10)
            task.lease_until = before_wait + timedelta(seconds=5)
            session.commit()

            original_scalar = session.scalar
            rows_locked = False

            def scalar_after_wait(*args, **kwargs):
                nonlocal rows_locked
                value = original_scalar(*args, **kwargs)
                rows_locked = True
                return value

            def observed_now():
                return after_wait if rows_locked else before_wait

            with patch.object(
                session, "scalar", side_effect=scalar_after_wait
            ), patch(
                "dure.control.service.utcnow", side_effect=observed_now
            ):
                self.assertFalse(extend_task(session, task, node.id))

            session.refresh(task)
            self.assertEqual(
                task.lease_until.replace(tzinfo=None),
                (before_wait + timedelta(seconds=5)).replace(tzinfo=None),
            )

    def test_task_heartbeat_refreshes_revoked_node_before_extension(self):
        with self.factory() as session:
            node, _deployment = self.deployment(session)
            create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.PROBE,
                deployment_id=None,
                options={},
            )
            task = claim_task(session, node.id)
            self.assertTrue(node.approved)

            with self.factory() as revoking_session:
                revoked = revoking_session.get(type(node), node.id)
                revoked.approved = False
                revoking_session.commit()

            self.assertTrue(node.approved)
            self.assertFalse(extend_task(session, task, node.id))
            session.refresh(node)
            self.assertFalse(node.approved)

    def test_overlapping_lineages_cannot_activate_on_the_same_node(self):
        with self.factory() as session:
            node, first = self.deployment(session)
            second_plan = copy.deepcopy(first.plan)
            second_plan["deployment_id"] = "independent-lineage"
            second = save_deployment(
                session,
                second_plan,
                accept_model_download=False,
                pull_image=False,
            )
            create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=first.id,
                options={},
            )

            with self.assertRaises(DeploymentRolloutConflictError) as context:
                create_tasks(
                    session,
                    node_ids=[node.id],
                    task_type=TaskType.APPLY_DEPLOYMENT,
                    deployment_id=second.id,
                    options={},
                )

            self.assertEqual(
                context.exception.code, "DEPLOYMENT_NODE_OPERATION_ACTIVE"
            )
            self.assertEqual(session.query(Task).count(), 1)
