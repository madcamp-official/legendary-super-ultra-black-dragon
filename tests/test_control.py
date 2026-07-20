from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from sqlalchemy import select

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import EnrollmentToken, NodeCredential, TaskStatus, TaskType, utcnow
from dure.control.service import (
    authenticate_node,
    claim_enrollment,
    claim_task,
    create_enrollment,
    create_tasks,
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
