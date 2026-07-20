from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from datetime import timedelta
from pathlib import Path

from sqlalchemy import func, select

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    TestClient = None

from dure.control.api import create_app
from dure.control.models import (
    AuditEvent,
    BenchmarkEvidence,
    BenchmarkRun,
    Deployment,
    NodeCredential,
    NodeProfileRecord,
    Task,
    TaskStatus,
    TaskType,
    utcnow,
)
from dure.control.service import cancel_task, create_tasks, secret_hash

from .test_benchmark import _multi_release, _node, _release


def _request(node, release, placement, *, request_id=None, workload_id=None):
    return {
        "request_id": request_id or str(uuid.uuid4()),
        "release_id": release.id,
        "placement_id": placement.id,
        "node_ids": [node.id],
        "workload_id": workload_id or "short-chat-1k-128",
        "dure_commit": "d" * 40,
    }


def _result(run_id: str, workload_id: str = "short-chat-1k-128") -> dict:
    return {
        "benchmark_id": run_id,
        "workload_id": workload_id,
        "metrics": {
            "duration_seconds": 900.0,
            "request_count": 200,
            "warmup_requests": 20,
            "oom_count": 0,
            "crash_count": 0,
            "restart_count": 0,
            "ttft_p95_ms": 900.0,
            "tpot_p95_ms": 90.0,
            "e2e_p95_ms": 4500.0,
            "throughput_tps": 12.0,
            "success_rate": 1.0,
            "vram_headroom_pct": 12.0,
            "quality_score": 0.9,
            "network_bandwidth_mbps": None,
            "network_rtt_ms": None,
            "packet_loss_pct": None,
            "nccl_all_reduce_ok": None,
        },
    }


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class BenchmarkTaskAPITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'benchmark-tasks.db'}"
        self.client = TestClient(
            create_app(
                database_url=url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.admin = {"Authorization": "Bearer admin-secret"}

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def fixture(self, key="task"):
        credential = f"credential-{key}"
        with self.client.app.state.session_factory() as session:
            node = _node(session, f"{key}-node")
            _, _, release, placements = _release(session, key)
            session.add(
                NodeCredential(
                    node_id=node.id,
                    credential_hash=secret_hash(credential),
                )
            )
            session.commit()
            return node, release, placements[0], credential

    def prepare(self, body):
        return self.client.post(
            "/v1/admin/benchmark-runs/prepare",
            headers=self.admin,
            json=body,
        )

    def apply(self, request_id, body=None):
        return self.client.post(
            f"/v1/admin/benchmark-runs/{request_id}/apply",
            headers=self.admin,
            json={"apply": True} if body is None else body,
        )

    def test_prepare_is_non_mutating_and_request_id_is_idempotent(self):
        node, release, placement, _ = self.fixture("prepare")
        body = _request(node, release, placement)

        first = self.prepare(body)
        second = self.prepare(body)

        self.assertEqual(first.status_code, 200, first.text)
        self.assertTrue(first.json()["created"])
        self.assertFalse(second.json()["created"])
        self.assertEqual(
            first.json()["benchmark_run"]["id"],
            second.json()["benchmark_run"]["id"],
        )
        self.assertEqual(first.json()["benchmark_run"]["status"], "PREPARED")
        with self.client.app.state.session_factory() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)), 0
            )

        conflict = self.prepare({**body, "workload_id": "quality-eval"})
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(
            conflict.json()["detail"]["code"], "BENCHMARK_REQUEST_CONFLICT"
        )
        new_request = self.prepare({**body, "request_id": str(uuid.uuid4())})
        self.assertEqual(new_request.status_code, 200)
        self.assertTrue(new_request.json()["created"])
        self.assertNotEqual(
            first.json()["benchmark_run"]["id"],
            new_request.json()["benchmark_run"]["id"],
        )

    def test_apply_requires_exact_true_and_creates_one_closed_task(self):
        node, release, placement, _ = self.fixture("apply")
        body = _request(node, release, placement)
        run = self.prepare(body).json()["benchmark_run"]

        self.assertEqual(self.apply(body["request_id"], {"apply": False}).status_code, 422)
        self.assertEqual(
            self.apply(body["request_id"], {"apply": True, "command": "id"}).status_code,
            422,
        )
        first = self.apply(body["request_id"])
        second = self.apply(body["request_id"])

        self.assertEqual(first.status_code, 200, first.text)
        self.assertTrue(first.json()["created"])
        self.assertFalse(second.json()["created"])
        task = first.json()["task"]
        self.assertEqual(task["id"], second.json()["task"]["id"])
        self.assertEqual(task["type"], "BENCHMARK")
        self.assertEqual(task["node_id"], node.id)
        self.assertIsNone(task["deployment_id"])
        self.assertEqual(task["payload"]["benchmark_id"], run["id"])
        self.assertEqual(task["payload"]["input_tokens"], 1024)
        self.assertEqual(task["payload"]["output_tokens"], 128)
        self.assertEqual(task["payload"]["concurrency"], 8)
        self.assertEqual(task["payload"]["warmup_requests"], 20)
        self.assertEqual(task["payload"]["request_count"], 200)
        self.assertEqual(task["payload"]["duration_seconds"], 900.0)
        unsafe = {
            "command",
            "docker_args",
            "env",
            "mounts",
            "python",
            "prompt",
            "token",
            "secret",
            "log",
            "stdout",
            "stderr",
            "model_path",
        }
        self.assertFalse(unsafe & set(task["payload"]))
        with self.client.app.state.session_factory() as session:
            with self.assertRaisesRegex(ValueError, "prepared benchmark run"):
                create_tasks(
                    session,
                    node_ids=[node.id],
                    task_type=TaskType.BENCHMARK,
                    deployment_id=None,
                    options={},
                )
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 1)
        generic = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [node.id],
                "type": "BENCHMARK",
                "deployment_id": None,
                "options": {},
            },
        )
        self.assertEqual(generic.status_code, 400)

    def test_prepare_blocks_multinode_and_apply_blocks_changed_profile(self):
        with self.client.app.state.session_factory() as session:
            nodes = [_node(session, f"multi-{index}") for index in range(3)]
            _, _, release, placement = _multi_release(session, "auto-multi")
            multi_body = {
                "request_id": str(uuid.uuid4()),
                "release_id": release.id,
                "placement_id": placement.id,
                "node_ids": [node.id for node in nodes],
                "workload_id": "short-chat-1k-128",
                "dure_commit": "d" * 40,
            }
        blocked = self.prepare(multi_body)
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(
            blocked.json()["detail"]["code"],
            "MULTI_NODE_BENCHMARK_UNSUPPORTED",
        )

        node, release, placement, _ = self.fixture("stale")
        body = _request(node, release, placement)
        self.assertEqual(self.prepare(body).status_code, 200)
        with self.client.app.state.session_factory() as session:
            record = session.get(NodeProfileRecord, node.id)
            changed = dict(record.profile)
            changed["memory_mib"] += 1
            record.profile = changed
            session.commit()
        stale = self.apply(body["request_id"])
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(
            stale.json()["detail"]["code"], "BENCHMARK_CONTEXT_CHANGED"
        )
        with self.client.app.state.session_factory() as session:
            run = session.scalar(
                select(BenchmarkRun).where(
                    BenchmarkRun.request_id == body["request_id"]
                )
            )
            self.assertEqual(run.status, "PREPARED")
            self.assertIsNone(run.task_id)

    def test_max_context_dimensions_are_derived_and_frozen_by_control_plane(self):
        node, release, placement, _ = self.fixture("max-context")
        body = _request(
            node, release, placement, workload_id="max-context"
        )

        prepared = self.prepare(body)
        applied = self.apply(body["request_id"])

        self.assertEqual(prepared.status_code, 200, prepared.text)
        run = prepared.json()["benchmark_run"]
        self.assertEqual(run["input_tokens"], 8192 - 256)
        self.assertEqual(run["output_tokens"], 256)
        self.assertEqual(run["concurrency"], 1)
        payload = applied.json()["task"]["payload"]
        self.assertEqual(payload["input_tokens"], run["input_tokens"])
        self.assertEqual(payload["output_tokens"], run["output_tokens"])
        self.assertEqual(payload["concurrency"], run["concurrency"])

    def test_strict_result_creates_one_evidence_and_replay_is_idempotent(self):
        node, release, placement, credential = self.fixture("complete")
        body = _request(node, release, placement)
        run = self.prepare(body).json()["benchmark_run"]
        task = self.apply(body["request_id"]).json()["task"]
        agent = {"Authorization": f"Bearer {credential}"}
        claimed = self.client.post(
            "/v1/agent/tasks/claim", headers=agent
        ).json()["task"]
        self.assertEqual(claimed["id"], task["id"])

        with self.client.app.state.session_factory() as session:
            _, queued, _ = create_tasks(
                session,
                node_ids=[node.id],
                task_type=TaskType.PROBE,
                deployment_id=None,
                options={},
            )
            self.assertEqual(len(queued), 1)
        blocked_claim = self.client.post(
            "/v1/agent/tasks/claim", headers=agent
        ).json()["task"]
        self.assertIsNone(blocked_claim)

        unsafe = _result(run["id"])
        unsafe["metrics"]["log"] = "raw secret output"
        rejected = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete",
            headers=agent,
            json={"result": unsafe},
        )
        self.assertEqual(rejected.status_code, 422)
        with self.client.app.state.session_factory() as session:
            stored_task = session.get(Task, task["id"])
            self.assertEqual(stored_task.status, TaskStatus.RUNNING.value)
            self.assertIsNone(stored_task.result)
            self.assertIsNone(stored_task.error)

        for field, value in (
            ("request_count", 199),
            ("warmup_requests", 19),
            ("oom_count", 2**63),
        ):
            with self.subTest(field=field):
                mismatched = _result(run["id"])
                mismatched["metrics"][field] = value
                rejected = self.client.post(
                    f"/v1/agent/tasks/{task['id']}/complete",
                    headers=agent,
                    json={"result": mismatched},
                )
                self.assertEqual(rejected.status_code, 422)

        result = _result(run["id"])
        first = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete",
            headers=agent,
            json={"result": result},
        )
        second = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete",
            headers=agent,
            json={"result": result},
        )
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(
            first.json()["benchmark_run"]["evidence_id"],
            second.json()["benchmark_run"]["evidence_id"],
        )
        with self.client.app.state.session_factory() as session:
            stored_run = session.scalar(
                select(BenchmarkRun).where(BenchmarkRun.id == run["id"])
            )
            stored_task = session.get(Task, task["id"])
            self.assertEqual(stored_run.status, "SUCCEEDED")
            self.assertEqual(stored_task.status, TaskStatus.SUCCEEDED.value)
            self.assertEqual(
                set(stored_task.result),
                {"benchmark_id", "workload_id", "evidence_id", "result_digest"},
            )
            self.assertNotIn("metrics", stored_task.result)
            self.assertIsNone(stored_task.error)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(BenchmarkEvidence)),
                1,
            )

    def test_failure_text_is_reduced_to_closed_code_without_raw_storage(self):
        node, release, placement, credential = self.fixture("failure")
        body = _request(node, release, placement)
        self.prepare(body)
        task = self.apply(body["request_id"]).json()["task"]
        agent = {"Authorization": f"Bearer {credential}"}
        self.client.post("/v1/agent/tasks/claim", headers=agent)
        raw_error = "token=private raw traceback and command output"

        failed = self.client.post(
            f"/v1/agent/tasks/{task['id']}/fail",
            headers=agent,
            json={"error": raw_error},
        )
        replayed = self.client.post(
            f"/v1/agent/tasks/{task['id']}/fail",
            headers=agent,
            json={"error": raw_error},
        )

        self.assertEqual(failed.status_code, 200, failed.text)
        self.assertEqual(replayed.status_code, 200, replayed.text)
        with self.client.app.state.session_factory() as session:
            stored_task = session.get(Task, task["id"])
            run = session.scalar(
                select(BenchmarkRun).where(BenchmarkRun.task_id == task["id"])
            )
            self.assertEqual(stored_task.error, "BENCHMARK_EXECUTION_FAILED")
            self.assertIsNone(stored_task.result)
            self.assertEqual(run.failure_code, "BENCHMARK_EXECUTION_FAILED")
            details = [
                event.detail
                for event in session.scalars(
                    select(AuditEvent).where(
                        AuditEvent.action.like("benchmark_run.%")
                    )
                )
            ]
            self.assertNotIn(raw_error, json.dumps(details, sort_keys=True))

    def test_later_execution_failure_blocks_older_passing_evidence(self):
        node, release, placement, credential = self.fixture("failed-recheck")
        agent = {"Authorization": f"Bearer {credential}"}

        passed_body = _request(node, release, placement)
        passed_run = self.prepare(passed_body).json()["benchmark_run"]
        passed_task = self.apply(passed_body["request_id"]).json()["task"]
        self.client.post("/v1/agent/tasks/claim", headers=agent)
        completed = self.client.post(
            f"/v1/agent/tasks/{passed_task['id']}/complete",
            headers=agent,
            json={"result": _result(passed_run["id"])},
        )
        self.assertEqual(completed.status_code, 200, completed.text)
        passing_evidence_id = completed.json()["benchmark_run"]["evidence_id"]

        failed_body = _request(node, release, placement)
        self.prepare(failed_body)
        failed_task = self.apply(failed_body["request_id"]).json()["task"]
        self.client.post("/v1/agent/tasks/claim", headers=agent)
        pending_promotion = self.client.post(
            f"/v1/admin/model-releases/{release.id}/promote",
            headers=self.admin,
        )
        self.assertEqual(pending_promotion.status_code, 409)
        pending_error = pending_promotion.json()["detail"]["details"][
            "placements"
        ][0]
        self.assertEqual(pending_error["code"], "BENCHMARK_RUN_PENDING")
        self.assertEqual(pending_error["benchmark_run_status"], "QUEUED")
        failed = self.client.post(
            f"/v1/agent/tasks/{failed_task['id']}/fail",
            headers=agent,
            json={"error": "BENCHMARK_RUNTIME_UNAVAILABLE"},
        )
        self.assertEqual(failed.status_code, 200, failed.text)

        promoted = self.client.post(
            f"/v1/admin/model-releases/{release.id}/promote",
            headers=self.admin,
        )
        self.assertEqual(promoted.status_code, 409, promoted.text)
        placement_error = promoted.json()["detail"]["details"]["placements"][0]
        self.assertEqual(placement_error["code"], "BENCHMARK_RUN_FAILED")
        self.assertEqual(placement_error["benchmark_run_status"], "FAILED")
        self.assertEqual(placement_error["evidence_id"], passing_evidence_id)
        self.assertEqual(
            placement_error["failure_code"], "BENCHMARK_RUNTIME_UNAVAILABLE"
        )

    def test_profile_change_at_collection_is_reduced_to_evidence_rejected(self):
        node, release, placement, credential = self.fixture("evidence-reject")
        body = _request(node, release, placement)
        run = self.prepare(body).json()["benchmark_run"]
        task = self.apply(body["request_id"]).json()["task"]
        agent = {"Authorization": f"Bearer {credential}"}
        self.client.post("/v1/agent/tasks/claim", headers=agent)
        with self.client.app.state.session_factory() as session:
            record = session.get(NodeProfileRecord, node.id)
            changed = dict(record.profile)
            changed["memory_mib"] += 1
            record.profile = changed
            session.commit()

        completed = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete",
            headers=agent,
            json={"result": _result(run["id"])},
        )

        self.assertEqual(completed.status_code, 200, completed.text)
        with self.client.app.state.session_factory() as session:
            stored_task = session.get(Task, task["id"])
            stored_run = session.get(BenchmarkRun, run["id"])
            self.assertEqual(stored_task.status, TaskStatus.FAILED.value)
            self.assertEqual(stored_task.error, "BENCHMARK_EVIDENCE_REJECTED")
            self.assertIsNone(stored_task.result)
            self.assertEqual(stored_run.status, "FAILED")
            self.assertEqual(
                stored_run.failure_code, "BENCHMARK_EVIDENCE_REJECTED"
            )
            self.assertIsNone(stored_run.evidence_id)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(BenchmarkEvidence)),
                0,
            )

    def test_different_requests_get_distinct_ordered_evidence(self):
        node, release, placement, credential = self.fixture("ordered")
        agent = {"Authorization": f"Bearer {credential}"}
        evidence_ids = []
        run_ids = []
        for _ in range(2):
            body = _request(node, release, placement)
            run = self.prepare(body).json()["benchmark_run"]
            run_ids.append(run["id"])
            task = self.apply(body["request_id"]).json()["task"]
            claimed = self.client.post(
                "/v1/agent/tasks/claim", headers=agent
            ).json()["task"]
            self.assertEqual(claimed["id"], task["id"])
            completed = self.client.post(
                f"/v1/agent/tasks/{task['id']}/complete",
                headers=agent,
                json={"result": _result(run["id"])},
            )
            self.assertEqual(completed.status_code, 200, completed.text)
            evidence_ids.append(
                completed.json()["benchmark_run"]["evidence_id"]
            )

        self.assertNotEqual(evidence_ids[0], evidence_ids[1])
        with self.client.app.state.session_factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(BenchmarkRun)), 2
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(BenchmarkEvidence)),
                2,
            )
            evidence = list(
                session.scalars(
                    select(BenchmarkEvidence).order_by(
                        BenchmarkEvidence.registration_sequence
                    )
                )
            )
            self.assertEqual(
                [item.registration_sequence for item in evidence], [1, 2]
            )
            self.assertEqual(
                [item.benchmark_run_id for item in evidence], run_ids
            )

    def test_latest_identical_failed_rerun_blocks_promotion(self):
        node, release, placement, credential = self.fixture("latest-rerun")
        agent = {"Authorization": f"Bearer {credential}"}
        evidence_ids = []
        for oom_count in (1, 0, 1):
            body = _request(node, release, placement)
            run = self.prepare(body).json()["benchmark_run"]
            task = self.apply(body["request_id"]).json()["task"]
            self.client.post("/v1/agent/tasks/claim", headers=agent)
            result = _result(run["id"])
            result["metrics"]["oom_count"] = oom_count
            completed = self.client.post(
                f"/v1/agent/tasks/{task['id']}/complete",
                headers=agent,
                json={"result": result},
            )
            self.assertEqual(completed.status_code, 200, completed.text)
            evidence_ids.append(
                completed.json()["benchmark_run"]["evidence_id"]
            )

        promoted = self.client.post(
            f"/v1/admin/model-releases/{release.id}/promote",
            headers=self.admin,
        )

        self.assertEqual(promoted.status_code, 409, promoted.text)
        placement_error = promoted.json()["detail"]["details"]["placements"][0]
        self.assertEqual(placement_error["evidence_id"], evidence_ids[-1])
        self.assertIn("OOM", placement_error["failure_codes"])
        with self.client.app.state.session_factory() as session:
            evidence = list(
                session.scalars(
                    select(BenchmarkEvidence).order_by(
                        BenchmarkEvidence.registration_sequence
                    )
                )
            )
            self.assertEqual(
                [item.registration_sequence for item in evidence], [1, 2, 3]
            )
            self.assertEqual(
                [item.status for item in evidence],
                ["FAILED", "PASSED", "FAILED"],
            )

    def test_collected_slo_failure_is_evidence_not_task_execution_failure(self):
        node, release, placement, credential = self.fixture("slo-failure")
        body = _request(node, release, placement)
        run = self.prepare(body).json()["benchmark_run"]
        task = self.apply(body["request_id"]).json()["task"]
        agent = {"Authorization": f"Bearer {credential}"}
        self.client.post("/v1/agent/tasks/claim", headers=agent)
        result = _result(run["id"])
        result["metrics"]["oom_count"] = 1

        completed = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete",
            headers=agent,
            json={"result": result},
        )

        self.assertEqual(completed.status_code, 200, completed.text)
        with self.client.app.state.session_factory() as session:
            stored_run = session.get(BenchmarkRun, run["id"])
            evidence = session.get(BenchmarkEvidence, stored_run.evidence_id)
            stored_task = session.get(Task, task["id"])
            self.assertEqual(stored_run.status, "SUCCEEDED")
            self.assertEqual(stored_task.status, TaskStatus.SUCCEEDED.value)
            self.assertEqual(evidence.status, "FAILED")
            self.assertIn("OOM", evidence.failure_codes)

    def test_cancel_updates_benchmark_run_without_creating_evidence(self):
        node, release, placement, _ = self.fixture("cancel")
        body = _request(node, release, placement)
        self.prepare(body)
        task = self.apply(body["request_id"]).json()["task"]

        canceled = self.client.post(
            f"/v1/admin/tasks/{task['id']}/cancel", headers=self.admin
        )

        self.assertEqual(canceled.status_code, 200, canceled.text)
        with self.client.app.state.session_factory() as session:
            stored_task = session.get(Task, task["id"])
            run = session.scalar(
                select(BenchmarkRun).where(BenchmarkRun.task_id == task["id"])
            )
            self.assertEqual(stored_task.status, TaskStatus.CANCELED.value)
            self.assertIsNone(stored_task.result)
            self.assertIsNone(stored_task.error)
            self.assertEqual(run.status, "FAILED")
            self.assertEqual(run.failure_code, "BENCHMARK_CANCELED")
            self.assertIsNone(run.evidence_id)

    def test_only_expired_running_benchmark_lease_can_be_canceled(self):
        node, _, placement, credential = self.fixture("cancel-expired")
        with self.client.app.state.session_factory() as session:
            release = placement.release_id
        body = {
            "request_id": str(uuid.uuid4()),
            "release_id": release,
            "placement_id": placement.id,
            "node_ids": [node.id],
            "workload_id": "short-chat-1k-128",
            "dure_commit": "d" * 40,
        }
        self.prepare(body)
        task = self.apply(body["request_id"]).json()["task"]
        agent = {"Authorization": f"Bearer {credential}"}
        self.client.post("/v1/agent/tasks/claim", headers=agent)

        active = self.client.post(
            f"/v1/admin/tasks/{task['id']}/cancel", headers=self.admin
        )
        self.assertEqual(active.status_code, 409)
        with self.client.app.state.session_factory() as session:
            stored = session.get(Task, task["id"])
            stored.lease_until = utcnow() - timedelta(seconds=1)
            session.commit()

        canceled = self.client.post(
            f"/v1/admin/tasks/{task['id']}/cancel", headers=self.admin
        )
        self.assertEqual(canceled.status_code, 200, canceled.text)
        with self.client.app.state.session_factory() as session:
            stored = session.get(Task, task["id"])
            run = session.scalar(
                select(BenchmarkRun).where(BenchmarkRun.task_id == task["id"])
            )
            self.assertEqual(stored.status, TaskStatus.CANCELED.value)
            self.assertIsNone(stored.lease_until)
            self.assertEqual(run.status, "FAILED")
            self.assertEqual(run.failure_code, "BENCHMARK_CANCELED")

    def test_cancel_refreshes_a_stale_expired_lease_before_deciding(self):
        node, _, placement, credential = self.fixture("cancel-race")
        body = {
            "request_id": str(uuid.uuid4()),
            "release_id": placement.release_id,
            "placement_id": placement.id,
            "node_ids": [node.id],
            "workload_id": "short-chat-1k-128",
            "dure_commit": "d" * 40,
        }
        self.prepare(body)
        task = self.apply(body["request_id"]).json()["task"]
        agent = {"Authorization": f"Bearer {credential}"}
        self.client.post("/v1/agent/tasks/claim", headers=agent)

        factory = self.client.app.state.session_factory
        with factory() as stale_session:
            stale = stale_session.get(Task, task["id"])
            stale.lease_until = utcnow() - timedelta(seconds=1)
            stale_session.commit()
            stale = stale_session.get(Task, task["id"])
            with factory() as renewal_session:
                renewed = renewal_session.get(Task, task["id"])
                renewed.lease_until = utcnow() + timedelta(minutes=5)
                renewal_session.commit()

            self.assertFalse(cancel_task(stale_session, stale))
            stale_session.rollback()

        with factory() as session:
            current = session.get(Task, task["id"])
            run = session.scalar(
                select(BenchmarkRun).where(BenchmarkRun.task_id == task["id"])
            )
            self.assertEqual(current.status, TaskStatus.RUNNING.value)
            self.assertGreater(
                current.lease_until, utcnow().replace(tzinfo=None)
            )
            self.assertEqual(run.status, "QUEUED")


if __name__ == "__main__":
    unittest.main()
