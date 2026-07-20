from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import func, select

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    TestClient = None

from dure.control.api import create_app
from dure.control.benchmark import (
    BENCHMARK_POLICY_VERSION,
    BENCHMARK_SUITE_ID,
    BenchmarkIdentityMismatchError,
    BenchmarkPromotionError,
    benchmark_inventory_fingerprint,
    promote_model_release,
    register_benchmark_evidence,
)
from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import (
    AuditEvent,
    BenchmarkEvidence,
    Deployment,
    Node,
    NodeProfileRecord,
    Task,
    utcnow,
)
from dure.control.service import (
    add_placement_profile,
    create_model_artifact,
    create_model_release,
    create_runtime_release,
    transition_model_release,
)

from .helpers import profile


def _hex(seed: str, length: int) -> str:
    value = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return (value * ((length // len(value)) + 1))[:length]


def _node(session, name: str):
    now = utcnow()
    node = Node(
        install_id=f"benchmark-{name}",
        display_name=name,
        hostname=name,
        agent_version="0.3.9",
        approved=True,
        last_seen=now,
    )
    session.add(node)
    session.flush()
    session.add(
        NodeProfileRecord(
            node_id=node.id,
            profile=profile("agent-local-name").to_dict(),
            updated_at=now,
        )
    )
    session.commit()
    return node


def _placement(session, release_id: str, profile_id: str, **overrides):
    values = {
        "release_id": release_id,
        "profile_id": profile_id,
        "topology": "single-gpu",
        "node_count": 1,
        "min_gpu_memory_mib": 8192,
        "min_disk_free_mib": 16384,
        "pipeline_parallel_size": 1,
        "tensor_parallel_size": 1,
        "requires_network_evidence": False,
        "requires_nccl": False,
        "min_bandwidth_mbps": None,
        "max_rtt_ms": None,
        "max_packet_loss_pct": None,
        "max_ttft_p95_ms": 1000.0,
        "max_tpot_p95_ms": 100.0,
        "max_e2e_p95_ms": 5000.0,
        "min_success_rate": 0.99,
        "min_vram_headroom_pct": 10.0,
        "min_throughput_tps": 10.0,
    }
    values.update(overrides)
    return add_placement_profile(session, **values)


def _release(session, key: str, *, placement_count: int = 1, validated: bool = True):
    artifact = create_model_artifact(
        session,
        model_id=f"model-{key}",
        repository=f"Benchmark/{key}",
        revision=_hex(f"revision-{key}", 40),
        manifest_digest="sha256:" + _hex(f"manifest-{key}", 64),
        quantization="awq",
        size_mib=8192,
        default_max_model_len=8192,
        layer_count=32,
        license_id="apache-2.0",
    )
    runtime = create_runtime_release(
        session,
        version=f"runtime-{key}",
        image=f"registry.example/{key}@sha256:{_hex(f'image-{key}', 64)}",
        vllm_version="0.9.0",
        cuda_version="12.8",
        gpu_architectures=["ampere"],
    )
    release = create_model_release(
        session,
        artifact_id=artifact.id,
        runtime_id=runtime.id,
        quality_rank=10,
    )
    placements = [
        _placement(session, release.id, f"single-{index}")
        for index in range(placement_count)
    ]
    if validated:
        transition_model_release(session, release.id, "VALIDATED")
    return artifact, runtime, release, placements


def _multi_release(session, key: str):
    artifact, runtime, release, _ = _release(
        session, key, placement_count=0, validated=False
    )
    placement = _placement(
        session,
        release.id,
        "pipeline-3",
        topology="pipeline",
        node_count=3,
        pipeline_parallel_size=3,
        requires_network_evidence=True,
        requires_nccl=True,
        min_bandwidth_mbps=1000,
        max_rtt_ms=5.0,
        max_packet_loss_pct=1.0,
    )
    transition_model_release(session, release.id, "VALIDATED")
    return artifact, runtime, release, placement


def _evidence_body(session, artifact, runtime, release, placement, nodes, **overrides):
    node_ids = [node.id for node in nodes]
    values = {
        "release_id": release.id,
        "placement_id": placement.id,
        "suite_id": BENCHMARK_SUITE_ID,
        "node_ids": node_ids,
        "inventory_fingerprint": benchmark_inventory_fingerprint(session, node_ids),
        "artifact_revision": artifact.revision,
        "artifact_manifest_digest": artifact.manifest_digest,
        "runtime_image": runtime.image,
        "dure_commit": "d" * 40,
        "policy_version": BENCHMARK_POLICY_VERSION,
        "input_tokens": 4096,
        "output_tokens": 256,
        "concurrency": 8,
        "warmup_requests": 20,
        "request_count": 200,
        "duration_seconds": 900.0,
        "oom_count": 0,
        "crash_count": 0,
        "restart_count": 0,
        "ttft_p95_ms": 900.0,
        "tpot_p95_ms": 90.0,
        "e2e_p95_ms": 4500.0,
        "throughput_tps": 12.0,
        "success_rate": 1.0,
        "vram_headroom_pct": 12.0,
        "quality_score": 0.90,
        "network_bandwidth_mbps": None,
        "network_rtt_ms": None,
        "packet_loss_pct": None,
        "nccl_all_reduce_ok": None,
    }
    values.update(overrides)
    return values


class BenchmarkServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.engine = make_engine(
            f"sqlite:///{Path(self.temporary.name) / 'benchmark.db'}"
        )
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self):
        self.engine.dispose()
        self.temporary.cleanup()

    def test_passing_evidence_promotes_and_retries_idempotently(self):
        with self.factory() as session:
            node = _node(session, "pass")
            artifact, runtime, release, placements = _release(session, "pass")
            body = _evidence_body(
                session, artifact, runtime, release, placements[0], [node]
            )

            evidence = register_benchmark_evidence(session, **body)
            duplicate = register_benchmark_evidence(session, **body)

            self.assertEqual(evidence.status, "PASSED")
            self.assertEqual(evidence.id, duplicate.id)
            self.assertEqual(evidence.registration_sequence, 1)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(BenchmarkEvidence)), 1
            )
            release = transition_model_release(session, release.id, "ACTIVE")
            frozen_ids = list(release.promotion_evidence_ids)
            frozen_digest = release.promotion_evidence_digest
            active, evidence_ids, changed = promote_model_release(session, release.id)
            self.assertFalse(changed)
            self.assertEqual(evidence_ids, frozen_ids)
            self.assertEqual(active.promotion_evidence_digest, frozen_digest)
            self.assertEqual(
                session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "model_release.promote")
                ),
                1,
            )

    def test_latest_failed_evidence_blocks_then_latest_pass_recovers(self):
        with self.factory() as session:
            node = _node(session, "latest")
            artifact, runtime, release, placements = _release(session, "latest")
            body = _evidence_body(
                session, artifact, runtime, release, placements[0], [node]
            )
            first = register_benchmark_evidence(session, **body)
            failed = register_benchmark_evidence(session, **dict(body, oom_count=1))

            self.assertEqual(first.status, "PASSED")
            self.assertEqual(failed.status, "FAILED")
            self.assertIn("OOM", failed.failure_codes)
            with self.assertRaises(BenchmarkPromotionError) as raised:
                promote_model_release(session, release.id)
            placement_error = raised.exception.details["placements"][0]
            self.assertEqual(placement_error["evidence_id"], failed.id)
            self.assertIn("OOM", placement_error["failure_codes"])

            latest = register_benchmark_evidence(
                session, **dict(body, concurrency=4)
            )
            active, ids, changed = promote_model_release(session, release.id)
            self.assertTrue(changed)
            self.assertEqual(active.status, "ACTIVE")
            self.assertEqual(ids, [latest.id])

    def test_latency_performance_quality_and_stability_failures_are_recorded(self):
        with self.factory() as session:
            node = _node(session, "slo")
            artifact, runtime, release, placements = _release(session, "slo")
            body = _evidence_body(
                session,
                artifact,
                runtime,
                release,
                placements[0],
                [node],
                warmup_requests=1,
                request_count=10,
                duration_seconds=100.0,
                crash_count=1,
                restart_count=1,
                ttft_p95_ms=1000.1,
                tpot_p95_ms=100.1,
                e2e_p95_ms=5000.1,
                throughput_tps=9.9,
                success_rate=0.98,
                vram_headroom_pct=9.9,
                quality_score=0.79,
            )

            evidence = register_benchmark_evidence(session, **body)

            self.assertEqual(evidence.status, "FAILED")
            self.assertTrue(
                {
                    "WARMUP_COUNT",
                    "MEASUREMENT_WINDOW",
                    "PROCESS_CRASH",
                    "RUNTIME_RESTART",
                    "TTFT_SLO",
                    "TPOT_SLO",
                    "E2E_SLO",
                    "THROUGHPUT_SLO",
                    "SUCCESS_RATE_SLO",
                    "VRAM_HEADROOM_SLO",
                    "QUALITY_SCORE",
                }
                <= set(evidence.failure_codes)
            )

    def test_identity_state_and_profile_changes_fail_closed_without_writes(self):
        with self.factory() as session:
            node = _node(session, "binding")
            artifact, runtime, release, placements = _release(session, "binding")
            body = _evidence_body(
                session, artifact, runtime, release, placements[0], [node]
            )
            for field, value in (
                ("artifact_revision", "a" * 40),
                ("artifact_manifest_digest", "sha256:" + "b" * 64),
                ("runtime_image", "registry.example/wrong@sha256:" + "c" * 64),
                ("inventory_fingerprint", "sha256:" + "e" * 64),
            ):
                with self.subTest(field=field):
                    with self.assertRaises(BenchmarkIdentityMismatchError):
                        register_benchmark_evidence(
                            session, **dict(body, **{field: value})
                        )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(BenchmarkEvidence)), 0
            )

            evidence = register_benchmark_evidence(session, **body)
            stored = session.get(NodeProfileRecord, node.id)
            changed = dict(stored.profile)
            changed["disk_free_mib"] -= 1
            stored.profile = changed
            session.commit()
            with self.assertRaises(BenchmarkPromotionError) as raised:
                promote_model_release(session, release.id)
            self.assertEqual(
                raised.exception.details["placements"][0]["code"], "PROFILE_CHANGED"
            )
            refreshed = _evidence_body(
                session, artifact, runtime, release, placements[0], [node]
            )
            current = register_benchmark_evidence(session, **refreshed)
            _, ids, _ = promote_model_release(session, release.id)
            self.assertEqual(ids, [current.id])
            self.assertNotEqual(ids, [evidence.id])

            transition_model_release(session, release.id, "DEPRECATED")
            with self.assertRaises(BenchmarkPromotionError):
                register_benchmark_evidence(session, **refreshed)

            draft_artifact, draft_runtime, draft, draft_placements = _release(
                session, "draft", validated=False
            )
            draft_body = _evidence_body(
                session,
                draft_artifact,
                draft_runtime,
                draft,
                draft_placements[0],
                [node],
            )
            with self.assertRaises(BenchmarkPromotionError):
                register_benchmark_evidence(session, **draft_body)

    def test_multinode_network_and_nccl_evidence_gate(self):
        with self.factory() as session:
            nodes = [_node(session, f"network-{index}") for index in range(3)]
            artifact, runtime, release, placement = _multi_release(session, "network")
            body = _evidence_body(
                session, artifact, runtime, release, placement, nodes
            )

            missing = register_benchmark_evidence(session, **body)
            self.assertTrue(
                {"NETWORK_EVIDENCE_MISSING", "NCCL_EVIDENCE_MISSING"}
                <= set(missing.failure_codes)
            )
            failed = register_benchmark_evidence(
                session,
                **dict(
                    body,
                    network_bandwidth_mbps=999.0,
                    network_rtt_ms=5.1,
                    packet_loss_pct=1.1,
                    nccl_all_reduce_ok=False,
                ),
            )
            self.assertTrue(
                {
                    "NETWORK_BANDWIDTH_SLO",
                    "NETWORK_RTT_SLO",
                    "NETWORK_PACKET_LOSS_SLO",
                    "NCCL_FAILED",
                }
                <= set(failed.failure_codes)
            )
            passed = register_benchmark_evidence(
                session,
                **dict(
                    body,
                    network_bandwidth_mbps=1000.0,
                    network_rtt_ms=5.0,
                    packet_loss_pct=1.0,
                    nccl_all_reduce_ok=True,
                ),
            )
            self.assertEqual(passed.status, "PASSED")
            _, ids, _ = promote_model_release(session, release.id)
            self.assertEqual(ids, [passed.id])

    def test_every_placement_requires_its_own_current_passing_evidence(self):
        with self.factory() as session:
            node = _node(session, "placements")
            artifact, runtime, release, placements = _release(
                session, "placements", placement_count=2
            )
            first = register_benchmark_evidence(
                session,
                **_evidence_body(
                    session, artifact, runtime, release, placements[0], [node]
                ),
            )
            with self.assertRaises(BenchmarkPromotionError) as raised:
                promote_model_release(session, release.id)
            self.assertEqual(
                raised.exception.details["placements"][0]["placement_id"],
                placements[1].id,
            )
            second = register_benchmark_evidence(
                session,
                **_evidence_body(
                    session, artifact, runtime, release, placements[1], [node]
                ),
            )
            _, ids, _ = promote_model_release(session, release.id)
            expected = [
                evidence.id
                for _, evidence in sorted(
                    ((placements[0].id, first), (placements[1].id, second))
                )
            ]
            self.assertEqual(ids, expected)

    def test_nonfinite_values_are_rejected_without_evidence(self):
        with self.factory() as session:
            node = _node(session, "finite")
            artifact, runtime, release, placements = _release(session, "finite")
            body = _evidence_body(
                session, artifact, runtime, release, placements[0], [node]
            )
            for field in ("duration_seconds", "throughput_tps", "network_bandwidth_mbps"):
                with self.subTest(field=field):
                    with self.assertRaisesRegex(ValueError, "finite"):
                        register_benchmark_evidence(
                            session, **dict(body, **{field: float("inf")})
                        )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(BenchmarkEvidence)), 0
            )


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class BenchmarkAPITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'benchmark-api.db'}"
        self.client = TestClient(
            create_app(database_url=url, admin_token="admin-secret", create_schema=True)
        )
        self.admin = {"Authorization": "Bearer admin-secret"}

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def fixture(self, key="api"):
        with self.client.app.state.session_factory() as session:
            node = _node(session, f"{key}-node")
            artifact, runtime, release, placements = _release(session, key)
            body = _evidence_body(
                session, artifact, runtime, release, placements[0], [node]
            )
            return node, release, body

    def test_strict_admin_evidence_api_is_idempotent_and_promotes_once(self):
        node, release, body = self.fixture()
        context_endpoint = "/v1/admin/benchmark-context"
        context_body = {
            "release_id": body["release_id"],
            "placement_id": body["placement_id"],
            "node_ids": body["node_ids"],
        }
        self.assertEqual(
            self.client.post(context_endpoint, json=context_body).status_code, 401
        )
        self.assertEqual(
            self.client.post(
                context_endpoint,
                headers=self.admin,
                json={**context_body, "command": "id"},
            ).status_code,
            422,
        )
        context = self.client.post(
            context_endpoint, headers=self.admin, json=context_body
        )
        self.assertEqual(context.status_code, 200)
        for key in (
            "release_id",
            "placement_id",
            "node_ids",
            "inventory_fingerprint",
            "artifact_revision",
            "artifact_manifest_digest",
            "runtime_image",
            "suite_id",
            "policy_version",
        ):
            self.assertEqual(context.json()["context"][key], body[key])

        endpoint = "/v1/admin/benchmark-evidence"
        self.assertEqual(self.client.post(endpoint, json=body).status_code, 401)
        for field, value in (
            ("prompt", "secret prompt"),
            ("token", "secret"),
            ("stdout", "log"),
            ("command", "id"),
            ("docker_args", ["--privileged"]),
            ("env", {"TOKEN": "secret"}),
            ("mounts", ["/etc:/host"]),
            ("metadata", {"anything": True}),
        ):
            with self.subTest(field=field):
                response = self.client.post(
                    endpoint, headers=self.admin, json={**body, field: value}
                )
                self.assertEqual(response.status_code, 422)
        infinite = self.client.post(
            endpoint,
            headers=self.admin,
            content=json.dumps(
                {**body, "throughput_tps": float("inf")}, allow_nan=True
            ),
        )
        self.assertEqual(infinite.status_code, 422)

        first = self.client.post(endpoint, headers=self.admin, json=body)
        second = self.client.post(endpoint, headers=self.admin, json=body)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json()["evidence"]["id"], second.json()["evidence"]["id"])
        listed = self.client.get(
            f"{endpoint}?release_id={release.id}", headers=self.admin
        ).json()["evidence"]
        self.assertEqual(len(listed), 1)

        promoted = self.client.post(
            f"/v1/admin/model-releases/{release.id}/promote", headers=self.admin
        )
        retried = self.client.post(
            f"/v1/admin/model-releases/{release.id}/promote", headers=self.admin
        )
        self.assertTrue(promoted.json()["changed"])
        self.assertFalse(retried.json()["changed"])
        self.assertEqual(
            promoted.json()["qualification"], retried.json()["qualification"]
        )
        with self.client.app.state.session_factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)), 0
            )
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)

    def test_transition_cannot_bypass_gate_and_binding_conflicts(self):
        _, release, body = self.fixture("gate")
        blocked = self.client.post(
            f"/v1/admin/model-releases/{release.id}/transition",
            headers=self.admin,
            json={"status": "ACTIVE"},
        )
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(blocked.json()["detail"]["code"], "BENCHMARK_GATE_FAILED")
        mismatch = self.client.post(
            "/v1/admin/benchmark-evidence",
            headers=self.admin,
            json={**body, "artifact_revision": "a" * 40},
        )
        self.assertEqual(mismatch.status_code, 409)


if __name__ == "__main__":
    unittest.main()
