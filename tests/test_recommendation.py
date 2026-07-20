from __future__ import annotations

import hashlib
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
from dure.control.benchmark import (
    BENCHMARK_POLICY_VERSION,
    BENCHMARK_SUITE_ID,
    benchmark_inventory_fingerprint,
    promote_model_release,
    register_benchmark_evidence,
)
from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import (
    AuditEvent,
    BenchmarkEvidence,
    Deployment,
    ModelArtifact,
    ModelRelease,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    RuntimeRelease,
    Task,
    utcnow,
)
from dure.control.recommendation import recommend_deployment
from dure.control.service import (
    add_placement_profile,
    create_model_artifact,
    create_model_release,
    create_runtime_release,
    transition_model_release,
)

from .helpers import profile


COUNTED_MODELS = (
    Node,
    NodeProfileRecord,
    ModelArtifact,
    RuntimeRelease,
    ModelRelease,
    PlacementProfileRecord,
    BenchmarkEvidence,
    Deployment,
    Task,
    AuditEvent,
)


def _hex(seed: str, length: int) -> str:
    value = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return (value * ((length // len(value)) + 1))[:length]


def _add_node(
    session,
    name: str,
    *,
    now,
    approved: bool = True,
    last_seen_age: int = 0,
    profile_age: int = 0,
    stored_profile: dict | None | str = "valid",
    compute_capability: str = "8.6",
):
    node = Node(
        install_id=f"install-{name}",
        display_name=name,
        hostname=name,
        agent_version="0.3.8",
        approved=approved,
        last_seen=now - timedelta(seconds=last_seen_age),
    )
    session.add(node)
    session.flush()
    if stored_profile is not None:
        value = profile(
            "agent-reported-id",
            compute_capability=compute_capability,
        ).to_dict()
        if stored_profile != "valid":
            value = stored_profile
        session.add(
            NodeProfileRecord(
                node_id=node.id,
                profile=value,
                updated_at=now - timedelta(seconds=profile_age),
            )
        )
    session.commit()
    return node


def _add_release(
    session,
    key: str,
    *,
    status: str = "ACTIVE",
    quality_rank: int = 10,
    model_id: str = "qwen-test-awq",
    gpu_architectures: list[str] | None = None,
    placement_overrides: dict | None = None,
    benchmark_nodes: list[Node] | None = None,
):
    artifact = create_model_artifact(
        session,
        model_id=model_id,
        repository=f"TestOrg/{key}-AWQ",
        revision=_hex(f"revision-{key}", 40),
        manifest_digest="sha256:" + _hex(f"manifest-{key}", 64),
        quantization="awq",
        size_mib=8192,
        default_max_model_len=8192,
        layer_count=32,
        license_id="apache-2.0",
    )
    architectures = gpu_architectures or ["ampere"]
    runtime = create_runtime_release(
        session,
        version=f"runtime-{key}",
        image=f"registry.example/{key}@sha256:{_hex(f'image-{key}', 64)}",
        vllm_version="0.9.0",
        cuda_version="12.8",
        gpu_architectures=architectures,
    )
    release = create_model_release(
        session,
        artifact_id=artifact.id,
        runtime_id=runtime.id,
        quality_rank=quality_rank,
    )
    values = {
        "release_id": release.id,
        "profile_id": "single-24g",
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
    values.update(placement_overrides or {})
    placement = add_placement_profile(session, **values)
    if status in {"VALIDATED", "ACTIVE", "DEPRECATED"}:
        transition_model_release(session, release.id, "VALIDATED")
    if status in {"ACTIVE", "DEPRECATED"}:
        if benchmark_nodes is None:
            capability_by_architecture = {
                "ampere": "8.6",
                "ada": "8.9",
                "hopper": "9.0",
                "blackwell": "10.0",
            }
            benchmark_capability = capability_by_architecture[architectures[0]]
            benchmark_nodes = [
                _add_node(
                    session,
                    f"benchmark-{key}-{index}",
                    now=utcnow(),
                    compute_capability=benchmark_capability,
                )
                for index in range(placement.node_count)
            ]
        node_ids = [node.id for node in benchmark_nodes]
        requires_network = (
            placement.requires_network_evidence or placement.node_count > 1
        )
        register_benchmark_evidence(
            session,
            release_id=release.id,
            placement_id=placement.id,
            suite_id=BENCHMARK_SUITE_ID,
            node_ids=node_ids,
            inventory_fingerprint=benchmark_inventory_fingerprint(session, node_ids),
            artifact_revision=artifact.revision,
            artifact_manifest_digest=artifact.manifest_digest,
            runtime_image=runtime.image,
            dure_commit=_hex(f"dure-{key}", 40),
            policy_version=BENCHMARK_POLICY_VERSION,
            input_tokens=4096,
            output_tokens=256,
            concurrency=8,
            warmup_requests=20,
            request_count=200,
            duration_seconds=900.0,
            oom_count=0,
            crash_count=0,
            restart_count=0,
            ttft_p95_ms=900.0,
            tpot_p95_ms=90.0,
            e2e_p95_ms=4500.0,
            throughput_tps=12.0,
            success_rate=1.0,
            vram_headroom_pct=12.0,
            quality_score=0.90,
            network_bandwidth_mbps=20000.0 if requires_network else None,
            network_rtt_ms=1.0 if requires_network else None,
            packet_loss_pct=0.0 if requires_network else None,
            nccl_all_reduce_ok=True if placement.requires_nccl else None,
        )
        promote_model_release(session, release.id)
    if status == "DEPRECATED":
        transition_model_release(session, release.id, "DEPRECATED")
    if status == "REVOKED":
        transition_model_release(session, release.id, "REVOKED")
    return release, placement


def _row_counts(session) -> dict[str, int]:
    return {
        model.__tablename__: session.scalar(select(func.count()).select_from(model))
        for model in COUNTED_MODELS
    }


class RecommendationServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.engine = make_engine(
            f"sqlite:///{Path(self.temporary.name) / 'recommendation.db'}"
        )
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)
        self.now = utcnow()

    def tearDown(self):
        self.engine.dispose()
        self.temporary.cleanup()

    def test_active_candidates_are_deterministic_distinct_and_read_only(self):
        with self.factory() as session:
            first = _add_node(session, "first", now=self.now)
            second = _add_node(session, "second", now=self.now)
            lower, _ = _add_release(session, "lower", quality_rank=10)
            higher, _ = _add_release(session, "higher", quality_rank=20)
            _add_release(session, "draft", status="DRAFT", quality_rank=100)
            _add_release(session, "old", status="DEPRECATED", quality_rank=200)
            before = _row_counts(session)

            forward = recommend_deployment(
                session,
                node_ids=[second.id, first.id],
                all_online=False,
                now=self.now,
            )
            reverse = recommend_deployment(
                session,
                node_ids=[first.id, second.id],
                all_online=False,
                now=self.now,
            )

            self.assertEqual(forward, reverse)
            recommendation = forward["recommendation"]
            self.assertEqual(recommendation["selected"]["model_release_id"], higher.id)
            self.assertEqual(len(recommendation["selected"]["artifact_revision"]), 40)
            self.assertTrue(
                recommendation["selected"]["runtime_image"].count("@sha256:") == 1
            )
            self.assertEqual(
                {item["model_release_id"] for item in recommendation["candidates"]},
                {lower.id, higher.id},
            )
            self.assertEqual(len({item["candidate_id"] for item in recommendation["candidates"]}), 2)
            self.assertEqual(recommendation["requested_node_ids"], sorted([first.id, second.id]))
            self.assertNotIn("agent-reported-id", recommendation["requested_node_ids"])
            self.assertTrue(recommendation["id"].startswith("sha256:"))
            self.assertTrue(recommendation["catalog_version"].startswith("sha256:"))
            core = dict(recommendation)
            recommendation_id = core.pop("id")
            expected_id = "sha256:" + hashlib.sha256(
                json.dumps(
                    core,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode("utf-8")
            ).hexdigest()
            self.assertEqual(recommendation_id, expected_id)
            self.assertEqual(_row_counts(session), before)

    def test_pending_offline_and_stale_profiles_have_distinct_rejections(self):
        with self.factory() as session:
            pending = _add_node(session, "pending", now=self.now, approved=False)
            offline = _add_node(session, "offline", now=self.now, last_seen_age=40)
            stale = _add_node(session, "stale", now=self.now, profile_age=91)
            _add_release(session, "eligible")

            result = recommend_deployment(
                session,
                node_ids=[stale.id, pending.id, offline.id],
                all_online=False,
                now=self.now,
            )["recommendation"]

            self.assertIsNone(result["selected"])
            codes = {item["code"] for item in result["candidates"][0]["rejections"]}
            self.assertTrue(
                {"NODE_PENDING", "NODE_OFFLINE", "PROFILE_STALE", "NODE_COUNT"}
                <= codes
            )

    def test_multinode_candidate_remains_fail_closed_until_evidence_is_linked(self):
        with self.factory() as session:
            nodes = [_add_node(session, f"node-{index}", now=self.now) for index in range(3)]
            release, _ = _add_release(
                session,
                "pipeline",
                quality_rank=72,
                placement_overrides={
                    "profile_id": "pipeline-3x24g",
                    "topology": "pipeline",
                    "node_count": 3,
                    "pipeline_parallel_size": 3,
                    "requires_network_evidence": True,
                    "requires_nccl": True,
                    "min_bandwidth_mbps": 10000,
                    "max_rtt_ms": 5.0,
                    "max_packet_loss_pct": 0.1,
                },
                benchmark_nodes=nodes,
            )

            self.assertEqual(release.status, "ACTIVE")
            self.assertEqual(len(release.promotion_evidence_ids), 1)
            evidence = session.get(BenchmarkEvidence, release.promotion_evidence_ids[0])
            self.assertIsNotNone(evidence)
            assert evidence is not None
            self.assertEqual(evidence.status, "PASSED")
            self.assertTrue(evidence.nccl_all_reduce_ok)

            result = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now,
            )["recommendation"]

            self.assertIsNone(result["selected"])
            codes = {item["code"] for item in result["candidates"][0]["rejections"]}
            self.assertIn("NETWORK_EVIDENCE", codes)

    def test_missing_invalid_profiles_and_runtime_architecture_fail_closed(self):
        with self.factory() as session:
            missing = _add_node(session, "missing", now=self.now, stored_profile=None)
            invalid = _add_node(session, "invalid", now=self.now, stored_profile={"bad": True})
            _add_release(session, "profiles")

            result = recommend_deployment(
                session,
                node_ids=[missing.id, invalid.id],
                all_online=False,
                now=self.now,
            )["recommendation"]
            codes = {item["code"] for item in result["candidates"][0]["rejections"]}
            self.assertTrue({"PROFILE_MISSING", "PROFILE_INVALID"} <= codes)

        with self.factory() as session:
            ampere = _add_node(session, "ampere", now=self.now)
            hopper_release, _ = _add_release(
                session,
                "hopper-only",
                quality_rank=20,
                gpu_architectures=["hopper"],
            )
            result = recommend_deployment(
                session,
                node_ids=[ampere.id],
                all_online=False,
                now=self.now,
            )["recommendation"]
            candidate = next(
                item
                for item in result["candidates"]
                if item["model_release_id"] == hopper_release.id
            )
            codes = {item["code"] for item in candidate["rejections"]}
            self.assertIn("RUNTIME_GPU_ARCH", codes)

    def test_no_active_candidate_is_a_successful_explained_result(self):
        with self.factory() as session:
            node = _add_node(session, "node", now=self.now)
            _add_release(session, "draft-only", status="DRAFT")

            result = recommend_deployment(
                session,
                node_ids=[node.id],
                all_online=False,
                now=self.now,
            )["recommendation"]

            self.assertIsNone(result["selected"])
            self.assertEqual(result["candidates"], [])
            self.assertEqual(result["rejections"][0]["code"], "NO_ACTIVE_CANDIDATE")


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class RecommendationAPITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'api-recommendation.db'}"
        self.client = TestClient(
            create_app(database_url=url, admin_token="admin-secret", create_schema=True)
        )
        self.admin = {"Authorization": "Bearer admin-secret"}
        self.now = utcnow()

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def test_admin_auth_strict_schema_and_unknown_node_are_enforced(self):
        endpoint = "/v1/admin/deployment-recommendations"
        node_id = str(uuid.uuid4())
        self.assertEqual(
            self.client.post(endpoint, json={"node_ids": [node_id]}).status_code,
            401,
        )
        invalid_bodies = [
            {},
            {"node_ids": [], "all_online": False},
            {"node_ids": [node_id], "all_online": True},
            {"node_ids": [node_id, node_id]},
            {"node_ids": ["not-a-uuid"]},
            {"all_online": "true"},
            {"all_online": True, "objective": "unsupported"},
        ]
        for body in invalid_bodies:
            with self.subTest(body=body):
                self.assertEqual(
                    self.client.post(endpoint, headers=self.admin, json=body).status_code,
                    422,
                )
        for field, value in (
            ("refresh", True),
            ("command", "id"),
            ("docker_args", ["--privileged"]),
            ("env", {"TOKEN": "secret"}),
            ("mounts", ["/etc:/host"]),
            ("allow_unverified_network", True),
        ):
            with self.subTest(field=field):
                response = self.client.post(
                    endpoint,
                    headers=self.admin,
                    json={"all_online": True, field: value},
                )
                self.assertEqual(response.status_code, 422)
        self.assertEqual(
            self.client.post(
                endpoint,
                headers=self.admin,
                json={"node_ids": [node_id]},
            ).status_code,
            404,
        )

    def test_api_returns_recommendation_without_deployment_or_task_mutation(self):
        with self.client.app.state.session_factory() as session:
            node = _add_node(session, "api-node", now=self.now)
            release, _ = _add_release(session, "api-release")
            before = _row_counts(session)

        response = self.client.post(
            "/v1/admin/deployment-recommendations",
            headers=self.admin,
            json={"node_ids": [node.id], "objective": "quality-first"},
        )

        self.assertEqual(response.status_code, 200)
        recommendation = response.json()["recommendation"]
        self.assertEqual(recommendation["selected"]["model_release_id"], release.id)
        self.assertIn("policy_version", recommendation)
        self.assertIn("catalog_version", recommendation)
        self.assertIn("inventory_fingerprint", recommendation)
        with self.client.app.state.session_factory() as session:
            self.assertEqual(_row_counts(session), before)
            self.assertEqual(session.scalar(select(func.count()).select_from(Deployment)), 0)
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)


if __name__ == "__main__":
    unittest.main()
