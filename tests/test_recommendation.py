from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta
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
    BenchmarkRun,
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
    aware,
    canonical_artifact_manifest_digest,
    create_model_artifact,
    create_model_release,
    create_runtime_release,
    register_artifact_manifest,
    transition_model_release,
)
from dure.control.stage_artifacts import (
    register_stage_artifact_evidence,
    register_stage_artifact_variant,
    stage_artifact_variant_dict,
    transition_stage_artifact_variant,
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
    BenchmarkRun,
    Deployment,
    Task,
    AuditEvent,
)

PIPELINE_OVERRIDES = {
    "profile_id": "pipeline-3x24g",
    "topology": "pipeline",
    "node_count": 3,
    "pipeline_parallel_size": 3,
    "requires_network_evidence": True,
    "requires_nccl": True,
    "min_bandwidth_mbps": 10000,
    "max_rtt_ms": 5.0,
    "max_packet_loss_pct": 0.1,
}


def _hex(seed: str, length: int) -> str:
    value = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return (value * ((length // len(value)) + 1))[:length]


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _source_manifest(*, weight_size_bytes: int = 8) -> dict:
    return {
        "schema_version": 1,
        "files": [
            {
                "path": "model.safetensors",
                "kind": "REGULAR",
                "size_bytes": weight_size_bytes,
                "sha256": _digest("a"),
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": weight_size_bytes,
                        "sha256": _digest("a"),
                    }
                ],
            },
            {
                "path": "config.json",
                "kind": "REGULAR",
                "size_bytes": 2,
                "sha256": _digest("b"),
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": 2,
                        "sha256": _digest("b"),
                    }
                ],
            },
        ],
    }


def _stage_manifest(rank: int, *, weight_size_bytes: int = 4) -> dict:
    weight_character = "0123456789abcdef"[rank]
    files = [
        ("model-rank-0-part-0.safetensors", weight_size_bytes, weight_character),
        ("config.json", 2, "b"),
        ("tokenizer.json", 3, "c"),
        ("tokenizer_config.json", 4, "d"),
        ("dure-stage.json", 5, "e"),
    ]
    return {
        "schema_version": 1,
        "files": [
            {
                "path": path,
                "kind": "REGULAR",
                "size_bytes": size_bytes,
                "sha256": _digest(character),
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": size_bytes,
                        "sha256": _digest(character),
                    }
                ],
            }
            for path, size_bytes, character in files
        ],
    }


def _add_node(
    session,
    name: str,
    *,
    now,
    agent_version: str = "0.3.20",
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
        agent_version=agent_version,
        approved=approved,
        last_seen=now - timedelta(seconds=last_seen_age),
    )
    session.add(node)
    session.flush()
    if stored_profile is not None:
        address_seed = hashlib.sha256(name.encode("utf-8")).digest()
        address = (
            f"10.{address_seed[0]}.{address_seed[1]}."
            f"{(address_seed[2] % 254) + 1}"
        )
        value = profile(
            "agent-reported-id",
            address=address,
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
    evidence_nodes: list[Node] | None = None,
    evidence_created_at: datetime | None = None,
    source_manifest: dict | None = None,
):
    manifest_digest = (
        canonical_artifact_manifest_digest(source_manifest)
        if source_manifest is not None
        else "sha256:" + _hex(f"manifest-{key}", 64)
    )
    artifact = create_model_artifact(
        session,
        model_id=model_id,
        repository=f"TestOrg/{key}-AWQ",
        revision=_hex(f"revision-{key}", 40),
        manifest_digest=manifest_digest,
        quantization="awq",
        size_mib=8192,
        default_max_model_len=8192,
        layer_count=32,
        license_id="apache-2.0",
    )
    if source_manifest is not None:
        register_artifact_manifest(
            session,
            artifact_id=artifact.id,
            manifest=source_manifest,
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
        if not evidence_nodes:
            raise ValueError("ACTIVE recommendation fixtures require evidence_nodes")
        node_ids = [node.id for node in evidence_nodes]
        requires_network = (
            placement.requires_network_evidence or placement.node_count > 1
        )
        evidence = register_benchmark_evidence(
            session,
            release_id=release.id,
            placement_id=placement.id,
            suite_id=BENCHMARK_SUITE_ID,
            node_ids=node_ids,
            inventory_fingerprint=benchmark_inventory_fingerprint(session, node_ids),
            artifact_revision=artifact.revision,
            artifact_manifest_digest=artifact.manifest_digest,
            runtime_image=runtime.image,
            dure_commit="d" * 40,
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
            network_bandwidth_mbps=(20000.0 if requires_network else None),
            network_rtt_ms=(1.0 if requires_network else None),
            packet_loss_pct=(0.0 if requires_network else None),
            nccl_all_reduce_ok=(True if placement.requires_nccl else None),
        )
        evidence.created_at = evidence_created_at or max(
            node.last_seen for node in evidence_nodes
        )
        session.commit()
        promoted, _, changed = promote_model_release(session, release.id)
        if not changed or promoted.status != "ACTIVE":
            raise AssertionError("fixture release was not promoted through benchmark evidence")
    if status == "DEPRECATED":
        transition_model_release(session, release.id, "DEPRECATED")
    if status == "REVOKED":
        transition_model_release(session, release.id, "REVOKED")
    return release, placement


def _register_validated_stage_variant(
    session,
    *,
    release: ModelRelease,
    placement: PlacementProfileRecord,
    exporter_character: str = "6",
    weight_size_bytes: int = 4,
) -> dict:
    artifact = session.get(ModelArtifact, release.artifact_id)
    runtime = session.get(RuntimeRelease, release.runtime_id)
    assert artifact is not None
    assert runtime is not None
    stages = []
    for rank in range(placement.pipeline_parallel_size):
        manifest = _stage_manifest(rank, weight_size_bytes=weight_size_bytes + rank)
        stages.append(
            {
                "pipeline_rank": rank,
                "tensor_rank": 0,
                "manifest_digest": canonical_artifact_manifest_digest(manifest),
                "tensor_key_count": 10 + rank,
                "tensor_keys_digest": _digest("0123456789abcdef"[rank + 8]),
                "weight_size_bytes": weight_size_bytes + rank,
                "manifest": manifest,
            }
        )
    variant, _ = register_stage_artifact_variant(
        session,
        source_manifest_digest=artifact.manifest_digest,
        runtime_image=runtime.image,
        vllm_version=runtime.vllm_version,
        exporter_build_digest=_digest(exporter_character),
        architecture="Qwen2ForCausalLM",
        quantization=artifact.quantization,
        tensor_parallel_size=placement.tensor_parallel_size,
        pipeline_parallel_size=placement.pipeline_parallel_size,
        loader_format="VLLM_SHARDED_STATE_V1",
        stages=stages,
    )
    register_stage_artifact_evidence(
        session,
        variant.artifact_set_digest,
        schema_version=1,
        variant_identity_digest=variant.artifact_set_digest,
        validation_run_id=str(uuid.uuid4()),
        kind="GPU_EXPORT_LOAD",
        status="PASSED",
        validator_version="validator-1",
        validator_build_digest=_digest("f"),
        failure_code=None,
        ranks=[
            {
                "pipeline_rank": item["pipeline_rank"],
                "tensor_rank": item["tensor_rank"],
                "manifest_digest": item["manifest_digest"],
                "tensor_keys_digest": item["tensor_keys_digest"],
                "loaded_tensor_count": item["tensor_key_count"],
                "loaded_weight_size_bytes": item["weight_size_bytes"],
            }
            for item in stages
        ],
    )
    transition_stage_artifact_variant(
        session, variant.artifact_set_digest, "VALIDATED"
    )
    return stage_artifact_variant_dict(session, variant)


def _register_network_evidence(
    session,
    *,
    release: ModelRelease,
    placement: PlacementProfileRecord,
    nodes: list[Node],
    created_at,
    bandwidth_mbps: float = 20000.0,
):
    artifact = session.get(ModelArtifact, release.artifact_id)
    runtime = session.get(RuntimeRelease, release.runtime_id)
    assert artifact is not None
    assert runtime is not None
    node_ids = [node.id for node in nodes]
    evidence = register_benchmark_evidence(
        session,
        release_id=release.id,
        placement_id=placement.id,
        suite_id=BENCHMARK_SUITE_ID,
        node_ids=node_ids,
        inventory_fingerprint=benchmark_inventory_fingerprint(session, node_ids),
        artifact_revision=artifact.revision,
        artifact_manifest_digest=artifact.manifest_digest,
        runtime_image=runtime.image,
        dure_commit="e" * 40,
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
        network_bandwidth_mbps=bandwidth_mbps,
        network_rtt_ms=1.0,
        packet_loss_pct=0.0,
        nccl_all_reduce_ok=True,
    )
    evidence.created_at = created_at
    session.commit()
    return evidence


def _add_unresolved_benchmark_run(
    session,
    *,
    release: ModelRelease,
    placement: PlacementProfileRecord,
    nodes: list[Node],
    status: str,
    updated_at,
) -> BenchmarkRun:
    artifact = session.get(ModelArtifact, release.artifact_id)
    runtime = session.get(RuntimeRelease, release.runtime_id)
    assert artifact is not None
    assert runtime is not None
    node_ids = sorted(node.id for node in nodes)
    run = BenchmarkRun(
        request_id=str(uuid.uuid4()),
        request_digest="sha256:" + _hex(str(uuid.uuid4()), 64),
        release_id=release.id,
        placement_id=placement.id,
        coordinator_node_id=node_ids[0],
        node_ids=node_ids,
        inventory_fingerprint=benchmark_inventory_fingerprint(session, node_ids),
        suite_id=BENCHMARK_SUITE_ID,
        policy_version=BENCHMARK_POLICY_VERSION,
        workload_id="long-chat-4k-256",
        dure_commit="f" * 40,
        model_id=artifact.model_id,
        repository=artifact.repository,
        artifact_revision=artifact.revision,
        artifact_manifest_digest=artifact.manifest_digest,
        quantization=artifact.quantization,
        runtime_image=runtime.image,
        input_tokens=4096,
        output_tokens=256,
        concurrency=4,
        warmup_requests=20,
        request_count=200,
        duration_seconds=900.0,
        status=status,
        failure_code=("BENCHMARK_EXECUTION_FAILED" if status == "FAILED" else None),
        created_at=updated_at,
        updated_at=updated_at,
    )
    session.add(run)
    session.commit()
    return run


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
            lower, _ = _add_release(
                session, "lower", quality_rank=10, evidence_nodes=[first]
            )
            higher, _ = _add_release(
                session, "higher", quality_rank=20, evidence_nodes=[first]
            )
            _add_release(session, "draft", status="DRAFT", quality_rank=100)
            _add_release(
                session,
                "old",
                status="DEPRECATED",
                quality_rank=200,
                evidence_nodes=[first],
            )
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
            for candidate in recommendation["candidates"]:
                self.assertEqual(candidate["model_cache_kind"], "FULL_SNAPSHOT")
                self.assertEqual(
                    candidate["full_snapshot_total_size_bytes"],
                    8192 * 1024 * 1024,
                )
                self.assertEqual(
                    candidate["full_snapshot_required_cache_bytes"],
                    (8192 * 2 + 64) * 1024 * 1024,
                )
                self.assertEqual(
                    candidate["full_snapshot_size_source"],
                    "MODEL_ARTIFACT_DECLARED_SIZE",
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

    def test_single_full_snapshot_disk_gate_uses_frozen_declared_size(self):
        with self.factory() as session:
            node = _add_node(session, "single-disk", now=self.now)
            record = session.get(NodeProfileRecord, node.id)
            record.profile = {**record.profile, "disk_free_mib": 16447}
            record.updated_at = self.now
            session.commit()
            _add_release(
                session,
                "single-disk",
                quality_rank=30,
                evidence_nodes=[node],
            )

            result = recommend_deployment(
                session,
                node_ids=[node.id],
                all_online=False,
                now=self.now,
            )["recommendation"]

            self.assertIsNone(result["selected"])
            self.assertEqual(len(result["candidates"]), 1)
            candidate = result["candidates"][0]
            self.assertEqual(candidate["model_cache_kind"], "FULL_SNAPSHOT")
            self.assertEqual(
                candidate["full_snapshot_total_size_bytes"],
                8192 * 1024 * 1024,
            )
            self.assertEqual(
                candidate["full_snapshot_required_cache_bytes"],
                16448 * 1024 * 1024,
            )
            self.assertIn(
                "DISK_SPACE",
                {item["code"] for item in candidate["rejections"]},
            )

    def test_single_full_snapshot_rejects_agent_before_preparation_minimum(self):
        with self.factory() as session:
            node = _add_node(
                session,
                "single-agent-old",
                now=self.now,
                agent_version="0.3.15",
            )
            _add_release(
                session,
                "single-agent-old",
                quality_rank=30,
                evidence_nodes=[node],
            )

            result = recommend_deployment(
                session,
                node_ids=[node.id],
                all_online=False,
                now=self.now,
            )["recommendation"]

            self.assertIsNone(result["selected"])
            self.assertEqual(len(result["candidates"]), 1)
            candidate = result["candidates"][0]
            self.assertFalse(candidate["feasible"])
            rejection = next(
                item
                for item in candidate["rejections"]
                if item["code"] == "FULL_SNAPSHOT_AGENT_VERSION"
            )
            self.assertEqual(rejection["node_ids"], [node.id])

    def test_single_full_snapshot_accepts_preparation_minimum_agent(self):
        with self.factory() as session:
            node = _add_node(
                session,
                "single-agent-minimum",
                now=self.now,
                agent_version="0.3.16",
            )
            release, _ = _add_release(
                session,
                "single-agent-minimum",
                quality_rank=30,
                evidence_nodes=[node],
            )

            result = recommend_deployment(
                session,
                node_ids=[node.id],
                all_online=False,
                now=self.now,
            )["recommendation"]

            self.assertIsNotNone(result["selected"], result["candidates"])
            self.assertEqual(result["selected"]["model_release_id"], release.id)
            self.assertNotIn(
                "FULL_SNAPSHOT_AGENT_VERSION",
                {
                    item["code"]
                    for item in result["candidates"][0]["rejections"]
                },
            )

    def test_pending_offline_and_stale_profiles_have_distinct_rejections(self):
        with self.factory() as session:
            pending = _add_node(session, "pending", now=self.now, approved=False)
            offline = _add_node(session, "offline", now=self.now, last_seen_age=40)
            stale = _add_node(session, "stale", now=self.now, profile_age=91)
            promoter = _add_node(session, "promoter", now=self.now)
            _add_release(session, "eligible", evidence_nodes=[promoter])

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

    def test_multinode_candidate_uses_only_the_evidenced_exact_node_set(self):
        with self.factory() as session:
            nodes = [_add_node(session, f"node-{index}", now=self.now) for index in range(3)]
            outsider = _add_node(session, "outsider", now=self.now)
            release, _ = _add_release(
                session,
                "pipeline",
                quality_rank=72,
                evidence_nodes=nodes,
                placement_overrides=PIPELINE_OVERRIDES,
            )

            self.assertEqual(release.status, "ACTIVE")
            self.assertEqual(len(release.promotion_evidence_ids), 1)
            evidence = session.get(
                BenchmarkEvidence,
                release.promotion_evidence_ids[0],
            )
            self.assertIsNotNone(evidence)
            assert evidence is not None
            self.assertEqual(evidence.status, "PASSED")
            self.assertTrue(evidence.nccl_all_reduce_ok)

            result = recommend_deployment(
                session,
                node_ids=[outsider.id, *(item.id for item in reversed(nodes))],
                all_online=False,
                now=self.now,
            )["recommendation"]

            selected = result["selected"]
            self.assertIsNotNone(selected, result["candidates"])
            assert selected is not None
            self.assertEqual(selected["model_release_id"], release.id)
            self.assertEqual(selected["node_ids"], sorted(item.id for item in nodes))
            self.assertNotIn(outsider.id, selected["node_ids"])
            self.assertEqual(selected["network_evidence_id"], evidence.id)
            self.assertEqual(
                selected["network_evidence_digest"], evidence.evidence_digest
            )
            self.assertEqual(
                selected["network_evidence_registered_at"],
                aware(evidence.created_at).isoformat(),
            )
            self.assertEqual(result["policy_version"], "central-quality-within-slo-v4")

    def test_validated_stage_is_selected_with_exact_rank_binding_before_full_disk_gate(self):
        with self.factory() as session:
            nodes = [
                _add_node(session, f"stage-node-{index}", now=self.now)
                for index in range(3)
            ]
            for node in nodes:
                record = session.get(NodeProfileRecord, node.id)
                record.profile = {**record.profile, "disk_free_mib": 100}
                record.updated_at = self.now
            session.commit()
            release, placement = _add_release(
                session,
                "stage-delivery",
                quality_rank=72,
                evidence_nodes=nodes,
                placement_overrides={
                    **PIPELINE_OVERRIDES,
                    "min_disk_free_mib": 64,
                },
                source_manifest=_source_manifest(
                    weight_size_bytes=8 * 1024 * 1024 * 1024
                ),
            )
            variant = _register_validated_stage_variant(
                session, release=release, placement=placement
            )

            result = recommend_deployment(
                session,
                node_ids=[item.id for item in reversed(nodes)],
                all_online=False,
                now=self.now,
            )["recommendation"]

            selected = result["selected"]
            self.assertIsNotNone(selected)
            assert selected is not None
            self.assertEqual(selected["model_cache_kind"], "STAGE")
            self.assertEqual(
                selected["stage_artifact"]["artifact_set_digest"],
                variant["artifact_set_digest"],
            )
            self.assertEqual(
                selected["stage_artifact"]["loader_format"],
                "VLLM_SHARDED_STATE_V1",
            )
            self.assertEqual(
                selected["stage_validation_evidence"]["status"], "PASSED"
            )
            self.assertEqual(
                [item["node_id"] for item in selected["stage_node_bindings"]],
                selected["rank_node_ids"],
            )
            self.assertEqual(
                [item["rank"] for item in selected["stage_node_bindings"]],
                [0, 1, 2],
            )
            self.assertTrue(
                all(
                    item["required_cache_bytes"]
                    == item["total_size_bytes"] * 2 + 64 * 1024 * 1024
                    for item in selected["stage_node_bindings"]
                )
            )
            immutable_core = dict(result)
            recommendation_id = immutable_core.pop("id")
            self.assertEqual(
                recommendation_id,
                "sha256:"
                + hashlib.sha256(
                    json.dumps(
                        immutable_core,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode("utf-8")
                ).hexdigest(),
            )
            full = next(
                item
                for item in result["candidates"]
                if item["model_cache_kind"] == "FULL_SNAPSHOT"
            )
            self.assertFalse(full["feasible"])
            self.assertIn(
                "DISK_SPACE", {item["code"] for item in full["rejections"]}
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

    def test_stage_variant_digest_breaks_ties_and_both_delivery_failures_are_explained(self):
        with self.factory() as session:
            nodes = [
                _add_node(session, f"stage-tie-{index}", now=self.now)
                for index in range(3)
            ]
            for node in nodes:
                record = session.get(NodeProfileRecord, node.id)
                record.profile = {**record.profile, "disk_free_mib": 100}
                record.updated_at = self.now
            session.commit()
            release, placement = _add_release(
                session,
                "stage-tie",
                quality_rank=72,
                evidence_nodes=nodes,
                placement_overrides={
                    **PIPELINE_OVERRIDES,
                    "min_disk_free_mib": 64,
                },
                source_manifest=_source_manifest(
                    weight_size_bytes=8 * 1024 * 1024 * 1024
                ),
            )
            first = _register_validated_stage_variant(
                session,
                release=release,
                placement=placement,
                exporter_character="6",
                weight_size_bytes=100 * 1024 * 1024,
            )
            second = _register_validated_stage_variant(
                session,
                release=release,
                placement=placement,
                exporter_character="7",
                weight_size_bytes=100 * 1024 * 1024,
            )

            result = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now,
            )["recommendation"]

            self.assertIsNone(result["selected"])
            stages = [
                item
                for item in result["candidates"]
                if item.get("model_cache_kind") == "STAGE"
            ]
            self.assertEqual(
                [item["stage_artifact"]["artifact_set_digest"] for item in stages],
                sorted(
                    [first["artifact_set_digest"], second["artifact_set_digest"]]
                ),
            )
            self.assertTrue(
                all(
                    "STAGE_DISK_SPACE"
                    in {rejection["code"] for rejection in item["rejections"]}
                    for item in stages
                ),
                stages,
            )
            full = next(
                item
                for item in result["candidates"]
                if item.get("model_cache_kind") == "FULL_SNAPSHOT"
            )
            self.assertIn(
                "DISK_SPACE", {item["code"] for item in full["rejections"]}
            )

    def test_multinode_evidence_for_a_different_node_set_is_rejected(self):
        with self.factory() as session:
            nodes = [_add_node(session, f"exact-{index}", now=self.now) for index in range(3)]
            replacement = _add_node(session, "replacement", now=self.now)
            _add_release(
                session,
                "different-set",
                evidence_nodes=nodes,
                placement_overrides=PIPELINE_OVERRIDES,
            )

            result = recommend_deployment(
                session,
                node_ids=[nodes[0].id, nodes[1].id, replacement.id],
                all_online=False,
                now=self.now,
            )["recommendation"]

            self.assertIsNone(result["selected"])
            codes = {item["code"] for item in result["candidates"][0]["rejections"]}
            self.assertIn("NETWORK_EVIDENCE", codes)

    def test_stale_and_future_multinode_evidence_fail_closed(self):
        with self.factory() as session:
            nodes = [_add_node(session, f"age-{index}", now=self.now) for index in range(3)]
            release, _ = _add_release(
                session,
                "evidence-age",
                evidence_nodes=nodes,
                placement_overrides=PIPELINE_OVERRIDES,
            )
            evidence = session.get(BenchmarkEvidence, release.promotion_evidence_ids[0])
            assert evidence is not None
            evidence.created_at = self.now - timedelta(hours=24)
            session.commit()
            boundary = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now,
            )["recommendation"]
            self.assertIsNotNone(boundary["selected"])

            evidence.created_at = self.now - timedelta(hours=24, microseconds=1)
            session.commit()

            stale = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now,
            )["recommendation"]
            self.assertIsNone(stale["selected"])
            self.assertIn(
                "NETWORK_EVIDENCE",
                {item["code"] for item in stale["candidates"][0]["rejections"]},
            )

            evidence.created_at = self.now + timedelta(microseconds=1)
            session.commit()
            future = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now,
            )["recommendation"]
            self.assertIsNone(future["selected"])
            self.assertIn(
                "NETWORK_EVIDENCE",
                {item["code"] for item in future["candidates"][0]["rejections"]},
            )

    def test_changed_profile_invalidates_exact_multinode_evidence(self):
        with self.factory() as session:
            nodes = [_add_node(session, f"changed-{index}", now=self.now) for index in range(3)]
            _add_release(
                session,
                "changed-profile",
                evidence_nodes=nodes,
                placement_overrides=PIPELINE_OVERRIDES,
            )
            record = session.get(NodeProfileRecord, nodes[0].id)
            assert record is not None
            changed = dict(record.profile)
            changed["cpu_count"] += 1
            record.profile = changed
            record.updated_at = self.now
            session.commit()

            result = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now,
            )["recommendation"]

            self.assertIsNone(result["selected"])
            self.assertIn(
                "NETWORK_EVIDENCE",
                {item["code"] for item in result["candidates"][0]["rejections"]},
            )

    def test_latest_failed_evidence_blocks_an_older_pass_for_the_same_nodes(self):
        with self.factory() as session:
            nodes = [_add_node(session, f"failed-{index}", now=self.now) for index in range(3)]
            release, placement = _add_release(
                session,
                "failed-latest",
                evidence_nodes=nodes,
                placement_overrides=PIPELINE_OVERRIDES,
            )
            failed = _register_network_evidence(
                session,
                release=release,
                placement=placement,
                nodes=nodes,
                bandwidth_mbps=9999.0,
                created_at=self.now + timedelta(seconds=10),
            )
            self.assertEqual(failed.status, "FAILED")

            result = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now + timedelta(seconds=20),
            )["recommendation"]

            self.assertIsNone(result["selected"])
            self.assertIn(
                "NETWORK_EVIDENCE",
                {item["code"] for item in result["candidates"][0]["rejections"]},
            )

    def test_newer_failed_and_queued_runs_block_without_task_mutation(self):
        with self.factory() as session:
            nodes = [_add_node(session, f"queued-{index}", now=self.now) for index in range(3)]
            release, placement = _add_release(
                session,
                "queued-latest",
                evidence_nodes=nodes,
                placement_overrides=PIPELINE_OVERRIDES,
            )
            failed_run = _add_unresolved_benchmark_run(
                session,
                release=release,
                placement=placement,
                nodes=nodes,
                status="FAILED",
                updated_at=self.now + timedelta(seconds=5),
            )
            before = _row_counts(session)

            failed_result = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now + timedelta(seconds=10),
            )["recommendation"]

            self.assertIsNone(failed_result["selected"])
            self.assertIn(
                "NETWORK_EVIDENCE",
                {
                    item["code"]
                    for item in failed_result["candidates"][0]["rejections"]
                },
            )
            self.assertEqual(_row_counts(session), before)
            self.assertEqual(session.get(BenchmarkRun, failed_run.id).status, "FAILED")

            failed_run.status = "SUCCEEDED"
            failed_run.failure_code = None
            failed_run.updated_at = self.now + timedelta(seconds=11)
            session.commit()
            queued_run = _add_unresolved_benchmark_run(
                session,
                release=release,
                placement=placement,
                nodes=nodes,
                status="QUEUED",
                updated_at=self.now + timedelta(seconds=15),
            )
            before = _row_counts(session)
            queued_result = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now + timedelta(seconds=20),
            )["recommendation"]
            self.assertIsNone(queued_result["selected"])
            self.assertIn(
                "NETWORK_EVIDENCE",
                {
                    item["code"]
                    for item in queued_result["candidates"][0]["rejections"]
                },
            )
            self.assertEqual(_row_counts(session), before)
            self.assertEqual(session.get(BenchmarkRun, queued_run.id).status, "QUEUED")
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)
            self.assertEqual(session.scalar(select(func.count()).select_from(Deployment)), 0)

    def test_newer_prepared_run_blocks_an_older_pass(self):
        with self.factory() as session:
            nodes = [
                _add_node(session, f"prepared-{index}", now=self.now)
                for index in range(3)
            ]
            release, placement = _add_release(
                session,
                "prepared-latest",
                evidence_nodes=nodes,
                placement_overrides=PIPELINE_OVERRIDES,
            )
            prepared = _add_unresolved_benchmark_run(
                session,
                release=release,
                placement=placement,
                nodes=nodes,
                status="PREPARED",
                updated_at=self.now + timedelta(seconds=5),
            )
            before = _row_counts(session)

            result = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now + timedelta(seconds=10),
            )["recommendation"]

            self.assertIsNone(result["selected"])
            self.assertIn(
                "NETWORK_EVIDENCE",
                {item["code"] for item in result["candidates"][0]["rejections"]},
            )
            self.assertEqual(_row_counts(session), before)
            self.assertEqual(session.get(BenchmarkRun, prepared.id).status, "PREPARED")

    def test_network_nccl_and_runtime_identity_are_rechecked(self):
        with self.factory() as session:
            nodes = [
                _add_node(session, f"recheck-{index}", now=self.now)
                for index in range(3)
            ]
            release, placement = _add_release(
                session,
                "recheck-gates",
                evidence_nodes=nodes,
                placement_overrides=PIPELINE_OVERRIDES,
            )
            evidence = session.get(
                BenchmarkEvidence,
                release.promotion_evidence_ids[0],
            )
            assert evidence is not None

            for label, mutate, restore in (
                (
                    "bandwidth",
                    lambda: setattr(placement, "min_bandwidth_mbps", 30000),
                    lambda: setattr(placement, "min_bandwidth_mbps", 10000),
                ),
                (
                    "nccl",
                    lambda: setattr(evidence, "nccl_all_reduce_ok", False),
                    lambda: setattr(evidence, "nccl_all_reduce_ok", True),
                ),
                (
                    "runtime",
                    lambda: setattr(
                        evidence,
                        "runtime_image",
                        f"registry.example/mismatch@sha256:{'0' * 64}",
                    ),
                    lambda: setattr(
                        evidence,
                        "runtime_image",
                        session.get(RuntimeRelease, release.runtime_id).image,
                    ),
                ),
            ):
                with self.subTest(label=label):
                    mutate()
                    session.commit()
                    result = recommend_deployment(
                        session,
                        node_ids=[item.id for item in nodes],
                        all_online=False,
                        now=self.now,
                    )["recommendation"]
                    self.assertIsNone(result["selected"])
                    self.assertIn(
                        "NETWORK_EVIDENCE",
                        {
                            item["code"]
                            for item in result["candidates"][0]["rejections"]
                        },
                    )
                    restore()
                    session.commit()

            restored = recommend_deployment(
                session,
                node_ids=[item.id for item in nodes],
                all_online=False,
                now=self.now,
            )["recommendation"]
            self.assertIsNotNone(restored["selected"])

    def test_missing_invalid_profiles_and_runtime_architecture_fail_closed(self):
        with self.factory() as session:
            missing = _add_node(session, "missing", now=self.now, stored_profile=None)
            invalid = _add_node(session, "invalid", now=self.now, stored_profile={"bad": True})
            promoter = _add_node(session, "profile-promoter", now=self.now)
            _add_release(session, "profiles", evidence_nodes=[promoter])

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
            hopper_profile = profile("hopper-promoter").to_dict()
            hopper_profile["gpus"][0]["compute_capability"] = "9.0"
            hopper = _add_node(
                session,
                "hopper-promoter",
                now=self.now,
                stored_profile=hopper_profile,
            )
            hopper_release, _ = _add_release(
                session,
                "hopper-only",
                quality_rank=20,
                gpu_architectures=["hopper"],
                evidence_nodes=[hopper],
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
        unknown = self.client.post(
            endpoint,
            headers=self.admin,
            json={"node_ids": [node_id]},
        )
        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(unknown.json()["detail"], f"unknown node(s): {node_id}")

    def test_api_returns_recommendation_without_deployment_or_task_mutation(self):
        with self.client.app.state.session_factory() as session:
            node = _add_node(session, "api-node", now=self.now)
            release, _ = _add_release(
                session, "api-release", evidence_nodes=[node]
            )
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

    def test_api_binds_exact_multinode_evidence_without_hybrid_assignment(self):
        with self.client.app.state.session_factory() as session:
            nodes = [
                _add_node(session, f"api-exact-{index}", now=self.now)
                for index in range(3)
            ]
            outsider = _add_node(session, "api-outsider", now=self.now)
            release, _ = _add_release(
                session,
                "api-pipeline",
                evidence_nodes=nodes,
                placement_overrides=PIPELINE_OVERRIDES,
            )
            evidence = session.get(
                BenchmarkEvidence,
                release.promotion_evidence_ids[0],
            )
            assert evidence is not None
            expected_nodes = sorted(node.id for node in nodes)
            requested_nodes = [outsider.id, *reversed(expected_nodes)]
            evidence_id = evidence.id
            evidence_digest = evidence.evidence_digest
            before = _row_counts(session)

        response = self.client.post(
            "/v1/admin/deployment-recommendations",
            headers=self.admin,
            json={"node_ids": requested_nodes, "objective": "quality-first"},
        )

        self.assertEqual(response.status_code, 200)
        selected = response.json()["recommendation"]["selected"]
        self.assertEqual(selected["node_ids"], expected_nodes)
        self.assertNotIn(outsider.id, selected["node_ids"])
        self.assertEqual(selected["network_evidence_id"], evidence_id)
        self.assertEqual(selected["network_evidence_digest"], evidence_digest)
        with self.client.app.state.session_factory() as session:
            self.assertEqual(_row_counts(session), before)
            self.assertEqual(session.scalar(select(func.count()).select_from(Deployment)), 0)
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)


if __name__ == "__main__":
    unittest.main()
