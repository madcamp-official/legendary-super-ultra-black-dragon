from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.artifact_prepare import validate_digest_pinned_runtime_image
from dure.catalog import (
    CatalogEntry,
    ModelCatalog,
    NetworkEvidenceBinding,
    PlacementProfile,
    StageArtifactDelivery,
    StageRankDelivery,
)
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
)
from dure.models import (
    VLLM_RAY_PP_BACKEND,
    VLLM_RAY_PP_RUNTIME_VERSION,
    DeploymentPlan,
    ModelSpec,
    NodeAssignment,
    NodeProfile,
    StageArtifactBinding,
)
from dure.planner import StrictRayPPTopologyError, strict_vllm_ray_pp_order
from dure.selector import InventoryNode, recommend_model
from dure.task import (
    TaskStatus,
    TaskType,
    benchmark_inventory_fingerprint,
)

from .benchmark import BENCHMARK_POLICY_VERSION, BENCHMARK_SUITE_ID
from .qualification import (
    ProfileQualificationError,
    validate_profile_qualification_evidence,
)

from .models import (
    AuditEvent,
    ArtifactChunk,
    ArtifactFileChunk,
    ArtifactManifest,
    ArtifactManifestFile,
    BenchmarkEvidence,
    BenchmarkRun,
    Deployment,
    DeploymentOperation,
    DeploymentRecommendationRecord,
    ModelArtifact,
    ModelRelease,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    ProfileQualificationEvidence,
    ProfileQualificationRun,
    RuntimeRelease,
    StageArtifactRank,
    StageArtifactValidationEvidence,
    StageArtifactValidationRank,
    StageArtifactVariant,
    Task,
    utcnow,
)
from .service import artifact_manifest_dict, aware, node_status
from .stage_artifacts import (
    StageArtifactConflictError,
    StageArtifactNotFoundError,
    stage_artifact_evidence_dict,
    validated_stage_artifact_projection,
)


POLICY_VERSION = "central-quality-within-slo-v4"
PROFILE_MAX_AGE = timedelta(seconds=90)
NETWORK_EVIDENCE_MAX_AGE = timedelta(hours=24)
GENERATION_NAMESPACE = uuid.UUID("74ebf646-2d77-4fcf-8524-1777a274eb93")
_MIB = 1024 * 1024
_CACHE_RESERVE_BYTES = 64 * _MIB
_FULL_SNAPSHOT_AGENT_VERSION = (0, 3, 16)
_AGENT_VERSION = re.compile(r"(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?")


def _agent_supports(value: str | None, minimum: tuple[int, int, int]) -> bool:
    if not isinstance(value, str):
        return False
    matched = _AGENT_VERSION.fullmatch(value)
    return bool(
        matched
        and tuple(int(part) for part in matched.groups()) >= minimum
    )


PRIVATE_IPV4_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)


class RecommendationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}

    def to_detail(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), **self.details}


class RecommendationNodeNotFoundError(RecommendationError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, code="RECOMMENDATION_NODE_NOT_FOUND", details=details)


class RecommendationNotFoundError(RecommendationError):
    def __init__(
        self,
        message: str = "deployment recommendation not found",
        *,
        code: str = "RECOMMENDATION_NOT_FOUND",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code=code, details=details)


class RecommendationStaleError(RecommendationError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, code="RECOMMENDATION_STALE", details=details)


class RecommendationNotAcceptableError(RecommendationError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "RECOMMENDATION_NOT_ACCEPTABLE",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code=code, details=details)


class RecommendationGenerationConflictError(RecommendationError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "RECOMMENDATION_GENERATION_CONFLICT",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code=code, details=details)


def _content_digest(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        normalized = [_canonical_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(",", ":")
            ),
        )
    return value


def canonical_inventory_snapshot(nodes: list[InventoryNode]) -> list[dict[str, Any]]:
    """Return the exact canonical payload bound by the selector fingerprint."""
    return [
        {
            "node_id": node.node_id,
            "approved": node.approved,
            "online": node.online,
            "profile_fresh": node.profile_fresh,
            "network_verified": node.network_verified,
            "profile_error": node.profile_error,
            "agent_version": node.agent_version,
            "profile": _canonical_value(node.profile.to_dict()) if node.profile else None,
        }
        for node in sorted(nodes, key=lambda item: item.node_id)
    ]


def _inventory_snapshot_fingerprint(snapshot: list[dict[str, Any]]) -> str:
    encoded = json.dumps(
        snapshot, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _exact_node_set(value: Any, *, node_count: int) -> tuple[str, ...] | None:
    if (
        not isinstance(value, list)
        or len(value) != node_count
        or any(not isinstance(item, str) for item in value)
        or len(value) != len(set(value))
    ):
        return None
    normalized = tuple(sorted(value))
    if list(normalized) != value:
        return None
    return normalized


def _network_measurements_qualify(
    evidence: BenchmarkEvidence,
    placement: PlacementProfileRecord,
) -> bool:
    bandwidth = evidence.network_bandwidth_mbps
    rtt = evidence.network_rtt_ms
    packet_loss = evidence.packet_loss_pct
    if bandwidth is None or rtt is None or packet_loss is None:
        return False
    if (
        placement.min_bandwidth_mbps is not None
        and bandwidth < placement.min_bandwidth_mbps
    ):
        return False
    if placement.max_rtt_ms is not None and rtt > placement.max_rtt_ms:
        return False
    if (
        placement.max_packet_loss_pct is not None
        and packet_loss > placement.max_packet_loss_pct
    ):
        return False
    return not placement.requires_nccl or evidence.nccl_all_reduce_ok is True


def _network_evidence_bindings(
    session: Session,
    *,
    release: ModelRelease,
    artifact: ModelArtifact,
    runtime: RuntimeRelease,
    placement: PlacementProfileRecord,
    inventory: list[InventoryNode],
    now: datetime,
) -> tuple[NetworkEvidenceBinding, ...]:
    requires_network = placement.requires_network_evidence or placement.node_count > 1
    requires_qualification = placement.origin == "AUTO"
    if not requires_network and not requires_qualification:
        return ()

    inventory_by_id = {node.node_id: node for node in inventory}
    if requires_qualification:
        rows = list(
            session.execute(
                select(
                    ProfileQualificationRun,
                    ProfileQualificationEvidence,
                )
                .outerjoin(
                    ProfileQualificationEvidence,
                    ProfileQualificationEvidence.run_id
                    == ProfileQualificationRun.id,
                )
                .where(
                    ProfileQualificationRun.placement_id == placement.id,
                    ProfileQualificationRun.status.in_(
                        ("QUALIFYING", "PASSED", "FAILED")
                    ),
                )
                .order_by(
                    ProfileQualificationRun.updated_at.desc(),
                    ProfileQualificationRun.id.desc(),
                )
            )
        )
        bindings: list[NetworkEvidenceBinding] = []
        seen_node_sets: set[tuple[str, ...]] = set()
        for qualification_run, qualification in rows:
            node_set = _exact_node_set(
                qualification_run.node_ids,
                node_count=placement.node_count,
            )
            if node_set is None or node_set in seen_node_sets:
                continue
            # The newest attempt for an exact node set is authoritative.  A
            # pending or failed requalification blocks reuse of an older pass.
            seen_node_sets.add(node_set)
            if (
                qualification_run.status != "PASSED"
                or qualification is None
                or qualification_run.evidence_id != qualification.id
            ):
                continue
            try:
                validate_profile_qualification_evidence(
                    session,
                    placement=placement,
                    evidence=qualification,
                    run=qualification_run,
                    now=now,
                    require_primary=False,
                )
            except ProfileQualificationError:
                continue
            registered_at = aware(qualification.created_at)
            age = now - registered_at
            if age < timedelta(0) or age > NETWORK_EVIDENCE_MAX_AGE:
                continue
            if any(
                node_id not in inventory_by_id
                or inventory_by_id[node_id].profile is None
                for node_id in node_set
            ):
                continue
            metrics = qualification.metrics
            network_ok = not requires_network or (
                type(metrics) is dict
                and type(metrics.get("network_bandwidth_mbps")) in {int, float}
                and (
                    placement.min_bandwidth_mbps is None
                    or metrics["network_bandwidth_mbps"]
                    >= placement.min_bandwidth_mbps
                )
                and type(metrics.get("network_rtt_ms")) in {int, float}
                and (
                    placement.max_rtt_ms is None
                    or metrics["network_rtt_ms"] <= placement.max_rtt_ms
                )
                and type(metrics.get("packet_loss_pct")) in {int, float}
                and (
                    placement.max_packet_loss_pct is None
                    or metrics["packet_loss_pct"]
                    <= placement.max_packet_loss_pct
                )
                and (
                    not placement.requires_nccl
                    or metrics.get("nccl_all_reduce_ok") is True
                )
            )
            if not network_ok:
                continue
            bindings.append(
                NetworkEvidenceBinding(
                    evidence_id=qualification.id,
                    evidence_digest=qualification.evidence_digest,
                    node_ids=node_set,
                    registered_at=registered_at.isoformat(),
                    rank_node_ids=tuple(qualification_run.rank_node_ids),
                )
            )
        # AUTO profiles never fall back to generic network verification or
        # benchmark evidence: deployment must use the exact qualified nodes.
        return tuple(
            sorted(
                bindings,
                key=lambda item: (
                    item.node_ids,
                    item.evidence_digest,
                    item.evidence_id,
                ),
            )
        )
    evidence_rows = list(
        session.scalars(
            select(BenchmarkEvidence)
            .where(
                BenchmarkEvidence.release_id == release.id,
                BenchmarkEvidence.placement_id == placement.id,
            )
            .order_by(
                BenchmarkEvidence.registration_sequence.desc(),
                BenchmarkEvidence.id.desc(),
            )
        )
    )
    unresolved_runs = list(
        session.scalars(
            select(BenchmarkRun)
            .where(
                BenchmarkRun.release_id == release.id,
                BenchmarkRun.placement_id == placement.id,
                BenchmarkRun.status.in_(("PREPARED", "QUEUED", "FAILED")),
            )
            .order_by(BenchmarkRun.updated_at.desc(), BenchmarkRun.id.desc())
        )
    )

    latest_run_by_nodes: dict[tuple[str, ...], BenchmarkRun] = {}
    for run in unresolved_runs:
        node_set = _exact_node_set(run.node_ids, node_count=placement.node_count)
        if node_set is not None and node_set not in latest_run_by_nodes:
            latest_run_by_nodes[node_set] = run

    bindings: list[NetworkEvidenceBinding] = []
    seen_node_sets: set[tuple[str, ...]] = set()
    for evidence in evidence_rows:
        node_set = _exact_node_set(
            evidence.node_ids,
            node_count=placement.node_count,
        )
        if node_set is None or node_set in seen_node_sets:
            continue
        # The first row is authoritative for this exact set even when it failed.
        seen_node_sets.add(node_set)
        if any(
            node_id not in inventory_by_id
            or inventory_by_id[node_id].profile is None
            for node_id in node_set
        ):
            continue
        registered_at = aware(evidence.created_at)
        age = now - registered_at
        if age < timedelta(0) or age > NETWORK_EVIDENCE_MAX_AGE:
            continue
        later_run = latest_run_by_nodes.get(node_set)
        if later_run is not None and aware(later_run.updated_at) > registered_at:
            continue
        if (
            evidence.status != "PASSED"
            or evidence.failure_codes
            or evidence.suite_id != BENCHMARK_SUITE_ID
            or evidence.policy_version != BENCHMARK_POLICY_VERSION
            or evidence.artifact_revision != artifact.revision
            or evidence.artifact_manifest_digest != artifact.manifest_digest
            or evidence.runtime_image != runtime.image
            or not _network_measurements_qualify(evidence, placement)
        ):
            continue
        profiles = [
            (node_id, inventory_by_id[node_id].profile)
            for node_id in node_set
        ]
        if evidence.inventory_fingerprint != benchmark_inventory_fingerprint(profiles):
            continue
        rank_node_ids: tuple[str, ...] = ()
        try:
            runtime_bindings = strict_vllm_ray_pp_order(
                [profile for _node_id, profile in profiles if profile is not None],
                head_node_id=node_set[0],
                minimum_gpu_memory_mib=placement.min_gpu_memory_mib,
            )
        except StrictRayPPTopologyError:
            # Keep the evidence visible so the strict topology evaluator can
            # return the precise denial code.  An empty rank binding can never
            # authorize a STAGE plan.
            pass
        else:
            rank_node_ids = tuple(item.profile.node_id for item in runtime_bindings)
        bindings.append(
            NetworkEvidenceBinding(
                evidence_id=evidence.id,
                evidence_digest=evidence.evidence_digest,
                node_ids=node_set,
                registered_at=registered_at.isoformat(),
                rank_node_ids=rank_node_ids,
            )
        )
    return tuple(
        sorted(
            bindings,
            key=lambda item: (item.node_ids, item.evidence_digest, item.evidence_id),
        )
    )


def _validated_stage_deliveries(
    session: Session,
    *,
    artifact: ModelArtifact,
    runtime: RuntimeRelease,
    placement: PlacementProfileRecord,
) -> list[tuple[StageArtifactDelivery, dict[str, Any]]]:
    """Project every exact, currently valid STAGE delivery deterministically."""

    if (
        placement.node_count not in {2, 3}
        or placement.tensor_parallel_size != 1
        or placement.pipeline_parallel_size != placement.node_count
    ):
        return []
    variants = list(
        session.scalars(
            select(StageArtifactVariant)
            .where(
                StageArtifactVariant.status == "VALIDATED",
                StageArtifactVariant.source_manifest_digest
                == artifact.manifest_digest,
                StageArtifactVariant.runtime_release_id == runtime.id,
                StageArtifactVariant.runtime_image == runtime.image,
                StageArtifactVariant.vllm_version == runtime.vllm_version,
                StageArtifactVariant.quantization == artifact.quantization,
                StageArtifactVariant.tensor_parallel_size
                == placement.tensor_parallel_size,
                StageArtifactVariant.pipeline_parallel_size
                == placement.pipeline_parallel_size,
            )
            .order_by(StageArtifactVariant.artifact_set_digest)
        )
    )
    deliveries: list[tuple[StageArtifactDelivery, dict[str, Any]]] = []
    for variant in variants:
        try:
            projection = validated_stage_artifact_projection(
                session, variant.artifact_set_digest
            )
            latest = session.scalar(
                select(StageArtifactValidationEvidence)
                .where(
                    StageArtifactValidationEvidence.variant_id
                    == variant.artifact_set_digest,
                    StageArtifactValidationEvidence.kind == "GPU_EXPORT_LOAD",
                )
                .order_by(
                    StageArtifactValidationEvidence.registration_sequence.desc(),
                    StageArtifactValidationEvidence.identity_digest.desc(),
                )
                .limit(1)
            )
            if latest is None:
                raise StageArtifactConflictError(
                    "validated stage artifact has no GPU evidence"
                )
            evidence = stage_artifact_evidence_dict(session, latest)
        except (
            StageArtifactConflictError,
            StageArtifactNotFoundError,
            ValueError,
        ):
            # A corrupt registry row is never made selectable.  FULL remains
            # an independently evaluated safe candidate.
            continue
        ranks = tuple(
            StageRankDelivery(
                rank=item["rank"],
                pipeline_rank=item["pipeline_rank"],
                tensor_rank=item["tensor_rank"],
                manifest_digest=item["manifest_digest"],
                tensor_key_count=item["tensor_key_count"],
                tensor_keys_digest=item["tensor_keys_digest"],
                weight_size_bytes=item["weight_size_bytes"],
                total_size_bytes=item["total_size_bytes"],
                file_count=item["file_count"],
            )
            for item in projection["ranks"]
        )
        delivery = StageArtifactDelivery(
            artifact_set_digest=projection["artifact_set_digest"],
            contract_identity_digest=projection["contract_identity_digest"],
            source_manifest_digest=projection["source_manifest_digest"],
            runtime_image=projection["runtime_image"],
            vllm_version=projection["vllm_version"],
            exporter_build_digest=projection["exporter_build_digest"],
            architecture=projection["architecture"],
            quantization=projection["quantization"],
            tensor_parallel_size=projection["tensor_parallel_size"],
            pipeline_parallel_size=projection["pipeline_parallel_size"],
            loader_format=projection["loader_format"],
            ranks=ranks,
        )
        stage_artifact = {
            key: projection[key]
            for key in (
                "artifact_set_digest",
                "contract_identity_digest",
                "source_manifest_digest",
                "runtime_image",
                "vllm_version",
                "exporter_build_digest",
                "architecture",
                "quantization",
                "tensor_parallel_size",
                "pipeline_parallel_size",
                "loader_format",
            )
        }
        closed_evidence = {
            key: evidence[key]
            for key in (
                "identity_digest",
                "variant_id",
                "validation_run_id",
                "registration_sequence",
                "schema_version",
                "kind",
                "status",
                "validator_version",
                "validator_build_digest",
                "rank_count",
                "failure_code",
                "ranks",
            )
        }
        deliveries.append(
            (
                delivery,
                {
                    "model_cache_kind": MODEL_CACHE_KIND_STAGE,
                    "stage_artifact": stage_artifact,
                    "stage_ranks": [
                        {
                            "rank": item.rank,
                            "pipeline_rank": item.pipeline_rank,
                            "tensor_rank": item.tensor_rank,
                            "manifest_digest": item.manifest_digest,
                            "tensor_key_count": item.tensor_key_count,
                            "tensor_keys_digest": item.tensor_keys_digest,
                            "weight_size_bytes": item.weight_size_bytes,
                            "total_size_bytes": item.total_size_bytes,
                            "file_count": item.file_count,
                            "required_cache_bytes": (
                                item.total_size_bytes * 2 + 64 * 1024 * 1024
                            ),
                        }
                        for item in ranks
                    ],
                    "stage_validation_evidence": closed_evidence,
                },
            )
        )
    return deliveries


def _full_snapshot_delivery_context(
    session: Session,
    *,
    artifact: ModelArtifact,
    placement: PlacementProfileRecord,
) -> tuple[int, dict[str, Any]]:
    """Return the deterministic FULL snapshot size gate and frozen wire fields."""

    total_size_bytes = artifact.size_mib * _MIB
    size_source = "MODEL_ARTIFACT_DECLARED_SIZE"
    manifest_record = session.get(ArtifactManifest, artifact.manifest_digest)
    if (
        manifest_record is not None
        and manifest_record.model_artifact_id == artifact.id
    ):
        try:
            manifest_projection = artifact_manifest_dict(session, manifest_record)
        except ValueError:
            # Legacy releases can predate normalized manifests. Their declared
            # artifact size remains the deterministic fallback input.
            pass
        else:
            total_size_bytes = manifest_projection["total_size_bytes"]
            size_source = "REGISTERED_ARTIFACT_MANIFEST"
    required_cache_bytes = max(
        total_size_bytes * 2 + _CACHE_RESERVE_BYTES,
        placement.min_disk_free_mib * _MIB,
    )
    return total_size_bytes, {
        "model_cache_kind": MODEL_CACHE_KIND_FULL_SNAPSHOT,
        "full_snapshot_total_size_bytes": total_size_bytes,
        "full_snapshot_required_cache_bytes": required_cache_bytes,
        "full_snapshot_size_source": size_source,
    }


def _active_catalog(
    session: Session,
    *,
    inventory: list[InventoryNode],
    now: datetime,
) -> tuple[ModelCatalog, dict[str, dict[str, Any]]]:
    rows = session.execute(
        select(ModelRelease, ModelArtifact, RuntimeRelease, PlacementProfileRecord)
        .join(ModelArtifact, ModelArtifact.id == ModelRelease.artifact_id)
        .join(RuntimeRelease, RuntimeRelease.id == ModelRelease.runtime_id)
        .join(
            PlacementProfileRecord,
            PlacementProfileRecord.release_id == ModelRelease.id,
        )
        .where(
            ModelRelease.status == "ACTIVE",
            PlacementProfileRecord.status == "ACTIVE",
        )
        .order_by(
            ModelRelease.quality_rank.desc(),
            ModelArtifact.model_id,
            ModelRelease.id,
            PlacementProfileRecord.id,
        )
    ).all()

    entries: list[CatalogEntry] = []
    contexts: dict[str, dict[str, Any]] = {}
    snapshot: list[dict[str, Any]] = []
    for release, artifact, runtime, placement in rows:
        full_candidate_id = f"{release.id}:{placement.id}"
        full_snapshot_size_bytes, full_delivery_context = (
            _full_snapshot_delivery_context(
                session,
                artifact=artifact,
                placement=placement,
            )
        )
        network_evidence = _network_evidence_bindings(
            session,
            release=release,
            artifact=artifact,
            runtime=runtime,
            placement=placement,
            inventory=inventory,
            now=now,
        )
        base_context = {
            "model_id": artifact.model_id,
            "model_release_id": release.id,
            "artifact_id": artifact.id,
            "artifact_repository": artifact.repository,
            "artifact_revision": artifact.revision,
            "artifact_manifest_digest": artifact.manifest_digest,
            "quantization": artifact.quantization,
            "runtime_release_id": runtime.id,
            "placement_id": placement.id,
            "placement_profile_id": placement.profile_id,
            "runtime_image": runtime.image,
        }
        if placement.node_count > 1:
            base_context.update(
                {
                    "execution_backend": VLLM_RAY_PP_BACKEND,
                    "runtime_vllm_version": runtime.vllm_version,
                }
            )
        artifact_snapshot = {
            "repository": artifact.repository,
            "revision": artifact.revision,
            "manifest_digest": artifact.manifest_digest,
            "quantization": artifact.quantization,
            "size_mib": artifact.size_mib,
            "default_max_model_len": artifact.default_max_model_len,
            "layer_count": artifact.layer_count,
        }
        runtime_snapshot = {
            "version": runtime.version,
            "vllm_version": runtime.vllm_version,
            "cuda_version": runtime.cuda_version,
            "gpu_architectures": sorted(runtime.gpu_architectures),
        }
        placement_snapshot = {
            "topology": placement.topology,
            "node_count": placement.node_count,
            "min_gpu_memory_mib": placement.min_gpu_memory_mib,
            "min_disk_free_mib": placement.min_disk_free_mib,
            "pipeline_parallel_size": placement.pipeline_parallel_size,
            "tensor_parallel_size": placement.tensor_parallel_size,
            "max_model_len": placement.max_model_len,
            "max_concurrency": placement.max_concurrency,
            "origin": placement.origin,
            "status": placement.status,
            "spec_digest": placement.spec_digest,
            "qualification_evidence_id": placement.qualification_evidence_id,
            "qualified_at": (
                aware(placement.qualified_at).isoformat()
                if placement.qualified_at is not None
                else None
            ),
            "activated_at": (
                aware(placement.activated_at).isoformat()
                if placement.activated_at is not None
                else None
            ),
            "requires_network_evidence": placement.requires_network_evidence,
            "requires_qualification_evidence": placement.origin == "AUTO",
            "requires_nccl": placement.requires_nccl,
            "min_bandwidth_mbps": placement.min_bandwidth_mbps,
            "max_rtt_ms": placement.max_rtt_ms,
            "max_packet_loss_pct": placement.max_packet_loss_pct,
            "max_ttft_p95_ms": placement.max_ttft_p95_ms,
            "max_tpot_p95_ms": placement.max_tpot_p95_ms,
            "max_e2e_p95_ms": placement.max_e2e_p95_ms,
            "min_success_rate": placement.min_success_rate,
            "min_vram_headroom_pct": placement.min_vram_headroom_pct,
            "min_throughput_tps": placement.min_throughput_tps,
        }
        evidence_snapshot = [
            {
                "evidence_id": item.evidence_id,
                "evidence_digest": item.evidence_digest,
                "node_ids": list(item.node_ids),
                "rank_node_ids": list(item.rank_node_ids),
                "registered_at": item.registered_at,
            }
            for item in network_evidence
        ]
        model = ModelSpec(
            model_id=artifact.model_id,
            repository=artifact.repository,
            quantization=artifact.quantization,
            checkpoint_gib=artifact.size_mib / 1024,
            min_gpu_memory_gib=placement.min_gpu_memory_mib / 1024,
            default_max_model_len=artifact.default_max_model_len,
            layer_count=artifact.layer_count,
        )
        catalog_placement = PlacementProfile(
            profile_id=placement.profile_id,
            node_count=placement.node_count,
            min_gpu_memory_mib=placement.min_gpu_memory_mib,
            min_disk_free_mib=placement.min_disk_free_mib,
            pipeline_parallel_size=placement.pipeline_parallel_size,
            tensor_parallel_size=placement.tensor_parallel_size,
            requires_network_evidence=(
                placement.requires_network_evidence or placement.node_count > 1
            ),
            requires_qualification_evidence=placement.origin == "AUTO",
        )

        delivery_candidates: list[
            tuple[str, StageArtifactDelivery | None, dict[str, Any]]
        ] = []
        for delivery, stage_context in _validated_stage_deliveries(
            session,
            artifact=artifact,
            runtime=runtime,
            placement=placement,
        ):
            delivery_candidates.append(
                (
                    f"{full_candidate_id}:STAGE:{delivery.artifact_set_digest}",
                    delivery,
                    stage_context,
                )
            )
        delivery_candidates.append(
            (
                full_candidate_id,
                None,
                full_delivery_context,
            )
        )

        for candidate_id, stage_delivery, delivery_context in delivery_candidates:
            context = {
                "candidate_id": candidate_id,
                **base_context,
                **delivery_context,
            }
            contexts[candidate_id] = context
            snapshot.append(
                {
                    **context,
                    "artifact": artifact_snapshot,
                    "runtime": runtime_snapshot,
                    "release": {
                        "status": release.status,
                        "quality_rank": release.quality_rank,
                    },
                    "placement": placement_snapshot,
                    "network_evidence": evidence_snapshot,
                }
            )
            entries.append(
                CatalogEntry(
                    model=model,
                    placement=catalog_placement,
                    quality_rank=release.quality_rank,
                    artifact_revision=artifact.revision,
                    candidate_id=candidate_id,
                    gpu_architectures=tuple(sorted(runtime.gpu_architectures)),
                    network_evidence=network_evidence,
                    stage_artifact=stage_delivery,
                    full_snapshot_size_bytes=full_snapshot_size_bytes,
                )
            )

    return (
        ModelCatalog(
            version=_content_digest(snapshot),
            policy_version=POLICY_VERSION,
            entries=tuple(entries),
        ),
        contexts,
    )


def _inventory_nodes(
    session: Session,
    *,
    node_ids: list[str],
    all_online: bool,
    now: datetime,
) -> list[InventoryNode]:
    if bool(node_ids) == all_online:
        raise ValueError("choose exactly one of node_ids or all_online")
    if len(node_ids) != len(set(node_ids)):
        raise ValueError("node_ids must not contain duplicates")

    statement = (
        select(Node, NodeProfileRecord)
        .outerjoin(NodeProfileRecord, NodeProfileRecord.node_id == Node.id)
        .order_by(Node.id)
    )
    if node_ids:
        statement = statement.where(Node.id.in_(sorted(node_ids)))
    rows = list(session.execute(statement).all())
    if node_ids:
        found = {node.id for node, _ in rows}
        missing = sorted(set(node_ids) - found)
        if missing:
            raise RecommendationNodeNotFoundError(
                f"unknown node(s): {', '.join(missing)}",
                details={"node_ids": missing},
            )
    else:
        rows = [
            (node, record)
            for node, record in rows
            if node.approved and node_status(node.last_seen, now) == "online"
        ]

    inventory: list[InventoryNode] = []
    for node, record in rows:
        profile: NodeProfile | None = None
        profile_error: str | None = None
        profile_fresh = False
        if record is None:
            profile_error = "missing"
        else:
            try:
                profile = NodeProfile.from_dict(record.profile)
                profile.node_id = node.id
                profile_age = now - aware(record.updated_at)
                profile_fresh = timedelta(0) <= profile_age <= PROFILE_MAX_AGE
            except (KeyError, TypeError, ValueError):
                profile = None
                profile_error = "invalid"
        inventory.append(
            InventoryNode(
                node_id=node.id,
                profile=profile,
                approved=node.approved,
                online=node_status(node.last_seen, now) == "online",
                profile_fresh=profile_fresh,
                network_verified=False,
                profile_error=profile_error,
                agent_version=node.agent_version,
            )
        )
    return inventory


def _strict_candidate_rejections(
    *,
    context: dict[str, Any],
    entry: CatalogEntry,
    node_ids: list[str],
    inventory_by_id: dict[str, InventoryNode],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Close unsupported strict runtime candidates before selection."""

    rejections: list[dict[str, Any]] = []
    rank_node_ids: list[str] = []

    def reject(code: str, detail: str, affected: list[str] | None = None) -> None:
        rejections.append(
            {
                "code": code,
                "detail": detail,
                "node_ids": sorted(affected if affected is not None else node_ids),
            }
        )

    placement = entry.placement
    minimum_agent = (
        (0, 3, 19)
        if context.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE
        else (0, 3, 18)
    )
    unsupported_agents = sorted(
        node_id
        for node_id in node_ids
        if node_id not in inventory_by_id
        or not _agent_supports(
            inventory_by_id[node_id].agent_version,
            minimum_agent,
        )
    )
    if unsupported_agents:
        reject(
            (
                "STAGE_AGENT_VERSION"
                if context.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE
                else "STRICT_AGENT_VERSION"
            ),
            (
                "STAGE 후보는 Dure Agent 0.3.19 이상이 필요합니다."
                if context.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE
                else "엄격한 다중 노드 후보는 Dure Agent 0.3.18 이상이 필요합니다."
            ),
            unsupported_agents,
        )
    if context.get("runtime_vllm_version") != VLLM_RAY_PP_RUNTIME_VERSION:
        reject(
            "STRICT_RUNTIME_VERSION",
            f"엄격한 다중 노드 실행은 vLLM {VLLM_RAY_PP_RUNTIME_VERSION}만 지원합니다.",
        )
    if (
        placement.node_count not in {2, 3}
        or placement.tensor_parallel_size != 1
        or placement.pipeline_parallel_size != placement.node_count
        or entry.model.layer_count < placement.node_count
    ):
        reject(
            "STRICT_TOPOLOGY",
            "엄격한 다중 노드 실행은 TP=1인 2·3단계 one-node-per-rank만 지원합니다.",
        )
    if entry.model.quantization != "awq":
        reject(
            "STRICT_QUANTIZATION",
            "엄격한 다중 노드 실행은 검증된 AWQ 모델만 지원합니다.",
        )
    try:
        validate_digest_pinned_runtime_image(context.get("runtime_image"))
    except ValueError:
        reject(
            "STRICT_RUNTIME_IMAGE",
            "엄격한 다중 노드 런타임 이미지는 OCI SHA-256 digest로 고정해야 합니다.",
        )

    profiles = [
        inventory_by_id[node_id].profile
        for node_id in node_ids
        if node_id in inventory_by_id
        and inventory_by_id[node_id].profile is not None
    ]
    if len(profiles) != placement.node_count or len(node_ids) != placement.node_count:
        reject(
            "STRICT_NODE_SET",
            "엄격한 다중 노드 후보에 필요한 정확한 프로필 집합이 없습니다.",
        )
        return rejections, rank_node_ids

    try:
        bindings = strict_vllm_ray_pp_order(
            profiles,
            head_node_id=node_ids[0],
            minimum_gpu_memory_mib=placement.min_gpu_memory_mib,
        )
    except StrictRayPPTopologyError as exc:
        if exc.reason in {
            "HEALTHY_GPU_COUNT",
            "GPU_MEMORY_INSUFFICIENT",
            "GPU_INDEX_INVALID",
        }:
            code = "STRICT_GPU_TOPOLOGY"
        elif exc.reason in {
            "PRIVATE_IPV4_REQUIRED",
            "PRIVATE_IPV4_AMBIGUOUS",
            "DUPLICATE_RUNTIME_ADDRESS",
            "DEFAULT_INTERFACE_REQUIRED",
            "DEFAULT_INTERFACE_ADDRESS_REQUIRED",
            "DEFAULT_INTERFACE_ADDRESS_MISMATCH",
        }:
            code = "STRICT_NETWORK"
        else:
            code = "STRICT_NODE_SET"
        reject(code, str(exc), list(exc.node_ids))
    else:
        rank_node_ids = [item.profile.node_id for item in bindings]

    interfaces = [profile.network.default_interface for profile in profiles]
    if (
        any(
            type(value) is not str
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,14}", value) is None
            for value in interfaces
        )
        or len(set(interfaces)) != 1
    ):
        reject(
            "STRICT_NETWORK",
            "엄격한 다중 노드의 기본 네트워크 인터페이스는 명시적이고 모두 같아야 합니다.",
        )
    return rejections, rank_node_ids


def evaluate_deployment_recommendation(
    session: Session,
    *,
    node_ids: list[str],
    all_online: bool,
    objective: str = "quality-first",
    now: datetime | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Evaluate without writing any recommendation, deployment, or task row."""
    if objective != "quality-first":
        raise ValueError("unsupported recommendation objective")
    evaluated_at = now or utcnow()
    with session.no_autoflush:
        inventory = _inventory_nodes(
            session,
            node_ids=node_ids,
            all_online=all_online,
            now=evaluated_at,
        )
        catalog, contexts = _active_catalog(
            session,
            inventory=inventory,
            now=evaluated_at,
        )
    result = recommend_model(inventory, catalog=catalog)
    inventory_snapshot = canonical_inventory_snapshot(inventory)
    if _inventory_snapshot_fingerprint(inventory_snapshot) != result.inventory_fingerprint:
        raise RecommendationNotAcceptableError(
            "selector and recommendation inventory fingerprints disagree",
            code="INVENTORY_FINGERPRINT_INCONSISTENT",
        )

    candidates = []
    entries_by_id = {entry.candidate_id: entry for entry in catalog.entries}
    inventory_by_id = {item.node_id: item for item in inventory}
    for evaluation in result.evaluations:
        context = contexts[evaluation.candidate_id]
        candidate_node_ids = list(evaluation.node_ids)
        candidate_rejections: list[dict[str, Any]] = []
        rank_node_ids: list[str] = []
        if (
            context.get("model_cache_kind") == MODEL_CACHE_KIND_FULL_SNAPSHOT
            and len(candidate_node_ids) == 1
        ):
            unsupported_agents = sorted(
                node_id
                for node_id in candidate_node_ids
                if node_id not in inventory_by_id
                or not _agent_supports(
                    inventory_by_id[node_id].agent_version,
                    _FULL_SNAPSHOT_AGENT_VERSION,
                )
            )
            if unsupported_agents:
                candidate_rejections.append(
                    {
                        "code": "FULL_SNAPSHOT_AGENT_VERSION",
                        "detail": (
                            "단일 GPU FULL_SNAPSHOT 후보는 모델 준비를 위해 "
                            "Dure Agent 0.3.16 이상이 필요합니다."
                        ),
                        "node_ids": unsupported_agents,
                    }
                )
        if context.get("execution_backend") == VLLM_RAY_PP_BACKEND:
            candidate_node_ids = sorted(candidate_node_ids)
            strict_rejections, rank_node_ids = _strict_candidate_rejections(
                context=context,
                entry=entries_by_id[evaluation.candidate_id],
                node_ids=candidate_node_ids,
                inventory_by_id=inventory_by_id,
            )
            candidate_rejections.extend(strict_rejections)
            expected_rank_nodes = list(evaluation.rank_node_ids)
            if context.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE and (
                not expected_rank_nodes or expected_rank_nodes != rank_node_ids
            ):
                candidate_rejections.append(
                    {
                        "code": "STAGE_RANK_BINDING",
                        "detail": (
                            "STAGE variant를 네트워크 증적의 정확한 node UUID→rank "
                            "결합에 고정할 수 없습니다."
                        ),
                        "node_ids": candidate_node_ids,
                    }
                )
        candidate = {
            **context,
            "quality_rank": evaluation.quality_rank,
            "feasible": evaluation.feasible and not candidate_rejections,
            "node_ids": candidate_node_ids,
            "rejections": [
                *(item.to_dict() for item in evaluation.rejections),
                *candidate_rejections,
            ],
        }
        if rank_node_ids:
            candidate["rank_node_ids"] = rank_node_ids
        if (
            context.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE
            and rank_node_ids
        ):
            stage_ranks = context.get("stage_ranks")
            if not isinstance(stage_ranks, list) or len(stage_ranks) != len(
                rank_node_ids
            ):
                candidate["feasible"] = False
                candidate["rejections"].append(
                    {
                        "code": "STAGE_RANK_BINDING",
                        "detail": "STAGE variant rank 집합이 배포 토폴로지와 다릅니다.",
                        "node_ids": candidate_node_ids,
                    }
                )
            else:
                candidate["stage_node_bindings"] = [
                    {"node_id": rank_node_ids[rank], **stage}
                    for rank, stage in enumerate(stage_ranks)
                ]
        if evaluation.network_evidence_id is not None:
            candidate.update(
                {
                    "network_evidence_id": evaluation.network_evidence_id,
                    "network_evidence_digest": evaluation.network_evidence_digest,
                    "network_evidence_registered_at": (
                        evaluation.network_evidence_registered_at
                    ),
                }
            )
        candidates.append(candidate)
    selected = next(
        (
            candidate
            for candidate in candidates
            if candidate["candidate_id"] == result.selected_candidate_id
            and candidate["feasible"]
        ),
        None,
    )
    if selected is None:
        selected = next(
            (candidate for candidate in candidates if candidate["feasible"]),
            None,
        )
    core = {
        "objective": objective,
        "selection_mode": "all_online" if all_online else "explicit_nodes",
        "requested_node_ids": sorted(item.node_id for item in inventory),
        "catalog_version": result.catalog_version,
        "policy_version": result.policy_version,
        "inventory_fingerprint": result.inventory_fingerprint,
        "selected": selected,
        "candidates": candidates,
        "rejections": (
            []
            if candidates
            else [
                {
                    "code": "NO_ACTIVE_CANDIDATE",
                    "detail": "no ACTIVE model release with a placement profile",
                    "node_ids": [],
                }
            ]
        ),
    }
    return {"recommendation": {"id": _content_digest(core), **core}}, inventory_snapshot


def _record_values(
    response: dict[str, Any], inventory_snapshot: list[dict[str, Any]]
) -> dict[str, Any]:
    recommendation = response["recommendation"]
    return {
        "id": recommendation["id"],
        "objective": recommendation["objective"],
        "selection_mode": recommendation["selection_mode"],
        "requested_node_ids": list(recommendation["requested_node_ids"]),
        "catalog_version": recommendation["catalog_version"],
        "policy_version": recommendation["policy_version"],
        "inventory_fingerprint": recommendation["inventory_fingerprint"],
        "recommendation_snapshot": recommendation,
        "inventory_snapshot": inventory_snapshot,
    }


def _record_matches(
    record: DeploymentRecommendationRecord,
    response: dict[str, Any],
    inventory_snapshot: list[dict[str, Any]],
) -> bool:
    expected = _record_values(response, inventory_snapshot)
    return all(getattr(record, key) == value for key, value in expected.items())


def _validate_stored_record(record: DeploymentRecommendationRecord) -> None:
    snapshot = record.recommendation_snapshot
    if not isinstance(snapshot, dict) or snapshot.get("id") != record.id:
        raise RecommendationNotAcceptableError(
            "stored recommendation snapshot is invalid",
            code="RECOMMENDATION_RECORD_INVALID",
        )
    core = dict(snapshot)
    core.pop("id", None)
    valid = (
        _content_digest(core) == record.id
        and snapshot.get("objective") == record.objective
        and snapshot.get("selection_mode") == record.selection_mode
        and snapshot.get("requested_node_ids") == record.requested_node_ids
        and snapshot.get("catalog_version") == record.catalog_version
        and snapshot.get("policy_version") == record.policy_version
        and snapshot.get("inventory_fingerprint") == record.inventory_fingerprint
        and _inventory_snapshot_fingerprint(record.inventory_snapshot)
        == record.inventory_fingerprint
    )
    if not valid:
        raise RecommendationNotAcceptableError(
            "stored recommendation integrity check failed",
            code="RECOMMENDATION_RECORD_INVALID",
        )


def persist_deployment_recommendation(
    session: Session,
    response: dict[str, Any],
    inventory_snapshot: list[dict[str, Any]],
) -> tuple[DeploymentRecommendationRecord, bool]:
    values = _record_values(response, inventory_snapshot)
    existing = session.get(DeploymentRecommendationRecord, values["id"])
    if existing is not None:
        if not _record_matches(existing, response, inventory_snapshot):
            raise RecommendationGenerationConflictError(
                "recommendation content ID is already bound to different content",
                code="RECOMMENDATION_SNAPSHOT_CONFLICT",
            )
        return existing, False

    record = DeploymentRecommendationRecord(**values)
    session.add(record)
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        existing = session.get(DeploymentRecommendationRecord, values["id"])
        if existing is None or not _record_matches(existing, response, inventory_snapshot):
            raise RecommendationGenerationConflictError(
                "recommendation snapshot could not be persisted",
                code="RECOMMENDATION_SNAPSHOT_CONFLICT",
            ) from exc
        return existing, False
    return record, True


def recommend_deployment(
    session: Session,
    *,
    node_ids: list[str],
    all_online: bool,
    objective: str = "quality-first",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate and idempotently persist an immutable recommendation snapshot."""
    response, inventory_snapshot = evaluate_deployment_recommendation(
        session,
        node_ids=node_ids,
        all_online=all_online,
        objective=objective,
        now=now,
    )
    persist_deployment_recommendation(session, response, inventory_snapshot)
    return response


def deployment_generation_dict(deployment: Deployment) -> dict[str, Any]:
    created_at = aware(deployment.created_at)
    return {
        "id": deployment.id,
        "lineage_id": deployment.lineage_id,
        "generation": deployment.generation,
        "previous_generation_id": deployment.previous_generation_id,
        "source_recommendation_id": deployment.source_recommendation_id,
        "status": deployment.status,
        "plan": deployment.plan,
        "accept_model_download": deployment.accept_model_download,
        "pull_image": deployment.pull_image,
        "created_at": created_at.isoformat(),
    }


def show_deployment_recommendation(
    session: Session, recommendation_id: str
) -> dict[str, Any]:
    record = session.get(DeploymentRecommendationRecord, recommendation_id)
    if record is None:
        raise RecommendationNotFoundError(details={"recommendation_id": recommendation_id})
    _validate_stored_record(record)
    deployment = session.scalar(
        select(Deployment).where(
            Deployment.source_recommendation_id == recommendation_id
        )
    )
    return {
        "recommendation": record.recommendation_snapshot,
        "inventory_snapshot": record.inventory_snapshot,
        "recorded_at": aware(record.created_at).isoformat(),
        "deployment": deployment_generation_dict(deployment) if deployment else None,
    }


def _lock_recommendation_inputs(
    session: Session, record: DeploymentRecommendationRecord
) -> None:
    if session.get_bind().dialect.name == "postgresql":
        session.execute(
            text(
                "LOCK TABLE artifact_chunks, artifact_manifests, "
                "artifact_manifest_files, artifact_file_chunks, benchmark_evidence, "
                "benchmark_runs, model_artifacts, model_releases, nodes, "
                "node_profiles, placement_profiles, runtime_releases, "
                "stage_artifact_variants, stage_artifact_ranks, "
                "stage_artifact_validation_evidence, "
                "stage_artifact_validation_ranks IN SHARE MODE"
            )
        )
        return
    # Keep the fallback lock order identical across callers.  SQLite ignores
    # FOR UPDATE, but this path is also exercised by databases that provide
    # row locks without PostgreSQL table locks.
    for model, order_by in (
        (ArtifactChunk, (ArtifactChunk.digest,)),
        (ArtifactManifest, (ArtifactManifest.digest,)),
        (ArtifactManifestFile, (ArtifactManifestFile.id,)),
        (ArtifactFileChunk, (ArtifactFileChunk.file_id, ArtifactFileChunk.ordinal)),
        (BenchmarkEvidence, (BenchmarkEvidence.id,)),
        (BenchmarkRun, (BenchmarkRun.id,)),
        (ModelArtifact, (ModelArtifact.id,)),
        (ModelRelease, (ModelRelease.id,)),
        (PlacementProfileRecord, (PlacementProfileRecord.id,)),
        (RuntimeRelease, (RuntimeRelease.id,)),
        (StageArtifactVariant, (StageArtifactVariant.artifact_set_digest,)),
        (StageArtifactRank, (StageArtifactRank.id,)),
        (
            StageArtifactValidationEvidence,
            (StageArtifactValidationEvidence.identity_digest,),
        ),
        (
            StageArtifactValidationRank,
            (StageArtifactValidationRank.evidence_id, StageArtifactValidationRank.rank),
        ),
    ):
        list(session.scalars(select(model).order_by(*order_by).with_for_update()))
    node_statement = select(Node).order_by(Node.id)
    profile_statement = select(NodeProfileRecord).order_by(NodeProfileRecord.node_id)
    if record.selection_mode == "explicit_nodes":
        node_statement = node_statement.where(Node.id.in_(record.requested_node_ids))
        profile_statement = profile_statement.where(
            NodeProfileRecord.node_id.in_(record.requested_node_ids)
        )
    list(session.scalars(node_statement.with_for_update()))
    list(session.scalars(profile_statement.with_for_update()))


def _best_gpu(profile: NodeProfile, minimum_mib: int):
    eligible = [
        gpu
        for gpu in profile.gpus
        if gpu.healthy and gpu.memory_mib >= minimum_mib
    ]
    if not eligible:
        raise RecommendationNotAcceptableError(
            "selected node no longer has an eligible GPU",
            code="GENERATION_GPU_UNAVAILABLE",
            details={"node_id": profile.node_id},
        )
    return min(
        eligible,
        key=lambda item: (-item.memory_mib, item.uuid, item.index),
    )


def _layer_partitions(layer_count: int, stages: int) -> list[tuple[int, int]]:
    base, extra = divmod(layer_count, stages)
    partitions = []
    cursor = 0
    for stage in range(stages):
        size = base + (1 if stage < extra else 0)
        partitions.append((cursor, cursor + size - 1))
        cursor += size
    return partitions


def _network_interface(profiles: list[NodeProfile]) -> str:
    interfaces = [profile.network.default_interface or "eth0" for profile in profiles]
    if any(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,14}", value) is None for value in interfaces):
        raise RecommendationNotAcceptableError(
            "selected node has an invalid network interface",
            code="GENERATION_NETWORK_INVALID",
        )
    if len(profiles) > 1 and len(set(interfaces)) != 1:
        raise RecommendationNotAcceptableError(
            "selected nodes do not share one representable network interface",
            code="GENERATION_NETWORK_UNSUPPORTED",
        )
    return interfaces[0]


def _ray_head_ip(profile: NodeProfile, *, multi_node: bool) -> str:
    addresses = []
    for value in profile.network.addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError:
            continue
        if (
            address.version == 4
            and any(address in network for network in PRIVATE_IPV4_NETWORKS)
            and not address.is_loopback
            and not address.is_link_local
            and not address.is_unspecified
            and not address.is_multicast
            and not address.is_reserved
        ):
            addresses.append(address)
    if addresses:
        return str(min(set(addresses), key=int))
    if not multi_node:
        return "127.0.0.1"
    raise RecommendationNotAcceptableError(
        "multi-node generation requires a valid IPv4 Ray head address",
        code="GENERATION_NETWORK_UNSUPPORTED",
    )


def _build_generation_plan(
    session: Session,
    *,
    recommendation: dict[str, Any],
    deployment_id: str,
    generation: int,
) -> dict[str, Any]:
    selected = recommendation.get("selected")
    if (
        not isinstance(selected, dict)
        or selected.get("feasible") is not True
        or selected.get("rejections")
    ):
        raise RecommendationNotAcceptableError(
            "recommendation has no feasible selected candidate",
            code="RECOMMENDATION_NOT_FEASIBLE",
        )
    release = session.get(ModelRelease, selected.get("model_release_id"))
    placement = session.get(PlacementProfileRecord, selected.get("placement_id"))
    artifact = session.get(ModelArtifact, selected.get("artifact_id"))
    runtime = session.get(RuntimeRelease, selected.get("runtime_release_id"))
    if (
        release is None
        or release.status != "ACTIVE"
        or placement is None
        or placement.release_id != release.id
        or artifact is None
        or artifact.id != release.artifact_id
        or runtime is None
        or runtime.id != release.runtime_id
    ):
        raise RecommendationStaleError(
            "selected registry binding is no longer active",
            details={"recommendation_id": recommendation.get("id")},
        )
    node_ids = selected.get("node_ids")
    if (
        not isinstance(node_ids, list)
        or any(type(node_id) is not str for node_id in node_ids)
        or len(node_ids) != len(set(node_ids))
        or len(node_ids) != placement.node_count
        or node_ids != sorted(node_ids)
    ):
        raise RecommendationNotAcceptableError(
            "selected node assignment does not match the placement profile",
            code="GENERATION_PLACEMENT_INVALID",
        )
    if (
        placement.tensor_parallel_size != 1
        or placement.pipeline_parallel_size != placement.node_count
    ):
        raise RecommendationNotAcceptableError(
            "current deployment plan cannot represent this placement topology",
            code="GENERATION_PLACEMENT_UNSUPPORTED",
        )
    profile_records = {
        item.node_id: item
        for item in session.scalars(
            select(NodeProfileRecord).where(NodeProfileRecord.node_id.in_(node_ids))
        )
    }
    profiles: list[NodeProfile] = []
    for node_id in node_ids:
        record = profile_records.get(node_id)
        if record is None:
            raise RecommendationStaleError(
                "selected node profile is missing",
                details={"node_id": node_id},
            )
        try:
            profile = NodeProfile.from_dict(record.profile)
        except (KeyError, TypeError, ValueError) as exc:
            raise RecommendationStaleError(
                "selected node profile is invalid",
                details={"node_id": node_id},
            ) from exc
        profile.node_id = node_id
        profiles.append(profile)

    partitions = _layer_partitions(artifact.layer_count, placement.node_count)
    multi_node = len(profiles) > 1
    execution_backend: str | None = None
    runtime_vllm_version: str | None = None
    model_cache_kind: str | None = None
    stage_artifact_binding: StageArtifactBinding | None = None
    selected_cache_kind = selected.get("model_cache_kind")
    if multi_node:
        if selected.get("execution_backend") != VLLM_RAY_PP_BACKEND:
            raise RecommendationNotAcceptableError(
                "multi-node generation is not bound to a supported execution backend",
                code="GENERATION_BACKEND_UNSUPPORTED",
            )
        if (
            selected.get("runtime_vllm_version") != runtime.vllm_version
            or runtime.vllm_version != VLLM_RAY_PP_RUNTIME_VERSION
        ):
            raise RecommendationNotAcceptableError(
                f"{VLLM_RAY_PP_BACKEND} requires vLLM "
                f"{VLLM_RAY_PP_RUNTIME_VERSION}",
                code="GENERATION_RUNTIME_UNSUPPORTED",
                details={"vllm_version": runtime.vllm_version},
            )
        if selected_cache_kind not in {
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
            MODEL_CACHE_KIND_STAGE,
        }:
            raise RecommendationNotAcceptableError(
                f"{VLLM_RAY_PP_BACKEND} requires an exact supported model cache",
                code="GENERATION_MODEL_CACHE_UNSUPPORTED",
            )
        try:
            bindings = strict_vllm_ray_pp_order(
                profiles,
                head_node_id=node_ids[0],
                minimum_gpu_memory_mib=placement.min_gpu_memory_mib,
            )
        except StrictRayPPTopologyError as exc:
            if exc.reason == "GPU_MEMORY_INSUFFICIENT":
                code = "GENERATION_GPU_UNAVAILABLE"
            elif exc.reason == "HEALTHY_GPU_COUNT":
                code = "GENERATION_GPU_TOPOLOGY_UNSUPPORTED"
            elif exc.reason in {
                "PRIVATE_IPV4_REQUIRED",
                "PRIVATE_IPV4_AMBIGUOUS",
                "DUPLICATE_RUNTIME_ADDRESS",
                "DEFAULT_INTERFACE_REQUIRED",
                "DEFAULT_INTERFACE_ADDRESS_REQUIRED",
                "DEFAULT_INTERFACE_ADDRESS_MISMATCH",
            }:
                code = "GENERATION_NETWORK_UNSUPPORTED"
            else:
                code = "GENERATION_PLACEMENT_INVALID"
            raise RecommendationNotAcceptableError(
                str(exc),
                code=code,
                details={"reason": exc.reason, "node_ids": list(exc.node_ids)},
            ) from exc
        ordered_profiles = [item.profile for item in bindings]
        rank_node_ids = [item.profile.node_id for item in bindings]
        if selected.get("rank_node_ids") != rank_node_ids:
            raise RecommendationStaleError(
                "selected node UUID to runtime rank binding changed",
                details={"recommendation_id": recommendation.get("id")},
            )
        stage_by_rank: dict[int, dict[str, Any]] = {}
        if selected_cache_kind == MODEL_CACHE_KIND_STAGE:
            current_deliveries = _validated_stage_deliveries(
                session,
                artifact=artifact,
                runtime=runtime,
                placement=placement,
            )
            selected_stage = selected.get("stage_artifact")
            selected_digest = (
                selected_stage.get("artifact_set_digest")
                if isinstance(selected_stage, dict)
                else None
            )
            current_context = next(
                (
                    context
                    for delivery, context in current_deliveries
                    if delivery.artifact_set_digest == selected_digest
                ),
                None,
            )
            if current_context is None or any(
                selected.get(key) != current_context.get(key)
                for key in (
                    "model_cache_kind",
                    "stage_artifact",
                    "stage_ranks",
                    "stage_validation_evidence",
                )
            ):
                raise RecommendationStaleError(
                    "selected STAGE variant is no longer exactly validated",
                    details={
                        "recommendation_id": recommendation.get("id"),
                        "artifact_set_digest": selected_digest,
                    },
                )
            expected_node_bindings = [
                {"node_id": rank_node_ids[rank], **stage}
                for rank, stage in enumerate(current_context["stage_ranks"])
            ]
            if selected.get("stage_node_bindings") != expected_node_bindings:
                raise RecommendationStaleError(
                    "selected STAGE rank binding changed",
                    details={
                        "recommendation_id": recommendation.get("id"),
                        "artifact_set_digest": selected_digest,
                    },
                )
            stage_by_rank = {
                item["rank"]: item for item in expected_node_bindings
            }
            try:
                stage_artifact_binding = StageArtifactBinding.from_dict(
                    current_context["stage_artifact"]
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise RecommendationNotAcceptableError(
                    "selected STAGE loader contract cannot be represented",
                    code="GENERATION_MODEL_CACHE_UNSUPPORTED",
                ) from exc
        elif any(
            key in selected
            for key in (
                "stage_artifact",
                "stage_ranks",
                "stage_node_bindings",
                "stage_validation_evidence",
            )
        ):
            raise RecommendationNotAcceptableError(
                "FULL_SNAPSHOT selection contains STAGE-only bindings",
                code="GENERATION_MODEL_CACHE_UNSUPPORTED",
            )
        if selected_cache_kind == MODEL_CACHE_KIND_FULL_SNAPSHOT:
            _, current_full_context = _full_snapshot_delivery_context(
                session,
                artifact=artifact,
                placement=placement,
            )
            if any(
                selected.get(key) != value
                for key, value in current_full_context.items()
            ):
                raise RecommendationStaleError(
                    "selected FULL_SNAPSHOT delivery identity changed",
                    details={"recommendation_id": recommendation.get("id")},
                )
        assignments = [
            NodeAssignment(
                node_id=binding.profile.node_id,
                gpu_index=binding.gpu_index,
                gpu_uuid=binding.gpu_uuid,
                rank=rank,
                pipeline_rank=rank,
                layer_start=partitions[rank][0],
                layer_end=partitions[rank][1],
                role="ray-head" if rank == 0 else "ray-worker",
                expected_runtime_rank=rank,
                runtime_address=binding.runtime_address,
                stage_manifest_digest=(
                    stage_by_rank[rank]["manifest_digest"]
                    if selected_cache_kind == MODEL_CACHE_KIND_STAGE
                    else None
                ),
                stage_tensor_keys_digest=(
                    stage_by_rank[rank]["tensor_keys_digest"]
                    if selected_cache_kind == MODEL_CACHE_KIND_STAGE
                    else None
                ),
            )
            for rank, binding in enumerate(bindings)
        ]
        head_ip = bindings[0].runtime_address
        execution_backend = VLLM_RAY_PP_BACKEND
        runtime_vllm_version = runtime.vllm_version
        model_cache_kind = selected_cache_kind
    else:
        if selected_cache_kind != MODEL_CACHE_KIND_FULL_SNAPSHOT:
            raise RecommendationNotAcceptableError(
                "single-node generation requires an exact FULL_SNAPSHOT model cache",
                code="GENERATION_MODEL_CACHE_UNSUPPORTED",
            )
        _, current_full_context = _full_snapshot_delivery_context(
            session,
            artifact=artifact,
            placement=placement,
        )
        if any(
            selected.get(key) != value
            for key, value in current_full_context.items()
        ):
            raise RecommendationStaleError(
                "selected FULL_SNAPSHOT delivery identity changed",
                details={"recommendation_id": recommendation.get("id")},
            )
        if any(
            key in selected
            for key in (
                "stage_artifact",
                "stage_ranks",
                "stage_node_bindings",
                "stage_validation_evidence",
            )
        ):
            raise RecommendationNotAcceptableError(
                "FULL_SNAPSHOT selection contains STAGE-only bindings",
                code="GENERATION_MODEL_CACHE_UNSUPPORTED",
            )
        ordered_profiles = profiles
        selected_gpu = _best_gpu(
            profiles[0], placement.min_gpu_memory_mib
        )
        assignments = [
            NodeAssignment(
                node_id=profiles[0].node_id,
                gpu_index=selected_gpu.index,
                gpu_uuid=selected_gpu.uuid,
                rank=0,
                pipeline_rank=0,
                layer_start=partitions[0][0],
                layer_end=partitions[0][1],
                role="ray-head",
            )
        ]
        head_ip = _ray_head_ip(profiles[0], multi_node=False)
        model_cache_kind = selected_cache_kind
    plan = DeploymentPlan(
        deployment_id=deployment_id,
        generation=generation,
        model=ModelSpec(
            model_id=artifact.model_id,
            repository=artifact.repository,
            quantization=artifact.quantization,
            checkpoint_gib=artifact.size_mib / 1024,
            min_gpu_memory_gib=placement.min_gpu_memory_mib / 1024,
            default_max_model_len=artifact.default_max_model_len,
            layer_count=artifact.layer_count,
        ),
        image=runtime.image,
        pipeline_parallel_size=placement.pipeline_parallel_size,
        tensor_parallel_size=placement.tensor_parallel_size,
        ray_head_node_id=ordered_profiles[0].node_id,
        ray_head_address=f"{head_ip}:6379",
        network_interface=_network_interface(ordered_profiles),
        model_revision=artifact.revision,
        model_path=(
            "/var/lib/dure/models/stages"
            if model_cache_kind == MODEL_CACHE_KIND_STAGE
            else f"/var/lib/dure/models/sha256-{artifact.manifest_digest.removeprefix('sha256:')}"
            if multi_node
            else f"/var/lib/dure/models/{artifact.model_id}--{artifact.revision}"
        ),
        assignments=assignments,
        max_model_len=artifact.default_max_model_len,
        warnings=(
            ["Network bandwidth and RTT must be verified before serving traffic"]
            if multi_node
            else []
        ),
        execution_backend=execution_backend,
        runtime_vllm_version=runtime_vllm_version,
        model_cache_kind=model_cache_kind,
        stage_artifact=stage_artifact_binding,
    )
    try:
        return plan.to_dict()
    except ValueError as exc:
        raise RecommendationNotAcceptableError(
            "generated deployment plan violates its execution contract",
            code="GENERATION_PLAN_INVALID",
            details={"reason": str(exc)},
        ) from exc


def _previous_generation(
    session: Session, previous_generation_id: str | None
) -> tuple[Deployment | None, str | None, int]:
    if previous_generation_id is None:
        return None, None, 1
    candidate = session.get(Deployment, previous_generation_id)
    if candidate is None:
        raise RecommendationNotFoundError(
            "previous deployment generation not found",
            code="PREVIOUS_GENERATION_NOT_FOUND",
            details={"previous_generation_id": previous_generation_id},
        )
    lineage_id = candidate.lineage_id or candidate.id
    root = session.scalar(
        select(Deployment)
        .where(Deployment.id == lineage_id)
        .with_for_update()
    )
    if root is None or (root.lineage_id or root.id) != lineage_id:
        raise RecommendationGenerationConflictError(
            "previous generation lineage root is invalid",
            code="PREVIOUS_GENERATION_LINEAGE_INVALID",
            details={"previous_generation_id": previous_generation_id},
        )
    previous = (
        root
        if root.id == previous_generation_id
        else session.scalar(
            select(Deployment)
            .where(Deployment.id == previous_generation_id)
            .with_for_update()
        )
    )
    if previous is None or (previous.lineage_id or previous.id) != lineage_id:
        raise RecommendationGenerationConflictError(
            "previous generation changed while its lineage was locked",
            code="PREVIOUS_GENERATION_LINEAGE_INVALID",
            details={"previous_generation_id": previous_generation_id},
        )
    if previous.status == "ROLLED_BACK":
        raise RecommendationGenerationConflictError(
            "a rolled-back generation cannot continue its old lineage",
            code="PREVIOUS_GENERATION_ROLLED_BACK",
            details={"previous_generation_id": previous_generation_id},
        )
    active_operation = session.scalar(
        select(DeploymentOperation)
        .where(DeploymentOperation.active_lineage_id == lineage_id)
    )
    if active_operation is not None:
        raise RecommendationGenerationConflictError(
            "deployment lineage has an active operation",
            code="DEPLOYMENT_OPERATION_ACTIVE",
            details={"operation_id": active_operation.id},
        )
    active_task_id = session.scalar(
        select(Task.id)
        .join(Deployment, Deployment.id == Task.deployment_id)
        .where(
            Deployment.lineage_id == lineage_id,
            Task.type.in_(
                {
                    TaskType.APPLY_DEPLOYMENT.value,
                    TaskType.START_DEPLOYMENT.value,
                    TaskType.STOP_DEPLOYMENT.value,
                    TaskType.RESTART_DEPLOYMENT.value,
                    TaskType.PREPARE_MODEL.value,
                    TaskType.PREPARE_IMAGE.value,
                }
            ),
            Task.status.in_(
                {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
            ),
        )
        .order_by(Task.created_at, Task.id)
        .limit(1)
    )
    if active_task_id is not None:
        raise RecommendationGenerationConflictError(
            "deployment lineage has a queued or running mutation",
            code="DEPLOYMENT_MUTATION_ACTIVE",
            details={"task_id": active_task_id},
        )
    latest = session.scalar(
        select(Deployment)
        .where(Deployment.lineage_id == lineage_id)
        .order_by(Deployment.generation.desc(), Deployment.id.desc())
        .with_for_update()
    )
    if latest is None or latest.id != previous.id:
        raise RecommendationGenerationConflictError(
            "previous generation is not the latest generation in its lineage",
            code="PREVIOUS_GENERATION_NOT_LATEST",
            details={
                "previous_generation_id": previous.id,
                "latest_generation_id": latest.id if latest else None,
            },
        )
    return previous, lineage_id, previous.generation + 1


def accept_deployment_recommendation(
    session: Session,
    recommendation_id: str,
    *,
    previous_generation_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    recommendation_record = session.scalar(
        select(DeploymentRecommendationRecord)
        .where(DeploymentRecommendationRecord.id == recommendation_id)
        .with_for_update()
    )
    if recommendation_record is None:
        raise RecommendationNotFoundError(
            details={"recommendation_id": recommendation_id}
        )
    _validate_stored_record(recommendation_record)
    existing = session.scalar(
        select(Deployment)
        .where(Deployment.source_recommendation_id == recommendation_id)
        .with_for_update()
    )
    if existing is not None:
        if existing.previous_generation_id != previous_generation_id:
            raise RecommendationGenerationConflictError(
                "recommendation was accepted with a different previous generation",
                code="RECOMMENDATION_ALREADY_ACCEPTED",
                details={
                    "deployment_id": existing.id,
                    "previous_generation_id": existing.previous_generation_id,
                },
            )
        return {"deployment": deployment_generation_dict(existing), "created": False}

    _lock_recommendation_inputs(session, recommendation_record)
    try:
        current, inventory_snapshot = evaluate_deployment_recommendation(
            session,
            node_ids=(
                list(recommendation_record.requested_node_ids)
                if recommendation_record.selection_mode == "explicit_nodes"
                else []
            ),
            all_online=recommendation_record.selection_mode == "all_online",
            objective=recommendation_record.objective,
            now=now,
        )
    except RecommendationNodeNotFoundError as exc:
        raise RecommendationStaleError(
            "recommendation node inventory no longer exists",
            details=exc.details,
        ) from exc
    current_snapshot = current["recommendation"]
    expected_snapshot = recommendation_record.recommendation_snapshot
    if (
        current_snapshot != expected_snapshot
        or inventory_snapshot != recommendation_record.inventory_snapshot
    ):
        changed_fields = [
            field
            for field in (
                "catalog_version",
                "policy_version",
                "inventory_fingerprint",
                "selected",
                "requested_node_ids",
            )
            if current_snapshot.get(field) != expected_snapshot.get(field)
        ]
        if inventory_snapshot != recommendation_record.inventory_snapshot:
            changed_fields.append("inventory_snapshot")
        raise RecommendationStaleError(
            "recommendation no longer matches current registry and inventory",
            details={
                "recommendation_id": recommendation_id,
                "changed_fields": changed_fields,
                "expected_inventory_fingerprint": recommendation_record.inventory_fingerprint,
                "current_inventory_fingerprint": current_snapshot.get(
                    "inventory_fingerprint"
                ),
            },
        )

    selected = current_snapshot.get("selected")
    selected_node_ids = (
        selected.get("node_ids") if type(selected) is dict else None
    )
    if (
        type(selected_node_ids) is list
        and selected_node_ids
        and all(type(node_id) is str for node_id in selected_node_ids)
    ):
        normalized_selected_node_ids = sorted(set(selected_node_ids))
        locked_selected_nodes = list(
            session.scalars(
                select(Node)
                .where(Node.id.in_(normalized_selected_node_ids))
                .order_by(Node.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        if (
            [node.id for node in locked_selected_nodes]
            != normalized_selected_node_ids
        ):
            raise RecommendationStaleError(
                "selected recommendation node inventory no longer exists",
                details={"node_ids": normalized_selected_node_ids},
            )
        active_quarantine = session.execute(
            select(Task.id, Task.node_id)
            .where(
                Task.node_id.in_(normalized_selected_node_ids),
                Task.type == TaskType.QUARANTINE_ARTIFACT_CACHE.value,
                Task.status.in_(
                    {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
                ),
            )
            .order_by(Task.created_at, Task.id)
            .limit(1)
        ).one_or_none()
        if active_quarantine is not None:
            raise RecommendationNotAcceptableError(
                "selected node has an active artifact cache quarantine",
                code="RECOMMENDATION_NODE_BUSY",
                details={
                    "node_id": active_quarantine.node_id,
                    "task_id": active_quarantine.id,
                    "task_type": TaskType.QUARANTINE_ARTIFACT_CACHE.value,
                },
            )

    previous, lineage_id, generation = _previous_generation(
        session, previous_generation_id
    )
    deployment_id = str(
        uuid.uuid5(GENERATION_NAMESPACE, f"deployment-generation:{recommendation_id}")
    )
    if lineage_id is None:
        lineage_id = deployment_id
    plan = _build_generation_plan(
        session,
        recommendation=current_snapshot,
        deployment_id=deployment_id,
        generation=generation,
    )
    deployment = Deployment(
        id=deployment_id,
        lineage_id=lineage_id,
        previous_generation_id=previous.id if previous else None,
        source_recommendation_id=recommendation_id,
        generation=generation,
        plan=plan,
        accept_model_download=False,
        pull_image=False,
        status="CREATED",
    )
    session.add(deployment)
    try:
        session.flush()
        session.add(
            AuditEvent(
                actor="admin",
                action="recommendation.accept",
                target=deployment.id,
                outcome="success",
                detail={
                    "recommendation_id": recommendation_id,
                    "previous_generation_id": previous_generation_id,
                    "generation": generation,
                },
            )
        )
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        existing = session.scalar(
            select(Deployment).where(
                Deployment.source_recommendation_id == recommendation_id
            )
        )
        if existing is not None and existing.previous_generation_id == previous_generation_id:
            return {
                "deployment": deployment_generation_dict(existing),
                "created": False,
            }
        raise RecommendationGenerationConflictError(
            "deployment generation could not be created concurrently",
            details={"recommendation_id": recommendation_id},
        ) from exc
    return {"deployment": deployment_generation_dict(deployment), "created": True}
