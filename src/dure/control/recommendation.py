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

from dure.catalog import (
    CatalogEntry,
    ModelCatalog,
    NetworkEvidenceBinding,
    PlacementProfile,
)
from dure.models import DeploymentPlan, ModelSpec, NodeAssignment, NodeProfile
from dure.selector import InventoryNode, recommend_model
from dure.task import (
    TaskStatus,
    TaskType,
    benchmark_inventory_fingerprint,
)

from .benchmark import BENCHMARK_POLICY_VERSION, BENCHMARK_SUITE_ID

from .models import (
    AuditEvent,
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
    RuntimeRelease,
    Task,
    utcnow,
)
from .service import aware, node_status


POLICY_VERSION = "central-quality-within-slo-v2"
PROFILE_MAX_AGE = timedelta(seconds=90)
NETWORK_EVIDENCE_MAX_AGE = timedelta(hours=24)
GENERATION_NAMESPACE = uuid.UUID("74ebf646-2d77-4fcf-8524-1777a274eb93")
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
    if not requires_network:
        return ()

    inventory_by_id = {node.node_id: node for node in inventory}
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
        bindings.append(
            NetworkEvidenceBinding(
                evidence_id=evidence.id,
                evidence_digest=evidence.evidence_digest,
                node_ids=node_set,
                registered_at=registered_at.isoformat(),
            )
        )
    return tuple(
        sorted(
            bindings,
            key=lambda item: (item.node_ids, item.evidence_digest, item.evidence_id),
        )
    )


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
        .where(ModelRelease.status == "ACTIVE")
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
        candidate_id = f"{release.id}:{placement.id}"
        network_evidence = _network_evidence_bindings(
            session,
            release=release,
            artifact=artifact,
            runtime=runtime,
            placement=placement,
            inventory=inventory,
            now=now,
        )
        context = {
            "candidate_id": candidate_id,
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
        contexts[candidate_id] = context
        snapshot.append(
            {
                **context,
                "artifact": {
                    "repository": artifact.repository,
                    "revision": artifact.revision,
                    "manifest_digest": artifact.manifest_digest,
                    "quantization": artifact.quantization,
                    "size_mib": artifact.size_mib,
                    "default_max_model_len": artifact.default_max_model_len,
                    "layer_count": artifact.layer_count,
                },
                "runtime": {
                    "version": runtime.version,
                    "vllm_version": runtime.vllm_version,
                    "cuda_version": runtime.cuda_version,
                    "gpu_architectures": sorted(runtime.gpu_architectures),
                },
                "release": {
                    "status": release.status,
                    "quality_rank": release.quality_rank,
                },
                "placement": {
                    "topology": placement.topology,
                    "node_count": placement.node_count,
                    "min_gpu_memory_mib": placement.min_gpu_memory_mib,
                    "min_disk_free_mib": placement.min_disk_free_mib,
                    "pipeline_parallel_size": placement.pipeline_parallel_size,
                    "tensor_parallel_size": placement.tensor_parallel_size,
                    "requires_network_evidence": placement.requires_network_evidence,
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
                },
                "network_evidence": [
                    {
                        "evidence_id": item.evidence_id,
                        "evidence_digest": item.evidence_digest,
                        "node_ids": list(item.node_ids),
                        "registered_at": item.registered_at,
                    }
                    for item in network_evidence
                ],
            }
        )
        entries.append(
            CatalogEntry(
                model=ModelSpec(
                    model_id=artifact.model_id,
                    repository=artifact.repository,
                    quantization=artifact.quantization,
                    checkpoint_gib=artifact.size_mib / 1024,
                    min_gpu_memory_gib=placement.min_gpu_memory_mib / 1024,
                    default_max_model_len=artifact.default_max_model_len,
                    layer_count=artifact.layer_count,
                ),
                placement=PlacementProfile(
                    profile_id=placement.profile_id,
                    node_count=placement.node_count,
                    min_gpu_memory_mib=placement.min_gpu_memory_mib,
                    min_disk_free_mib=placement.min_disk_free_mib,
                    pipeline_parallel_size=placement.pipeline_parallel_size,
                    tensor_parallel_size=placement.tensor_parallel_size,
                    requires_network_evidence=(
                        placement.requires_network_evidence or placement.node_count > 1
                    ),
                ),
                quality_rank=release.quality_rank,
                artifact_revision=artifact.revision,
                candidate_id=candidate_id,
                gpu_architectures=tuple(sorted(runtime.gpu_architectures)),
                network_evidence=network_evidence,
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
            )
        )
    return inventory


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
    for evaluation in result.evaluations:
        candidate = {
            **contexts[evaluation.candidate_id],
            "quality_rank": evaluation.quality_rank,
            "feasible": evaluation.feasible,
            "node_ids": list(evaluation.node_ids),
            "rejections": [item.to_dict() for item in evaluation.rejections],
        }
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
        ),
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
                "LOCK TABLE model_artifacts, model_releases, nodes, "
                "node_profiles, placement_profiles, runtime_releases IN SHARE MODE"
            )
        )
        return
    list(
        session.scalars(
            select(ModelRelease).order_by(ModelRelease.id).with_for_update()
        )
    )
    node_statement = select(Node).order_by(Node.id)
    profile_statement = select(NodeProfileRecord).order_by(NodeProfileRecord.node_id)
    if record.selection_mode == "explicit_nodes":
        node_statement = node_statement.where(Node.id.in_(record.requested_node_ids))
        profile_statement = profile_statement.where(
            NodeProfileRecord.node_id.in_(record.requested_node_ids)
        )
    list(session.scalars(node_statement.with_for_update()))
    list(session.scalars(profile_statement.with_for_update()))


def _best_gpu_index(profile: NodeProfile, minimum_mib: int) -> int:
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
    return max(eligible, key=lambda item: (item.memory_mib, -item.index)).index


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
    if any(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,31}", value) is None for value in interfaces):
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
        or len(node_ids) != len(set(node_ids))
        or len(node_ids) != placement.node_count
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
    assignments = [
        NodeAssignment(
            node_id=profile.node_id,
            gpu_index=_best_gpu_index(profile, placement.min_gpu_memory_mib),
            rank=rank,
            pipeline_rank=rank,
            layer_start=partitions[rank][0],
            layer_end=partitions[rank][1],
            role="ray-head" if rank == 0 else "ray-worker",
        )
        for rank, profile in enumerate(profiles)
    ]
    head_ip = _ray_head_ip(profiles[0], multi_node=len(profiles) > 1)
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
        ray_head_node_id=profiles[0].node_id,
        ray_head_address=f"{head_ip}:6379",
        network_interface=_network_interface(profiles),
        model_revision=artifact.revision,
        model_path=(
            f"/var/lib/dure/models/{artifact.model_id}--{artifact.revision}"
        ),
        assignments=assignments,
        max_model_len=artifact.default_max_model_len,
        warnings=(
            ["Network bandwidth and RTT must be verified before serving traffic"]
            if len(profiles) > 1
            else []
        ),
    )
    return plan.to_dict()


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
