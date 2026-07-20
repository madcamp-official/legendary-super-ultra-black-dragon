from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dure.catalog import CatalogEntry, ModelCatalog, PlacementProfile
from dure.models import ModelSpec, NodeProfile
from dure.selector import InventoryNode, recommend_model

from .models import (
    ModelArtifact,
    ModelRelease,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    RuntimeRelease,
    utcnow,
)
from .service import aware, node_status


POLICY_VERSION = "central-quality-within-slo-v1"
PROFILE_MAX_AGE = timedelta(seconds=90)


def _content_digest(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _active_catalog(
    session: Session,
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
        context = {
            "candidate_id": candidate_id,
            "model_id": artifact.model_id,
            "model_release_id": release.id,
            "artifact_id": artifact.id,
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
            raise ValueError(f"unknown node(s): {', '.join(missing)}")
    else:
        rows = [
            (node, record)
            for node, record in rows
            if node.approved and node_status(node.last_seen, now) == "online"
        ]

    missing_profiles = sorted(node.id for node, record in rows if record is None)
    if missing_profiles:
        raise ValueError(
            f"stored profile is missing for node(s): {', '.join(missing_profiles)}"
        )

    inventory: list[InventoryNode] = []
    for node, record in rows:
        try:
            profile = NodeProfile.from_dict(record.profile)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid stored profile for node: {node.id}") from exc
        profile_age = now - aware(record.updated_at)
        inventory.append(
            InventoryNode(
                node_id=node.id,
                profile=profile,
                approved=node.approved,
                online=node_status(node.last_seen, now) == "online",
                profile_fresh=timedelta(0) <= profile_age <= PROFILE_MAX_AGE,
                # Network/NCCL evidence is introduced by the benchmark evidence PR.
                # Until then every central multi-node recommendation fails closed.
                network_verified=False,
            )
        )
    return inventory


def recommend_deployment(
    session: Session,
    *,
    node_ids: list[str],
    all_online: bool,
    objective: str = "quality-first",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a deterministic recommendation without persisting or dispatching anything."""
    if objective != "quality-first":
        raise ValueError("unsupported recommendation objective")
    evaluated_at = now or utcnow()
    inventory = _inventory_nodes(
        session,
        node_ids=node_ids,
        all_online=all_online,
        now=evaluated_at,
    )
    catalog, contexts = _active_catalog(session)
    result = recommend_model(inventory, catalog=catalog)

    candidates: list[dict[str, Any]] = []
    for evaluation in result.evaluations:
        candidates.append(
            {
                **contexts[evaluation.candidate_id],
                "quality_rank": evaluation.quality_rank,
                "feasible": evaluation.feasible,
                "node_ids": list(evaluation.node_ids),
                "rejections": [item.to_dict() for item in evaluation.rejections],
            }
        )
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
    }
    return {"recommendation": {"id": _content_digest(core), **core}}
