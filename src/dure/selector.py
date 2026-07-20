from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

from .catalog import CatalogEntry, ModelCatalog, STATIC_CATALOG
from .models import GPUProfile, NodeProfile


@dataclass(frozen=True)
class InventoryNode:
    profile: NodeProfile
    approved: bool = True
    online: bool = True
    profile_fresh: bool = True
    network_verified: bool = False


@dataclass(frozen=True)
class Rejection:
    code: str
    detail: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateEvaluation:
    model_id: str
    placement_profile_id: str
    quality_rank: int
    feasible: bool
    node_ids: tuple[str, ...]
    rejections: tuple[Rejection, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "model_id": self.model_id,
            "placement_profile_id": self.placement_profile_id,
            "quality_rank": self.quality_rank,
            "feasible": self.feasible,
            "node_ids": list(self.node_ids),
            "rejections": [item.to_dict() for item in self.rejections],
        }


@dataclass(frozen=True)
class ModelRecommendation:
    catalog_version: str
    policy_version: str
    inventory_fingerprint: str
    selected_model_id: str | None
    selected_node_ids: tuple[str, ...]
    evaluations: tuple[CandidateEvaluation, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "catalog_version": self.catalog_version,
            "policy_version": self.policy_version,
            "inventory_fingerprint": self.inventory_fingerprint,
            "selected_model_id": self.selected_model_id,
            "selected_node_ids": list(self.selected_node_ids),
            "evaluations": [item.to_dict() for item in self.evaluations],
        }


def inventory_fingerprint(nodes: list[InventoryNode]) -> str:
    payload = [
        {
            "node_id": node.profile.node_id,
            "approved": node.approved,
            "online": node.online,
            "profile_fresh": node.profile_fresh,
            "network_verified": node.network_verified,
            "profile": node.profile.to_dict(),
        }
        for node in sorted(nodes, key=lambda item: item.profile.node_id)
    ]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _best_gpu(node: InventoryNode, minimum_mib: int) -> GPUProfile | None:
    eligible = [
        gpu
        for gpu in node.profile.gpus
        if gpu.healthy and gpu.driver_version and gpu.memory_mib >= minimum_mib
    ]
    return max(eligible, key=lambda gpu: (gpu.memory_mib, -gpu.index), default=None)


def _has_cached_model(node: InventoryNode, entry: CatalogEntry) -> bool:
    return any(
        model.complete
        and model.model_id in {entry.model.model_id, entry.model.repository}
        for model in node.profile.installed_models
    )


def _evaluate(entry: CatalogEntry, nodes: list[InventoryNode]) -> CandidateEvaluation:
    placement = entry.placement
    available: list[tuple[InventoryNode, GPUProfile]] = []
    excluded_status: list[str] = []
    insufficient_gpu: list[str] = []
    insufficient_disk: list[str] = []
    missing_runtime: list[str] = []
    missing_network: list[str] = []

    for node in sorted(nodes, key=lambda item: item.profile.node_id):
        node_id = node.profile.node_id
        if not node.approved or not node.online or not node.profile_fresh:
            excluded_status.append(node_id)
            continue
        gpu = _best_gpu(node, placement.min_gpu_memory_mib)
        if gpu is None:
            insufficient_gpu.append(node_id)
            continue
        if node.profile.disk_free_mib < placement.min_disk_free_mib:
            insufficient_disk.append(node_id)
            continue
        runtime = node.profile.runtime
        if (
            placement.requires_engine_ready
            and not runtime.engine_ready
            or placement.requires_nvidia_runtime
            and not runtime.nvidia_runtime
        ):
            missing_runtime.append(node_id)
            continue
        if placement.requires_network_evidence and not node.network_verified:
            missing_network.append(node_id)
            continue
        available.append((node, gpu))

    available.sort(
        key=lambda item: (
            not _has_cached_model(item[0], entry),
            -item[1].memory_mib,
            item[0].profile.node_id,
        )
    )
    selected = tuple(item[0].profile.node_id for item in available[: placement.node_count])
    rejections: list[Rejection] = []
    if excluded_status:
        rejections.append(
            Rejection("NODE_STATUS", f"승인·온라인·최신 상태가 아닌 노드: {', '.join(excluded_status)}")
        )
    if insufficient_gpu:
        rejections.append(
            Rejection(
                "GPU_MEMORY",
                f"정상 GPU 또는 {placement.min_gpu_memory_mib} MiB VRAM이 부족한 노드: "
                f"{', '.join(insufficient_gpu)}",
            )
        )
    if insufficient_disk:
        rejections.append(
            Rejection(
                "DISK_SPACE",
                f"{placement.min_disk_free_mib} MiB 디스크 여유가 부족한 노드: "
                f"{', '.join(insufficient_disk)}",
            )
        )
    if missing_runtime:
        rejections.append(
            Rejection("RUNTIME", f"Docker/NVIDIA 런타임이 준비되지 않은 노드: {', '.join(missing_runtime)}")
        )
    if missing_network:
        rejections.append(
            Rejection("NETWORK_EVIDENCE", f"네트워크/NCCL 증적이 없는 노드: {', '.join(missing_network)}")
        )
    feasible = len(selected) == placement.node_count
    if not feasible:
        rejections.append(
            Rejection(
                "NODE_COUNT",
                f"적격 노드 {len(available)}개, 필요 노드 {placement.node_count}개",
            )
        )
    else:
        rejections = []
    return CandidateEvaluation(
        model_id=entry.model.model_id,
        placement_profile_id=placement.profile_id,
        quality_rank=entry.quality_rank,
        feasible=feasible,
        node_ids=selected,
        rejections=tuple(rejections),
    )


def recommend_model(
    nodes: list[InventoryNode],
    *,
    catalog: ModelCatalog = STATIC_CATALOG,
    model_id: str | None = None,
) -> ModelRecommendation:
    node_ids = [node.profile.node_id for node in nodes]
    duplicate_ids = sorted({node_id for node_id in node_ids if node_ids.count(node_id) > 1})
    if duplicate_ids:
        raise ValueError(f"duplicate inventory node(s): {', '.join(duplicate_ids)}")

    entries = list(catalog.entries)
    if model_id is not None:
        try:
            entries = [catalog.entry(model_id)]
        except KeyError as exc:
            raise ValueError(f"unknown model: {model_id}") from exc
    entries.sort(key=lambda entry: (-entry.quality_rank, entry.model.model_id))
    evaluations = tuple(_evaluate(entry, nodes) for entry in entries)
    selected = next((item for item in evaluations if item.feasible), None)
    return ModelRecommendation(
        catalog_version=catalog.version,
        policy_version=catalog.policy_version,
        inventory_fingerprint=inventory_fingerprint(nodes),
        selected_model_id=selected.model_id if selected else None,
        selected_node_ids=selected.node_ids if selected else (),
        evaluations=evaluations,
    )
