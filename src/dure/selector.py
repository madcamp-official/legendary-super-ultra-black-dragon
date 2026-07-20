from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

from .catalog import CatalogEntry, ModelCatalog, STATIC_CATALOG
from .models import GPUProfile, NodeProfile


@dataclass(frozen=True)
class InventoryNode:
    node_id: str
    profile: NodeProfile
    approved: bool
    online: bool
    profile_fresh: bool
    network_verified: bool

    @classmethod
    def local(cls, profile: NodeProfile, *, network_verified: bool = False) -> "InventoryNode":
        return cls(
            node_id=profile.node_id,
            profile=profile,
            approved=True,
            online=True,
            profile_fresh=True,
            network_verified=network_verified,
        )


@dataclass(frozen=True)
class Rejection:
    code: str
    detail: str
    node_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {"code": self.code, "detail": self.detail, "node_ids": list(self.node_ids)}


@dataclass(frozen=True)
class CandidateEvaluation:
    candidate_id: str
    model_id: str
    placement_profile_id: str
    quality_rank: int
    feasible: bool
    node_ids: tuple[str, ...]
    rejections: tuple[Rejection, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
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
    selected_candidate_id: str | None
    selected_model_id: str | None
    selected_node_ids: tuple[str, ...]
    evaluations: tuple[CandidateEvaluation, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "catalog_version": self.catalog_version,
            "policy_version": self.policy_version,
            "inventory_fingerprint": self.inventory_fingerprint,
            "selected_candidate_id": self.selected_candidate_id,
            "selected_model_id": self.selected_model_id,
            "selected_node_ids": list(self.selected_node_ids),
            "evaluations": [item.to_dict() for item in self.evaluations],
        }


def inventory_fingerprint(nodes: list[InventoryNode]) -> str:
    def canonical(value):
        if isinstance(value, dict):
            return {key: canonical(value[key]) for key in sorted(value)}
        if isinstance(value, list):
            normalized = [canonical(item) for item in value]
            return sorted(
                normalized,
                key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
            )
        return value

    payload = [
        {
            "node_id": node.node_id,
            "approved": node.approved,
            "online": node.online,
            "profile_fresh": node.profile_fresh,
            "network_verified": node.network_verified,
            "profile": canonical(node.profile.to_dict()),
        }
        for node in sorted(nodes, key=lambda item: item.node_id)
    ]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _best_gpu(node: InventoryNode, minimum_mib: int) -> GPUProfile | None:
    eligible = [
        gpu
        for gpu in node.profile.gpus
        if gpu.healthy and gpu.memory_mib >= minimum_mib
    ]
    return max(eligible, key=lambda gpu: (gpu.memory_mib, -gpu.index), default=None)


def _has_cached_model(node: InventoryNode, entry: CatalogEntry) -> bool:
    if entry.artifact_revision is None:
        return False
    return any(
        model.complete
        and model.model_id in {entry.model.model_id, entry.model.repository}
        and model.revision == entry.artifact_revision
        and model.quantization == entry.model.quantization
        for model in node.profile.installed_models
    )


def _compute_capability_at_least(actual: str | None, minimum: str | None) -> bool:
    if minimum is None:
        return True
    if actual is None:
        return False
    try:
        return tuple(int(part) for part in actual.split(".")) >= tuple(
            int(part) for part in minimum.split(".")
        )
    except ValueError:
        return False


def _evaluate(
    entry: CatalogEntry,
    nodes: list[InventoryNode],
    *,
    allow_unverified_network: bool,
) -> CandidateEvaluation:
    placement = entry.placement
    available: list[tuple[InventoryNode, GPUProfile]] = []
    pending: list[str] = []
    offline: list[str] = []
    stale: list[str] = []
    insufficient_gpu: list[str] = []
    missing_driver: list[str] = []
    unsupported_compute: list[str] = []
    insufficient_disk: list[str] = []
    missing_runtime: list[str] = []
    missing_network: list[str] = []

    for node in sorted(nodes, key=lambda item: item.node_id):
        node_id = node.node_id
        if not node.approved:
            pending.append(node_id)
            continue
        if not node.online:
            offline.append(node_id)
            continue
        if not node.profile_fresh:
            stale.append(node_id)
            continue
        gpu = _best_gpu(node, placement.min_gpu_memory_mib)
        if gpu is None:
            insufficient_gpu.append(node_id)
            continue
        if not gpu.driver_version:
            missing_driver.append(node_id)
            continue
        if not _compute_capability_at_least(
            gpu.compute_capability, placement.min_compute_capability
        ):
            unsupported_compute.append(node_id)
            continue
        if node.profile.disk_free_mib < placement.min_disk_free_mib:
            insufficient_disk.append(node_id)
            continue
        runtime = node.profile.runtime
        if (
            placement.requires_engine_ready
            and not runtime.engine_ready
            or runtime.engine != placement.required_engine
            or placement.requires_nvidia_runtime
            and not runtime.nvidia_runtime
        ):
            missing_runtime.append(node_id)
            continue
        if (
            placement.requires_network_evidence
            and not allow_unverified_network
            and not node.network_verified
        ):
            missing_network.append(node_id)
            continue
        available.append((node, gpu))

    available.sort(
        key=lambda item: (
            not _has_cached_model(item[0], entry),
            -item[1].memory_mib,
            item[0].node_id,
        )
    )
    selected = tuple(item[0].node_id for item in available[: placement.node_count])
    rejections: list[Rejection] = []
    for code, label, node_ids in (
        ("NODE_PENDING", "승인되지 않은 노드", pending),
        ("NODE_OFFLINE", "오프라인 노드", offline),
        ("PROFILE_STALE", "오래된 프로필 노드", stale),
    ):
        if node_ids:
            rejections.append(Rejection(code, f"{label}: {', '.join(node_ids)}", tuple(node_ids)))
    if insufficient_gpu:
        rejections.append(
            Rejection(
                "GPU_MEMORY",
                f"정상 GPU 또는 {placement.min_gpu_memory_mib} MiB VRAM이 부족한 노드: "
                f"{', '.join(insufficient_gpu)}",
                tuple(insufficient_gpu),
            )
        )
    if missing_driver:
        rejections.append(
            Rejection("GPU_DRIVER", f"NVIDIA 드라이버 정보가 없는 노드: {', '.join(missing_driver)}", tuple(missing_driver))
        )
    if unsupported_compute:
        rejections.append(
            Rejection(
                "COMPUTE_CAPABILITY",
                f"최소 compute capability {placement.min_compute_capability}를 충족하지 못한 노드: "
                f"{', '.join(unsupported_compute)}",
                tuple(unsupported_compute),
            )
        )
    if insufficient_disk:
        rejections.append(
            Rejection(
                "DISK_SPACE",
                f"{placement.min_disk_free_mib} MiB 디스크 여유가 부족한 노드: "
                f"{', '.join(insufficient_disk)}",
                tuple(insufficient_disk),
            )
        )
    if missing_runtime:
        rejections.append(
            Rejection("RUNTIME", f"Docker/NVIDIA 런타임이 준비되지 않은 노드: {', '.join(missing_runtime)}", tuple(missing_runtime))
        )
    if missing_network:
        rejections.append(
            Rejection("NETWORK_EVIDENCE", f"네트워크/NCCL 증적이 없는 노드: {', '.join(missing_network)}", tuple(missing_network))
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
        candidate_id=entry.candidate_id
        or f"local:{entry.model.model_id}:{entry.placement.profile_id}",
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
    allow_unverified_network: bool = False,
) -> ModelRecommendation:
    node_ids = [node.node_id for node in nodes]
    duplicate_ids = sorted({node_id for node_id in node_ids if node_ids.count(node_id) > 1})
    if duplicate_ids:
        raise ValueError(f"duplicate inventory node(s): {', '.join(duplicate_ids)}")

    entries = list(catalog.entries)
    if model_id is not None:
        try:
            entries = [catalog.entry(model_id)]
        except KeyError as exc:
            raise ValueError(f"unknown model: {model_id}") from exc
    entries.sort(
        key=lambda entry: (
            -entry.quality_rank,
            entry.model.model_id,
            entry.candidate_id or "",
            entry.placement.profile_id,
        )
    )
    evaluations = tuple(
        _evaluate(entry, nodes, allow_unverified_network=allow_unverified_network)
        for entry in entries
    )
    selected = next((item for item in evaluations if item.feasible), None)
    return ModelRecommendation(
        catalog_version=catalog.version,
        policy_version=catalog.policy_version,
        inventory_fingerprint=inventory_fingerprint(nodes),
        selected_candidate_id=selected.candidate_id if selected else None,
        selected_model_id=selected.model_id if selected else None,
        selected_node_ids=selected.node_ids if selected else (),
        evaluations=evaluations,
    )
