from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

from .catalog import CatalogEntry, ModelCatalog, NetworkEvidenceBinding, STATIC_CATALOG
from .models import GPUProfile, NodeProfile


_MIB = 1024 * 1024
_STAGE_CACHE_RESERVE_BYTES = 64 * _MIB


@dataclass(frozen=True)
class InventoryNode:
    node_id: str
    profile: NodeProfile | None
    approved: bool
    online: bool
    profile_fresh: bool
    network_verified: bool
    profile_error: str | None = None
    agent_version: str | None = None

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
    network_evidence_id: str | None = None
    network_evidence_digest: str | None = None
    network_evidence_registered_at: str | None = None
    rank_node_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "candidate_id": self.candidate_id,
            "model_id": self.model_id,
            "placement_profile_id": self.placement_profile_id,
            "quality_rank": self.quality_rank,
            "feasible": self.feasible,
            "node_ids": list(self.node_ids),
            "rejections": [item.to_dict() for item in self.rejections],
        }
        if self.network_evidence_id is not None:
            result.update(
                {
                    "network_evidence_id": self.network_evidence_id,
                    "network_evidence_digest": self.network_evidence_digest,
                    "network_evidence_registered_at": (
                        self.network_evidence_registered_at
                    ),
                }
            )
        if self.rank_node_ids:
            result["rank_node_ids"] = list(self.rank_node_ids)
        return result


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
            "profile_error": node.profile_error,
            "agent_version": node.agent_version,
            "profile": canonical(node.profile.to_dict()) if node.profile else None,
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
    return min(
        eligible,
        key=lambda gpu: (-gpu.memory_mib, gpu.uuid, gpu.index),
        default=None,
    )


def _has_cached_model(node: InventoryNode, entry: CatalogEntry) -> bool:
    if entry.artifact_revision is None or node.profile is None:
        return False
    return any(
        model.complete
        and model.model_id in {entry.model.model_id, entry.model.repository}
        and model.revision == entry.artifact_revision
        and model.quantization == entry.model.quantization
        for model in node.profile.installed_models
    )


def _gpu_architecture(compute_capability: str | None) -> str | None:
    if compute_capability is None:
        return None
    try:
        major, minor = (int(part) for part in compute_capability.split(".", 1))
    except (TypeError, ValueError):
        return None
    if major >= 10:
        return "blackwell"
    if major == 9:
        return "hopper"
    if major == 8 and minor >= 9:
        return "ada"
    if major == 8:
        return "ampere"
    return None


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


def _select_exact_network_evidence(
    entry: CatalogEntry,
    available: list[tuple[InventoryNode, GPUProfile]],
) -> tuple[
    NetworkEvidenceBinding | None,
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
]:
    """Choose one complete evidence-bound node set without combining sets."""
    placement = entry.placement
    ranking = {item[0].node_id: index for index, item in enumerate(available)}
    available_by_id = {item[0].node_id: item[0] for item in available}
    choices: list[
        tuple[
            tuple[object, ...],
            NetworkEvidenceBinding,
            tuple[str, ...],
            tuple[str, ...],
            tuple[str, ...],
        ]
    ] = []
    for binding in entry.network_evidence:
        node_ids = tuple(sorted(binding.node_ids))
        if len(node_ids) != placement.node_count or len(set(node_ids)) != len(node_ids):
            continue
        if any(node_id not in ranking for node_id in node_ids):
            continue
        rank_node_ids = binding.rank_node_ids or node_ids
        if (
            len(rank_node_ids) != placement.node_count
            or len(set(rank_node_ids)) != len(rank_node_ids)
            or set(rank_node_ids) != set(node_ids)
        ):
            rank_node_ids = ()
        disk_failures: tuple[str, ...] = ()
        if entry.stage_artifact is not None and rank_node_ids:
            by_rank = {
                item.rank: item for item in entry.stage_artifact.ranks
            }
            failed: list[str] = []
            for rank, node_id in enumerate(rank_node_ids):
                stage = by_rank.get(rank)
                if stage is None:
                    failed.append(node_id)
                    continue
                required_bytes = (
                    stage.total_size_bytes * 2 + _STAGE_CACHE_RESERVE_BYTES
                )
                profile = available_by_id[node_id].profile
                if (
                    profile is None
                    or profile.disk_free_mib * _MIB < required_bytes
                ):
                    failed.append(node_id)
            disk_failures = tuple(sorted(failed))
        choices.append(
            (
                (
                    bool(disk_failures),
                    tuple(sorted(ranking[node_id] for node_id in node_ids)),
                    node_ids,
                    binding.evidence_id,
                    binding.evidence_digest,
                    binding.registered_at,
                ),
                binding,
                node_ids,
                rank_node_ids,
                disk_failures,
            )
        )
    if not choices:
        return None, (), (), ()
    _, binding, node_ids, rank_node_ids, disk_failures = min(
        choices, key=lambda item: item[0]
    )
    selected = tuple(sorted(node_ids, key=ranking.__getitem__))
    return binding, selected, rank_node_ids, disk_failures


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
    missing_profile: list[str] = []
    invalid_profile: list[str] = []
    stale: list[str] = []
    insufficient_gpu: list[str] = []
    missing_driver: list[str] = []
    unsupported_compute: list[str] = []
    unsupported_runtime_arch: list[str] = []
    insufficient_disk: list[str] = []
    missing_runtime: list[str] = []
    missing_network: list[str] = []
    missing_qualification: list[str] = []
    stage_disk_failures: tuple[str, ...] = ()
    rank_node_ids: tuple[str, ...] = ()
    full_required_bytes = (
        max(
            entry.full_snapshot_size_bytes * 2 + _STAGE_CACHE_RESERVE_BYTES,
            placement.min_disk_free_mib * _MIB,
        )
        if entry.full_snapshot_size_bytes is not None
        else placement.min_disk_free_mib * _MIB
    )

    for node in sorted(nodes, key=lambda item: item.node_id):
        node_id = node.node_id
        if not node.approved:
            pending.append(node_id)
            continue
        if not node.online:
            offline.append(node_id)
            continue
        if node.profile is None:
            if node.profile_error == "invalid":
                invalid_profile.append(node_id)
            else:
                missing_profile.append(node_id)
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
        if (
            entry.gpu_architectures
            and _gpu_architecture(gpu.compute_capability) not in entry.gpu_architectures
        ):
            unsupported_runtime_arch.append(node_id)
            continue
        if (
            entry.stage_artifact is None
            and node.profile.disk_free_mib * _MIB < full_required_bytes
        ):
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
        available.append((node, gpu))

    available.sort(
        key=lambda item: (
            not _has_cached_model(item[0], entry),
            -item[1].memory_mib,
            item[0].node_id,
        )
    )
    selected_evidence = None
    if placement.requires_qualification_evidence:
        if entry.network_evidence:
            (
                selected_evidence,
                selected,
                rank_node_ids,
                stage_disk_failures,
            ) = _select_exact_network_evidence(entry, available)
            network_eligible = available if selected_evidence is not None else []
        else:
            selected = ()
            network_eligible = []
        if selected_evidence is None:
            missing_qualification.extend(item[0].node_id for item in available)
    elif not placement.requires_network_evidence or allow_unverified_network:
        network_eligible = available
        selected = tuple(
            item[0].node_id for item in network_eligible[: placement.node_count]
        )
    elif entry.network_evidence:
        (
            selected_evidence,
            selected,
            rank_node_ids,
            stage_disk_failures,
        ) = _select_exact_network_evidence(entry, available)
        network_eligible = available if selected_evidence is not None else []
        if selected_evidence is None:
            missing_network.extend(item[0].node_id for item in available)
    else:
        network_eligible = [item for item in available if item[0].network_verified]
        missing_network.extend(
            item[0].node_id for item in available if not item[0].network_verified
        )
        selected = tuple(
            item[0].node_id for item in network_eligible[: placement.node_count]
        )
    rejections: list[Rejection] = []
    if missing_profile:
        rejections.append(
            Rejection(
                "PROFILE_MISSING",
                f"stored profile is missing: {', '.join(missing_profile)}",
                tuple(missing_profile),
            )
        )
    if invalid_profile:
        rejections.append(
            Rejection(
                "PROFILE_INVALID",
                f"stored profile is invalid: {', '.join(invalid_profile)}",
                tuple(invalid_profile),
            )
        )
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
    if unsupported_runtime_arch:
        rejections.append(
            Rejection(
                "RUNTIME_GPU_ARCH",
                "runtime does not support the GPU architecture on node(s): "
                f"{', '.join(unsupported_runtime_arch)}",
                tuple(unsupported_runtime_arch),
            )
        )
    if insufficient_disk:
        full_required_mib = (full_required_bytes + _MIB - 1) // _MIB
        rejections.append(
            Rejection(
                "DISK_SPACE",
                f"{full_required_mib} MiB 디스크 여유가 부족한 노드: "
                f"{', '.join(insufficient_disk)}",
                tuple(insufficient_disk),
            )
        )
    if stage_disk_failures:
        by_rank = {
            node_id: rank for rank, node_id in enumerate(rank_node_ids)
        }
        requirements = []
        stage_ranks = (
            {item.rank: item for item in entry.stage_artifact.ranks}
            if entry.stage_artifact is not None
            else {}
        )
        for node_id in stage_disk_failures:
            rank = by_rank.get(node_id)
            stage = stage_ranks.get(rank)
            if rank is not None and stage is not None:
                required_bytes = (
                    stage.total_size_bytes * 2 + _STAGE_CACHE_RESERVE_BYTES
                )
                requirements.append(
                    f"{node_id}(rank={rank}, required_bytes={required_bytes})"
                )
            else:
                requirements.append(node_id)
        rejections.append(
            Rejection(
                "STAGE_DISK_SPACE",
                "rank별 STAGE 캐시 여유 공간이 부족한 노드: "
                + ", ".join(requirements),
                stage_disk_failures,
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
    if missing_qualification:
        rejections.append(
            Rejection(
                "QUALIFICATION_EVIDENCE",
                "exact qualification node/GPU 결합 증적이 없는 노드: "
                f"{', '.join(missing_qualification)}",
                tuple(missing_qualification),
            )
        )
    feasible = (
        len(selected) == placement.node_count and not stage_disk_failures
    )
    if len(selected) != placement.node_count:
        rejections.append(
            Rejection(
                "NODE_COUNT",
                f"적격 노드 {len(network_eligible)}개, 필요 노드 {placement.node_count}개",
            )
        )
    if feasible:
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
        network_evidence_id=(
            selected_evidence.evidence_id if selected_evidence is not None else None
        ),
        network_evidence_digest=(
            selected_evidence.evidence_digest if selected_evidence is not None else None
        ),
        network_evidence_registered_at=(
            selected_evidence.registered_at if selected_evidence is not None else None
        ),
        rank_node_ids=rank_node_ids,
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
            0 if entry.stage_artifact is not None else 1,
            (
                entry.stage_artifact.artifact_set_digest
                if entry.stage_artifact is not None
                else ""
            ),
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
