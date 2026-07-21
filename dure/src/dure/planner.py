from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass, replace

from .catalog import STATIC_CATALOG
from .models import (
    DeploymentPlan,
    ModelSpec,
    NodeAssignment,
    NodeProfile,
    canonical_private_ipv4,
)
from .selector import InventoryNode, recommend_model


MODELS: dict[str, ModelSpec] = STATIC_CATALOG.models
_NETWORK_INTERFACE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,14}")


class StrictRayPPTopologyError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        reason: str,
        node_ids: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.node_ids = node_ids


@dataclass(frozen=True)
class StrictRayPPNode:
    profile: NodeProfile
    gpu_index: int
    gpu_uuid: str
    runtime_address: str


def _canonical_node_uuid(value: str) -> bool:
    try:
        return str(uuid.UUID(value)) == value
    except (AttributeError, ValueError):
        return False


def _private_runtime_address(profile: NodeProfile) -> str:
    candidates = profile.network.default_interface_addresses
    if not candidates:
        raise StrictRayPPTopologyError(
            "node has no address bound to its default interface",
            reason="DEFAULT_INTERFACE_ADDRESS_REQUIRED",
            node_ids=(profile.node_id,),
        )
    addresses: set[str] = set()
    for value in candidates:
        try:
            addresses.add(canonical_private_ipv4(value))
        except ValueError:
            continue
    if not addresses:
        raise StrictRayPPTopologyError(
            "node has no canonical private IPv4 runtime address",
            reason="PRIVATE_IPV4_REQUIRED",
            node_ids=(profile.node_id,),
        )
    if len(addresses) != 1:
        raise StrictRayPPTopologyError(
            "node has multiple private IPv4 addresses on its default interface",
            reason="PRIVATE_IPV4_AMBIGUOUS",
            node_ids=(profile.node_id,),
        )
    selected = next(iter(addresses))
    if selected not in profile.network.addresses:
        raise StrictRayPPTopologyError(
            "default-interface address is absent from the node address inventory",
            reason="DEFAULT_INTERFACE_ADDRESS_MISMATCH",
            node_ids=(profile.node_id,),
        )
    return selected


def strict_vllm_ray_pp_order(
    profiles: list[NodeProfile],
    *,
    head_node_id: str,
    minimum_gpu_memory_mib: int = 0,
) -> list[StrictRayPPNode]:
    """Bind a strict vLLM 0.9.0 Ray PP node set to deterministic ranks."""
    if len(profiles) not in {2, 3}:
        raise StrictRayPPTopologyError(
            "strict Ray pipeline parallelism requires exactly two or three nodes",
            reason="NODE_COUNT_UNSUPPORTED",
        )
    if type(minimum_gpu_memory_mib) is not int or minimum_gpu_memory_mib < 0:
        raise ValueError("minimum_gpu_memory_mib must be a non-negative integer")
    node_ids = [profile.node_id for profile in profiles]
    if any(not _canonical_node_uuid(node_id) for node_id in node_ids):
        raise StrictRayPPTopologyError(
            "strict Ray pipeline node IDs must be canonical UUIDs",
            reason="NODE_ID_INVALID",
            node_ids=tuple(node_ids),
        )
    if len(node_ids) != len(set(node_ids)):
        raise StrictRayPPTopologyError(
            "strict Ray pipeline node IDs must be unique",
            reason="DUPLICATE_NODE_ID",
            node_ids=tuple(node_ids),
        )
    if head_node_id not in set(node_ids) or not _canonical_node_uuid(head_node_id):
        raise StrictRayPPTopologyError(
            "strict Ray pipeline head must identify exactly one selected node",
            reason="HEAD_NODE_INVALID",
            node_ids=(head_node_id,),
        )

    bindings: list[StrictRayPPNode] = []
    for profile in profiles:
        if (
            type(profile.network.default_interface) is not str
            or _NETWORK_INTERFACE.fullmatch(profile.network.default_interface)
            is None
        ):
            raise StrictRayPPTopologyError(
                "strict Ray pipeline requires an explicit safe default interface",
                reason="DEFAULT_INTERFACE_REQUIRED",
                node_ids=(profile.node_id,),
            )
        healthy_gpus = [gpu for gpu in profile.gpus if gpu.healthy]
        if not healthy_gpus:
            raise StrictRayPPTopologyError(
                "strict Ray pipeline requires a healthy GPU on every selected node",
                reason="HEALTHY_GPU_COUNT",
                node_ids=(profile.node_id,),
            )
        eligible_gpus = [
            gpu
            for gpu in healthy_gpus
            if type(gpu.memory_mib) is int
            and gpu.memory_mib >= minimum_gpu_memory_mib
        ]
        if not eligible_gpus:
            raise StrictRayPPTopologyError(
                "strict Ray pipeline node does not meet the GPU memory requirement",
                reason="GPU_MEMORY_INSUFFICIENT",
                node_ids=(profile.node_id,),
            )
        gpu = min(
            eligible_gpus,
            key=lambda item: (-item.memory_mib, item.uuid, item.index),
        )
        if type(gpu.index) is not int or gpu.index < 0:
            raise StrictRayPPTopologyError(
                "strict Ray pipeline GPU index is invalid",
                reason="GPU_INDEX_INVALID",
                node_ids=(profile.node_id,),
            )
        if (
            type(gpu.uuid) is not str
            or not gpu.uuid.startswith("GPU-")
            or len(gpu.uuid) > 128
        ):
            raise StrictRayPPTopologyError(
                "strict Ray pipeline selected GPU UUID is invalid",
                reason="GPU_UUID_INVALID",
                node_ids=(profile.node_id,),
            )
        bindings.append(
            StrictRayPPNode(
                profile=profile,
                gpu_index=gpu.index,
                gpu_uuid=gpu.uuid,
                runtime_address=_private_runtime_address(profile),
            )
        )

    runtime_addresses = [item.runtime_address for item in bindings]
    if len(runtime_addresses) != len(set(runtime_addresses)):
        duplicate_nodes = tuple(
            sorted(
                item.profile.node_id
                for item in bindings
                if runtime_addresses.count(item.runtime_address) > 1
            )
        )
        raise StrictRayPPTopologyError(
            "strict Ray pipeline runtime addresses must be unique",
            reason="DUPLICATE_RUNTIME_ADDRESS",
            node_ids=duplicate_nodes,
        )

    head = next(item for item in bindings if item.profile.node_id == head_node_id)
    workers = sorted(
        (item for item in bindings if item.profile.node_id != head_node_id),
        key=lambda item: item.runtime_address,
    )
    return [head, *workers]


def _gpu_for(profile: NodeProfile, gpu_index: int):
    return next(gpu for gpu in profile.gpus if gpu.index == gpu_index)


def classify_node(profile: NodeProfile) -> tuple[str, list[str]]:
    capabilities = ["node-agent", "network-probe"]
    if profile.memory_mib >= 3072 and profile.disk_free_mib >= 4096:
        capabilities.extend(["utility-controller", "api-gateway"])
    if profile.disk_free_mib >= 51200:
        capabilities.append("artifact-cache")
    if profile.has_healthy_gpu:
        capabilities.extend(["gpu-worker", "ray-worker"])
        if max(gpu.memory_mib for gpu in profile.gpus) >= 22528:
            capabilities.append("large-model-stage")
        return "gpu-worker", capabilities
    return "utility", capabilities


def recommend_local_model(profile: NodeProfile) -> ModelSpec | None:
    if not profile.has_healthy_gpu:
        return None
    largest_gib = max(gpu.memory_mib for gpu in profile.gpus if gpu.healthy) / 1024
    for key in ("qwen2.5-32b-awq", "qwen2.5-14b-awq", "qwen2.5-7b-awq"):
        model = MODELS[key]
        if largest_gib >= model.min_gpu_memory_gib:
            return model
    return None


def _safe_id(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9-]+", "-", value).strip("-").lower()
    return cleaned or "dure"


def _layer_partitions(layer_count: int, stages: int) -> list[tuple[int, int]]:
    base, extra = divmod(layer_count, stages)
    partitions: list[tuple[int, int]] = []
    cursor = 0
    for stage in range(stages):
        size = base + (1 if stage < extra else 0)
        partitions.append((cursor, cursor + size - 1))
        cursor += size
    return partitions


def build_plan(
    profiles: list[NodeProfile],
    *,
    model_id: str = "auto",
    image: str = "vllm/vllm-openai:latest",
    ray_port: int = 6379,
    network_interface: str | None = None,
) -> DeploymentPlan | None:
    node_ids = [profile.node_id for profile in profiles]
    duplicates = sorted({node_id for node_id in node_ids if node_ids.count(node_id) > 1})
    if duplicates:
        raise ValueError(f"duplicate node profile(s): {', '.join(duplicates)}")

    healthy: list[tuple[NodeProfile, int]] = []
    for profile in profiles:
        node_gpus = [gpu for gpu in profile.gpus if gpu.healthy]
        if node_gpus:
            # The MVP launches one Ray container per node. Prefer the largest GPU
            # until multi-GPU-per-node assignments are implemented explicitly.
            gpu = min(
                node_gpus,
                key=lambda item: (-item.memory_mib, item.uuid, item.index),
            )
            healthy.append((profile, gpu.index))

    if not healthy:
        return None

    if model_id == "auto":
        recommendation = recommend_model(
            [
                InventoryNode.local(profile)
                for profile, _gpu_index in healthy
            ],
            allow_unverified_network=True,
        )
        if recommendation.selected_model_id is None:
            return None
        model = MODELS[recommendation.selected_model_id]
        by_node_id = {item[0].node_id: item for item in healthy}
        selected = [by_node_id[node_id] for node_id in recommendation.selected_node_ids]
    else:
        if model_id not in MODELS:
            raise ValueError(f"unknown model: {model_id}")
        model = MODELS[model_id]
        required_stages = 3 if model_id == "qwen2.5-72b-awq" else 1
        eligible = [
            item
            for item in healthy
            if _gpu_for(item[0], item[1]).memory_mib / 1024 >= model.min_gpu_memory_gib
        ]
        if len(eligible) < required_stages:
            raise ValueError(
                f"{model_id} requires {required_stages} eligible GPU node(s), found {len(eligible)}"
            )
        selected = eligible[:required_stages]

    stages = len(selected)
    partitions = _layer_partitions(model.layer_count, stages)
    head_profile = selected[0][0]
    head_ip = head_profile.network.addresses[0] if head_profile.network.addresses else "127.0.0.1"
    interface = (
        network_interface
        or head_profile.network.default_interface
        or "eth0"
    )
    assignments = []
    for rank, ((profile, gpu_index), (start, end)) in enumerate(zip(selected, partitions)):
        assignments.append(
            NodeAssignment(
                node_id=profile.node_id,
                gpu_index=gpu_index,
                gpu_uuid=_gpu_for(profile, gpu_index).uuid,
                rank=rank,
                pipeline_rank=rank,
                layer_start=start,
                layer_end=end,
                role="ray-head" if rank == 0 else "ray-worker",
            )
        )

    warnings: list[str] = []
    if "@sha256:" not in image:
        warnings.append("Container image is unpinned; use an immutable digest before production")
    driver_versions = {
        _gpu_for(profile, gpu_index).driver_version for profile, gpu_index in selected
    }
    if len(driver_versions) > 1:
        warnings.append("Selected nodes use different NVIDIA driver versions")
    if stages > 1:
        warnings.append("Network bandwidth and RTT must be benchmarked before serving traffic")

    deployment_id = f"{_safe_id(model.model_id)}-{int(time.time())}"
    return DeploymentPlan(
        deployment_id=deployment_id,
        generation=1,
        model=replace(model),
        image=image,
        pipeline_parallel_size=stages,
        tensor_parallel_size=1,
        ray_head_node_id=head_profile.node_id,
        ray_head_address=f"{head_ip}:{ray_port}",
        network_interface=interface,
        model_revision=None,
        model_path=f"/var/lib/dure/models/{model.model_id}",
        assignments=assignments,
        max_model_len=model.default_max_model_len,
        warnings=warnings,
    )
