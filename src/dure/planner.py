from __future__ import annotations

import re
import time
from dataclasses import replace

from .catalog import STATIC_CATALOG
from .models import DeploymentPlan, ModelSpec, NodeAssignment, NodeProfile
from .selector import InventoryNode, recommend_model


MODELS: dict[str, ModelSpec] = STATIC_CATALOG.models


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
            gpu = max(node_gpus, key=lambda item: item.memory_mib)
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
