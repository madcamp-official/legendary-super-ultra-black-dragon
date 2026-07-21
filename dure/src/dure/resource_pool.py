from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Iterable, Mapping

from .models import GPUProfile
from .selector import InventoryNode, inventory_fingerprint


FLEET_MODEL_IDS = frozenset(
    {
        "qwen2.5-7b-awq",
        "qwen2.5-14b-awq",
        "qwen2.5-32b-awq",
        "qwen2.5-72b-awq",
    }
)
FLEET_TENSOR_PARALLEL_SIZE = 1


@dataclass(frozen=True)
class GpuSlot:
    node_id: str
    gpu_index: int
    gpu_uuid: str
    name: str
    memory_mib: int
    compute_capability: str | None
    driver_version: str

    @classmethod
    def from_profile(cls, node_id: str, gpu: GPUProfile) -> "GpuSlot":
        return cls(
            node_id=node_id,
            gpu_index=gpu.index,
            gpu_uuid=gpu.uuid,
            name=gpu.name,
            memory_mib=gpu.memory_mib,
            compute_capability=gpu.compute_capability,
            driver_version=gpu.driver_version,
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class GpuPoolNode:
    node_id: str
    selected_gpu: GpuSlot | None
    unavailable_reason: str | None
    occupancy_reason: str | None
    profile_fingerprint: str | None
    host_architecture: str | None
    disk_free_mib: int | None
    runtime_engine: str | None
    runtime_ready: bool
    nvidia_runtime: bool
    network_verified: bool
    network_zone: str | None
    network_interface: str | None
    network_addresses: tuple[str, ...]
    cached_model_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "selected_gpu": (
                self.selected_gpu.to_dict()
                if self.selected_gpu is not None
                else None
            ),
            "unavailable_reason": self.unavailable_reason,
            "occupancy_reason": self.occupancy_reason,
            "profile_fingerprint": self.profile_fingerprint,
            "host_architecture": self.host_architecture,
            "disk_free_mib": self.disk_free_mib,
            "runtime_engine": self.runtime_engine,
            "runtime_ready": self.runtime_ready,
            "nvidia_runtime": self.nvidia_runtime,
            "network_verified": self.network_verified,
            "network_zone": self.network_zone,
            "network_interface": self.network_interface,
            "network_addresses": list(self.network_addresses),
            "cached_model_ids": list(self.cached_model_ids),
        }


@dataclass(frozen=True)
class GpuPoolSnapshot:
    inventory_fingerprint: str
    nodes: tuple[GpuPoolNode, ...]

    @property
    def selected_slots(self) -> tuple[GpuSlot, ...]:
        return tuple(
            node.selected_gpu
            for node in self.nodes
            if node.selected_gpu is not None
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "inventory_fingerprint": self.inventory_fingerprint,
            "nodes": [node.to_dict() for node in self.nodes],
        }


def _profile_fingerprint(value: dict[str, object]) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _select_gpu(node: InventoryNode) -> tuple[GpuSlot | None, str | None]:
    if not node.approved:
        return None, "NODE_PENDING"
    if not node.online:
        return None, "NODE_OFFLINE"
    if node.profile is None:
        return None, (
            "PROFILE_INVALID" if node.profile_error == "invalid" else "PROFILE_MISSING"
        )
    if not node.profile_fresh:
        return None, "PROFILE_STALE"
    runtime = node.profile.runtime
    if (
        runtime.engine != "docker"
        or not runtime.engine_ready
        or not runtime.nvidia_runtime
    ):
        return None, "RUNTIME_UNAVAILABLE"
    healthy = [gpu for gpu in node.profile.gpus if gpu.healthy]
    if not healthy:
        return None, "GPU_UNAVAILABLE"
    valid = [
        gpu
        for gpu in healthy
        if type(gpu.index) is int
        and gpu.index >= 0
        and type(gpu.uuid) is str
        and gpu.uuid.startswith("GPU-")
        and len(gpu.uuid) <= 128
        and type(gpu.memory_mib) is int
        and gpu.memory_mib > 0
    ]
    if not valid:
        return None, "GPU_IDENTITY_INVALID"
    indexes = Counter(gpu.index for gpu in valid)
    uuids = Counter(gpu.uuid for gpu in valid)
    if any(count > 1 for count in indexes.values()) or any(
        count > 1 for count in uuids.values()
    ):
        return None, "GPU_IDENTITY_DUPLICATE"
    selected = min(
        valid,
        key=lambda item: (-item.memory_mib, item.uuid, item.index),
    )
    return GpuSlot.from_profile(node.node_id, selected), None


def build_gpu_pool_snapshot(
    nodes: Iterable[InventoryNode],
    *,
    occupied_node_ids: Iterable[str] = (),
    occupancy_reasons: Mapping[str, str] | None = None,
    network_zones: Mapping[str, str] | None = None,
) -> GpuPoolSnapshot:
    """Normalize an unbounded inventory into at most one selected GPU per node."""

    normalized = list(nodes)
    node_ids = [node.node_id for node in normalized]
    duplicates = sorted(
        node_id for node_id, count in Counter(node_ids).items() if count > 1
    )
    if duplicates:
        raise ValueError(f"duplicate inventory node(s): {', '.join(duplicates)}")
    occupied = set(occupied_node_ids)
    if any(type(node_id) is not str for node_id in occupied):
        raise ValueError("occupied node IDs must be strings")
    reason_by_node = dict(occupancy_reasons or {})
    zone_by_node = dict(network_zones or {})
    if any(
        type(key) is not str or type(value) is not str
        for key, value in reason_by_node.items()
    ):
        raise ValueError("occupancy reasons must map node ID strings to strings")
    if any(
        type(key) is not str or type(value) is not str
        for key, value in zone_by_node.items()
    ):
        raise ValueError("network zones must map node ID strings to strings")

    pool_nodes: list[GpuPoolNode] = []
    for node in sorted(normalized, key=lambda item: item.node_id):
        slot, unavailable_reason = _select_gpu(node)
        occupancy_reason = None
        if node.node_id in occupied:
            slot = None
            unavailable_reason = "NODE_OCCUPIED"
            occupancy_reason = reason_by_node.get(
                node.node_id, "RESERVED_OR_ACTIVE_WORK"
            )
        profile = node.profile
        cached_model_ids = (
            tuple(
                sorted(
                    {
                        model.model_id
                        for model in profile.installed_models
                        if model.complete and model.model_id in FLEET_MODEL_IDS
                    }
                )
            )
            if profile is not None
            else ()
        )
        pool_nodes.append(
            GpuPoolNode(
                node_id=node.node_id,
                selected_gpu=slot,
                unavailable_reason=unavailable_reason,
                occupancy_reason=occupancy_reason,
                profile_fingerprint=(
                    _profile_fingerprint(profile.to_dict())
                    if profile is not None
                    else None
                ),
                host_architecture=(
                    profile.architecture if profile is not None else None
                ),
                disk_free_mib=(profile.disk_free_mib if profile is not None else None),
                runtime_engine=(
                    profile.runtime.engine if profile is not None else None
                ),
                runtime_ready=(
                    profile.runtime.engine_ready if profile is not None else False
                ),
                nvidia_runtime=(
                    profile.runtime.nvidia_runtime if profile is not None else False
                ),
                network_verified=node.network_verified,
                network_zone=zone_by_node.get(node.node_id),
                network_interface=(
                    profile.network.default_interface
                    if profile is not None
                    else None
                ),
                network_addresses=(
                    tuple(sorted(set(profile.network.addresses)))
                    if profile is not None
                    else ()
                ),
                cached_model_ids=cached_model_ids,
            )
        )

    return GpuPoolSnapshot(
        inventory_fingerprint="sha256:" + inventory_fingerprint(normalized),
        nodes=tuple(pool_nodes),
    )
