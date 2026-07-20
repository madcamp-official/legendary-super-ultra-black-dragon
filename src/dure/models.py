from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class GPUProfile:
    index: int
    name: str
    uuid: str
    driver_version: str
    memory_mib: int
    compute_capability: str | None = None
    healthy: bool = True

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "GPUProfile":
        return cls(**value)


@dataclass
class NetworkProfile:
    default_interface: str | None = None
    addresses: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "NetworkProfile":
        return cls(**value)


@dataclass
class RuntimeProfile:
    engine: str | None = None
    engine_ready: bool = False
    nvidia_runtime: bool = False
    ray_available: bool = False
    ray_version: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeProfile":
        return cls(**value)


@dataclass
class InstalledModelProfile:
    source: str
    model_id: str
    path: str | None = None
    revision: str | None = None
    quantization: str | None = None
    size_mib: int | None = None
    complete: bool = True

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "InstalledModelProfile":
        return cls(**value)


@dataclass
class WorkloadProfile:
    name: str
    runtime: str
    image: str
    status: str
    deployment_id: str | None = None
    generation: str | None = None
    model_id: str | None = None
    dure_managed: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "WorkloadProfile":
        return cls(**value)


@dataclass
class NodeProfile:
    node_id: str
    hostname: str
    os_name: str
    os_version: str
    kernel: str
    architecture: str
    virtualization: str | None
    cpu_model: str
    cpu_count: int
    memory_mib: int
    memory_available_mib: int
    swap_mib: int
    disk_total_mib: int
    disk_free_mib: int
    gpus: list[GPUProfile]
    network: NetworkProfile
    runtime: RuntimeProfile
    installed_models: list[InstalledModelProfile] = field(default_factory=list)
    workloads: list[WorkloadProfile] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)

    @property
    def total_gpu_memory_mib(self) -> int:
        return sum(gpu.memory_mib for gpu in self.gpus if gpu.healthy)

    @property
    def has_healthy_gpu(self) -> bool:
        return any(gpu.healthy for gpu in self.gpus)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "NodeProfile":
        data = dict(value)
        data["gpus"] = [GPUProfile.from_dict(item) for item in data.get("gpus", [])]
        data["network"] = NetworkProfile.from_dict(data.get("network", {}))
        data["runtime"] = RuntimeProfile.from_dict(data.get("runtime", {}))
        data["installed_models"] = [
            InstalledModelProfile.from_dict(item) for item in data.get("installed_models", [])
        ]
        data["workloads"] = [
            WorkloadProfile.from_dict(item) for item in data.get("workloads", [])
        ]
        return cls(**data)


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    repository: str
    quantization: str
    checkpoint_gib: float
    min_gpu_memory_gib: float
    default_max_model_len: int
    layer_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class NodeAssignment:
    node_id: str
    gpu_index: int
    rank: int
    pipeline_rank: int
    layer_start: int
    layer_end: int
    role: str = "ray-worker"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "NodeAssignment":
        return cls(**value)


@dataclass
class DeploymentPlan:
    deployment_id: str
    generation: int
    model: ModelSpec
    image: str
    pipeline_parallel_size: int
    tensor_parallel_size: int
    ray_head_node_id: str
    ray_head_address: str
    network_interface: str
    model_revision: str | None
    model_path: str
    assignments: list[NodeAssignment]
    gpu_memory_utilization: float = 0.90
    max_model_len: int = 8192
    warnings: list[str] = field(default_factory=list)

    @property
    def world_size(self) -> int:
        return self.pipeline_parallel_size * self.tensor_parallel_size

    def assignment_for(self, node_id: str) -> NodeAssignment | None:
        return next((item for item in self.assignments if item.node_id == node_id), None)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["world_size"] = self.world_size
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DeploymentPlan":
        data = dict(value)
        data.pop("world_size", None)
        data["model"] = ModelSpec(**data["model"])
        data["assignments"] = [NodeAssignment.from_dict(item) for item in data["assignments"]]
        return cls(**data)


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str
    blocking: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
