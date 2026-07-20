from __future__ import annotations

import ipaddress
import re
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from .model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
)
from .stage_cache import stage_contract_identity_digest


VLLM_RAY_PP_BACKEND = "VLLM_RAY_PP_V1"
VLLM_RAY_PP_RUNTIME_VERSION = "0.9.0"
VLLM_STAGE_ARCHITECTURE = "Qwen2ForCausalLM"
VLLM_STAGE_LOADER_FORMAT = "VLLM_SHARDED_STATE_V1"
_SHA256_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_PRIVATE_IPV4_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)


def canonical_private_ipv4(value: Any) -> str:
    """Return a canonical RFC1918 IPv4 address or raise ``ValueError``."""
    if type(value) is not str:
        raise ValueError("runtime address must be a string")
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise ValueError("runtime address must be a canonical private IPv4 address") from exc
    if (
        not isinstance(address, ipaddress.IPv4Address)
        or str(address) != value
        or not any(address in network for network in _PRIVATE_IPV4_NETWORKS)
        or address.is_loopback
        or address.is_link_local
        or address.is_unspecified
        or address.is_multicast
        or address.is_reserved
    ):
        raise ValueError("runtime address must be a canonical private IPv4 address")
    return str(address)


def _canonical_uuid(value: Any, *, field_name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{field_name} must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a canonical UUID") from exc
    if str(parsed) != value:
        raise ValueError(f"{field_name} must be a canonical UUID")
    return value


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
    default_interface_addresses: list[str] = field(default_factory=list)

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
    manifest_digest: str | None = None
    cache_kind: str | None = None
    verification_version: int | None = None
    artifact_set_digest: str | None = None
    contract_identity_digest: str | None = None
    source_manifest_digest: str | None = None
    runtime_image: str | None = None
    vllm_version: str | None = None
    exporter_build_digest: str | None = None
    architecture: str | None = None
    loader_format: str | None = None
    tensor_parallel_size: int | None = None
    pipeline_parallel_size: int | None = None
    pipeline_rank: int | None = None
    tensor_rank: int | None = None
    tensor_keys_digest: str | None = None
    cache_identity_digest: str | None = None

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
        value = asdict(self)
        if not self.network.default_interface_addresses:
            value["network"].pop("default_interface_addresses", None)
        stage_profile_fields = {
            "artifact_set_digest",
            "contract_identity_digest",
            "source_manifest_digest",
            "runtime_image",
            "vllm_version",
            "exporter_build_digest",
            "architecture",
            "loader_format",
            "tensor_parallel_size",
            "pipeline_parallel_size",
            "pipeline_rank",
            "tensor_rank",
            "tensor_keys_digest",
            "cache_identity_digest",
        }
        for model in value["installed_models"]:
            for key in stage_profile_fields:
                if model.get(key) is None:
                    model.pop(key, None)
        return value

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


@dataclass(frozen=True)
class StageArtifactBinding:
    """Immutable registry projection consumed by the stage-local runtime."""

    artifact_set_digest: str
    contract_identity_digest: str
    source_manifest_digest: str
    runtime_image: str
    vllm_version: str
    exporter_build_digest: str
    architecture: str
    quantization: str
    tensor_parallel_size: int
    pipeline_parallel_size: int
    loader_format: str

    def validate(self) -> None:
        digests = (
            self.artifact_set_digest,
            self.contract_identity_digest,
            self.source_manifest_digest,
            self.exporter_build_digest,
        )
        if any(
            type(value) is not str or _SHA256_DIGEST.fullmatch(value) is None
            for value in digests
        ):
            raise ValueError("stage artifact identity digests are invalid")
        if (
            type(self.runtime_image) is not str
            or type(self.vllm_version) is not str
            or self.architecture != VLLM_STAGE_ARCHITECTURE
            or self.quantization != "awq"
            or type(self.tensor_parallel_size) is not int
            or type(self.pipeline_parallel_size) is not int
            or self.loader_format != VLLM_STAGE_LOADER_FORMAT
        ):
            raise ValueError("stage artifact loader contract is invalid")
        expected_contract_digest = stage_contract_identity_digest(
            source_manifest_digest=self.source_manifest_digest,
            runtime_image=self.runtime_image,
            vllm_version=self.vllm_version,
            exporter_build_digest=self.exporter_build_digest,
            architecture=self.architecture,
            quantization=self.quantization,
            tensor_parallel_size=self.tensor_parallel_size,
            pipeline_parallel_size=self.pipeline_parallel_size,
            loader_format=self.loader_format,
        )
        if self.contract_identity_digest != expected_contract_digest:
            raise ValueError("stage artifact contract identity digest is inconsistent")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "StageArtifactBinding":
        if type(value) is not dict:
            raise ValueError("stage_artifact must be an object")
        expected = {
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
        }
        if any(type(key) is not str for key in value) or set(value) != expected:
            raise ValueError("stage_artifact does not match the closed wire schema")
        binding = cls(**value)
        binding.validate()
        return binding


@dataclass
class NodeAssignment:
    node_id: str
    gpu_index: int
    rank: int
    pipeline_rank: int
    layer_start: int
    layer_end: int
    role: str = "ray-worker"
    expected_runtime_rank: int | None = None
    runtime_address: str | None = None
    stage_manifest_digest: str | None = None
    stage_tensor_keys_digest: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for key in (
            "expected_runtime_rank",
            "runtime_address",
            "stage_manifest_digest",
            "stage_tensor_keys_digest",
        ):
            if value[key] is None:
                value.pop(key)
        return value

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
    execution_backend: str | None = None
    runtime_vllm_version: str | None = None
    model_cache_kind: str | None = None
    stage_artifact: StageArtifactBinding | None = None

    @property
    def world_size(self) -> int:
        return self.pipeline_parallel_size * self.tensor_parallel_size

    def assignment_for(self, node_id: str) -> NodeAssignment | None:
        return next((item for item in self.assignments if item.node_id == node_id), None)

    def validate_execution_contract(self) -> None:
        strict_assignment_fields = any(
            assignment.expected_runtime_rank is not None
            or assignment.runtime_address is not None
            or assignment.stage_manifest_digest is not None
            or assignment.stage_tensor_keys_digest is not None
            for assignment in self.assignments
        )
        if self.execution_backend is None:
            if (
                self.runtime_vllm_version is not None
                or self.model_cache_kind is not None
                or self.stage_artifact is not None
                or strict_assignment_fields
            ):
                raise ValueError("legacy deployment plan contains strict backend metadata")
            return
        if self.execution_backend != VLLM_RAY_PP_BACKEND:
            raise ValueError(f"unknown execution backend: {self.execution_backend}")
        if self.runtime_vllm_version != VLLM_RAY_PP_RUNTIME_VERSION:
            raise ValueError(
                f"{VLLM_RAY_PP_BACKEND} requires vLLM "
                f"{VLLM_RAY_PP_RUNTIME_VERSION}"
            )
        if self.model_cache_kind not in {
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
            MODEL_CACHE_KIND_STAGE,
        }:
            raise ValueError(
                f"{VLLM_RAY_PP_BACKEND} requires a supported model cache kind"
            )
        if self.model.quantization != "awq":
            raise ValueError(f"{VLLM_RAY_PP_BACKEND} requires AWQ quantization")
        if type(self.tensor_parallel_size) is not int or self.tensor_parallel_size != 1:
            raise ValueError(f"{VLLM_RAY_PP_BACKEND} requires tensor_parallel_size=1")
        if (
            type(self.pipeline_parallel_size) is not int
            or self.pipeline_parallel_size not in {2, 3}
            or self.pipeline_parallel_size != len(self.assignments)
        ):
            raise ValueError(
                f"{VLLM_RAY_PP_BACKEND} requires exactly 2 or 3 "
                "pipeline stages with one stage per node"
            )
        if (
            type(self.model.layer_count) is not int
            or self.model.layer_count < self.pipeline_parallel_size
        ):
            raise ValueError("pipeline stage count exceeds model layer count")

        _canonical_uuid(self.ray_head_node_id, field_name="ray_head_node_id")
        expected_ranks = list(range(self.pipeline_parallel_size))
        stage_mode = self.model_cache_kind == MODEL_CACHE_KIND_STAGE
        if stage_mode:
            if self.stage_artifact is None:
                raise ValueError("STAGE pipeline requires an immutable stage artifact binding")
            self.stage_artifact.validate()
            if (
                self.stage_artifact.runtime_image != self.image
                or self.stage_artifact.vllm_version != self.runtime_vllm_version
                or self.stage_artifact.quantization != self.model.quantization
                or self.stage_artifact.tensor_parallel_size
                != self.tensor_parallel_size
                or self.stage_artifact.pipeline_parallel_size
                != self.pipeline_parallel_size
            ):
                raise ValueError("stage artifact binding does not match the deployment plan")
            if self.model_path != "/var/lib/dure/models/stages":
                raise ValueError("STAGE model_path must be the fixed Dure stage root")
        elif self.stage_artifact is not None or any(
            assignment.stage_manifest_digest is not None
            or assignment.stage_tensor_keys_digest is not None
            for assignment in self.assignments
        ):
            raise ValueError("FULL_SNAPSHOT plan contains STAGE-only metadata")
        if any(type(item.rank) is not int for item in self.assignments):
            raise ValueError("assignment ranks must be integers")
        if any(type(item.pipeline_rank) is not int for item in self.assignments):
            raise ValueError("pipeline ranks must be integers")
        if any(
            type(item.expected_runtime_rank) is not int for item in self.assignments
        ):
            raise ValueError("runtime ranks must be integers")
        if [item.rank for item in self.assignments] != expected_ranks:
            raise ValueError("assignments must be ordered by contiguous rank")
        if [item.pipeline_rank for item in self.assignments] != expected_ranks:
            raise ValueError("pipeline ranks must be contiguous and match assignment rank")
        if [item.expected_runtime_rank for item in self.assignments] != expected_ranks:
            raise ValueError("runtime ranks must be contiguous and match assignment rank")

        node_ids: list[str] = []
        runtime_addresses: list[str] = []
        expected_layer_start = 0
        for expected_rank, assignment in enumerate(self.assignments):
            node_ids.append(
                _canonical_uuid(assignment.node_id, field_name="assignment node_id")
            )
            runtime_addresses.append(canonical_private_ipv4(assignment.runtime_address))
            if type(assignment.gpu_index) is not int or assignment.gpu_index < 0:
                raise ValueError("gpu_index must be a non-negative integer")
            if stage_mode and (
                type(assignment.stage_manifest_digest) is not str
                or _SHA256_DIGEST.fullmatch(assignment.stage_manifest_digest) is None
                or type(assignment.stage_tensor_keys_digest) is not str
                or _SHA256_DIGEST.fullmatch(assignment.stage_tensor_keys_digest) is None
            ):
                raise ValueError("STAGE assignment identity digests are invalid")
            if (
                type(assignment.layer_start) is not int
                or type(assignment.layer_end) is not int
                or assignment.layer_start != expected_layer_start
                or assignment.layer_end < assignment.layer_start
            ):
                raise ValueError("pipeline layer ranges must be contiguous and non-empty")
            expected_layer_start = assignment.layer_end + 1
            expected_role = "ray-head" if expected_rank == 0 else "ray-worker"
            if assignment.role != expected_role:
                raise ValueError("assignment role does not match runtime rank")
        if expected_layer_start != self.model.layer_count:
            raise ValueError("pipeline layer ranges must cover every model layer exactly once")
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("assignment node UUIDs must be unique")
        if len(runtime_addresses) != len(set(runtime_addresses)):
            raise ValueError("runtime addresses must be unique")
        if stage_mode and len(
            {assignment.stage_manifest_digest for assignment in self.assignments}
        ) != len(self.assignments):
            raise ValueError("stage manifests must be unique per pipeline rank")
        if node_ids[0] != self.ray_head_node_id:
            raise ValueError("ray_head_node_id must identify runtime rank 0")
        if self.ray_head_address != f"{runtime_addresses[0]}:6379":
            raise ValueError("ray_head_address must identify runtime rank 0 on port 6379")
        if runtime_addresses[1:] != sorted(runtime_addresses[1:]):
            raise ValueError("worker runtime ranks must be ordered by runtime address")

    def to_dict(self) -> dict[str, Any]:
        self.validate_execution_contract()
        value = asdict(self)
        value["assignments"] = [item.to_dict() for item in self.assignments]
        for key in (
            "execution_backend",
            "runtime_vllm_version",
            "model_cache_kind",
            "stage_artifact",
        ):
            if value[key] is None:
                value.pop(key)
        value["world_size"] = self.world_size
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DeploymentPlan":
        data = dict(value)
        serialized_world_size = data.pop("world_size", None)
        data["model"] = ModelSpec(**data["model"])
        data["assignments"] = [NodeAssignment.from_dict(item) for item in data["assignments"]]
        if data.get("stage_artifact") is not None:
            data["stage_artifact"] = StageArtifactBinding.from_dict(
                data["stage_artifact"]
            )
        plan = cls(**data)
        plan.validate_execution_contract()
        if (
            plan.execution_backend == VLLM_RAY_PP_BACKEND
            and (
                type(serialized_world_size) is not int
                or serialized_world_size != plan.world_size
            )
        ):
            raise ValueError("serialized world_size does not match the execution contract")
        return plan


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str
    blocking: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
