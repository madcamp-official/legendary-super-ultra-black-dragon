#!/usr/bin/env python3
"""vLLM 0.9.0 STAGE Ray pipeline parallel 실제 GPU 수용 검사.

기본 실행은 호스트나 Ray cluster를 변경하지 않고 ``NOT_RUN``(77)을
반환한다. 명시적으로 opt-in한 경우에도 root 소유 고정 설정과 모든 Ray
노드의 고정 ``/models/model`` mount만 사용한다. 명령, 임의 환경 변수,
Docker 인자, mount 또는 host path는 입력으로 받지 않는다.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import ipaddress
import json
import multiprocessing
import os
import re
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from dure.stage_cache import (
    STAGE_ARCHITECTURE,
    STAGE_CACHE_KIND,
    STAGE_CONTROL_LOADER_FORMAT,
    STAGE_NATIVE_LOADER_FORMAT,
    STAGE_VLLM_VERSION,
    StageCacheError,
    StageCacheIdentity,
    stage_contract_identity_digest,
)


PINNED_VLLM_VERSION = "0.9.0"
BACKEND = "VLLM_RAY_PP_V1"
CONFIG_PATH = Path("/etc/dure/acceptance-vllm-stage-ray-pp-v1.json")
MODEL_PATH = Path("/models/model")
CONFIG_MAX_BYTES = 128 * 1024
BACKEND_TIMEOUT_SECONDS = 1800
NODE_REHASH_TIMEOUT_SECONDS = 600
MAX_STAGE_FILES = 200_000
MAX_STAGE_BYTES = (1 << 63) - 1
DURE_NODE_RESOURCE_PREFIX = "dure_node_"
OPT_IN_NAME = "DURE_RUN_VLLM_STAGE_RAY_PP_ACCEPTANCE"
ACCEPTANCE_ENV_PREFIXES = (
    OPT_IN_NAME,
    "DURE_VLLM_STAGE_RAY_PP_ACCEPTANCE_",
)
ALLOWED_ACCEPTANCE_ENV = frozenset({OPT_IN_NAME})
FORBIDDEN_PROCESS_ENV = frozenset(
    {"PYTHONPATH", "PYTHONHOME", "LD_PRELOAD"}
)
OCI_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9._:/-]*[a-z0-9])?$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
PRIVATE_IPV4_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
CONFIG_FIELDS = frozenset(
    {
        "schema_version",
        "backend",
        "vllm_version",
        "validation_run_id",
        "deployment_id",
        "generation",
        "runtime_image",
        "repository",
        "revision",
        "stage_artifact",
        "ordered_bindings",
    }
)
STAGE_FIELDS = frozenset(
    {
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
)
NODE_FIELDS = frozenset(
    {
        "node_id",
        "runtime_address",
        "pipeline_rank",
        "runtime_rank",
        "tensor_rank",
        "stage_manifest_digest",
        "stage_tensor_key_count",
        "stage_tensor_keys_digest",
        "stage_weight_size_bytes",
        "stage_total_size_bytes",
        "stage_file_count",
        "stage_cache_identity_digest",
    }
)
REHASH_RESULT_FIELDS = frozenset(
    {
        "status",
        "ray_node_id",
        "manifest_digest",
        "tensor_keys_digest",
        "cache_identity_digest",
        "total_size_bytes",
        "file_count",
    }
)


class AcceptanceFailure(RuntimeError):
    """중앙 증적에 기록해도 되는 폐쇄형 실행 실패."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class PrerequisiteMissing(AcceptanceFailure):
    """Ray 연결과 실제 분산 load 전에 발견한 전제 조건 부족."""


@dataclass(frozen=True)
class StageArtifactContract:
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

    def identity_document(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "source_manifest_digest": self.source_manifest_digest,
            "runtime_image": self.runtime_image,
            "vllm_version": self.vllm_version,
            "exporter_build_digest": self.exporter_build_digest,
            "architecture": self.architecture,
            "quantization": self.quantization,
            "tensor_parallel_size": self.tensor_parallel_size,
            "pipeline_parallel_size": self.pipeline_parallel_size,
            "loader_format": self.loader_format,
        }


@dataclass(frozen=True)
class ExpectedBinding:
    node_id: str
    runtime_address: str
    pipeline_rank: int
    runtime_rank: int
    tensor_rank: int
    stage_manifest_digest: str
    stage_tensor_key_count: int
    stage_tensor_keys_digest: str
    stage_weight_size_bytes: int
    stage_total_size_bytes: int
    stage_file_count: int
    stage_cache_identity_digest: str


@dataclass(frozen=True)
class AcceptanceContract:
    validation_run_id: str
    deployment_id: str
    generation: int
    runtime_image: str
    repository: str
    revision: str
    stage_artifact: StageArtifactContract
    ordered_bindings: tuple[ExpectedBinding, ...]

    @property
    def world_size(self) -> int:
        return len(self.ordered_bindings)

    def identity_for(self, binding: ExpectedBinding) -> StageCacheIdentity:
        try:
            return StageCacheIdentity(
                repository=self.repository,
                revision=self.revision,
                manifest_digest=binding.stage_manifest_digest,
                quantization=self.stage_artifact.quantization,
                artifact_set_digest=self.stage_artifact.artifact_set_digest,
                contract_identity_digest=(
                    self.stage_artifact.contract_identity_digest
                ),
                source_manifest_digest=(
                    self.stage_artifact.source_manifest_digest
                ),
                runtime_image=self.stage_artifact.runtime_image,
                vllm_version=self.stage_artifact.vllm_version,
                exporter_build_digest=(
                    self.stage_artifact.exporter_build_digest
                ),
                architecture=self.stage_artifact.architecture,
                loader_format=self.stage_artifact.loader_format,
                tensor_parallel_size=(
                    self.stage_artifact.tensor_parallel_size
                ),
                pipeline_parallel_size=(
                    self.stage_artifact.pipeline_parallel_size
                ),
                pipeline_rank=binding.pipeline_rank,
                tensor_rank=binding.tensor_rank,
                tensor_keys_digest=binding.stage_tensor_keys_digest,
            )
        except StageCacheError as exc:
            raise PrerequisiteMissing(
                "STAGE_IDENTITY_INVALID",
                "rank별 STAGE cache identity가 유효하지 않습니다.",
            ) from exc

    @classmethod
    def parse(cls, raw: object) -> "AcceptanceContract":
        document = _exact_object(raw, CONFIG_FIELDS, "설정")
        if (
            type(document["schema_version"]) is not int
            or document["schema_version"] != 1
            or document["backend"] != BACKEND
            or document["vllm_version"] != PINNED_VLLM_VERSION
        ):
            raise PrerequisiteMissing(
                "CONTRACT_INVALID",
                "STAGE Ray 설정 버전과 backend가 고정 계약과 다릅니다.",
            )
        validation_run_id = _canonical_uuid4(
            document["validation_run_id"], "validation_run_id"
        )
        deployment_id = _canonical_uuid(
            document["deployment_id"], "deployment_id"
        )
        generation = document["generation"]
        if type(generation) is not int or generation < 1:
            raise PrerequisiteMissing(
                "CONTRACT_INVALID", "generation은 1 이상의 정수여야 합니다."
            )
        runtime_image = document["runtime_image"]
        if not _digest_pinned_image(runtime_image):
            raise PrerequisiteMissing(
                "CONTRACT_INVALID",
                "runtime_image는 OCI SHA-256 digest로 고정해야 합니다.",
            )
        repository = document["repository"]
        revision = document["revision"]
        if type(repository) is not str or type(revision) is not str:
            raise PrerequisiteMissing(
                "CONTRACT_INVALID", "모델 repository와 revision이 필요합니다."
            )

        stage_value = _exact_object(
            document["stage_artifact"], STAGE_FIELDS, "stage_artifact"
        )
        stage = StageArtifactContract(**stage_value)
        for field in (
            stage.artifact_set_digest,
            stage.contract_identity_digest,
            stage.source_manifest_digest,
            stage.exporter_build_digest,
        ):
            if type(field) is not str or DIGEST_RE.fullmatch(field) is None:
                raise PrerequisiteMissing(
                    "CONTRACT_INVALID", "STAGE identity digest가 유효하지 않습니다."
                )
        if (
            stage.runtime_image != runtime_image
            or stage.vllm_version != STAGE_VLLM_VERSION
            or stage.architecture != STAGE_ARCHITECTURE
            or stage.quantization != "awq"
            or type(stage.tensor_parallel_size) is not int
            or stage.tensor_parallel_size != 1
            or type(stage.pipeline_parallel_size) is not int
            or stage.pipeline_parallel_size not in (2, 3)
            or stage.loader_format != STAGE_CONTROL_LOADER_FORMAT
        ):
            raise PrerequisiteMissing(
                "STAGE_CONTRACT_MISMATCH",
                "STAGE runtime 계약이 vLLM 0.9.0 TP=1 PP=2/3과 다릅니다.",
            )
        calculated_contract = stage_contract_identity_digest(
            source_manifest_digest=stage.source_manifest_digest,
            runtime_image=stage.runtime_image,
            vllm_version=stage.vllm_version,
            exporter_build_digest=stage.exporter_build_digest,
            architecture=stage.architecture,
            quantization=stage.quantization,
            tensor_parallel_size=stage.tensor_parallel_size,
            pipeline_parallel_size=stage.pipeline_parallel_size,
            loader_format=stage.loader_format,
        )
        if stage.contract_identity_digest != calculated_contract:
            raise PrerequisiteMissing(
                "STAGE_CONTRACT_MISMATCH",
                "STAGE contract identity digest가 입력과 일치하지 않습니다.",
            )

        raw_bindings = document["ordered_bindings"]
        if (
            type(raw_bindings) is not list
            or len(raw_bindings) != stage.pipeline_parallel_size
        ):
            raise PrerequisiteMissing(
                "TOPOLOGY_UNSUPPORTED",
                "수용 검사는 정확히 2대 또는 3대의 GPU 노드만 지원합니다.",
            )
        bindings: list[ExpectedBinding] = []
        for index, value in enumerate(raw_bindings):
            binding_value = _exact_object(
                value, NODE_FIELDS, f"ordered_bindings[{index}]"
            )
            binding = ExpectedBinding(
                node_id=_canonical_uuid4(binding_value["node_id"], "node_id"),
                runtime_address=_private_ipv4(
                    binding_value["runtime_address"]
                ),
                pipeline_rank=binding_value["pipeline_rank"],
                runtime_rank=binding_value["runtime_rank"],
                tensor_rank=binding_value["tensor_rank"],
                stage_manifest_digest=binding_value[
                    "stage_manifest_digest"
                ],
                stage_tensor_key_count=binding_value[
                    "stage_tensor_key_count"
                ],
                stage_tensor_keys_digest=binding_value[
                    "stage_tensor_keys_digest"
                ],
                stage_weight_size_bytes=binding_value[
                    "stage_weight_size_bytes"
                ],
                stage_total_size_bytes=binding_value[
                    "stage_total_size_bytes"
                ],
                stage_file_count=binding_value["stage_file_count"],
                stage_cache_identity_digest=binding_value[
                    "stage_cache_identity_digest"
                ],
            )
            if (
                type(binding.pipeline_rank) is not int
                or binding.pipeline_rank != index
                or type(binding.runtime_rank) is not int
                or binding.runtime_rank != index
                or type(binding.tensor_rank) is not int
                or binding.tensor_rank != 0
                or type(binding.stage_tensor_key_count) is not int
                or binding.stage_tensor_key_count < 1
                or type(binding.stage_weight_size_bytes) is not int
                or not 1 <= binding.stage_weight_size_bytes <= MAX_STAGE_BYTES
                or type(binding.stage_total_size_bytes) is not int
                or not 1 <= binding.stage_total_size_bytes <= MAX_STAGE_BYTES
                or binding.stage_total_size_bytes
                < binding.stage_weight_size_bytes
                or type(binding.stage_file_count) is not int
                or not 1 <= binding.stage_file_count <= MAX_STAGE_FILES
            ):
                raise PrerequisiteMissing(
                    "RANK_CONTRACT_INVALID",
                    "rank별 topology 또는 STAGE 크기 계약이 유효하지 않습니다.",
                )
            for digest in (
                binding.stage_manifest_digest,
                binding.stage_tensor_keys_digest,
                binding.stage_cache_identity_digest,
            ):
                if type(digest) is not str or DIGEST_RE.fullmatch(digest) is None:
                    raise PrerequisiteMissing(
                        "RANK_CONTRACT_INVALID",
                        "rank별 STAGE digest가 유효하지 않습니다.",
                    )
            bindings.append(binding)

        if (
            len({item.node_id for item in bindings}) != len(bindings)
            or len({item.runtime_address for item in bindings}) != len(bindings)
            or len({item.stage_manifest_digest for item in bindings})
            != len(bindings)
            or len({item.stage_cache_identity_digest for item in bindings})
            != len(bindings)
        ):
            raise PrerequisiteMissing(
                "DUPLICATE_NODE", "rank별 node 또는 STAGE identity가 중복됩니다."
            )
        expected_order = [
            bindings[0],
            *sorted(bindings[1:], key=lambda item: item.runtime_address),
        ]
        if bindings != expected_order:
            raise PrerequisiteMissing(
                "RANK_ORDER_INVALID",
                "worker 노드는 vLLM 0.9.0의 IP 문자열 오름차순이어야 합니다.",
            )

        contract = cls(
            validation_run_id=validation_run_id,
            deployment_id=deployment_id,
            generation=generation,
            runtime_image=runtime_image,
            repository=repository,
            revision=revision,
            stage_artifact=stage,
            ordered_bindings=tuple(bindings),
        )
        if stage.artifact_set_digest != _artifact_set_digest(stage, bindings):
            raise PrerequisiteMissing(
                "STAGE_ARTIFACT_SET_MISMATCH",
                "artifact-set digest가 rank별 STAGE identity와 다릅니다.",
            )
        for binding in bindings:
            if (
                contract.identity_for(binding).cache_identity_digest
                != binding.stage_cache_identity_digest
            ):
                raise PrerequisiteMissing(
                    "STAGE_CACHE_IDENTITY_MISMATCH",
                    "rank별 cache identity digest가 STAGE 계약과 다릅니다.",
                )
        return contract


@dataclass(frozen=True)
class RuntimeNode:
    ray_node_id: str
    node_address: str
    gpu_count: float
    dure_node_id: str


@dataclass(frozen=True)
class NodeRehashEvidence:
    node_id: str
    runtime_address: str
    pipeline_rank: int
    manifest_digest: str
    tensor_keys_digest: str
    cache_identity_digest: str
    total_size_bytes: int
    file_count: int


@dataclass(frozen=True)
class RuntimeResult:
    ordered_addresses: tuple[str, ...]
    generated_token_count: int
    node_rehashes: tuple[NodeRehashEvidence, ...]


class AcceptanceBackend(Protocol):
    def preflight(self, contract: AcceptanceContract) -> None: ...

    def run(self, contract: AcceptanceContract) -> RuntimeResult: ...


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _digest_json(value: object) -> str:
    return "sha256:" + hashlib.sha256(
        _canonical_json(value).encode("utf-8")
    ).hexdigest()


def _artifact_set_digest(
    stage: StageArtifactContract,
    bindings: Sequence[ExpectedBinding],
) -> str:
    value = {
        "schema_version": 1,
        "contract": stage.identity_document(),
        "stages": [
            {
                "rank": item.pipeline_rank,
                "pipeline_rank": item.pipeline_rank,
                "tensor_rank": item.tensor_rank,
                "manifest_digest": item.stage_manifest_digest,
                "tensor_key_count": item.stage_tensor_key_count,
                "tensor_keys_digest": item.stage_tensor_keys_digest,
                "weight_size_bytes": item.stage_weight_size_bytes,
            }
            for item in sorted(
                bindings, key=lambda binding: binding.pipeline_rank
            )
        ],
    }
    return _digest_json(value)


def _closed_json(value: str) -> object:
    def unique_object(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    return json.loads(value, object_pairs_hook=unique_object)


def _read_bounded_root_json(path: Path, *, label: str) -> object:
    """Read one root-owned, non-writable regular JSON file without links."""

    descriptor = -1
    try:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != 0
            or observed.st_mode & 0o022
            or observed.st_size > CONFIG_MAX_BYTES
        ):
            raise ValueError(f"{label} is not a trusted bounded regular file")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_mode,
            before.st_uid,
        )
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != 0
            or before.st_mode & 0o022
            or before.st_size > CONFIG_MAX_BYTES
            or before.st_dev != observed.st_dev
            or before.st_ino != observed.st_ino
        ):
            raise ValueError(f"{label} identity changed")
        payload = bytearray()
        while len(payload) <= CONFIG_MAX_BYTES:
            block = os.read(
                descriptor,
                min(8192, CONFIG_MAX_BYTES + 1 - len(payload)),
            )
            if not block:
                break
            payload.extend(block)
        after = os.fstat(descriptor)
        if (
            len(payload) > CONFIG_MAX_BYTES
            or len(payload) != before.st_size
            or identity
            != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_mode,
                after.st_uid,
            )
        ):
            raise ValueError(f"{label} changed while being read")
        return _closed_json(payload.decode("utf-8"))
    except (OSError, UnicodeError, ValueError, RecursionError) as exc:
        raise PrerequisiteMissing(
            "TRUSTED_FILE_INVALID",
            f"고정 {label} 파일을 안전하게 읽을 수 없습니다.",
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _exact_object(
    value: object, expected_fields: frozenset[str], label: str
) -> dict[str, Any]:
    if (
        type(value) is not dict
        or any(type(key) is not str for key in value)
        or set(value) != expected_fields
    ):
        raise PrerequisiteMissing(
            "CONTRACT_INVALID", f"{label} 필드가 폐쇄형 계약과 다릅니다."
        )
    return value


def _canonical_uuid(value: object, label: str) -> str:
    if type(value) is not str:
        raise PrerequisiteMissing(
            "CONTRACT_INVALID", f"{label}은 canonical UUID여야 합니다."
        )
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError) as exc:
        raise PrerequisiteMissing(
            "CONTRACT_INVALID", f"{label}은 canonical UUID여야 합니다."
        ) from exc
    if str(parsed) != value:
        raise PrerequisiteMissing(
            "CONTRACT_INVALID", f"{label}은 canonical UUID여야 합니다."
        )
    return value


def _canonical_uuid4(value: object, label: str) -> str:
    normalized = _canonical_uuid(value, label)
    if UUID(normalized).version != 4:
        raise PrerequisiteMissing(
            "CONTRACT_INVALID", f"{label}은 canonical UUIDv4여야 합니다."
        )
    return normalized


def _private_ipv4(value: object) -> str:
    if type(value) is not str:
        raise PrerequisiteMissing(
            "CONTRACT_INVALID", "runtime_address는 사설 IPv4 주소여야 합니다."
        )
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise PrerequisiteMissing(
            "CONTRACT_INVALID", "runtime_address는 사설 IPv4 주소여야 합니다."
        ) from exc
    if (
        not isinstance(address, ipaddress.IPv4Address)
        or not any(address in network for network in PRIVATE_IPV4_NETWORKS)
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_unspecified
        or address.is_reserved
        or str(address) != value
    ):
        raise PrerequisiteMissing(
            "PUBLIC_RAY_ADDRESS",
            "Ray node address는 canonical 사설 IPv4여야 합니다.",
        )
    return value


def _digest_pinned_image(value: object) -> bool:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 512
        or value.count("@") != 1
        or any(
            ord(character) < 0x21 or ord(character) > 0x7E
            for character in value
        )
        or "\\" in value
    ):
        return False
    name, digest = value.rsplit("@", 1)
    segments = name.split("/")
    return (
        OCI_NAME_RE.fullmatch(name) is not None
        and DIGEST_RE.fullmatch(digest) is not None
        and all(segment not in {"", ".", ".."} for segment in segments)
        and ":" not in segments[-1]
        and "//" not in name
    )


def _runtime_nodes_from_ray(raw_nodes: object) -> tuple[RuntimeNode, ...]:
    if type(raw_nodes) is not list:
        raise AcceptanceFailure(
            "RAY_TOPOLOGY_INVALID", "Ray node 목록 형식이 유효하지 않습니다."
        )
    nodes: list[RuntimeNode] = []
    for value in raw_nodes:
        if type(value) is not dict:
            raise AcceptanceFailure(
                "RAY_TOPOLOGY_INVALID", "Ray node 항목 형식이 유효하지 않습니다."
            )
        if value.get("Alive") is not True:
            continue
        resources = value.get("Resources")
        if type(resources) is not dict:
            raise AcceptanceFailure(
                "RAY_TOPOLOGY_INVALID",
                "활성 Ray node resource 형식이 유효하지 않습니다.",
            )
        gpu_count = resources.get("GPU")
        if type(gpu_count) not in (int, float):
            raise AcceptanceFailure(
                "GPU_PLACEMENT_INVALID",
                "활성 Ray node의 GPU resource가 유효하지 않습니다.",
            )
        node_id = value.get("NodeID")
        address = value.get("NodeManagerAddress")
        if type(node_id) is not str or not node_id or type(address) is not str:
            raise AcceptanceFailure(
                "RAY_TOPOLOGY_INVALID",
                "Ray GPU node identity가 유효하지 않습니다.",
            )
        markers = [
            (key, count)
            for key, count in resources.items()
            if type(key) is str and key.startswith(DURE_NODE_RESOURCE_PREFIX)
        ]
        if (
            len(markers) != 1
            or type(markers[0][1]) not in (int, float)
            or float(markers[0][1]) != 1.0
        ):
            raise AcceptanceFailure(
                "DURE_NODE_BINDING_INVALID",
                "Ray GPU node의 Dure UUID resource 결합이 정확하지 않습니다.",
            )
        try:
            dure_node_id = str(
                UUID(hex=markers[0][0][len(DURE_NODE_RESOURCE_PREFIX) :])
            )
        except (ValueError, AttributeError) as exc:
            raise AcceptanceFailure(
                "DURE_NODE_BINDING_INVALID",
                "Ray GPU node의 Dure UUID resource 형식이 유효하지 않습니다.",
            ) from exc
        nodes.append(
            RuntimeNode(node_id, address, float(gpu_count), dure_node_id)
        )
    return tuple(nodes)


def _validate_cluster_nodes(
    contract: AcceptanceContract, runtime_nodes: Sequence[RuntimeNode]
) -> None:
    if len(runtime_nodes) != contract.world_size:
        raise AcceptanceFailure(
            "NODE_SET_MISMATCH", "Ray GPU node 수가 고정 topology와 다릅니다."
        )
    if len({item.ray_node_id for item in runtime_nodes}) != len(runtime_nodes):
        raise AcceptanceFailure(
            "DUPLICATE_NODE", "Ray가 중복 node identity를 보고했습니다."
        )
    if any(item.gpu_count != 1.0 for item in runtime_nodes):
        raise AcceptanceFailure(
            "GPU_PLACEMENT_INVALID",
            "각 Ray node는 정확히 GPU 하나만 제공해야 합니다.",
        )
    expected = {
        item.runtime_address: item.node_id
        for item in contract.ordered_bindings
    }
    observed = {
        item.node_address: item.dure_node_id for item in runtime_nodes
    }
    if observed != expected:
        raise AcceptanceFailure(
            "DURE_NODE_BINDING_INVALID",
            "Ray 주소와 Dure node UUID 결합이 고정 계약과 다릅니다.",
        )


def _validate_actor_ranks(
    contract: AcceptanceContract,
    runtime_nodes: Sequence[RuntimeNode],
    actor_pairs: object,
) -> tuple[str, ...]:
    if type(actor_pairs) is not list or len(actor_pairs) != contract.world_size:
        raise AcceptanceFailure(
            "MISSING_RANK", "vLLM worker rank 수가 고정 topology와 다릅니다."
        )
    address_by_ray_id = {
        item.ray_node_id: item.node_address for item in runtime_nodes
    }
    ordered_addresses: list[str] = []
    seen_ray_ids: set[str] = set()
    for pair in actor_pairs:
        if type(pair) not in (tuple, list) or len(pair) != 2:
            raise AcceptanceFailure(
                "RANK_ATTESTATION_INVALID",
                "worker rank 응답 형식이 유효하지 않습니다.",
            )
        ray_node_id, gpu_ids = pair
        if type(ray_node_id) is not str or ray_node_id not in address_by_ray_id:
            raise AcceptanceFailure(
                "UNKNOWN_WORKER_NODE",
                "worker가 고정 node 집합 밖에서 실행됐습니다.",
            )
        if ray_node_id in seen_ray_ids:
            raise AcceptanceFailure(
                "DUPLICATE_RANK",
                "한 node에 둘 이상의 pipeline rank가 배치됐습니다.",
            )
        if (
            type(gpu_ids) not in (tuple, list)
            or len(gpu_ids) != 1
            or not (
                (type(gpu_ids[0]) is int and gpu_ids[0] >= 0)
                or (
                    type(gpu_ids[0]) is str
                    and gpu_ids[0].isdigit()
                    and str(int(gpu_ids[0])) == gpu_ids[0]
                )
            )
        ):
            raise AcceptanceFailure(
                "GPU_PLACEMENT_INVALID",
                "각 worker는 정확히 GPU 하나를 점유해야 합니다.",
            )
        seen_ray_ids.add(ray_node_id)
        ordered_addresses.append(address_by_ray_id[ray_node_id])
    expected = [item.runtime_address for item in contract.ordered_bindings]
    if ordered_addresses != expected:
        raise AcceptanceFailure(
            "RANK_BINDING_MISMATCH",
            "실제 vLLM runtime rank 순서가 node UUID 계약과 다릅니다.",
        )
    return tuple(ordered_addresses)


def _rehash_stage_cache_on_worker(identity_document: dict) -> dict:
    """Fully validate the fixed local mount without returning raw errors."""

    try:
        import ray

        from dure.stage_cache import (
            StageCacheIdentity as WorkerStageCacheIdentity,
        )
        from dure.stage_cache import validate_materialized_stage_cache

        identity = WorkerStageCacheIdentity.from_dict(identity_document)
        validation = validate_materialized_stage_cache(
            MODEL_PATH,
            identity,
            require_canonical_path=False,
        )
        return {
            "status": "PASSED",
            "ray_node_id": str(ray.get_runtime_context().get_node_id()),
            "manifest_digest": validation.manifest_digest,
            "tensor_keys_digest": identity.tensor_keys_digest,
            "cache_identity_digest": validation.cache_identity_digest,
            "total_size_bytes": validation.total_size_bytes,
            "file_count": validation.file_count,
        }
    except BaseException:
        return {"status": "FAILED"}


def _validate_node_rehash_results(
    contract: AcceptanceContract,
    runtime_nodes: Sequence[RuntimeNode],
    raw_results: object,
) -> tuple[NodeRehashEvidence, ...]:
    if type(raw_results) is not list or len(raw_results) != contract.world_size:
        raise AcceptanceFailure(
            "STAGE_REHASH_FAILED",
            "모든 Ray node의 STAGE mount 재해시 증적이 필요합니다.",
        )
    node_by_ray_id = {item.ray_node_id: item for item in runtime_nodes}
    evidence: list[NodeRehashEvidence] = []
    seen_ray_ids: set[str] = set()
    for binding, value in zip(contract.ordered_bindings, raw_results):
        if (
            type(value) is not dict
            or set(value) != REHASH_RESULT_FIELDS
            or value.get("status") != "PASSED"
            or type(value.get("ray_node_id")) is not str
            or value["ray_node_id"] in seen_ray_ids
            or type(value.get("manifest_digest")) is not str
            or type(value.get("tensor_keys_digest")) is not str
            or type(value.get("cache_identity_digest")) is not str
            or type(value.get("total_size_bytes")) is not int
            or type(value.get("file_count")) is not int
        ):
            raise AcceptanceFailure(
                "STAGE_REHASH_FAILED",
                "Ray node의 STAGE mount 재해시가 실패했습니다.",
            )
        runtime_node = node_by_ray_id.get(value["ray_node_id"])
        if (
            runtime_node is None
            or runtime_node.node_address != binding.runtime_address
            or runtime_node.dure_node_id != binding.node_id
            or value["manifest_digest"] != binding.stage_manifest_digest
            or value["tensor_keys_digest"]
            != binding.stage_tensor_keys_digest
            or value["cache_identity_digest"]
            != binding.stage_cache_identity_digest
            or value["total_size_bytes"]
            != binding.stage_total_size_bytes
            or value["file_count"] != binding.stage_file_count
        ):
            raise AcceptanceFailure(
                "STAGE_REHASH_MISMATCH",
                "재해시된 STAGE mount가 rank별 identity와 다릅니다.",
            )
        seen_ray_ids.add(value["ray_node_id"])
        evidence.append(
            NodeRehashEvidence(
                node_id=binding.node_id,
                runtime_address=binding.runtime_address,
                pipeline_rank=binding.pipeline_rank,
                manifest_digest=binding.stage_manifest_digest,
                tensor_keys_digest=binding.stage_tensor_keys_digest,
                cache_identity_digest=binding.stage_cache_identity_digest,
                total_size_bytes=binding.stage_total_size_bytes,
                file_count=binding.stage_file_count,
            )
        )
    return tuple(evidence)


def _rehash_all_nodes(
    ray,
    contract: AcceptanceContract,
    runtime_nodes: Sequence[RuntimeNode],
) -> tuple[NodeRehashEvidence, ...]:
    remote_rehash = ray.remote(_rehash_stage_cache_on_worker)
    references = []
    for binding in contract.ordered_bindings:
        resource = DURE_NODE_RESOURCE_PREFIX + binding.node_id.replace("-", "")
        references.append(
            remote_rehash.options(
                num_cpus=0,
                resources={resource: 0.001},
            ).remote(contract.identity_for(binding).to_dict())
        )
    try:
        raw_results = ray.get(
            references,
            timeout=NODE_REHASH_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        raise AcceptanceFailure(
            "STAGE_REHASH_FAILED",
            "모든 Ray node에서 STAGE mount를 재해시하지 못했습니다.",
        ) from exc
    return _validate_node_rehash_results(
        contract, runtime_nodes, raw_results
    )


def _expected_node_rehashes(
    contract: AcceptanceContract,
) -> tuple[NodeRehashEvidence, ...]:
    return tuple(
        NodeRehashEvidence(
            node_id=item.node_id,
            runtime_address=item.runtime_address,
            pipeline_rank=item.pipeline_rank,
            manifest_digest=item.stage_manifest_digest,
            tensor_keys_digest=item.stage_tensor_keys_digest,
            cache_identity_digest=item.stage_cache_identity_digest,
            total_size_bytes=item.stage_total_size_bytes,
            file_count=item.stage_file_count,
        )
        for item in contract.ordered_bindings
    )


def _vllm_load_kwargs(contract: AcceptanceContract) -> dict[str, object]:
    return {
        "model": str(MODEL_PATH),
        "tokenizer": str(MODEL_PATH),
        "load_format": STAGE_NATIVE_LOADER_FORMAT,
        "quantization": "awq",
        "tensor_parallel_size": 1,
        "pipeline_parallel_size": contract.world_size,
        "distributed_executor_backend": "ray",
        "trust_remote_code": False,
        "enable_lora": False,
        "enforce_eager": True,
        "gpu_memory_utilization": 0.90,
        "max_model_len": 128,
    }


class RealVllmStageRayBackend:
    """모든 rank mount를 재해시한 뒤 vLLM Ray executor를 실제 실행한다."""

    def __init__(self) -> None:
        self._runtime_ray = None
        self._runtime_async_engine = None
        self._runtime_executor = None

    def preflight(self, contract: AcceptanceContract) -> None:
        for distribution, expected in (
            ("vllm", PINNED_VLLM_VERSION),
            ("ray", None),
            ("dure", None),
        ):
            try:
                observed = importlib.metadata.version(distribution)
            except importlib.metadata.PackageNotFoundError as exc:
                raise PrerequisiteMissing(
                    "RUNTIME_MISSING",
                    "고정 STAGE GPU 수용 검사 runtime이 설치되지 않았습니다.",
                ) from exc
            if expected is not None and observed != expected:
                raise PrerequisiteMissing(
                    "VLLM_VERSION_MISMATCH",
                    f"vLLM은 정확히 {PINNED_VLLM_VERSION}이어야 합니다.",
                )
        if (
            not MODEL_PATH.is_dir()
            or not (MODEL_PATH / "config.json").is_file()
            or not (MODEL_PATH / "dure-stage.json").is_file()
            or not (MODEL_PATH / ".dure-stage-manifest.json").is_file()
            or not (MODEL_PATH / ".dure-model.json").is_file()
        ):
            raise PrerequisiteMissing(
                "STAGE_MODEL_MISSING",
                "고정 STAGE model mount의 필수 파일이 없습니다.",
            )

    def run(self, contract: AcceptanceContract) -> RuntimeResult:
        try:
            return self._run(contract)
        finally:
            if self._runtime_async_engine is not None:
                try:
                    self._runtime_async_engine.shutdown_background_loop()
                except Exception:
                    pass
            if self._runtime_executor is not None:
                try:
                    self._runtime_executor.shutdown()
                except Exception:
                    pass
            if self._runtime_ray is not None:
                try:
                    self._runtime_ray.shutdown()
                except Exception:
                    pass

    def _run(self, contract: AcceptanceContract) -> RuntimeResult:
        os.environ["VLLM_USE_V1"] = "0"
        os.environ["VLLM_USE_RAY_SPMD_WORKER"] = "0"
        os.environ["VLLM_RAY_PER_WORKER_GPUS"] = "1.0"
        os.environ["VLLM_RAY_BUNDLE_INDICES"] = ""
        os.environ["VLLM_USE_RAY_COMPILED_DAG"] = "0"
        os.environ["VLLM_HOST_IP"] = (
            contract.ordered_bindings[0].runtime_address
        )
        os.environ["VLLM_ATTENTION_BACKEND"] = "FLASH_ATTN"
        os.environ["RAY_ADDRESS"] = (
            f"{contract.ordered_bindings[0].runtime_address}:6379"
        )

        try:
            import ray

            self._runtime_ray = ray
            ray.init(
                address=(
                    f"{contract.ordered_bindings[0].runtime_address}:6379"
                ),
                logging_level="ERROR",
            )
        except Exception as exc:
            raise AcceptanceFailure(
                "RAY_CLUSTER_UNAVAILABLE",
                "기존 Ray cluster에 연결하지 못했습니다.",
            ) from exc

        runtime_nodes = _runtime_nodes_from_ray(ray.nodes())
        _validate_cluster_nodes(contract, runtime_nodes)
        node_rehashes = _rehash_all_nodes(ray, contract, runtime_nodes)

        try:
            from vllm import (
                AsyncEngineArgs,
                AsyncLLMEngine,
                SamplingParams,
            )

            llm = AsyncLLMEngine.from_engine_args(
                AsyncEngineArgs(**_vllm_load_kwargs(contract))
            )
            self._runtime_async_engine = llm
        except Exception as exc:
            raise AcceptanceFailure(
                "VLLM_STAGE_DISTRIBUTED_LOAD_FAILED",
                "vLLM sharded_state Ray pipeline load가 실패했습니다.",
            ) from exc

        try:
            executor = llm.engine.model_executor
            self._runtime_executor = executor
            if (
                executor.__class__.__name__ != "RayDistributedExecutor"
                or executor.__class__.__module__
                != "vllm.executor.ray_distributed_executor"
            ):
                raise AcceptanceFailure(
                    "EXECUTOR_MISMATCH",
                    "실제 executor가 고정 Ray 계약과 다릅니다.",
                )
            parallel = executor.parallel_config
            if (
                parallel.tensor_parallel_size != 1
                or parallel.pipeline_parallel_size != contract.world_size
                or parallel.world_size != contract.world_size
            ):
                raise AcceptanceFailure(
                    "TOPOLOGY_MISMATCH",
                    "실제 vLLM topology가 STAGE 계약과 다릅니다.",
                )
            workers = [executor.driver_dummy_worker, *executor.workers]
            if any(worker is None for worker in workers):
                raise AcceptanceFailure(
                    "DRIVER_MISSING",
                    "Ray driver GPU worker를 확인할 수 없습니다.",
                )
            actor_pairs = ray.get(
                [
                    worker.get_node_and_gpu_ids.remote()
                    for worker in workers
                ],
                timeout=30,
            )
            ordered_addresses = _validate_actor_ranks(
                contract, runtime_nodes, actor_pairs
            )
        except AcceptanceFailure:
            raise
        except Exception as exc:
            raise AcceptanceFailure(
                "RANK_ATTESTATION_FAILED",
                "실제 worker rank를 검증하지 못했습니다.",
            ) from exc

        try:
            output = asyncio.run(
                _generate_minimal_output(
                    llm,
                    SamplingParams(
                        temperature=0.0,
                        min_tokens=1,
                        max_tokens=4,
                    ),
                    request_id=contract.validation_run_id,
                )
            )
            if output is None or not output.outputs:
                raise RuntimeError("empty generation")
            token_count = len(output.outputs[0].token_ids)
            if token_count < 1 or token_count > 4:
                raise RuntimeError("invalid token count")
        except Exception as exc:
            raise AcceptanceFailure(
                "INFERENCE_FAILED", "고정 최소 추론 검사가 실패했습니다."
            ) from exc
        return RuntimeResult(
            ordered_addresses,
            token_count,
            node_rehashes,
        )


async def _generate_minimal_output(
    engine,
    sampling_params,
    *,
    request_id: str,
):
    final_output = None
    async for output in engine.generate(
        "대한민국의 수도는",
        sampling_params,
        request_id=request_id,
    ):
        final_output = output
    return final_output


def _load_contract(path: Path = CONFIG_PATH) -> AcceptanceContract:
    try:
        raw = _read_bounded_root_json(path, label="수용 검사 설정")
    except PrerequisiteMissing as exc:
        raise PrerequisiteMissing(
            "CONFIG_UNAVAILABLE",
            "고정 STAGE 수용 검사 설정을 읽을 수 없습니다.",
        ) from exc
    return AcceptanceContract.parse(raw)


def _emit(stream, status: str, **detail: object) -> None:
    print(
        json.dumps(
            {"status": status, **detail},
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        file=stream,
    )


def _unexpected_runtime_environment(
    environ: Mapping[str, str], contract: AcceptanceContract
) -> list[str]:
    expected = {
        "VLLM_USE_V1": "0",
        "VLLM_USE_RAY_SPMD_WORKER": "0",
        "VLLM_RAY_PER_WORKER_GPUS": "1.0",
        "VLLM_RAY_BUNDLE_INDICES": "",
        "VLLM_USE_RAY_COMPILED_DAG": "0",
        "VLLM_HOST_IP": contract.ordered_bindings[0].runtime_address,
        "VLLM_ATTENTION_BACKEND": "FLASH_ATTN",
        "RAY_ADDRESS": (
            f"{contract.ordered_bindings[0].runtime_address}:6379"
        ),
    }
    unexpected = [name for name in FORBIDDEN_PROCESS_ENV if environ.get(name)]
    for name, value in environ.items():
        if name.startswith("VLLM_") or name == "RAY_ADDRESS":
            if name not in expected or value != expected[name]:
                unexpected.append(name)
    return sorted(set(unexpected))


def _pipeline_rank_contract_detail(contract: AcceptanceContract) -> str:
    current = contract.ordered_bindings[0]
    stage = contract.stage_artifact
    detail = {
        "schema_version": 1,
        "backend": BACKEND,
        "vllm_version": PINNED_VLLM_VERSION,
        "node_id": current.node_id,
        "runtime_address": current.runtime_address,
        "pipeline_rank": current.pipeline_rank,
        "runtime_rank": current.runtime_rank,
        "ordered_bindings": [
            {
                "node_id": item.node_id,
                "runtime_address": item.runtime_address,
                "pipeline_rank": item.pipeline_rank,
                "runtime_rank": item.runtime_rank,
            }
            for item in contract.ordered_bindings
        ],
        "stage_artifact": {
            "artifact_set_digest": stage.artifact_set_digest,
            "contract_identity_digest": stage.contract_identity_digest,
            "source_manifest_digest": stage.source_manifest_digest,
            "loader_format": stage.loader_format,
            "stage_manifest_digest": current.stage_manifest_digest,
            "stage_tensor_keys_digest": current.stage_tensor_keys_digest,
            "stage_cache_identity_digest": (
                current.stage_cache_identity_digest
            ),
        },
    }
    return json.dumps(
        detail,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def _node_rehash_wire(value: NodeRehashEvidence) -> dict[str, object]:
    return {
        "node_id": value.node_id,
        "runtime_address": value.runtime_address,
        "pipeline_rank": value.pipeline_rank,
        "manifest_digest": value.manifest_digest,
        "tensor_keys_digest": value.tensor_keys_digest,
        "cache_identity_digest": value.cache_identity_digest,
        "total_size_bytes": value.total_size_bytes,
        "file_count": value.file_count,
    }


def _real_backend_child(connection, contract: AcceptanceContract) -> None:
    """Run GPU work in a supervised process without exposing library logs."""

    null_descriptor = -1
    try:
        null_descriptor = os.open(os.devnull, os.O_WRONLY)
        os.dup2(null_descriptor, 1)
        os.dup2(null_descriptor, 2)
        try:
            result = RealVllmStageRayBackend().run(contract)
            payload = {
                "status": "PASSED",
                "ordered_addresses": list(result.ordered_addresses),
                "generated_token_count": result.generated_token_count,
                "node_rehashes": [
                    _node_rehash_wire(item)
                    for item in result.node_rehashes
                ],
            }
        except AcceptanceFailure as exc:
            payload = {
                "status": "FAILED",
                "code": exc.code,
                "message": exc.message,
            }
        except BaseException:
            payload = {"status": "ERROR"}
        connection.send(payload)
    except BaseException:
        pass
    finally:
        connection.close()
        if null_descriptor >= 0:
            os.close(null_descriptor)


def _runtime_result_from_payload(
    payload: object, contract: AcceptanceContract
) -> RuntimeResult:
    expected_fields = {
        "status",
        "ordered_addresses",
        "generated_token_count",
        "node_rehashes",
    }
    if (
        type(payload) is not dict
        or set(payload) != expected_fields
        or payload.get("status") != "PASSED"
        or type(payload.get("ordered_addresses")) is not list
        or any(
            type(item) is not str for item in payload["ordered_addresses"]
        )
        or type(payload.get("generated_token_count")) is not int
        or type(payload.get("node_rehashes")) is not list
        or len(payload["node_rehashes"]) != contract.world_size
    ):
        raise AcceptanceFailure(
            "VLLM_STAGE_RAY_PP_ACCEPTANCE_FAILED",
            "실제 GPU 수용 검사 프로세스가 유효한 결과를 반환하지 않았습니다.",
        )
    rehashes: list[NodeRehashEvidence] = []
    fields = {
        "node_id",
        "runtime_address",
        "pipeline_rank",
        "manifest_digest",
        "tensor_keys_digest",
        "cache_identity_digest",
        "total_size_bytes",
        "file_count",
    }
    for value in payload["node_rehashes"]:
        if (
            type(value) is not dict
            or set(value) != fields
            or type(value.get("node_id")) is not str
            or type(value.get("runtime_address")) is not str
            or type(value.get("pipeline_rank")) is not int
            or type(value.get("manifest_digest")) is not str
            or type(value.get("tensor_keys_digest")) is not str
            or type(value.get("cache_identity_digest")) is not str
            or type(value.get("total_size_bytes")) is not int
            or type(value.get("file_count")) is not int
        ):
            raise AcceptanceFailure(
                "VLLM_STAGE_RAY_PP_ACCEPTANCE_FAILED",
                "node 재해시 결과가 폐쇄형 계약과 다릅니다.",
            )
        rehashes.append(NodeRehashEvidence(**value))
    return RuntimeResult(
        tuple(payload["ordered_addresses"]),
        payload["generated_token_count"],
        tuple(rehashes),
    )


def _run_real_backend_bounded(
    contract: AcceptanceContract,
    *,
    timeout: float = BACKEND_TIMEOUT_SECONDS,
) -> RuntimeResult:
    if type(timeout) not in (int, float) or timeout <= 0:
        raise ValueError("timeout must be positive")
    context = multiprocessing.get_context("fork")
    receive, send = context.Pipe(duplex=False)
    process = context.Process(
        target=_real_backend_child,
        args=(send, contract),
        name="dure-vllm-stage-ray-pp-acceptance",
    )
    process.start()
    send.close()
    try:
        if not receive.poll(timeout):
            process.terminate()
            process.join(10)
            if process.is_alive():
                process.kill()
                process.join(10)
            raise AcceptanceFailure(
                "ACCEPTANCE_TIMEOUT",
                "고정 시간 안에 실제 STAGE GPU 검사가 끝나지 않았습니다.",
            )
        try:
            payload = receive.recv()
        except EOFError as exc:
            raise AcceptanceFailure(
                "VLLM_STAGE_RAY_PP_ACCEPTANCE_FAILED",
                "실제 GPU 수용 검사 프로세스가 결과 없이 종료되었습니다.",
            ) from exc
    finally:
        receive.close()
        if process.is_alive():
            process.terminate()
        process.join(10)
        if process.is_alive():
            process.kill()
            process.join(10)

    if (
        type(payload) is dict
        and set(payload) == {"status", "code", "message"}
        and payload.get("status") == "FAILED"
        and type(payload.get("code")) is str
        and type(payload.get("message")) is str
    ):
        raise AcceptanceFailure(payload["code"], payload["message"])
    return _runtime_result_from_payload(payload, contract)


def run_acceptance(
    *,
    argv: Sequence[str],
    environ: Mapping[str, str],
    contract_loader=_load_contract,
    backend_factory=RealVllmStageRayBackend,
) -> int:
    if len(argv) != 1:
        _emit(
            sys.stdout,
            "NOT_RUN",
            code="INPUT_NOT_ALLOWED",
            reason="수용 검사는 명령행 인자를 받지 않습니다.",
        )
        return 77
    unexpected_env = sorted(
        name
        for name in environ
        if any(
            name.startswith(prefix)
            for prefix in ACCEPTANCE_ENV_PREFIXES
        )
        and name not in ALLOWED_ACCEPTANCE_ENV
    )
    if unexpected_env:
        _emit(
            sys.stdout,
            "NOT_RUN",
            code="INPUT_NOT_ALLOWED",
            reason="허용하지 않은 수용 검사 환경 입력이 있습니다.",
        )
        return 77
    if environ.get(OPT_IN_NAME) != "1":
        _emit(
            sys.stdout,
            "NOT_RUN",
            code="OPT_IN_REQUIRED",
            reason=(
                f"{OPT_IN_NAME}=1이 아니므로 실제 GPU 검사를 시작하지 "
                "않았습니다."
            ),
        )
        return 77

    try:
        contract = contract_loader()
        if _unexpected_runtime_environment(environ, contract):
            raise PrerequisiteMissing(
                "PROCESS_ENVIRONMENT_UNTRUSTED",
                "고정 수용 검사와 충돌하는 프로세스 환경 입력이 있습니다.",
            )
        backend = backend_factory()
        backend.preflight(contract)
    except PrerequisiteMissing as exc:
        _emit(sys.stdout, "NOT_RUN", code=exc.code, reason=exc.message)
        return 77
    except Exception:
        _emit(
            sys.stdout,
            "NOT_RUN",
            code="PREFLIGHT_FAILED",
            reason="고정 STAGE 검사 전제 조건을 확인하지 못했습니다.",
        )
        return 77

    # 이 지점부터 Ray 연결, 모든 node 재해시, vLLM load와 GPU 실행이
    # 시작된다. 이후 오류는 전제 부족이 아니라 실제 실행 실패다.
    try:
        result = (
            _run_real_backend_bounded(contract)
            if backend_factory is RealVllmStageRayBackend
            else backend.run(contract)
        )
        expected_addresses = tuple(
            item.runtime_address for item in contract.ordered_bindings
        )
        if result.ordered_addresses != expected_addresses:
            raise AcceptanceFailure(
                "RANK_BINDING_MISMATCH",
                "backend rank 결과가 고정 STAGE 계약과 다릅니다.",
            )
        if type(result.generated_token_count) is not int or not (
            1 <= result.generated_token_count <= 4
        ):
            raise AcceptanceFailure(
                "INFERENCE_FAILED", "고정 최소 추론 결과가 유효하지 않습니다."
            )
        if result.node_rehashes != _expected_node_rehashes(contract):
            raise AcceptanceFailure(
                "STAGE_REHASH_MISMATCH",
                "node별 재해시 증적이 고정 STAGE 계약과 다릅니다.",
            )
    except AcceptanceFailure as exc:
        _emit(sys.stderr, "FAILED", code=exc.code, message=exc.message)
        return 1
    except Exception:
        _emit(
            sys.stderr,
            "FAILED",
            code="VLLM_STAGE_RAY_PP_ACCEPTANCE_FAILED",
            message="실제 STAGE GPU 검사 중 분류되지 않은 오류가 발생했습니다.",
        )
        return 1

    _emit(
        sys.stdout,
        "PASSED",
        validation_run_id=contract.validation_run_id,
        deployment_id=contract.deployment_id,
        generation=contract.generation,
        runtime_image_declared=contract.runtime_image,
        runtime_image_attested=False,
        model_cache_kind=STAGE_CACHE_KIND,
        artifact_set_digest=(
            contract.stage_artifact.artifact_set_digest
        ),
        contract_identity_digest=(
            contract.stage_artifact.contract_identity_digest
        ),
        model_content_rehashed=True,
        rank_evidence_kind="VLLM_0_9_0_STAGE_SHARDED_STATE_ACTOR_ORDER",
        node_rehashes=[
            _node_rehash_wire(item) for item in result.node_rehashes
        ],
        checks=[
            {
                "name": "pipeline-rank-contract",
                "ok": True,
                "detail": _pipeline_rank_contract_detail(contract),
                "blocking": True,
            }
        ],
        generated_token_count=result.generated_token_count,
    )
    return 0


def main() -> int:
    return run_acceptance(argv=sys.argv, environ=os.environ)


if __name__ == "__main__":
    raise SystemExit(main())
