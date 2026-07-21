#!/usr/bin/env python3
"""vLLM 0.9.0 Ray pipeline parallel의 실제 GPU 수용 검사.

이 스크립트는 신뢰된 2대 또는 3대 GPU 노드에서 운영자가 수동으로 실행하는
검사 도구다. 기본 실행은 아무 호스트 변경도 하지 않고 ``NOT_RUN``(77)을
반환한다. 명시적으로 opt-in한 경우에도 고정 설정 파일과 고정 모델 mount만
사용하며 command, 환경 변수 묶음, Docker 인자, mount 또는 host path를 입력으로
받지 않는다.
"""

from __future__ import annotations

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


PINNED_VLLM_VERSION = "0.9.0"
BACKEND = "VLLM_RAY_PP_V1"
CONFIG_PATH = Path("/etc/dure/acceptance-vllm-ray-pp-v1.json")
MODEL_PATH = Path("/models/model")
MODEL_MARKER = ".dure-model.json"
CONFIG_MAX_BYTES = 64 * 1024
BACKEND_TIMEOUT_SECONDS = 1800
DURE_NODE_RESOURCE_PREFIX = "dure_node_"
OPT_IN_NAME = "DURE_RUN_VLLM_RAY_PP_ACCEPTANCE"
ACCEPTANCE_ENV_PREFIXES = (
    "DURE_RUN_VLLM_RAY_PP_ACCEPTANCE",
    "DURE_VLLM_RAY_PP_ACCEPTANCE_",
)
ALLOWED_ACCEPTANCE_ENV = frozenset({OPT_IN_NAME})
FORBIDDEN_PROCESS_ENV = frozenset({"PYTHONPATH", "PYTHONHOME", "LD_PRELOAD"})
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
        "model_manifest_digest",
        "ordered_bindings",
    }
)
NODE_FIELDS = frozenset(
    {
        "node_id",
        "runtime_address",
        "pipeline_rank",
        "runtime_rank",
    }
)


class AcceptanceFailure(RuntimeError):
    """중앙 증적에 넣어도 되는 폐쇄형 실패."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class PrerequisiteMissing(AcceptanceFailure):
    """실제 분산 load 시작 전에 발견한 전제 조건 부족."""


@dataclass(frozen=True)
class ExpectedBinding:
    node_id: str
    runtime_address: str
    pipeline_rank: int
    runtime_rank: int


@dataclass(frozen=True)
class AcceptanceContract:
    validation_run_id: str
    deployment_id: str
    generation: int
    runtime_image: str
    model_manifest_digest: str
    ordered_bindings: tuple[ExpectedBinding, ...]

    @property
    def world_size(self) -> int:
        return len(self.ordered_bindings)

    @classmethod
    def parse(cls, raw: object) -> "AcceptanceContract":
        document = _exact_object(raw, CONFIG_FIELDS, "설정")
        if type(document["schema_version"]) is not int or document["schema_version"] != 1:
            raise PrerequisiteMissing(
                "CONTRACT_INVALID", "설정 schema_version은 정확히 1이어야 합니다."
            )
        if document["backend"] != BACKEND:
            raise PrerequisiteMissing(
                "CONTRACT_INVALID", f"backend는 정확히 {BACKEND}이어야 합니다."
            )
        if document["vllm_version"] != PINNED_VLLM_VERSION:
            raise PrerequisiteMissing(
                "CONTRACT_INVALID",
                f"vLLM 계약 버전은 정확히 {PINNED_VLLM_VERSION}이어야 합니다.",
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
                "CONTRACT_INVALID", "runtime_image는 OCI SHA-256 digest로 고정해야 합니다."
            )
        model_manifest_digest = document["model_manifest_digest"]
        if type(model_manifest_digest) is not str or not DIGEST_RE.fullmatch(
            model_manifest_digest
        ):
            raise PrerequisiteMissing(
                "CONTRACT_INVALID", "model_manifest_digest 형식이 유효하지 않습니다."
            )
        raw_bindings = document["ordered_bindings"]
        if type(raw_bindings) is not list or len(raw_bindings) not in (2, 3):
            raise PrerequisiteMissing(
                "TOPOLOGY_UNSUPPORTED",
                "수용 검사는 정확히 2대 또는 3대의 GPU 노드만 지원합니다.",
            )
        bindings: list[ExpectedBinding] = []
        for index, value in enumerate(raw_bindings):
            binding = _exact_object(
                value, NODE_FIELDS, f"ordered_bindings[{index}]"
            )
            node_id = _canonical_uuid4(binding["node_id"], "node_id")
            runtime_address = _private_ipv4(binding["runtime_address"])
            pipeline_rank = binding["pipeline_rank"]
            runtime_rank = binding["runtime_rank"]
            if type(pipeline_rank) is not int or type(runtime_rank) is not int:
                raise PrerequisiteMissing(
                    "CONTRACT_INVALID", "rank는 정수여야 합니다."
                )
            if pipeline_rank != index or runtime_rank != index:
                raise PrerequisiteMissing(
                    "RANK_CONTRACT_INVALID",
                    "TP=1 계약에서는 pipeline rank와 runtime rank가 순서대로 같아야 합니다.",
                )
            bindings.append(
                ExpectedBinding(
                    node_id, runtime_address, pipeline_rank, runtime_rank
                )
            )

        if len({item.node_id for item in bindings}) != len(bindings):
            raise PrerequisiteMissing(
                "DUPLICATE_NODE", "ordered_bindings에 중복 node UUID가 있습니다."
            )
        if len({item.runtime_address for item in bindings}) != len(bindings):
            raise PrerequisiteMissing(
                "DUPLICATE_NODE", "ordered_bindings에 중복 runtime address가 있습니다."
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
        return cls(
            validation_run_id=validation_run_id,
            deployment_id=deployment_id,
            generation=generation,
            runtime_image=runtime_image,
            model_manifest_digest=model_manifest_digest,
            ordered_bindings=tuple(bindings),
        )


@dataclass(frozen=True)
class RuntimeNode:
    ray_node_id: str
    node_address: str
    gpu_count: float
    dure_node_id: str


@dataclass(frozen=True)
class RuntimeResult:
    ordered_addresses: tuple[str, ...]
    generated_token_count: int


class AcceptanceBackend(Protocol):
    def preflight(self, contract: AcceptanceContract) -> None: ...

    def run(self, contract: AcceptanceContract) -> RuntimeResult: ...


class RealVllmRayBackend:
    """vLLM 0.9.0의 공식 Ray executor를 실제로 load하는 backend."""

    def __init__(self) -> None:
        self._runtime_ray = None
        self._runtime_executor = None

    def preflight(self, contract: AcceptanceContract) -> None:
        for distribution, expected in (
            ("vllm", PINNED_VLLM_VERSION),
            ("ray", None),
        ):
            try:
                observed = importlib.metadata.version(distribution)
            except importlib.metadata.PackageNotFoundError as exc:
                raise PrerequisiteMissing(
                    "RUNTIME_MISSING", "고정 GPU 수용 검사 runtime이 설치되지 않았습니다."
                ) from exc
            if expected is not None and observed != expected:
                raise PrerequisiteMissing(
                    "VLLM_VERSION_MISMATCH",
                    f"vLLM은 정확히 {PINNED_VLLM_VERSION}이어야 합니다.",
                )
        marker_path = MODEL_PATH / MODEL_MARKER
        if not MODEL_PATH.is_dir() or not (MODEL_PATH / "config.json").is_file():
            raise PrerequisiteMissing(
                "MODEL_MISSING", "고정 모델 mount에 검증된 모델이 없습니다."
            )
        try:
            marker = _read_bounded_root_json(marker_path, label="모델 marker")
        except PrerequisiteMissing as exc:
            raise PrerequisiteMissing(
                "MODEL_MARKER_INVALID", "고정 모델 marker를 검증할 수 없습니다."
            ) from exc
        if type(marker) is not dict:
            raise PrerequisiteMissing(
                "MODEL_MARKER_INVALID", "고정 모델 marker가 JSON 객체가 아닙니다."
            )
        if (
            marker.get("manifest_digest") != contract.model_manifest_digest
            or marker.get("cache_kind") != "FULL_SNAPSHOT"
            or str(marker.get("quantization", "")).lower() != "awq"
        ):
            raise PrerequisiteMissing(
                "MODEL_IDENTITY_MISMATCH",
                "모델 marker가 고정 FULL_SNAPSHOT AWQ 계약과 일치하지 않습니다.",
            )

    def run(self, contract: AcceptanceContract) -> RuntimeResult:
        try:
            return self._run(contract)
        finally:
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
        # 이 네 값은 사용자 입력이 아니라 VLLM_RAY_PP_V1의 고정 계약이다.
        os.environ["VLLM_USE_V1"] = "0"
        os.environ["VLLM_USE_RAY_SPMD_WORKER"] = "0"
        os.environ["VLLM_RAY_PER_WORKER_GPUS"] = "1.0"
        os.environ["VLLM_RAY_BUNDLE_INDICES"] = ""
        os.environ["VLLM_USE_RAY_COMPILED_DAG"] = "0"
        os.environ["VLLM_HOST_IP"] = contract.ordered_bindings[0].runtime_address
        os.environ["VLLM_ATTENTION_BACKEND"] = "FLASH_ATTN"
        os.environ["RAY_ADDRESS"] = (
            f"{contract.ordered_bindings[0].runtime_address}:6379"
        )

        try:
            import ray

            self._runtime_ray = ray
            ray.init(
                address=f"{contract.ordered_bindings[0].runtime_address}:6379",
                logging_level="ERROR",
            )
        except Exception as exc:
            raise AcceptanceFailure(
                "RAY_CLUSTER_UNAVAILABLE", "기존 Ray cluster에 연결하지 못했습니다."
            ) from exc

        runtime_nodes = _runtime_nodes_from_ray(ray.nodes())
        _validate_cluster_nodes(contract, runtime_nodes)

        try:
            from vllm import LLM, SamplingParams

            llm = LLM(
                model=str(MODEL_PATH),
                tokenizer=str(MODEL_PATH),
                load_format="auto",
                quantization="awq",
                tensor_parallel_size=1,
                pipeline_parallel_size=contract.world_size,
                distributed_executor_backend="ray",
                trust_remote_code=False,
                enable_lora=False,
                enforce_eager=True,
                gpu_memory_utilization=0.90,
                max_model_len=128,
            )
        except Exception as exc:
            raise AcceptanceFailure(
                "VLLM_DISTRIBUTED_LOAD_FAILED",
                "vLLM Ray pipeline model load가 실패했습니다.",
            ) from exc

        try:
            executor = llm.llm_engine.model_executor
            self._runtime_executor = executor
            if (
                executor.__class__.__name__ != "RayDistributedExecutor"
                or executor.__class__.__module__
                != "vllm.executor.ray_distributed_executor"
            ):
                raise AcceptanceFailure(
                    "EXECUTOR_MISMATCH", "실제 executor가 고정 Ray 계약과 다릅니다."
                )
            parallel = executor.parallel_config
            if (
                parallel.tensor_parallel_size != 1
                or parallel.pipeline_parallel_size != contract.world_size
                or parallel.world_size != contract.world_size
            ):
                raise AcceptanceFailure(
                    "TOPOLOGY_MISMATCH", "실제 vLLM topology가 고정 계약과 다릅니다."
                )
            workers = [executor.driver_dummy_worker, *executor.workers]
            if any(worker is None for worker in workers):
                raise AcceptanceFailure(
                    "DRIVER_MISSING", "Ray driver GPU worker를 확인할 수 없습니다."
                )
            actor_pairs = ray.get(
                [worker.get_node_and_gpu_ids.remote() for worker in workers],
                timeout=30,
            )
            ordered_addresses = _validate_actor_ranks(
                contract, runtime_nodes, actor_pairs
            )
        except AcceptanceFailure:
            raise
        except Exception as exc:
            raise AcceptanceFailure(
                "RANK_ATTESTATION_FAILED", "실제 worker rank를 검증하지 못했습니다."
            ) from exc

        try:
            outputs = llm.generate(
                ["대한민국의 수도는"],
                SamplingParams(temperature=0.0, min_tokens=1, max_tokens=4),
                use_tqdm=False,
            )
            if not outputs or not outputs[0].outputs:
                raise RuntimeError("empty generation")
            token_count = len(outputs[0].outputs[0].token_ids)
            if token_count < 1 or token_count > 4:
                raise RuntimeError("invalid token count")
        except Exception as exc:
            raise AcceptanceFailure(
                "INFERENCE_FAILED", "고정 최소 추론 검사가 실패했습니다."
            ) from exc
        return RuntimeResult(ordered_addresses, token_count)


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
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(path, flags)
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
    if type(value) is not dict:
        raise PrerequisiteMissing(
            "CONTRACT_INVALID", f"{label}은 JSON 객체여야 합니다."
        )
    actual = set(value)
    if actual != expected_fields:
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
            "PUBLIC_RAY_ADDRESS", "Ray node address는 canonical 사설 IPv4여야 합니다."
        )
    return value


def _digest_pinned_image(value: object) -> bool:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 512
        or value.count("@") != 1
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
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
                "RAY_TOPOLOGY_INVALID", "활성 Ray node resource 형식이 유효하지 않습니다."
            )
        gpu_count = resources.get("GPU")
        if type(gpu_count) not in (int, float):
            raise AcceptanceFailure(
                "GPU_PLACEMENT_INVALID", "활성 Ray node의 GPU resource가 유효하지 않습니다."
            )
        node_id = value.get("NodeID")
        address = value.get("NodeManagerAddress")
        if type(node_id) is not str or not node_id or type(address) is not str:
            raise AcceptanceFailure(
                "RAY_TOPOLOGY_INVALID", "Ray GPU node identity가 유효하지 않습니다."
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
        nodes.append(RuntimeNode(node_id, address, float(gpu_count), dure_node_id))
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
            "GPU_PLACEMENT_INVALID", "각 Ray node는 정확히 GPU 하나만 제공해야 합니다."
        )
    expected = {item.runtime_address for item in contract.ordered_bindings}
    observed = {item.node_address for item in runtime_nodes}
    if observed != expected:
        raise AcceptanceFailure(
            "NODE_SET_MISMATCH", "Ray GPU node address 집합이 고정 topology와 다릅니다."
        )
    expected_nodes = {
        item.runtime_address: item.node_id for item in contract.ordered_bindings
    }
    observed_nodes = {
        item.node_address: item.dure_node_id for item in runtime_nodes
    }
    if observed_nodes != expected_nodes:
        raise AcceptanceFailure(
            "DURE_NODE_BINDING_INVALID",
            "Ray node 주소와 Dure node UUID 결합이 고정 계약과 다릅니다.",
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
                "RANK_ATTESTATION_INVALID", "worker rank 응답 형식이 유효하지 않습니다."
            )
        ray_node_id, gpu_ids = pair
        if type(ray_node_id) is not str or ray_node_id not in address_by_ray_id:
            raise AcceptanceFailure(
                "UNKNOWN_WORKER_NODE", "worker가 고정 node 집합 밖에서 실행됐습니다."
            )
        if ray_node_id in seen_ray_ids:
            raise AcceptanceFailure(
                "DUPLICATE_RANK", "한 node에 둘 이상의 pipeline rank가 배치됐습니다."
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
                "GPU_PLACEMENT_INVALID", "각 worker는 정확히 GPU 하나를 점유해야 합니다."
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


def _load_contract(path: Path = CONFIG_PATH) -> AcceptanceContract:
    try:
        raw = _read_bounded_root_json(path, label="수용 검사 설정")
    except PrerequisiteMissing as exc:
        raise PrerequisiteMissing(
            "CONFIG_UNAVAILABLE", "고정 수용 검사 설정을 읽을 수 없습니다."
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
        "RAY_ADDRESS": f"{contract.ordered_bindings[0].runtime_address}:6379",
    }
    unexpected = [name for name in FORBIDDEN_PROCESS_ENV if environ.get(name)]
    for name, value in environ.items():
        if name.startswith("VLLM_") or name == "RAY_ADDRESS":
            if name not in expected or value != expected[name]:
                unexpected.append(name)
    return sorted(set(unexpected))


def _pipeline_rank_contract_detail(contract: AcceptanceContract) -> str:
    current = contract.ordered_bindings[0]
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
    }
    return json.dumps(
        detail,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _real_backend_child(connection, contract: AcceptanceContract) -> None:
    """Run GPU work in a supervised process without emitting library logs."""

    null_descriptor = -1
    try:
        null_descriptor = os.open(os.devnull, os.O_WRONLY)
        os.dup2(null_descriptor, 1)
        os.dup2(null_descriptor, 2)
        try:
            result = RealVllmRayBackend().run(contract)
            payload = (
                "ok",
                list(result.ordered_addresses),
                result.generated_token_count,
            )
        except AcceptanceFailure as exc:
            payload = ("failed", exc.code, exc.message)
        except BaseException:
            payload = ("error",)
        connection.send(payload)
    except BaseException:
        pass
    finally:
        connection.close()
        if null_descriptor >= 0:
            os.close(null_descriptor)


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
        name="dure-vllm-ray-pp-acceptance",
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
                "고정 시간 안에 실제 GPU 수용 검사가 끝나지 않았습니다.",
            )
        try:
            payload = receive.recv()
        except EOFError as exc:
            raise AcceptanceFailure(
                "VLLM_RAY_PP_ACCEPTANCE_FAILED",
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
        type(payload) in (tuple, list)
        and len(payload) == 3
        and payload[0] == "ok"
        and type(payload[1]) is list
        and all(type(item) is str for item in payload[1])
        and type(payload[2]) is int
    ):
        return RuntimeResult(tuple(payload[1]), payload[2])
    if (
        type(payload) in (tuple, list)
        and len(payload) == 3
        and payload[0] == "failed"
        and type(payload[1]) is str
        and type(payload[2]) is str
    ):
        raise AcceptanceFailure(payload[1], payload[2])
    raise AcceptanceFailure(
        "VLLM_RAY_PP_ACCEPTANCE_FAILED",
        "실제 GPU 수용 검사 프로세스가 유효한 결과를 반환하지 않았습니다.",
    )


def run_acceptance(
    *,
    argv: Sequence[str],
    environ: Mapping[str, str],
    contract_loader=_load_contract,
    backend_factory=RealVllmRayBackend,
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
        if any(name.startswith(prefix) for prefix in ACCEPTANCE_ENV_PREFIXES)
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
            reason=f"{OPT_IN_NAME}=1이 아니므로 실제 GPU 검사를 시작하지 않았습니다.",
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
            reason="고정 수용 검사 전제 조건을 확인하지 못했습니다.",
        )
        return 77

    # 이 지점부터 Ray 연결, 분산 model load와 GPU 실행이 시작된다. 이후의 모든
    # 오류는 전제 조건 부족이 아니라 실제 실행 실패로 기록한다.
    try:
        result = (
            _run_real_backend_bounded(contract)
            if backend_factory is RealVllmRayBackend
            else backend.run(contract)
        )
        expected_addresses = tuple(
            item.runtime_address for item in contract.ordered_bindings
        )
        if result.ordered_addresses != expected_addresses:
            raise AcceptanceFailure(
                "RANK_BINDING_MISMATCH", "backend rank 결과가 고정 계약과 다릅니다."
            )
        if type(result.generated_token_count) is not int or not (
            1 <= result.generated_token_count <= 4
        ):
            raise AcceptanceFailure(
                "INFERENCE_FAILED", "고정 최소 추론 결과가 유효하지 않습니다."
            )
    except AcceptanceFailure as exc:
        _emit(sys.stderr, "FAILED", code=exc.code, message=exc.message)
        return 1
    except Exception:
        _emit(
            sys.stderr,
            "FAILED",
            code="VLLM_RAY_PP_ACCEPTANCE_FAILED",
            message="실제 GPU 수용 검사 중 분류되지 않은 오류가 발생했습니다.",
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
        model_manifest_digest=contract.model_manifest_digest,
        model_manifest_marker_verified=True,
        model_content_rehashed=False,
        rank_evidence_kind="VLLM_0_9_0_SOURCE_PINNED_ACTOR_ORDER",
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
